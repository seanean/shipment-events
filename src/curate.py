import logging
import traceback

from db import (
    get_engine, 
    get_insert_statement, 
    insert_row_builder,
    insert_pipeline_batch_run,
    get_batch,
    get_latest_run_id, 
    get_latest_raw_offset_id, 
    BatchParameters
)
from sqlalchemy import text
from datetime import datetime, UTC
from typing import Literal # Any, cast, Tuple
# from uuid import uuid5, NAMESPACE_DNS
# from copy import deepcopy
from config_util import get_config, resolve_config, _REPO_ROOT_PATH
from app_logger import configure_logger # only required if running cleanse.py directly
from dataclasses import dataclass
from psycopg import sql


logger = logging.getLogger(__name__)


_CUR_BATCH_SIZE = 1 # 200

# TODO: not sure if todo, but a lot of this logic can overlap with cleansed.
# that said, I'm going to replace CUR with dbt so maybe I don't refactor yet.
def curate(data: Literal["shipment_status", "shipment_products"],
           envt: Literal["dev"]) -> None:
    params_cur = BatchParameters()

    # initializing variables
    params_cur.started_at = datetime.now(UTC)
    params_cur.job_name = f'curate_{data}_{envt}'

    # preparing config
    config_path = _REPO_ROOT_PATH.joinpath(f"config/{envt}.yaml")
    loaded_config = get_config(data, config_path)
    config = resolve_config(loaded_config)

    # what run is this?
    engine = get_engine() # one time
    params_cur.latest_run_id = get_latest_run_id(engine, envt)
    params_cur.run_id = params_cur.latest_run_id + 1
    params_cur.batch_id = 1
    logger.info(f"Latest run ID: {params_cur.latest_run_id}, current run ID: {params_cur.run_id}")

    # what cln data do we start from?
    params_cur.latest_raw_offset_id = get_latest_raw_offset_id(engine, params_cur)
    params_cur.from_id_exclusive = params_cur.latest_raw_offset_id

    # get insert statement
    insert_qry = get_insert_statement(config.cur_insert_sql_path)

    while True:
        # do cln insert
        try:
            with engine.begin() as conn:
                logger.info(f"Executing insert query")
                # row = conn.execute(text(insert_qry),{"cur_batch_size": _CUR_BATCH_SIZE,
                #                         "run_id": params_cur.run_id,
                #                         "batch_id": params_cur.batch_id,
                #                         "job_name": params_cur.job_name,
                #                         "started_at": params_cur.started_at,
                #                         "starting_from_id_exclusive": params_cur.latest_raw_offset_id,
                #                         "from_id_exclusive": params_cur.from_id_exclusive}).fetchone()
                
                # run batch.
                conn.exec_driver_sql(sql.SQL(insert_qry).format(
                                                            params_cur.from_id_exclusive,
                                                            _CUR_BATCH_SIZE,
                                                            params_cur.run_id,
                                                            params_cur.batch_id,
                                                            params_cur.job_name,
                                                            params_cur.started_at,
                                                            params_cur.latest_raw_offset_id,
                                                            params_cur.from_id_exclusive,
                                                            params_cur.job_name,
                                                            params_cur.job_name
                                                        ))
                logger.info(f"Curated run {params_cur.run_id} batch {params_cur.batch_id} complete for {data} query executed successfully.")

                # detect if there are more cln records to process in a next batch.
                result = conn.execute(text("""SELECT
                                                            (
                                                                SELECT COUNT(1)
                                                                FROM cln.shipment_products
                                                                WHERE raw_offset_id > (
                                                                    SELECT MAX(to_id_inclusive)
                                                                    FROM meta.pipeline_run 
                                                                    WHERE job_name = :job_name
                                                                        AND status = 'success'
                                                                    )
                                                            )
                                                        + 
                                                            (
                                                                SELECT COUNT(1)
                                                                FROM cln.shipment_status
                                                                WHERE raw_offset_id > (
                                                                    SELECT MAX(to_id_inclusive)
                                                                    FROM meta.pipeline_run 
                                                                    WHERE job_name = :job_name
                                                                        AND status = 'success'
                                                                    )
                                                            );

                                                        """), {"job_name": params_cur.job_name}).fetchone()
                assert result is not None
                still_to_process = result if result is not None else 0
                if still_to_process[0] > 0:
                    logger.info(f"Curated batches continuing. {still_to_process} events remaining.")
                else:
                    logger.info(f"Curated batches complete. {still_to_process} events remaining.")
                    break
        # if cur insert failed, insert a pipeline run failure
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            try:
                params_cur.status = 'failed'
                params_cur.error_message = str(e)
                params_cur.traceback_message = traceback.format_exc()
                insert_pipeline_batch_run(engine, params_cur)
            # if that failed too, that's not good
            except Exception as meta_e:
                logger.error(f'Error inserting pipeline failed: {meta_e}')
                raise meta_e from e
            raise e
        # prepare for next loop iteration
        params_cur.batch_id +=1
        params_cur.from_id_exclusive = get_latest_raw_offset_id(engine, params_cur)
        params_cur.rows_read = 0
        params_cur.rows_written = 0
        params_cur.error_message = None
        params_cur.traceback_message = None
        params_cur.started_at = datetime.now(UTC)


def main() -> None:
    # curate(data="shipment_status", envt="dev")
    curate(data="shipment_products", envt="dev")

if __name__ == '__main__':
    logger = configure_logger()
    main()