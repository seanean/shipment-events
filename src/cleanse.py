import logging
import traceback
import pandas as pd
from dataclasses import dataclass

from db import get_engine, get_insert_statement, insert_row_builder
from sqlalchemy import text
from datetime import datetime, UTC
from typing import Any, Literal, cast, Tuple
from uuid import uuid5, NAMESPACE_DNS, UUID
from copy import deepcopy
from config_util import get_config, resolve_config, _REPO_ROOT_PATH
from app_logger import configure_logger # only required if running cleanse.py directly

logger = logging.getLogger(__name__)

_BATCH_SIZE = 1 # 200
_ROOT_NAMESPACE = uuid5(NAMESPACE_DNS, 'shipment-events')

@dataclass(frozen=False)
class CleansedParameters:
    latest_run_id: int | None = None
    latest_raw_offset_id: int | None = None
    run_id: int | None = None
    batch_id: int = 1
    job_name: str | None = None
    status: str | None = None
    started_at: datetime | None = None
    starting_from_id_exclusive: int = 0
    from_id_exclusive: int = 0
    to_id_inclusive: int = 0
    rows_read: int = 0
    rows_written: int = 0
    error_message: str | None = None
    traceback_message: str | None = None


def cleanse(data: Literal["shipment_status", "shipment_products"],
                    envt: Literal["dev"]) -> None:
    logger.info(f'Running cleansed for {data} in {envt} environment')

    params = CleansedParameters()
    
    # initializing variables
    params.started_at = datetime.now(UTC)
    params.job_name = f'cleanse_{data}_{envt}'


    # preparing config
    config_path = _REPO_ROOT_PATH.joinpath(f"config/{envt}.yaml")
    loaded_config = get_config(data, config_path)
    config = resolve_config(loaded_config)

    # what run is this?
    try:
        params.latest_run_id = get_latest_run_id(envt)
        params.run_id = params.latest_run_id + 1
        logger.info(f"Latest run ID: {params.latest_run_id}, current run ID: {params.run_id}")
    except Exception as e:
        logger.error(f"Error getting latest run id: {e}")
        raise e

    # what raw data do we start from?
    try:
        params.latest_raw_offset_id = get_latest_raw_offset_id(params.job_name)
        logger.info(f"Latest raw offset id: {params.latest_raw_offset_id}")
    except Exception as e:
        logger.error(f"Error getting latest raw offset id: {e}. {traceback.format_exc()}")
        try:
            params.status = 'failed'
            params.error_message = str(e)
            params.traceback_message = traceback.format_exc()
            insert_pipeline_batch_run(params)
        except Exception as meta_e:
            logger.error(f'Error inserting pipeline failed: {meta_e}')
            raise meta_e from e
        raise e

    # start getting batches. go until a batch is empty
    params.from_id_exclusive = params.latest_raw_offset_id
    while True:
        batch_df, params.rows_read = get_raw_batch(data, envt, params.from_id_exclusive, _BATCH_SIZE)
        if batch_df.empty:
            logger.info(f'Batch is empty, exiting')
            params.status = 'success'
            params.to_id_inclusive = params.from_id_exclusive # latest success will still have same to_id
            insert_pipeline_batch_run(params)
            break

        # get latest per event ID. alternative approach would be based on event timestamp
        batch_df_merged, params.to_id_inclusive = merge_df(batch_df, partition_by="event_id", order_by="offset_id")

        # converting to dict because df.apply is not vectorized and list comprehensions should be faster
        batch_dict_list = batch_df_merged.to_dict(orient="records")

        # cleanse logic
        for row in batch_dict_list:
            row['payload_cln'] = add_uuids(row['payload'], data)

        # prepare cln insert
        insert_qry = get_insert_statement(config.cln_insert_sql_path)
        insert_rows = [
            insert_row_builder(
                target_table=config.cln_target_table,
                content=cast(dict[str, Any], row),
                meta_source_filepath=row["meta_source_file_path"],
            )
            for row in batch_dict_list
        ]
    
        engine = get_engine()

        # do cln insert
        try:
            with engine.begin() as conn:
                logger.info(f"Executing insert query")
                result = conn.execute(text(insert_qry),insert_rows,)
                params.rows_written = result.rowcount
                logger.info(f"Insert query executed successfully, {params.rows_written} rows written")
        
        # if cln insert failed, insert a pipeline run failure
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            try:
                params.status = 'failed'
                params.error_message = str(e)
                params.traceback_message = traceback.format_exc()
                # TODO: to check, if inserting a batch fails after x rows, will any of the rows be inserted or not?
                # this would drive whether params.rows_written = 0 would be accurate or if I need to pull a row count from the failed insert.
                insert_pipeline_batch_run(params)
            # if that failed too, that's not good
            except Exception as meta_e:
                logger.error(f'Error inserting pipeline failed: {meta_e}')
                raise meta_e from e
            raise e

        # after cln insert success, insert a pipeline run success
        try:
            params.status = 'success'
            insert_pipeline_batch_run(params)
        except Exception as meta_e:
            logger.error(f'Error inserting pipeline success: {meta_e}')
            raise meta_e

        params.batch_id +=1
        params.from_id_exclusive = params.to_id_inclusive


def get_latest_raw_offset_id(job_name: str) -> int:
    logger.info(f"Getting latest raw offset id for {job_name}")
    offset_qry = f"SELECT MAX(to_id_inclusive) FROM meta.pipeline_run WHERE job_name = :job_name AND status = 'success'"
    engine = get_engine()
    try:
        with engine.begin() as conn:
            logger.info(f"Retrieving latest raw offset id")
            result = conn.execute(text(offset_qry), {"job_name": job_name}).fetchone()[0]
            latest_offset_id = result if result is not None else 0
            logger.info(f"Latest raw offset id: {latest_offset_id}")
            return latest_offset_id
    except Exception as e:
        logger.error(f"Error getting latest raw offset id: {e}")
        raise e

def get_latest_run_id(envt: str) -> int:
    logger.info(f"Getting latest run_id for in {envt} environment")
    run_qry = f"SELECT MAX(run_id) FROM meta.pipeline_run"
    engine = get_engine()
    try:
        with engine.begin() as conn:
            logger.info(f"Retrieving latest run_id")
            result = conn.execute(text(run_qry)).fetchone()[0]
            latest_run_id = result if result is not None else 0
            logger.info(f"Latest run id: {latest_run_id}")
            return latest_run_id
    except Exception as e:
        logger.error(f"Error getting latest run id: {e}")
        raise e

def get_raw_batch(data: str, envt: str, offset_id: int, batch_size: int) -> Tuple[pd.DataFrame, int]:
    logger.info(f"Getting raw batch for {data} in {envt} environment from {offset_id} to {offset_id + batch_size}")
    batch_qry = f'SELECT * FROM raw.{data} WHERE offset_id > :offset_id LIMIT :batch_size'
    engine = get_engine()
    try:
        with engine.begin() as conn:
            logger.info(f"Retrieving latest raw offset id")
            result = conn.execute(text(batch_qry), {"offset_id": offset_id, "batch_size": batch_size})
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
            return df, df.shape[0]
    except Exception as e:
        logger.error(f"Error getting latest raw offset id: {e}")
        raise e

def merge_df(df: pd.DataFrame, partition_by, order_by) -> Tuple[pd.DataFrame, int]:
    return df.loc[df[order_by].isin(df.groupby(partition_by)[order_by].max())], df[order_by].max()


def add_uuids(payload: dict[str, Any], data: str) -> dict[str, Any]:
    payload_cln = deepcopy(payload)
    match data:
        case "shipment_status":
            return add_shipment_status_uuids(payload_cln)
        case "shipment_products":
            return add_shipment_products_uuids(payload_cln)
        case _:
            raise ValueError(f"Invalid data: {data}")

def build_uuid(*args: str) -> str:
    return str(uuid5(_ROOT_NAMESPACE, ''.join(args)))

def add_shipment_status_uuids(payload_cln: dict[str, Any]) -> dict[str, Any]:
    payload_cln["event_data"]["shipment_uuid"] = build_uuid(payload_cln["event_data"]["shipment_business_id"])
    payload_cln["event_data"]["shipment_status_uuid"] = build_uuid(payload_cln["event_data"]["shipment_business_id"], payload_cln["event_data"]["status"])

    return payload_cln

def add_shipment_products_uuids(payload_cln: dict[str, Any]) -> dict[str, Any]:
    payload_cln["event_data"]["shipment_uuid"] = build_uuid(payload_cln["event_data"]["shipment_business_id"])

    for item in payload_cln["event_data"]["products"]:
        item["shipment_product_uuid"] = build_uuid(payload_cln["event_data"]["shipment_business_id"], item["product_id"])

    return payload_cln

def insert_pipeline_batch_run(iparams: CleansedParameters):
    insert_qry = """INSERT INTO meta.pipeline_run (
                        run_id, batch_id, 
                        job_name, status,
                        started_at, finished_at,
                        starting_from_id_exclusive, from_id_exclusive,
                        to_id_inclusive, rows_read,
                        rows_written, error_message,
                        traceback_message, meta_insert_timestamp)
                    VALUES (
                        :run_id, :batch_id,
                        :job_name, :status,
                        :started_at, NOW(),
                        :starting_from_id_exclusive, :from_id_exclusive,
                        :to_id_inclusive, :rows_read,
                        :rows_written, :error_message,
                        :traceback_message, NOW())"""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(insert_qry), {"run_id": iparams.run_id, "batch_id": iparams.batch_id,
                                        "job_name": iparams.job_name, "status": iparams.status,
                                        "started_at": iparams.started_at,
                                        "starting_from_id_exclusive": iparams.starting_from_id_exclusive, "from_id_exclusive": iparams.from_id_exclusive,
                                        "to_id_inclusive": iparams.to_id_inclusive, "rows_read": iparams.rows_read,
                                        "rows_written": iparams.rows_written, "error_message": iparams.error_message,
                                        "traceback_message": iparams.traceback_message})

def main() -> None:
    # to be created later when I want to add __init__.py and trigger via cli with args
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-d", "--data", required=True, choices=["shipment_status", "shipment_products"], help="The data to ingest")
    # parser.add_argument("-e","--environment", default="dev", choices=["dev", "prod"], help="The environment to ingest from")
    # args = parser.parse_args()
    # ingest_raw(args.data, args.environment)
    print('hello cleansed world :)')
    cleanse('shipment_status', 'dev')
    cleanse('shipment_products', 'dev')

if __name__ == "__main__":
    logger = configure_logger()
    main()
