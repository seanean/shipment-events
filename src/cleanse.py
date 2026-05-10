import logging
import traceback
import pandas as pd

from db import (
    get_engine, 
    get_insert_statement, 
    insert_row_builder, 
    get_latest_run_id, 
    get_latest_raw_offset_id, 
    BatchParameters, 
    insert_pipeline_batch_run,
    get_batch
)

from sqlalchemy import text, Engine
from datetime import datetime, UTC
from typing import Any, Literal, cast, Tuple
from uuid import uuid5, NAMESPACE_DNS
from copy import deepcopy
from config_util import get_config, resolve_config, _REPO_ROOT_PATH
from app_logger import configure_logger # only required if running cleanse.py directly

logger = logging.getLogger(__name__)

_CLN_BATCH_SIZE = 1 # 200
_ROOT_NAMESPACE = uuid5(NAMESPACE_DNS, 'shipment-events')

def cleanse(data: Literal["shipment_status", "shipment_products"],
                    envt: Literal["dev"]) -> None:
    logger.debug(f'Running cleansed for {data} in {envt} environment')

    params = BatchParameters()
    
    # initializing variables
    params.started_at = datetime.now(UTC)
    params.job_name = f'cleanse_{data}_{envt}'


    # preparing config
    config_path = _REPO_ROOT_PATH.joinpath(f"config/{envt}.yaml")
    loaded_config = get_config(data, config_path)
    config = resolve_config(loaded_config)

    # what run is this?
    engine = get_engine() # one time
    params.latest_run_id = get_latest_run_id(engine, envt)
    params.run_id = params.latest_run_id + 1
    logger.info(f"Latest run ID: {params.latest_run_id}, current run ID: {params.run_id}")

    # what raw data do we start from?
    params.latest_raw_offset_id = get_latest_raw_offset_id(engine, params)
    params.from_id_exclusive = params.latest_raw_offset_id

    # get insert statement
    insert_qry = get_insert_statement(config.cln_insert_sql_path)

    # start getting batches. go until a batch is empty
    while True:
        batch_df, params.rows_read = get_batch(engine, config.raw_target_table, envt, params, _CLN_BATCH_SIZE)
        if batch_df.empty:
            logger.info(f'Batch is empty, exiting')
            params.status = 'success'
            params.to_id_inclusive = params.from_id_exclusive # latest success will still have same to_id
            insert_pipeline_batch_run(engine, params)
            break

        # get latest per event ID.
        batch_df_merged, params.to_id_inclusive = merge_df(batch_df, partition_by="event_id", order_by="offset_id")

        # converting to dict because df.apply is not vectorized and list comprehensions should be faster
        batch_dict_list = batch_df_merged.to_dict(orient="records")

        # cleanse logic
        for row in batch_dict_list:
            row['payload_cln'] = add_uuids(row['payload'], data)

        # prepare cln insert
        insert_rows = [
            insert_row_builder(event_type=data,
                target_table=config.cln_target_table,
                content=cast(dict[str, Any], row),
                meta_source_filepath=row["meta_source_file_path"],
            )
            for row in batch_dict_list
        ]
    
        # do cln insert
        try:
            with engine.begin() as conn:
                logger.debug(f"Executing insert query")
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
                insert_pipeline_batch_run(engine, params)
            # if that failed too, that's not good
            except Exception as meta_e:
                logger.error(f'Error inserting pipeline failed: {meta_e}')
                raise meta_e from e
            raise e

        # after cln insert success, insert a pipeline run success
        try:
            params.status = 'success'
            params.error_message = None
            params.traceback_message = None
            insert_pipeline_batch_run(engine, params)
        except Exception as meta_e:
            logger.error(f'Error inserting pipeline success: {meta_e}')
            raise meta_e

        # prepare for next loop iteration
        params.batch_id +=1
        params.from_id_exclusive = params.to_id_inclusive
        params.rows_read = 0
        params.rows_written = 0
        params.error_message = None
        params.started_at = datetime.now(UTC)
        params.traceback_message = None

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
