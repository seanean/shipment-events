# import os
# import shutil
# import yaml
import logging
# import json
# from pathlib import Path
# from jsonschema import Draft202012Validator, ValidationError, SchemaError
from db import get_engine, get_insert_statement, insert_row_builder
from sqlalchemy import text
from datetime import datetime, UTC
import traceback
# from psycopg.types.json import Jsonb
# from dataclasses import dataclass
from typing import Any, Literal, cast
import pandas as pd
# only required if running cleanse.py directly:
from app_logger import configure_logger
from uuid import uuid5, NAMESPACE_DNS, UUID
from copy import deepcopy
from config_util import get_config, resolve_config, _REPO_ROOT_PATH
logger = logging.getLogger(__name__)

_BATCH_SIZE = 1 # 200
_ROOT_NAMESPACE = uuid5(NAMESPACE_DNS, 'shipment-events')


def cleanse(data: Literal["shipment_status", "shipment_products"],
                    envt: Literal["dev"]) -> None:
    logger.info(f'Running cleansed for {data} in {envt} environment')
    
    # initializing variables
    run_started_at = datetime.now(UTC)
    job_name = f'cleanse_{data}_{envt}'
    config_path = _REPO_ROOT_PATH.joinpath(f"config/{envt}.yaml")
    batch_id = 1

    # preparing config
    loaded_config = get_config(data, config_path)
    config = resolve_config(loaded_config)

    try:
        latest_run_id = get_latest_run_id(envt)
        run_id = latest_run_id + 1
        logger.info(f"Latest run ID: {latest_run_id}, current run ID: {run_id}")
    except Exception as e:
        logger.error(f"Error getting latest run id: {e}")
        raise e

    try:
        latest_raw_offset_id = get_latest_raw_offset_id(job_name)
        logger.info(f"Latest raw offset id: {latest_raw_offset_id}")
    except Exception as e:
        logger.error(f"Error getting latest raw offset id: {e}. {traceback.format_exc()}")
        try:
            insert_pipeline_batch_run(run_id=run_id, batch_id=batch_id, job_name=job_name, status='failed', started_at=run_started_at,
                                            starting_from_id_exclusive=0,
                                            from_id_exclusive=0, to_id_inclusive=0,
                                            rows_read=0, rows_written=0, error_message=str(e), traceback_message=traceback.format_exc())
        except Exception as meta_e:
            logger.error(f'Error inserting pipeline failed: {meta_e}')
            raise meta_e
        raise e
    


    from_id_exclusive = latest_raw_offset_id
    # run until batches don't return anything else.
    while True:
        batch_df = get_raw_batch(data, envt, from_id_exclusive, _BATCH_SIZE)
        if batch_df.empty:
            logger.info(f'Batch is empty, exiting')
            insert_pipeline_batch_run(run_id=run_id, batch_id=1, job_name=job_name, status='success', started_at=run_started_at,
                                    starting_from_id_exclusive=latest_raw_offset_id,
                                    from_id_exclusive=from_id_exclusive, to_id_inclusive=from_id_exclusive,
                                    rows_read=0, rows_written=0, error_message=None, traceback_message=None)
            break
        rows_read = batch_df.shape[0]

        # get latest per event ID. alternative approach would be based on event timestamp
        batch_df_merged = merge_df(batch_df, partition_by="event_id", order_by="offset_id")
        to_id_inclusive = batch_df_merged["offset_id"].max()

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
                meta_insert_timestamp=datetime.now(UTC),
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
                rows_written = result.rowcount
                logger.info(f"Insert query executed successfully, {result.rowcount} rows written")
        
        # if cln insert failed, insert a pipeline run failure
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            try:
                insert_pipeline_batch_run(run_id=run_id, batch_id=batch_id, job_name=job_name, status='failed', started_at=run_started_at,
                                            starting_from_id_exclusive=latest_raw_offset_id,
                                            from_id_exclusive=from_id_exclusive, to_id_inclusive=to_id_inclusive,
                                            rows_read=rows_read, rows_written=0, error_message=str(e), traceback_message=traceback.format_exc())
            # if that failed too, that's not good
            except Exception as meta_e:
                logger.error(f'Error inserting pipeline failed: {meta_e}')
                raise meta_e
            raise e

        # after cln insert success, insert a pipeline run success
        try:
            insert_pipeline_batch_run(run_id=run_id, batch_id=batch_id, job_name=job_name, status='success', started_at=run_started_at,
                                        starting_from_id_exclusive=latest_raw_offset_id,
                                        from_id_exclusive=from_id_exclusive, to_id_inclusive=to_id_inclusive,
                                        rows_read=rows_read, rows_written=rows_written, error_message=None, traceback_message=None)
        except Exception as meta_e:
            logger.error(f'Error inserting pipeline success: {meta_e}')
            raise meta_e

        batch_id +=1
        from_id_exclusive = to_id_inclusive


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

def get_raw_batch(data: str, envt: str, offset_id: int, batch_size: int) -> pd.DataFrame:
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
            return df
    except Exception as e:
        logger.error(f"Error getting latest raw offset id: {e}")
        raise e

def merge_df(df: pd.DataFrame, partition_by, order_by) -> pd.DataFrame:
    return df.loc[df[order_by].isin(df.groupby(partition_by)[order_by].max())]


def add_uuids(payload: dict[str, Any], data: str) -> dict[str, Any]:
    payload_cln = deepcopy(payload)
    match data:
        case "shipment_status":
            return add_shipment_status_uuids(payload_cln)
        case "shipment_products":
            return add_shipment_products_uuids(payload_cln)
        case _:
            raise ValueError(f"Invalid data: {data}")

def _build_uuid(*args: str) -> str:
    return str(uuid5(_ROOT_NAMESPACE, ''.join(args)))

def add_shipment_status_uuids(payload_cln: dict[str, Any]) -> dict[str, Any]:
    payload_cln["event_data"]["shipment_uuid"] = _build_uuid(payload_cln["event_data"]["shipment_business_id"])
    payload_cln["event_data"]["shipment_status_uuid"] = _build_uuid(payload_cln["event_data"]["shipment_business_id"], payload_cln["event_data"]["status"])

    return payload_cln

def add_shipment_products_uuids(payload_cln: dict[str, Any]) -> dict[str, Any]:
    payload_cln["event_data"]["shipment_uuid"] = _build_uuid(payload_cln["event_data"]["shipment_business_id"])

    for item in payload_cln["event_data"]["products"]:
        item["shipment_product_uuid"] = _build_uuid(payload_cln["event_data"]["shipment_business_id"], item["product_id"])

    return payload_cln

def insert_pipeline_batch_run(run_id: int, batch_id: int, job_name: str, status: str, started_at: datetime, starting_from_id_exclusive: int,
                                from_id_exclusive: int, to_id_inclusive: int, rows_read: int, rows_written: int, error_message: str | None, traceback_message: str | None):
    insert_qry = """INSERT INTO meta.pipeline_run (run_id, batch_id, job_name, status, started_at, finished_at, starting_from_id_exclusive,
                    from_id_exclusive, to_id_inclusive, rows_read, rows_written, error_message, traceback_message)
                    VALUES (:run_id, :batch_id, :job_name, :status, :started_at, NOW(), :starting_from_id_exclusive,
                            :from_id_exclusive, :to_id_inclusive, :rows_read, :rows_written, :error_message, :traceback_message)"""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(insert_qry), {"run_id": run_id, "batch_id": batch_id, "job_name": job_name, "status": status, "started_at": started_at, 
                                        "starting_from_id_exclusive": starting_from_id_exclusive, "from_id_exclusive": from_id_exclusive, "to_id_inclusive": to_id_inclusive, 
                                        "rows_read": rows_read, "rows_written": rows_written, "error_message": error_message, "traceback_message": traceback_message})

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
