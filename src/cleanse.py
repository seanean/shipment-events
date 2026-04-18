# import os
# import shutil
# import yaml
import logging
# import json
# from pathlib import Path
# from jsonschema import Draft202012Validator, ValidationError, SchemaError
from db import get_engine
from sqlalchemy import text
# from datetime import datetime, UTC
# import traceback
# from psycopg.types.json import Jsonb
# from dataclasses import dataclass
from typing import Any, Literal
import pandas as pd
# only required if running cleanse.py directly:
from app_logger import configure_logger

logger = logging.getLogger(__name__)

_BATCH_SIZE = 3 # 200

def cleanse(data: Literal["shipment_status", "shipment_products"],
                    envt: Literal["dev"]) -> None:
    logger.info(f'Running cleansed for {data} in {envt} environment')

    try:
        latest_raw_offset_id = get_latest_raw_offset_id(data, envt)
        logger.info(f"Latest raw offset id: {latest_raw_offset_id}")
    except Exception as e:
        logger.error(f"Error getting latest raw offset id: {e}", exc_info=True)
        # TODO: insert failed pipeline run
        # TODO: maybe raise e and handle in main?
        raise e

    batch_df = get_raw_batch(data, envt, latest_raw_offset_id, _BATCH_SIZE)
    if batch_df.empty:
        logger.info(f'Batch is empty, exiting')
        # todo: insert success with no rows + same offset
        return
    logger.info(batch_df)

    batch_df_merged = batch_df.loc[batch_df["offset_id"].isin(batch_df.groupby("event_id")["offset_id"].max())]

    logger.info(batch_df_merged)
    # merge
    # uuids
    # insert cln
    # insert run


    # while batch:
    #     batch = get_raw_batch(data, envt, latest_raw_offset_id, _BATCH_SIZE)


    # get raw data from offset_id to latest or offset + _BATCH_SIZE
    # merge raw on event id
    # generate uuids
    # insert into cleansed
    # insert batch success
    # insert pipeline run success
    # insert pipeline run failure
    # handle empty batch
    

def get_latest_raw_offset_id(data: str, envt: str) -> int:
    logger.info(f"Getting latest raw offset id for {data} in {envt} environment")
    offset_qry = f"SELECT MAX(to_id_inclusive) FROM meta.pipeline_run WHERE job_name = :job_name AND status = 'success'"
    job_name = f'cleanse_{data}_{envt}'
    engine = get_engine()
    try:
        with engine.begin() as conn:
            logger.info(f"Retrieving latest raw offset id")
            result = conn.execute(text(offset_qry), {"job_name": job_name}).fetchone()[0]
            latest_offset_id = result if result is not None else 0
            logger.info(f"Latest raw offset id: {latest_offset_id}")
            return latest_offset_id
    except Exception as e:
        logger.error(f"Error getting latest raw offset id: {e}", exc_info=True)
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
        logger.error(f"Error getting latest raw offset id: {e}", exc_info=True)
        raise e



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
