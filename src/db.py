from sqlalchemy import create_engine, Engine, text
from dotenv import load_dotenv
import os
from pathlib import Path
import logging
from datetime import datetime
from psycopg.types.json import Jsonb
from typing import Any, Tuple
from dataclasses import dataclass
import traceback
import pandas as pd

@dataclass(frozen=False)
class BatchParameters:
    latest_run_id: int | None = None
    latest_timestamp: str | None = None
    run_id: int | None = None
    batch_id: int = 1
    job_name: str | None = None
    status: str | None = None
    started_at: datetime | None = None
    from_timestamp_exclusive: str = None
    to_timestamp_inclusive: str = None
    rows_read: int = 0
    rows_written: int = 0
    error_message: str | None = None
    traceback_message: str | None = None


logger = logging.getLogger(__name__)

_engine = None
# Singleton engine, so you always get the same one
def get_engine() -> Engine:
    global _engine
    if _engine is None:
        load_dotenv()
        db_user = 'shrw'
        db_password = os.getenv('POSTGRES_RW_PW')
        db_host = os.getenv('POSTGRES_HOST')
        db_port = os.getenv('POSTGRES_PORT')
        db_name = os.getenv('POSTGRES_DB')
        db_url = f'postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        _engine = create_engine(db_url)
    return _engine

def get_insert_statement(insert_sql_path: Path) -> str:
    logger.info(f"Getting insert query from {insert_sql_path}")
    with open(insert_sql_path, encoding="utf-8") as f:
        insert_qry = f.read()
        logger.debug(f"Insert query retrieved successfully")
        logger.debug(f"Insert query: {insert_qry}")
        return insert_qry

def insert_row_builder(target_table: str, content: dict[str, Any],
                       meta_source_filepath: str, error_message: str | None = None,
                       traceback_message: str | None = None, ) -> dict[str, Any]:
    logger.info(f"Building insert row for {target_table}")

    # these are meant to mirror the :parameters that are present in the sql queries
    match target_table:
        case "raw.shipment_status":
            return {"payload": Jsonb(content), "event_id": content["event_id"],
                        "event_tmst": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "meta_source_file_path": meta_source_filepath}
        case "raw.shipment_products":
            return {"payload": Jsonb(content), "event_id": content["event_id"],
                        "event_tmst": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_status":
            return {"payload": Jsonb(content), "event_id": content["event_id"],
                        "event_tmst": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_products":
            return {"payload": Jsonb(content), "event_id": content["event_id"],
                        "event_tmst": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}
        case "cln.shipment_status":
            return {"payload_cln": Jsonb(content["payload_cln"]), "event_id": content["event_id"],
                        "event_tmst": content["event_tmst"],
                        "event_name": content["event_name"],
                        "raw_offset_id": content["offset_id"],
                        "meta_update_tmst": None,
                        "meta_source_file_path": meta_source_filepath}
        case "cln.shipment_products":
            return {"payload_cln": Jsonb(content["payload_cln"]), "event_id": content["event_id"],
                        "event_tmst": content["event_tmst"],
                        "event_name": content["event_name"],
                        "raw_offset_id": content["offset_id"],
                        "meta_update_tmst": None,
                        "meta_source_file_path": meta_source_filepath}
        case _:
            logger.error(f"Whatcha talkin' bout Willis? Unknown table: {target_table}")
            raise ValueError(f"Whatcha talkin' bout Willis? Unknown table: {target_table}")

def get_latest_run_id(engine: Engine, envt: str) -> int:
    logger.debug(f"Getting latest run_id for in {envt} environment")
    run_qry = f"SELECT MAX(run_id) FROM meta.pipeline_run"
    try:
        with engine.begin() as conn:
            logger.debug(f"Retrieving latest run_id")
            row = conn.execute(text(run_qry)).fetchone()
            assert row is not None # because of fetchone() return type making mypy sad
            result = row[0]
            latest_run_id = result if result is not None else 0
            logger.info(f"Latest {envt} run id: {latest_run_id}")
            return latest_run_id
    except Exception as e:
        logger.error(f"Error getting latest run id: {e}")
        raise e

def get_latest_timestamp(engine: Engine, iparams: BatchParameters) -> int:
    logger.debug(f"Getting latest timestamp id for {iparams.job_name}")
    offset_qry = f"SELECT MAX(to_timestamp_inclusive) FROM meta.pipeline_run WHERE job_name = :job_name AND status = 'success'"
    try:
        with engine.begin() as conn:
            logger.debug(f"Retrieving latest timestamp")
            row = conn.execute(text(offset_qry), {"job_name": iparams.job_name}).fetchone()
            assert row is not None # because of fetchone() return type making mypy sad
            result = row[0]
            latest_timestamp = result if result is not None else '1900-01-01 00:00:00'
            logger.info(f"Latest timestamp for {iparams.job_name}: {latest_timestamp}")
            return latest_timestamp
    except Exception as e:
        logger.error(f"Error getting latest timestamp: {e}")
        try:
            iparams.status = 'failed'
            iparams.error_message = str(e)
            iparams.traceback_message = traceback.format_exc()
            insert_pipeline_batch_run(engine, iparams)
        except Exception as meta_e:
            logger.error(f'Error inserting pipeline failed: {meta_e}')
            raise meta_e from e
        raise e


def insert_pipeline_batch_run(engine: Engine, iparams: BatchParameters):
    insert_qry = """INSERT INTO meta.pipeline_run (
                        run_id, batch_id, 
                        job_name, status,
                        started_at, finished_at,
                        starting_from_timestamp_exclusive, from_timestamp_exclusive,
                        to_timestamp_inclusive, rows_read,
                        rows_written, error_message,
                        traceback_message, meta_insert_tmst)
                    VALUES (
                        :run_id, :batch_id,
                        :job_name, :status,
                        :started_at, NOW(),
                        :starting_from_timestamp_exclusive, :from_timestamp_exclusive,
                        :to_timestamp_inclusive, :rows_read,
                        :rows_written, :error_message,
                        :traceback_message, NOW())"""
    with engine.begin() as conn:
        conn.execute(text(insert_qry), {"run_id": iparams.run_id, "batch_id": iparams.batch_id,
                                        "job_name": iparams.job_name, "status": iparams.status,
                                        "started_at": iparams.started_at,
                                        "starting_from_timestamp_exclusive": iparams.latest_timestamp, "from_timestamp_exclusive": iparams.from_timestamp_exclusive,
                                        "to_timestamp_inclusive": iparams.to_timestamp_inclusive, "rows_read": iparams.rows_read,
                                        "rows_written": iparams.rows_written, "error_message": iparams.error_message,
                                        "traceback_message": iparams.traceback_message})


def get_batch(engine: Engine, source_table: str, envt: str, 
                  iparams: BatchParameters, batch_size: int) -> Tuple[pd.DataFrame, int]:
    event_tmst = iparams.from_timestamp_exclusive
    logger.info(f"Getting batch of {batch_size} records from {source_table} in {envt} environment from {event_tmst}")
    batch_qry = f'SELECT * FROM {source_table} WHERE event_tmst > :event_tmst LIMIT :batch_size'
    try:
        with engine.begin() as conn:
            result = conn.execute(text(batch_qry), {"event_tmst": event_tmst, "batch_size": batch_size})
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
            return df, df.shape[0]
    except Exception as e:
        logger.error(f"Error getting batch: {e}")
        try:
            iparams.status = 'failed'
            iparams.error_message = str(e)
            iparams.traceback_message = traceback.format_exc()
            insert_pipeline_batch_run(engine, iparams)
        except Exception as meta_e:
            logger.error(f'Error inserting pipeline failed: {meta_e}')
            raise meta_e from e
        raise e