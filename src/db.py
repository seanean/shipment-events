from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from pathlib import Path
import logging
from datetime import datetime
from psycopg.types.json import Jsonb
from typing import Any


logger = logging.getLogger(__name__)

_engine = None
# Singleton engine, so you always get the same one
def get_engine():
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
        logger.info(f"Insert query retrieved successfully")
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
                        "event_timestamp": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "meta_source_file_path": meta_source_filepath}
        case "raw.shipment_products":
            return {"payload": Jsonb(content), "event_id": content["event_id"],
                        "event_timestamp": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_status":
            return {"payload": Jsonb(content), "event_id": content["event_id"],
                        "event_timestamp": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_products":
            return {"payload": Jsonb(content), "event_id": content["event_id"],
                        "event_timestamp": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}
        case "cln.shipment_status":
            return {"payload_cln": Jsonb(content["payload_cln"]), "event_id": content["event_id"],
                        "event_timestamp": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "raw_offset_id": content["offset_id"],
                        "meta_update_timestamp": None,
                        "meta_source_file_path": meta_source_filepath}
        case "cln.shipment_products":
            return {"payload_cln": Jsonb(content["payload_cln"]), "event_id": content["event_id"],
                        "event_timestamp": content["event_timestamp"],
                        "event_name": content["event_name"],
                        "raw_offset_id": content["offset_id"],
                        "meta_update_timestamp": None,
                        "meta_source_file_path": meta_source_filepath}
        case _:
            logger.error(f"Whatcha talkin' bout Willis? Unknown table: {target_table}")
            raise ValueError(f"Whatcha talkin' bout Willis? Unknown table: {target_table}")