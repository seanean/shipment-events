import os
import shutil
import logging
from app_logger import configure_logger
import json
from pathlib import Path
from jsonschema import Draft202012Validator, ValidationError, SchemaError
from db import get_engine, get_insert_statement, insert_row_builder
from sqlalchemy import text, Engine
import traceback
from typing import Any, Literal
from config_util import get_config, resolve_config, _REPO_ROOT_PATH

logger = logging.getLogger(__name__)


def ingest_raw(data: Literal["shipment_status", "shipment_products"],
               envt: Literal["dev"]):

    logger.info(f'Ingesting raw for {data} in {envt} environment')

    events_processed = 0
    events_to_raw = 0
    events_to_quarantine = 0
    events_rolled_back = 0

    config_path = _REPO_ROOT_PATH.joinpath(f"config/{envt}.yaml")
    loaded_config = get_config(data, config_path)
    config = resolve_config(loaded_config)

    schema = get_schema(config.schema_path)
    validator = validate_schema(schema)

    for dirpath, dirnames, filenames in os.walk(config.lz_pending_path):
        logger.info(f"Processing pending folder: {dirpath}")
        for filename in filenames:
            pending_filepath = os.path.join(dirpath, filename)
            pending_dt_partition = os.path.basename(dirpath)
            logger.info(f"Processing file {pending_filepath}")

            pending_file = get_file(pending_filepath)
            events_processed += 1
            engine = get_engine()

            try:
                validate_file(pending_file, validator)
            except ValidationError as e:
                # flow for invalid data
                insert_to_table(engine, config.quarantine_insert_sql_path, config.quarantine_target_table, pending_file,
                                pending_filepath, str(e), traceback.format_exc())
                quarantine_folder = os.path.join(config.lz_quarantine_path, pending_dt_partition)
                try:
                    store_file(filename, pending_filepath, quarantine_folder)
                    events_to_quarantine += 1
                except Exception as e:
                    logger.error(f"Error moving file {filename} to quarantine folder: {e}", exc_info=True)
                    rollback_insert(engine, config.quarantine_target_table, pending_filepath)
                    events_rolled_back += 1
                continue

            # flow for valid data
            insert_to_table(engine, config.raw_insert_sql_path, config.raw_target_table, pending_file, pending_filepath)
            archive_folder = os.path.join(config.lz_archive_path, pending_dt_partition)
            try:
                store_file(filename, pending_filepath, archive_folder)
                events_to_raw += 1
            except Exception as e:
                logger.error(f"Error moving file {filename} to archive folder: {e}", exc_info=True)
                rollback_insert(engine, config.raw_target_table, pending_filepath)
                events_rolled_back += 1
    
    logger.info(f"{data} events processed: {events_processed}")
    logger.info(f"{data} events to raw: {events_to_raw}")
    logger.info(f"{data} events to quarantine: {events_to_quarantine}")
    logger.info(f"{data} events rolled back: {events_rolled_back}")

    cleanup_pending_lz(config.lz_pending_path)
    logger.info(f"Pending folder cleanup successful")

def get_schema(schema_path: Path) -> dict[str, Any]:
    logger.info(f"Getting schema from {schema_path}")
    with open(schema_path) as stream:
        schema = json.load(stream)
        logger.info(f"Schema retrieved successfully")
        logger.debug(f"Schema: {schema}")
    return schema

def validate_schema(schema: dict[str, Any]) -> Draft202012Validator:
    logger.info(f"Validating schema")
    try:
        Draft202012Validator.check_schema(schema)
        logger.info(f"Schema is valid")
    except SchemaError as e:
        logger.error(f"Schema is invalid: {e}", exc_info=True)
        logger.error(f"Schema: {schema}")
        raise e
    validator = Draft202012Validator(schema)
    logger.info(f"Schema validator created successfully")
    return validator

def get_file(filepath: str) -> dict[str, Any]:
    logger.info(f"Getting file from {filepath}")
    with open(filepath, 'r') as f:
        data = json.load(f)
        logger.info(f"File retrieved successfully")
        logger.debug(f"File: {data}")
        return data

def validate_file(file: dict[str, Any], validator: Draft202012Validator) -> None:
    try:
        logger.info(f"Validating file")
        validator.validate(instance=file)
        logger.info(f"File is valid")
        logger.debug(f"File: {file}")
    except ValidationError as e:
        logger.error(f"File is invalid: {e}", exc_info=True)
        logger.error(f"File: {file}")
        raise e


def insert_to_table(engine: Engine, insert_sql_path: Path, target_table: str, 
                    file: dict[str, Any], source_filepath: str, error_message: str | None = None, 
                    traceback_message: str | None = None) -> None:
    logger.info(f"Inserting file into {target_table}")
    insert_row = insert_row_builder(target_table, file, source_filepath, 
                                    error_message, traceback_message)                        
    logger.info(f"Insert row builder successful")
    logger.debug(f"Insert row: {insert_row}")

    logger.info(f"Engine retrieved successfully")
    logger.debug(f"Engine: {engine}")

    insert_qry = get_insert_statement(insert_sql_path)

    with engine.begin() as conn:
        logger.info(f"Executing insert query")
        conn.execute(text(insert_qry),insert_row,)
        logger.info(f"Insert query executed successfully")

def store_file(filename: str, source_filepath: str, target_folder: str) -> None:
    logger.info(f"Moving file {filename} from {source_filepath} to {target_folder}")
    
    if not os.path.exists(target_folder):
        logger.info(f"Target folder {target_folder} does not exist, creating it")
        os.makedirs(target_folder)
        logger.info(f"Target folder {target_folder} created successfully")

    destination_filepath = os.path.join(target_folder, filename)
    shutil.move(source_filepath, destination_filepath)
    logger.info(f"File {filename} moved successfully to {destination_filepath}")

def rollback_insert(engine: Engine, target_table: str, source_filepath: str) -> None:
    logger.info(f"Rolling back insert for {target_table}")
    delete_qry = f"DELETE FROM {target_table} WHERE meta_source_file_path = :meta_source_file_path"
    with engine.begin() as conn:
        logger.info(f"Executing delete query")
        conn.execute(text(delete_qry), {"meta_source_file_path": source_filepath})
        logger.info(f"Delete query executed successfully")

def cleanup_pending_lz(pending_folder: Path) -> None:
    logger.info(f"Cleaning up pending folder {pending_folder}")
    for dirpath, dirnames, filenames in os.walk(pending_folder, topdown=False):
        if len(os.listdir(dirpath)) == 0:
            logger.info(f"Pending folder {dirpath} is empty, removing it")
            os.rmdir(dirpath)
            logger.info(f"Pending folder {dirpath} removed successfully")

def main() -> None:
    # to be created later when I want to add __init__.py and trigger via cli with args
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-d", "--data", required=True, choices=["shipment_status", "shipment_products"], help="The data to ingest")
    # parser.add_argument("-e","--environment", default="dev", choices=["dev", "prod"], help="The environment to ingest from")
    # args = parser.parse_args()
    # ingest_raw(args.data, args.environment)
    print('hello world :)')
    ingest_raw('shipment_status', 'dev')
    ingest_raw('shipment_products', 'dev')

if __name__ == "__main__":
    logger = configure_logger()
    main()
