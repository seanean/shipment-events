import os
import shutil
import yaml
import logging
import json
from pathlib import Path
from jsonschema import Draft202012Validator, ValidationError, SchemaError
from db import get_engine
from sqlalchemy import text
from datetime import datetime, UTC
import traceback
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

# shipment-events(parent2) > src(parent1)>ingest_raw.py
_REPO_ROOT_PATH = Path(__file__).resolve().parent.parent


def ingest_raw(data, envt):

    logger.info(f'Ingesting raw for {data} in {envt} environment')

    config_path = _REPO_ROOT_PATH.joinpath(f"config/{envt}.yaml")
    loaded_config = get_config(data, config_path)
    config = resolve_config(loaded_config)

    schema = get_schema(config['schema_path'])
    validator = validate_schema(schema)

    for dirpath, dirnames, filenames in os.walk(config['lz_pending_path']):
        logger.info(f"Processing pending folder: {dirpath}")
        for filename in filenames:
            pending_filepath = os.path.join(dirpath, filename)
            pending_dt_partition = os.path.basename(dirpath)
            logger.info(f"Processing file {pending_filepath}")

            pending_file = get_file(pending_filepath)

            try:
                validate_file(pending_file, validator)
            except ValidationError as e:
                # flow for invalid data
                insert_to_table(config['quarantine_insert_sql_path'],config['quarantine_target_table'], pending_file,
                                pending_filepath, str(e), traceback.format_exc())
                quarantine_folder = os.path.join(config['lz_quarantine_path'], pending_dt_partition)
                try:
                    store_file(filename, pending_filepath, quarantine_folder)
                except Exception as e:
                    logger.error(f"Error moving file {filename} to quarantine folder: {e}")
                    rollback_insert(config['quarantine_target_table'], pending_filepath)
                continue

            # flow for valid data
            insert_to_table(config['raw_insert_sql_path'], config['raw_target_table'], pending_file, pending_filepath)
            archive_folder = os.path.join(config['lz_archive_path'], pending_dt_partition)
            try:
                store_file(filename, pending_filepath, archive_folder)
            except Exception as e:
                logger.error(f"Error moving file {filename} to archive folder: {e}")
                rollback_insert(config['raw_target_table'], pending_filepath)

    cleanup_pending_lz(config['lz_pending_path'])
    logger.info(f"Pending folder cleanup successful")

def get_config(data, config_path):
    logger.info(f"Getting config for {data} from {config_path}")
    with open(config_path) as stream:
        config = yaml.safe_load(stream)
        logger.info(f"Config retrieved successfully")
        logger.debug(f"Config for {data}: {config[data]}")
    return config[data]

def resolve_config(loaded_config):
    logger.info(f"Resolving config for loaded_config")
    resolved_config = {
        'lz_pending_path': _REPO_ROOT_PATH.joinpath(loaded_config['lz_pending_path']),
        'lz_archive_path': _REPO_ROOT_PATH.joinpath(loaded_config['lz_archive_path']),
        'lz_quarantine_path': _REPO_ROOT_PATH.joinpath(loaded_config['lz_quarantine_path']),
        'schema_path': _REPO_ROOT_PATH.joinpath(loaded_config['schema_path']),
        'raw_insert_sql_path': _REPO_ROOT_PATH.joinpath(loaded_config['raw_insert_sql_path']),
        'quarantine_insert_sql_path': _REPO_ROOT_PATH.joinpath(loaded_config['quarantine_insert_sql_path']),
        'raw_target_table': f'{loaded_config['raw_db_schema']}.{loaded_config['raw_table']}',
        'quarantine_target_table': f'{loaded_config['quarantine_db_schema']}.{loaded_config['quarantine_table']}'
        # if at some point it's needed, we can also resolve the raw_table and raw_db_schema parameters
    }
    return resolved_config

def get_schema(schema_path):
    logger.info(f"Getting schema from {schema_path}")
    with open(schema_path) as stream:
        schema = json.load(stream)
        logger.info(f"Schema retrieved successfully")
        logger.debug(f"Schema: {schema}")
    return schema

def validate_schema(schema):
    logger.info(f"Validating schema")
    try:
        Draft202012Validator.check_schema(schema)
        logger.info(f"Schema is valid")
    except SchemaError as e:
        logger.error(f"Schema is invalid: {e}")
        logger.error(f"Schema: {schema}")
        raise e
    validator = Draft202012Validator(schema)
    logger.info(f"Schema validator created successfully")
    return validator

def get_file(filepath):
    logger.info(f"Getting file from {filepath}")
    with open(filepath, 'r') as f:
        data = json.load(f)
        logger.info(f"File retrieved successfully")
        logger.debug(f"File: {data}")
        return data

def validate_file(file, validator):
    try:
        logger.info(f"Validating file")
        validator.validate(instance=file)
        logger.info(f"File is valid")
        logger.debug(f"File: {file}")
    except ValidationError as e:
        logger.error(f"File is invalid: {e}")
        logger.error(f"File: {file}")
        raise e


def insert_to_table(insert_sql_path, target_table, file, source_filepath, error_message=None, traceback_message=None):
    logger.info(f"Inserting file into {target_table}")
    insert_row = insert_row_builder(target_table, file, source_filepath, error_message, traceback_message, datetime.now(UTC))                        
    logger.info(f"Insert row builder successful")
    logger.debug(f"Insert row: {insert_row}")

    engine = get_engine()
    logger.info(f"Engine retrieved successfully")
    logger.debug(f"Engine: {engine}")

    insert_qry = get_insert_statement(insert_sql_path)

    with engine.begin() as conn:
        logger.info(f"Executing insert query")
        conn.execute(text(insert_qry),insert_row,)
        logger.info(f"Insert query executed successfully")

def get_insert_statement(insert_sql_path):
    logger.info(f"Getting insert query from {insert_sql_path}")
    with open(insert_sql_path, encoding="utf-8") as f:
        insert_qry = f.read()
        logger.info(f"Insert query retrieved successfully")
        logger.debug(f"Insert query: {insert_qry}")
        return insert_qry

def insert_row_builder(target_table, file, source_filepath, error_message, traceback_message, meta_insert_timestamp):
    logger.info(f"Building insert row for {target_table}")

    match target_table:
        case "raw.shipment_status":
            return {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}
        case "raw.shipment_products":
            return {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}
        case "quarantine.shipment_status":
            return {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}
        case "quarantine.shipment_products":
            return {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}
        case _:
            logger.error(f"Whatcha talkin' bout Willis? Unknown table: {target_table}")
            raise ValueError(f"Whatcha talkin' bout Willis? Unknown table: {target_table}")

def store_file(filename, source_filepath, target_folder):
    logger.info(f"Moving file {filename} from {source_filepath} to {target_folder}")
    
    if not os.path.exists(target_folder):
        logger.info(f"Target folder {target_folder} does not exist, creating it")
        os.makedirs(target_folder)
        logger.info(f"Target folder {target_folder} created successfully")

    destination_filepath = os.path.join(target_folder, filename)
    shutil.move(source_filepath, destination_filepath)
    logger.info(f"File {filename} moved successfully to {destination_filepath}")

def rollback_insert(target_table, source_filepath):
    logger.info(f"Rolling back insert for {target_table}")
    delete_qry = f"DELETE FROM {target_table} WHERE meta_source_file_path = :meta_source_file_path"
    engine = get_engine()
    with engine.begin() as conn:
        logger.info(f"Executing delete query")
        conn.execute(text(delete_qry), {"meta_source_file_path": source_filepath})
        logger.info(f"Delete query executed successfully")

def cleanup_pending_lz(pending_folder):
    logger.info(f"Cleaning up pending folder {pending_folder}")
    for dirpath, dirnames, filenames in os.walk(pending_folder, topdown=False):
        if len(os.listdir(dirpath)) == 0:
            logger.info(f"Pending folder {dirpath} is empty, removing it")
            os.rmdir(dirpath)
            logger.info(f"Pending folder {dirpath} removed successfully")

def main():
    # to be created later when I want to add __init__.py and trigger via cli with args
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-d", "--data", required=True, choices=["shipment_status", "shipment_products"], help="The data to ingest")
    # parser.add_argument("-e","--environment", default="dev", choices=["dev", "prod"], help="The environment to ingest from")
    # args = parser.parse_args()
    # ingest_raw(args.data, args.environment)
    print('hello world :)')

if __name__ == "__main__":
    main()
