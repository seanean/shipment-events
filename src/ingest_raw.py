import os
import shutil
from sqlalchemy.engine.row import RowMapping
import yaml
import logging
import json
from jsonschema import Draft202012Validator, ValidationError
from db import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)

def ingest_raw(data, envt):

    logger.info(f'Ingesting raw for {data} in {envt} environment')

    config = get_config(data, envt)

    schema = get_schema(config['schema_path'])
    validator = validate_schema(schema)

    for dirpath, dirnames, filenames in os.walk(config['landing_zone_pending_path']):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            logger.info(f"Dirpath: {dirpath}, Dirnames: {dirnames}, Filenames: {filenames}, Filepath: {filepath}, Processing file: {filename}")

            file = get_file(filepath)
            validate_file(file, validator)
            insert_to_raw_table(file, data)
            archive_file(filename, filepath, config['landing_zone_archive_path'])

    cleanup_pending_lz(config['landing_zone_pending_path'])
    logger.info(f"Pending folder cleanup successful")

def get_config(data, envt):
    logger.info(f"Getting config for {data} in {envt} environment")
    with open(f"config/{envt}.yaml") as stream:
        config = yaml.safe_load(stream)
        logger.info(f"Config retrieved successfully")
        logger.debug(f"Config for {data}: {config[data]}")
    return config[data]

def get_schema(schema_path):
    logger.info(f"Getting schema from {schema_path}")
    with open(schema_path) as stream:
        schema = json.load(stream)
        logger.info(f"Schema retrieved successfully")
        logger.debug(f"Schema: {schema}")
    return schema

def validate_schema(schema):
    logger.info(f"Validating schema")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    logger.info(f"Schema validation successful")
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

def insert_to_raw_table(file, data):
    logger.info(f"Inserting file into raw table for {data}")
    insert_row = insert_row_builder(file, data)                        
    logger.info(f"Insert row builder successful")
    logger.debug(f"Insert row: {insert_row}")

    engine = get_engine()
    logger.info(f"Engine retrieved successfully")
    logger.debug(f"Engine: {engine}")

    insert_qry = get_raw_insert_statement(data)

    with engine.begin() as conn:
        logger.info(f"Executing insert query")
        conn.execute(text(insert_qry),insert_row,)
        logger.info(f"Insert query executed successfully")
        # temp execution just to see result in terminal
        result = conn.execute(text(f"select * from raw.{data};")).mappings().all()
        for i, row in enumerate(result):
            logger.info(f"{data} Row {i}: {row}")


def get_raw_insert_statement(data):
    logger.info(f"Getting insert query for {data}")
    path = f"db/sql/raw/raw_{data}_insert.sql"
    with open(path, encoding="utf-8") as f:
        insert_qry = f.read()
        logger.info(f"Insert query retrieved successfully")
        logger.debug(f"Insert query: {insert_qry}")
        return insert_qry

def insert_row_builder(file, data):
    logger.info(f"Building insert row for {data}")
    match data:
        case "shipment_status":
            return [{"payload": json.dumps(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"]}]
        case "shipment_products":
            return [{"payload": json.dumps(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"]}]
        case _:
            logger.error(f"Whatcha talkin' bout Willis? Unknown raw table: {data}")
            raise ValueError(f"Whatcha talkin' bout Willis? Unknown raw table: {data}")

def archive_file(filename, filepath, archive_folder):
    logger.info(f"Archiving file {filename} from {filepath} to {archive_folder}")
    pending_dt_partition = os.path.basename(os.path.dirname(filepath))
    archive_folder = os.path.join(archive_folder, pending_dt_partition)
    
    if not os.path.exists(archive_folder):
        logger.info(f"Archive folder {archive_folder} does not exist, creating it")
        os.makedirs(archive_folder)
        logger.info(f"Archive folder {archive_folder} created successfully")

    destination_filepath = os.path.join(archive_folder, filename)
    shutil.move(filepath, destination_filepath)
    logger.info(f"File {filename} archived successfully to {destination_filepath}")

def cleanup_pending_lz(pending_folder):
    logger.info(f"Cleaning up pending folder {pending_folder}")
    for dirpath, dirnames, filenames in os.walk(pending_folder):
        if len(os.listdir(dirpath)) == 0:
            logger.info(f"Pending folder {dirpath} is empty, removing it")
            os.rmdir(dirpath)
            logger.info(f"Pending folder {dirpath} removed successfully")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", required=True, choices=["shipment_status", "shipment_products"], help="The data to ingest")
    parser.add_argument("-e","--environment", default="dev", choices=["dev", "prod"], help="The environment to ingest from")
    args = parser.parse_args()
    ingest_raw(args.data, args.environment)

if __name__ == "__main__":
    main()
