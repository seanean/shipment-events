import os
import yaml
import logging
import json
from jsonschema import Draft202012Validator, ValidationError
from db import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)

def ingest_raw(data, envt):

    logger.info(f'Ingest_raw ({data}, {envt})')

    config = get_config(data, envt)

    schema = get_schema(config['schema_path'])
    validator = validate_schema(schema)

    for dirpath, dirnames, filenames in os.walk(config['landing_zone_path']):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            logger.info(f"Dirpath: {dirpath}, Dirnames: {dirnames}, Filenames: {filenames}, Filepath: {filepath}, Processing file: {filename}")

            file = get_file(filepath)
            validate_file(file, validator)
            insert_to_raw_table(file, data)


def get_config(data, envt):
    with open(f"config/{envt}.yaml") as stream:
        config = yaml.safe_load(stream)
        logger.info(f"Config: {config[data]}")
    return config[data]

def get_schema(schema_path):
    with open(schema_path) as stream:
        schema = json.load(stream)
        logger.info(f"Schema: {schema}")
    return schema

def validate_schema(schema):
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    return validator

def get_file(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
        logger.info(f"Data: {data}")
        return data

def validate_file(file, validator):
    try:
        validator.validate(instance=file)
        logger.info(f"File is valid")
    except ValidationError as e:
        logger.error(f"File is invalid: {e}")
        raise e

def insert_to_raw_table(file, data):

    insert_row = insert_row_builder(file, data)                        
    logger.info(insert_row)

    engine = get_engine()

    insert_qry = get_raw_insert_statement(data)

    with engine.begin() as conn:
        conn.execute(text(insert_qry),insert_row,)
        # just to see
        result = conn.execute(text(f"select * from raw.{data};"))
        logger.info(result.all())

def get_raw_insert_statement(data):
    path = f"db/sql/raw/raw_{data}_insert.sql"
    with open(path, encoding="utf-8") as f:
        return f.read()

def insert_row_builder(file, data):
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
            raise ValueError(f"Whatcha talkin' bout Willis? Unknown raw table: {data}")
        

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", required=True, choices=["shipment_status", "shipment_products"], help="The data to ingest")
    parser.add_argument("-e","--environment", default="dev", choices=["dev", "prod"], help="The environment to ingest from")
    args = parser.parse_args()
    ingest_raw(args.data, args.environment)

if __name__ == "__main__":
    main()
