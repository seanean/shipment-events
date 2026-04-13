from ingest_raw import resolve_config, _REPO_ROOT_PATH, validate_schema, validate_file, insert_row_builder
from pathlib import Path
from jsonschema import Draft202012Validator, SchemaError, ValidationError
import pytest
from psycopg.types.json import Jsonb
from datetime import datetime, UTC

# resolve_config tests
def test_resolve_config():
    loaded_config = {
        'lz_pending_path': 'landing-zone/pending/shipment_status',
        'lz_archive_path': 'landing-zone/archive/shipment_status',
        'lz_quarantine_path': 'landing-zone/quarantine/shipment_status',
        'schema_path': 'schemas/shipment_status.json',
        'raw_table': 'shipment_status',
        'quarantine_table': 'shipment_status'
    }
    result = resolve_config(loaded_config)

    # type checks
    assert isinstance(result['lz_pending_path'], Path)
    assert isinstance(result['lz_archive_path'], Path)
    assert isinstance(result['lz_quarantine_path'], Path)
    assert isinstance(result['schema_path'], Path)
    assert isinstance(result['raw_table'], str)
    assert isinstance(result['quarantine_table'], str)
    # path checks
    assert result['lz_pending_path'] == _REPO_ROOT_PATH.joinpath(loaded_config['lz_pending_path']   )
    assert result['lz_archive_path'] == _REPO_ROOT_PATH.joinpath(loaded_config['lz_archive_path'])
    assert result['lz_quarantine_path'] == _REPO_ROOT_PATH.joinpath(loaded_config['lz_quarantine_path'])
    assert result['schema_path'] == _REPO_ROOT_PATH.joinpath(loaded_config['schema_path'])
    # table checks
    assert loaded_config['raw_table'] == result['raw_table']
    assert loaded_config['quarantine_table'] == result['quarantine_table']

# validate_schema tests

def test_validate_schema_accepts_valid_schema():
    schema = {
        "type": "object",
        "properties": {
            "something": {"type": "string"}
        },
        "required": ["something"]
    }
    validator = validate_schema(schema)
    assert validator is not None
    assert isinstance(validator, Draft202012Validator)
    assert validator.is_valid({"something": "test"})

def test_validate_schema_rejects_invalid_schema():
    schema = {
        "type": 123
    }
    with pytest.raises(SchemaError):
        validate_schema(schema)

# validate_file tests

def test_validate_file_accepts_valid_file():
    schema = {
        "type": "object",
        "properties": {
            "something": {"type": "string"}
        },
        "required": ["something"]
    }
    file = {
        "something": "test"
    }
    
    validator = Draft202012Validator(schema)
    validate_file(file, validator)


def test_validate_file_rejects_invalid_file():
    schema = {
        "type": "object",
        "properties": {
            "something": {"type": "string"}
        },
        "required": ["something"]
    }
    file = {
        "barry": "manilow"
    }
    
    validator = Draft202012Validator(schema)
    with pytest.raises(ValidationError):
        validate_file(file, validator)

# insert_row_builder tests
#parametrize the db_schema and table
@pytest.mark.parametrize("db_schema, table", 
                        [("raw", "shipment_status"), ("raw", "shipment_products"),
                        ("quarantine", "shipment_status"), ("quarantine", "shipment_products")])

def test_insert_row_builder_returns_valid_dict(db_schema, table):
    source_filepath = "test.json"
    error_message = "error"
    traceback_message = "traceback"
    # default timestamp of 1/1/1970 00:00:00
    meta_insert_timestamp = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
    # file based on shipment_status.json schema
    file = {
        "event_id": "123",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "event_name": "shipment_status"
        }
    match (db_schema, table):
        case ("raw", "shipment_status"):
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}
        case ("raw", "shipment_products"):
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}
        case ("quarantine", "shipment_status"):
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}
        case ("quarantine", "shipment_products"):
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_insert_timestamp": meta_insert_timestamp,
                        "meta_source_file_path": source_filepath}

    result = insert_row_builder(db_schema, file, table, source_filepath, error_message, traceback_message, meta_insert_timestamp)
    
    # for each key in expected_result, check if result[key] == expected_result[key]
    for key in expected_result:
        if key == "payload":
            assert result[key].obj == expected_result[key].obj
        else:
            assert result[key] == expected_result[key]

def test_insert_row_builder_raises_valueerror_for_unknown_table():
    db_schema = "unknown"
    table = "unknown"
    source_filepath = "test.json"
    error_message = "error"
    traceback_message = "traceback"
    meta_insert_timestamp = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
    file = {
        "event_id": "123",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "event_name": "shipment_status"
    }
    with pytest.raises(ValueError):
        insert_row_builder(db_schema, file, table, source_filepath, error_message, traceback_message, meta_insert_timestamp)