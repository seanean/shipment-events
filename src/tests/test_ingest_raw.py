from ingest_raw import (
    validate_schema,
    validate_file,
    get_schema,
    get_file, 
    store_file,
    cleanup_pending_lz
)
from config_util import get_config, resolve_config, _REPO_ROOT_PATH
from db import get_insert_statement, insert_row_builder
from pathlib import Path
from jsonschema import Draft202012Validator, SchemaError, ValidationError
import pytest
from psycopg.types.json import Jsonb
import json

# resolve_config tests
def test_resolve_config() -> None:
    loaded_config = {
        'lz_pending_path': 'landing-zone/pending/shipment_status',
        'lz_archive_path': 'landing-zone/archive/shipment_status',
        'lz_quarantine_path': 'landing-zone/quarantine/shipment_status',
        'schema_path': 'schemas/shipment_status.json',
        'raw_db_schema': 'raw',
        'raw_table': 'shipment_status',
        'raw_insert_sql_path': 'db/sql/raw/raw_shipment_status_insert.sql',
        'quarantine_db_schema': 'quarantine',
        'quarantine_table': 'shipment_status',
        'quarantine_insert_sql_path': 'db/sql/quarantine/quarantine_shipment_status_insert.sql',
        'cln_db_schema': 'cln',
        'cln_table': 'shipment_status',
        'cln_insert_sql_path': 'db/sql/cln/cln_shipment_status_insert.sql'

    }
    result = resolve_config(loaded_config)

    # type checks
    assert isinstance(result.lz_pending_path, Path)
    assert isinstance(result.lz_archive_path, Path)
    assert isinstance(result.lz_quarantine_path, Path)
    assert isinstance(result.schema_path, Path)
    assert isinstance(result.raw_insert_sql_path, Path)
    assert isinstance(result.quarantine_insert_sql_path, Path)
    assert isinstance(result.raw_target_table, str)
    assert isinstance(result.quarantine_target_table, str)
    assert isinstance(result.cln_target_table, str)
    # path checks
    assert result.lz_pending_path == _REPO_ROOT_PATH.joinpath(loaded_config['lz_pending_path']   )
    assert result.lz_archive_path == _REPO_ROOT_PATH.joinpath(loaded_config['lz_archive_path'])
    assert result.lz_quarantine_path == _REPO_ROOT_PATH.joinpath(loaded_config['lz_quarantine_path'])
    assert result.schema_path == _REPO_ROOT_PATH.joinpath(loaded_config['schema_path'])
    assert result.raw_insert_sql_path == _REPO_ROOT_PATH.joinpath(loaded_config['raw_insert_sql_path'])
    assert result.quarantine_insert_sql_path == _REPO_ROOT_PATH.joinpath(loaded_config['quarantine_insert_sql_path'])
    assert result.cln_insert_sql_path == _REPO_ROOT_PATH.joinpath(loaded_config['cln_insert_sql_path'])
    # table checks
    assert result.raw_target_table == f'{loaded_config['raw_db_schema']}.{loaded_config['raw_table']}'
    assert result.quarantine_target_table == f'{loaded_config['quarantine_db_schema']}.{loaded_config['quarantine_table']}'
    assert result.cln_target_table == f'{loaded_config['cln_db_schema']}.{loaded_config['cln_table']}'

# validate_schema tests

def test_validate_schema_accepts_valid_schema() -> None:
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

def test_validate_schema_rejects_invalid_schema() -> None:
    schema = {
        "type": 123
    }
    with pytest.raises(SchemaError):
        validate_schema(schema)

# validate_file tests

def test_validate_file_accepts_valid_file() -> None:
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


def test_validate_file_rejects_invalid_file() -> None:
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
@pytest.mark.parametrize("target_table", 
                        ["raw.shipment_status", "raw.shipment_products",
                        "quarantine.shipment_status", "quarantine.shipment_products"])

def test_insert_row_builder_returns_valid_dict(target_table: str) -> None:
    meta_source_filepath = "test.json"
    error_message = "error"
    traceback_message = "traceback"
    # file based on shipment_status.json schema
    file = {
        "event_id": "123",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "event_name": "shipment_status"
        }
    match target_table:
        case "raw.shipment_status":
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "meta_source_file_path": meta_source_filepath}
        case "raw.shipment_products":
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_status":
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_products":
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_timestamp": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}

    result = insert_row_builder(target_table, file, 
                                meta_source_filepath, error_message, traceback_message)
    
    # for each key in expected_result, check if result[key] == expected_result[key]
    for key in expected_result:
        if key == "payload":
            assert result[key].obj == expected_result[key].obj
        else:
            assert result[key] == expected_result[key]

def test_insert_row_builder_raises_valueerror_for_unknown_table() -> None:
    target_table = "unknown"
    meta_source_filepath = "test.json"
    error_message = "error"
    traceback_message = "traceback"
    file = {
        "event_id": "123",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "event_name": "shipment_status"
    }
    with pytest.raises(ValueError):
        insert_row_builder(target_table, file, meta_source_filepath, error_message, traceback_message)


def test_get_schema_returns_dict(tmp_path: Path) -> None:
    schema_path = tmp_path / "schema.json"
    schema = {"type": "string"}
    with open(schema_path, "w") as f:
        json.dump(schema, f)
    result = get_schema(schema_path)
    assert result == schema
    assert isinstance(result, dict)

def test_get_file_returns_dict(tmp_path: Path) -> None:
    file_path = tmp_path / "file.json"
    file = {"type": "string"}
    with open(file_path, "w") as f:
        json.dump(file, f)
    result = get_file(file_path)
    assert result == file
    assert isinstance(result, dict)

def test_get_insert_statement_returns_string(tmp_path: Path) -> None:
    insert_sql_path = tmp_path / "insert.sql"
    insert_sql = "INSERT INTO test (id, name) VALUES (:id, :name)"
    with open(insert_sql_path, "w") as f:
        f.write(insert_sql)
    result = get_insert_statement(insert_sql_path)
    assert result == insert_sql
    assert isinstance(result, str)

def test_store_file_removes_original_creates_target(tmp_path: Path) -> None:
    filename = "test.json"
    source_filepath = tmp_path / "test.json"
    target_folder = tmp_path / "target"
    target_filepath = target_folder / filename
  
    with open(source_filepath, "w") as f:
        f.write("banana")

    result = store_file(filename, source_filepath, target_folder)
    assert result is None
    assert not source_filepath.exists()
    assert target_filepath.exists()

# next: cleanup_pending_lz
def test_cleanup_pending_lz_removes_empty_folders(tmp_path: Path) -> None:
    pending_folder = tmp_path / "pending"
    pending_folder.mkdir()

    empty_child_folder = pending_folder / "2026-01-01"
    empty_child_folder.mkdir()

    non_empty_child_folder = pending_folder / "2026-01-02"
    non_empty_child_folder.mkdir()
    with open(non_empty_child_folder / "test.json", "w") as f:
        f.write("banana")

    result = cleanup_pending_lz(pending_folder)
    assert result is None
    assert not empty_child_folder.exists()
    assert non_empty_child_folder.exists()

def test_get_config_returns_dict(tmp_path: Path) -> None  :
    config_path = tmp_path / "config.yaml"
    config = {"event_name": {"stuff": "things"}}
    with open(config_path, "w") as f:
        json.dump(config, f)
    result = get_config("event_name", config_path)
    assert result == config["event_name"]
    assert isinstance(result, dict)