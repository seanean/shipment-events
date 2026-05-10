from ingest_raw import (
    validate_schema,
    validate_file,
    get_schema,
    get_file, 
    store_file,
    cleanup_pending_lz
)
from db import get_insert_statement, insert_row_builder
from pathlib import Path
from jsonschema import Draft202012Validator, SchemaError, ValidationError
import pytest
from psycopg.types.json import Jsonb
import json


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
    event_type = 'shipment_status'
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
                        "event_tmst": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "event_type": event_type,
                        "meta_source_file_path": meta_source_filepath}
        case "raw.shipment_products":
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_tmst": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "event_type": event_type,
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_status":
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_tmst": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "event_type": event_type,
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}
        case "quarantine.shipment_products":
            expected_result = {"payload": Jsonb(file), "event_id": file["event_id"],
                        "event_tmst": file["event_timestamp"],
                        "event_name": file["event_name"],
                        "event_type": event_type,
                        "error_message": error_message,
                        "traceback_message": traceback_message,
                        "meta_source_file_path": meta_source_filepath}

    result = insert_row_builder(event_type, target_table, file, 
                                meta_source_filepath, error_message, traceback_message)
    
    # for each key in expected_result, check if result[key] == expected_result[key]
    for key in expected_result:
        if key == "payload":
            a = result[key]
            b = expected_result[key]
            assert isinstance(a, Jsonb)
            assert isinstance(b, Jsonb)
            assert a.obj == b.obj
        else:
            assert result[key] == expected_result[key]

def test_insert_row_builder_raises_valueerror_for_unknown_table() -> None:
    data = 'shipment_status'
    target_table = "unknown"
    meta_source_filepath = "test.json"
    error_message = "error"
    traceback_message = "traceback"
    file = {
        "event_id": "123",
        "event_tmst": "2026-01-01T00:00:00Z",
        "event_name": "shipment_status"
    }
    with pytest.raises(ValueError):
        insert_row_builder(data, target_table, file, meta_source_filepath, error_message, traceback_message)


def test_get_schema_returns_dict(tmp_path: Path) -> None:
    schema_path = tmp_path / "schema.json"
    schema = {"type": "string"}
    with open(schema_path, "w") as f:
        json.dump(schema, f)
    result = get_schema(schema_path)
    assert result == schema
    assert isinstance(result, dict)

def test_get_file_returns_dict(tmp_path: Path) -> None:
    file_path = str(tmp_path / "file.json")
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

    store_file(filename, str(source_filepath), str(target_folder))
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

    cleanup_pending_lz(pending_folder)
    assert not empty_child_folder.exists()
    assert non_empty_child_folder.exists()
