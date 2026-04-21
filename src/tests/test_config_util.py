from config_util import get_config, resolve_config, _REPO_ROOT_PATH
from pathlib import Path
import json
from typing import Any

# resolve_config tests
def test_resolve_config() -> None:
    loaded_config: dict[str, Any] = {
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
        'cln_insert_sql_path': 'db/sql/cln/cln_shipment_status_insert.sql',
        'curated_db_schema': 'cur',
        'curated_tables': [
            {
                'cur_table': 'shipment_header',
                'cur_insert_sql_path': 'db/sql/cur/shipment_status__cur_shipment_header_insert.sql',
            },
            {
                'cur_table': 'shipment_status',
                'cur_insert_sql_path': 'db/sql/cur/cur_shipment_status_insert.sql',
            },
        ]
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
    assert isinstance(result.curated_tables, list)
    assert all(isinstance(table, dict) for table in result.curated_tables)
    assert all(isinstance(table['cur_target_table'], str) for table in result.curated_tables)
    assert all(isinstance(table['cur_insert_sql_path'], Path) for table in result.curated_tables)
    # path checks
    assert result.lz_pending_path == _REPO_ROOT_PATH.joinpath(loaded_config['lz_pending_path'])
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
    assert result.curated_tables == [
        {
            'cur_target_table': f'{loaded_config['curated_db_schema']}.{row['cur_table']}',
            'cur_insert_sql_path': _REPO_ROOT_PATH / row['cur_insert_sql_path'],
        }
        for row in loaded_config['curated_tables']
    ]


def test_get_config_returns_dict(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config = {"event_name": {"stuff": "things"}}
    with open(config_path, "w") as f:
        json.dump(config, f)
    result = get_config("event_name", config_path)
    assert result == config["event_name"]
    assert isinstance(result, dict)