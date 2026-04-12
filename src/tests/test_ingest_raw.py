from ingest_raw import resolve_config, _REPO_ROOT_PATH
from pathlib import Path

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