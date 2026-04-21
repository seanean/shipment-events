import yaml
from pathlib import Path
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_REPO_ROOT_PATH = Path(__file__).resolve().parent.parent

# data class for resolved config makes typing easier
@dataclass(frozen=True)
class ResolvedConfig:
    lz_pending_path: Path
    lz_archive_path: Path
    lz_quarantine_path: Path
    schema_path: Path
    raw_insert_sql_path: Path
    quarantine_insert_sql_path: Path
    raw_target_table: str
    quarantine_target_table: str
    cln_insert_sql_path: Path
    cln_target_table: str
    curated_tables: list[dict[str, str | Path]]

def get_config(data: str, config_path: Path) -> dict:
    logger.info(f"Getting config for {data} from {config_path}")
    with open(config_path) as stream:
        config: dict = yaml.safe_load(stream)
        logger.info(f"Config retrieved successfully")
        logger.debug(f"Config for {data}: {config[data]}")
    return config[data]

def resolve_config(loaded_config: dict[str, Any]) -> ResolvedConfig:
    logger.info(f"Resolving config for loaded_config")
    return ResolvedConfig(
        lz_pending_path=_REPO_ROOT_PATH / loaded_config['lz_pending_path'],
        lz_archive_path=_REPO_ROOT_PATH / loaded_config['lz_archive_path'],
        lz_quarantine_path=_REPO_ROOT_PATH / loaded_config['lz_quarantine_path'],
        schema_path=_REPO_ROOT_PATH / loaded_config['schema_path'],
        raw_insert_sql_path=_REPO_ROOT_PATH / loaded_config['raw_insert_sql_path'],
        quarantine_insert_sql_path=_REPO_ROOT_PATH / loaded_config['quarantine_insert_sql_path'],
        raw_target_table=f'{loaded_config['raw_db_schema']}.{loaded_config['raw_table']}',
        quarantine_target_table=f'{loaded_config['quarantine_db_schema']}.{loaded_config['quarantine_table']}',
        cln_target_table=f'{loaded_config['cln_db_schema']}.{loaded_config['cln_table']}',
        cln_insert_sql_path=_REPO_ROOT_PATH / loaded_config['cln_insert_sql_path'],
        # if at some point it's needed, we can also resolve the raw_table and raw_db_schema parameters
        curated_tables = [
            {
                'table': table['table'],
                'insert_sql_path': _REPO_ROOT_PATH / table['insert_sql_path']
            }
            for table in loaded_config['curated_tables']
        ]
    )
        