import logging
import traceback

from db import get_engine, get_insert_statement, insert_row_builder
# from sqlalchemy import text, Engine
# from datetime import datetime, UTC
# from typing import Any, Literal, cast, Tuple
# from uuid import uuid5, NAMESPACE_DNS
# from copy import deepcopy
from config_util import get_config, resolve_config, _REPO_ROOT_PATH
from app_logger import configure_logger # only required if running cleanse.py directly

logger = logging.getLogger(__name__)


