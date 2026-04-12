#!/usr/bin/env bash
set -euo pipefail

DDL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/init-ddl"

for f in $(printf '%s\n' "$DDL_DIR"/*.sql | sort); do
  [[ -f "$f" ]] || continue
  PGPASSWORD="$POSTGRES_RW_PW" psql -v ON_ERROR_STOP=1 \
    --username shrw --dbname "$POSTGRES_DB" \
    --file="$f"
done