#!/usr/bin/env bash
# Reuse production raw-table SQL against authorized local PG; inference audit only.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
: "${PG_URL:?Set the authorized local PostgreSQL URL}"
case "$PG_URL" in
  postgresql://*@192.168.64.2:5432/horse_racing|postgresql://*@localhost:5432/horse_racing) ;;
  *) echo 'Only local PostgreSQL is allowed' >&2; exit 1 ;;
esac
export PYTHONDONTWRITEBYTECODE=1 PIPELINE_MAX_MEMORY_GB=4 PIPELINE_MAX_THREADS=2
export PIPELINE_SPILL_TEMP_DIR="$ROOT/production-upcoming-raw-sim-v1/spill"
uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python - "$ROOT" "$REPO" "$PG_URL" <<'PY'
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import duckdb

root, repo = Path(sys.argv[1]), Path(sys.argv[2])
layer_dir = repo / 'apps/pc-keiba-viewer/src/scripts/finish-position-features'
source_path = layer_dir / 'add-similar-race-features.py'
sys.path.insert(0, str(layer_dir))
spec = importlib.util.spec_from_file_location('research_similarity_layer', source_path)
if spec is None or spec.loader is None:
    raise RuntimeError('Cannot load the existing production similarity implementation')
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
input_glob = str(root / 'production-upcoming-features/83/grade-career/race_year=2026/*.parquet')
output = root / 'production-upcoming-raw-sim-v1/83/final'
if output.exists():
    raise FileExistsError('Do not overwrite a completed inference audit')
with duckdb.connect(':memory:') as connection:
    connection.execute("set threads=2; set memory_limit='4GB'; set preserve_insertion_order=false")
    module.install_and_attach_pg(connection, sys.argv[3])
    counts = connection.execute("""
        select 'raw_nvd_se' as source, count(*) as rows
        from pg.nvd_se where kaisai_nen || kaisai_tsukihi = '20260912' and keibajo_code = '83'
        union all
        select 'race_entry_corner_features', count(*)
        from pg.race_entry_corner_features where race_date = '20260912' and keibajo_code = '83'
    """).fetchall()
    module.stage_target_similarity_scope(connection, input_glob)
    scopes = module.fetch_target_similarity_scopes(connection)
    module.stage_similar_history(connection, '20100101', 'ban-ei', focused_target=True, target_scopes=scopes)
    module.stage_race_summary(connection)
    module.stage_target_races(connection, input_glob, 'ban-ei')
    module.stage_target_match_level(connection)
    module.stage_similar_pool(connection)
    module.stage_race_level_features(connection)
    module.stage_entity_features(connection, 'ban-ei')
    keys = module.fetch_target_race_keys(connection)
    module.stage_target_entities(connection, '20100101', 'ban-ei', focused_target=True, target_keys=keys)
    target_rows = connection.execute('select count(*) from target_entities').fetchone()
    output.mkdir(parents=True)
    connection.execute(f"copy ({module.append_features_sql(input_glob)}) to '{output}' (format parquet, partition_by(race_year))")
report = {
    'purpose': 'Native raw-source inference replay only; not new training or production writes',
    'source': 'authorized local PostgreSQL raw tables',
    'production_implementation_sha256': hashlib.sha256(source_path.read_bytes()).hexdigest(),
    'target_source_counts': counts,
    'target_entity_rows': None if target_rows is None else target_rows[0],
    'feature_output': str(output.relative_to(root)),
    'feature_math_changed': False,
    'new_training': False,
    'production_changed': False,
}
(root / 'audit-raw-similarity-source.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report, indent=2))
PY
