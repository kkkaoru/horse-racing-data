"""Persistent trial registry for multi-dimensional evaluation experiments.

Stores per-cell (category x class x subgroup x season) trial results in a
DuckDB file, supports deduplication by feature-set hash, and offers accuracy
vector search via DuckDB's built-in ``list_cosine_similarity``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Final, TypedDict

import duckdb

TRIAL_REGISTRY_DIR: Final[str] = "."
DUCKDB_MEMORY_LIMIT: Final[str] = "6GB"
DUCKDB_THREADS: Final[int] = 4
FEATURE_HASH_LENGTH: Final[int] = 16
ACCURACY_VECTOR_LENGTH: Final[int] = 6


class CellKey(TypedDict):
    class_code: str
    subgroup_dimension: str
    subgroup_value: str
    season_band: str


class SimilarTrial(TypedDict):
    trial_id: str
    class_code: str
    subgroup_value: str
    season_band: str
    feature_set_hash: str
    rank1_accuracy: float | None
    rank2_accuracy: float | None
    rank3_accuracy: float | None
    rank4_accuracy: float | None
    rank5_accuracy: float | None
    rank6_accuracy: float | None
    verdict: str | None
    similarity: float


class TrialRow(TypedDict):
    trial_id: str
    category: str
    class_code: str
    subgroup_dimension: str
    subgroup_value: str
    season_band: str
    feature_set_hash: str
    feature_count: int
    verdict: str | None


def _registry_path(category: str, base_dir: str = TRIAL_REGISTRY_DIR) -> str:
    return str(Path(base_dir) / f"trial_registry_{category}.duckdb")


def compute_feature_set_hash(feature_names: list[str]) -> str:
    """SHA256 hash (first 16 hex chars) of sorted feature names for dedup."""
    canonical = json.dumps(
        sorted(feature_names), ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:FEATURE_HASH_LENGTH]


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create the trials table and supporting indexes if absent (idempotent)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS trials (
            trial_id           VARCHAR PRIMARY KEY,
            category           VARCHAR NOT NULL,
            class_code         VARCHAR NOT NULL,
            subgroup_dimension VARCHAR NOT NULL,
            subgroup_value     VARCHAR NOT NULL,
            season_band        VARCHAR NOT NULL,
            feature_set_hash   VARCHAR NOT NULL,
            feature_names      VARCHAR NOT NULL,
            feature_count      INTEGER NOT NULL,
            focus_features     VARCHAR,
            exclude_features   VARCHAR,
            rank1_accuracy     DOUBLE,
            rank2_accuracy     DOUBLE,
            rank3_accuracy     DOUBLE,
            rank4_accuracy     DOUBLE,
            rank5_accuracy     DOUBLE,
            rank6_accuracy     DOUBLE,
            top1_accuracy      DOUBLE,
            place2_accuracy    DOUBLE,
            place3_accuracy    DOUBLE,
            ndcg_at_3          DOUBLE,
            race_count         INTEGER,
            rank1_lb95         DOUBLE,
            rank2_lb95         DOUBLE,
            rank3_lb95         DOUBLE,
            rank4_lb95         DOUBLE,
            rank5_lb95         DOUBLE,
            rank6_lb95         DOUBLE,
            verdict            VARCHAR,
            verdict_reason     VARCHAR,
            model_version      VARCHAR,
            train_window_start VARCHAR,
            train_window_end   VARCHAR,
            blind_year         INTEGER,
            created_at         VARCHAR NOT NULL
                               DEFAULT (strftime(now(), '%Y-%m-%dT%H:%M:%S'))
        )
    """)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_trials_cell "
        "ON trials (category, class_code, subgroup_value, season_band)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_trials_feature_hash "
        "ON trials (category, feature_set_hash)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_trials_dedup "
        "ON trials (category, class_code, subgroup_dimension, "
        "subgroup_value, season_band, feature_set_hash)"
    )


def trial_exists(
    con: duckdb.DuckDBPyConnection,
    category: str,
    class_code: str,
    subgroup_dimension: str,
    subgroup_value: str,
    season_band: str,
    feature_set_hash: str,
) -> bool:
    """Return True if this exact cell x feature combination already has a trial."""
    result = con.execute(
        "SELECT 1 FROM trials WHERE category = ? AND class_code = ? "
        "AND subgroup_dimension = ? AND subgroup_value = ? "
        "AND season_band = ? AND feature_set_hash = ? LIMIT 1",
        [
            category,
            class_code,
            subgroup_dimension,
            subgroup_value,
            season_band,
            feature_set_hash,
        ],
    ).fetchone()
    return result is not None


def find_duplicate_cells(
    con: duckdb.DuckDBPyConnection,
    category: str,
    feature_set_hash: str,
    cells: list[CellKey],
) -> list[CellKey]:
    """Return the subset of cells already covered by a trial with this hash."""
    return [
        cell
        for cell in cells
        if trial_exists(
            con,
            category,
            cell["class_code"],
            cell["subgroup_dimension"],
            cell["subgroup_value"],
            cell["season_band"],
            feature_set_hash,
        )
    ]


def get_untested_cells(
    con: duckdb.DuckDBPyConnection,
    category: str,
    feature_set_hash: str,
    all_cells: list[CellKey],
) -> list[CellKey]:
    """Return cells from all_cells not yet tested with this feature set."""
    return [
        cell
        for cell in all_cells
        if not trial_exists(
            con,
            category,
            cell["class_code"],
            cell["subgroup_dimension"],
            cell["subgroup_value"],
            cell["season_band"],
            feature_set_hash,
        )
    ]


def register_trial(
    con: duckdb.DuckDBPyConnection,
    trial_id: str,
    category: str,
    class_code: str,
    subgroup_dimension: str,
    subgroup_value: str,
    season_band: str,
    feature_names: list[str],
    accuracies: dict[str, float],
    verdict: str = "PENDING",
    verdict_reason: str = "",
    model_version: str = "",
    train_window_start: str = "",
    train_window_end: str = "",
    blind_year: int | None = None,
    focus_features: list[str] | None = None,
    exclude_features: list[str] | None = None,
) -> None:
    """Insert one trial result, raising if the cell x feature combo already exists."""
    feature_set_hash = compute_feature_set_hash(feature_names)
    if trial_exists(
        con,
        category,
        class_code,
        subgroup_dimension,
        subgroup_value,
        season_band,
        feature_set_hash,
    ):
        raise ValueError(
            f"duplicate trial for cell ({category}, {class_code}, "
            f"{subgroup_dimension}, {subgroup_value}, {season_band}) "
            f"with feature hash {feature_set_hash}"
        )
    con.execute(
        "INSERT INTO trials ("
        "trial_id, category, class_code, subgroup_dimension, subgroup_value, "
        "season_band, feature_set_hash, feature_names, feature_count, "
        "focus_features, exclude_features, "
        "rank1_accuracy, rank2_accuracy, rank3_accuracy, "
        "rank4_accuracy, rank5_accuracy, rank6_accuracy, "
        "top1_accuracy, place2_accuracy, place3_accuracy, ndcg_at_3, race_count, "
        "rank1_lb95, rank2_lb95, rank3_lb95, rank4_lb95, rank5_lb95, rank6_lb95, "
        "verdict, verdict_reason, model_version, "
        "train_window_start, train_window_end, blind_year"
        ") VALUES ("
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?, ?)",
        [
            trial_id,
            category,
            class_code,
            subgroup_dimension,
            subgroup_value,
            season_band,
            feature_set_hash,
            json.dumps(feature_names, ensure_ascii=False),
            len(feature_names),
            json.dumps(focus_features, ensure_ascii=False)
            if focus_features is not None
            else None,
            json.dumps(exclude_features, ensure_ascii=False)
            if exclude_features is not None
            else None,
            accuracies.get("rank1_accuracy"),
            accuracies.get("rank2_accuracy"),
            accuracies.get("rank3_accuracy"),
            accuracies.get("rank4_accuracy"),
            accuracies.get("rank5_accuracy"),
            accuracies.get("rank6_accuracy"),
            accuracies.get("top1_accuracy"),
            accuracies.get("place2_accuracy"),
            accuracies.get("place3_accuracy"),
            accuracies.get("ndcg_at_3"),
            accuracies.get("race_count"),
            accuracies.get("rank1_lb95"),
            accuracies.get("rank2_lb95"),
            accuracies.get("rank3_lb95"),
            accuracies.get("rank4_lb95"),
            accuracies.get("rank5_lb95"),
            accuracies.get("rank6_lb95"),
            verdict,
            verdict_reason,
            model_version,
            train_window_start,
            train_window_end,
            blind_year,
        ],
    )


def search_similar_accuracy(
    con: duckdb.DuckDBPyConnection,
    category: str,
    target_vector: list[float],
    top_k: int = 10,
) -> list[SimilarTrial]:
    """Find trials with the most similar rank1-6 accuracy vectors.

    Uses DuckDB's ``list_cosine_similarity`` on the 6-element accuracy vector.
    Only trials with a non-NULL rank1_accuracy are considered.
    """
    rows = con.execute(
        "SELECT trial_id, class_code, subgroup_value, season_band, "
        "feature_set_hash, rank1_accuracy, rank2_accuracy, rank3_accuracy, "
        "rank4_accuracy, rank5_accuracy, rank6_accuracy, verdict, "
        "list_cosine_similarity("
        "[rank1_accuracy, rank2_accuracy, rank3_accuracy, "
        "rank4_accuracy, rank5_accuracy, rank6_accuracy], ?::DOUBLE[]"
        ") AS similarity "
        "FROM trials WHERE category = ? AND rank1_accuracy IS NOT NULL "
        "ORDER BY similarity DESC LIMIT ?",
        [target_vector, category, top_k],
    ).fetchall()
    return [_row_to_similar(row) for row in rows]


def search_by_cell(
    con: duckdb.DuckDBPyConnection,
    category: str,
    class_code: str | None = None,
    subgroup_value: str | None = None,
    season_band: str | None = None,
    verdict: str | None = None,
) -> list[TrialRow]:
    """Search trials by cell dimensions; any None dimension is a wildcard."""
    clauses = ["category = ?"]
    params: list[str] = [category]
    for column, value in (
        ("class_code", class_code),
        ("subgroup_value", subgroup_value),
        ("season_band", season_band),
        ("verdict", verdict),
    ):
        if value is not None:
            clauses.append(f"{column} = ?")
            params.append(value)
    rows = con.execute(
        "SELECT trial_id, category, class_code, subgroup_dimension, "
        "subgroup_value, season_band, feature_set_hash, feature_count, verdict "
        f"FROM trials WHERE {' AND '.join(clauses)} ORDER BY trial_id",
        params,
    ).fetchall()
    return [_row_to_trial(row) for row in rows]


def connect(
    category: str,
    base_dir: str = TRIAL_REGISTRY_DIR,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Open a registry connection, ensuring schema exists when writable."""
    con = duckdb.connect(_registry_path(category, base_dir), read_only=read_only)
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    if not read_only:
        ensure_schema(con)
    return con


def _opt_float(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


def _row_to_similar(row: tuple[object, ...]) -> SimilarTrial:
    return SimilarTrial(
        trial_id=str(row[0]),
        class_code=str(row[1]),
        subgroup_value=str(row[2]),
        season_band=str(row[3]),
        feature_set_hash=str(row[4]),
        rank1_accuracy=_opt_float(row[5]),
        rank2_accuracy=_opt_float(row[6]),
        rank3_accuracy=_opt_float(row[7]),
        rank4_accuracy=_opt_float(row[8]),
        rank5_accuracy=_opt_float(row[9]),
        rank6_accuracy=_opt_float(row[10]),
        verdict=str(row[11]) if row[11] is not None else None,
        similarity=float(row[12]) if isinstance(row[12], (int, float)) else 0.0,
    )


def _row_to_trial(row: tuple[object, ...]) -> TrialRow:
    return TrialRow(
        trial_id=str(row[0]),
        category=str(row[1]),
        class_code=str(row[2]),
        subgroup_dimension=str(row[3]),
        subgroup_value=str(row[4]),
        season_band=str(row[5]),
        feature_set_hash=str(row[6]),
        feature_count=int(row[7]) if isinstance(row[7], (int, float)) else 0,
        verdict=str(row[8]) if row[8] is not None else None,
    )
