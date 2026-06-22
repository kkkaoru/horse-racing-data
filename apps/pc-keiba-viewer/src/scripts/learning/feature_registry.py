"""DuckDB-backed registry for Walk-Forward feature evaluation results."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, SupportsFloat, SupportsInt, TypedDict, cast

import duckdb

DEFAULT_DB_PATH: Final[Path] = Path("feature_registry.duckdb")
NDCG_IMPROVEMENT_THRESHOLD: Final[float] = 0.005


class FeatureEntry(TypedDict):
    id: int
    trial_id: str
    ndcg_at_3: float
    is_active: bool
    feature_names: list[str]
    definition_json: str
    created_at: str


class FeatureRegistry:
    """Context-manager wrapper around a DuckDB feature trial store."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self._db_path: Path = db_path
        self._con: duckdb.DuckDBPyConnection | None = None

    def open(self) -> None:
        self._con = duckdb.connect(str(self._db_path))
        self._ensure_schema()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> "FeatureRegistry":
        self.open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _ensure_schema(self) -> None:
        assert self._con is not None
        self._con.execute("CREATE SEQUENCE IF NOT EXISTS seq_feature_trials_id START 1")
        self._con.execute("CREATE SEQUENCE IF NOT EXISTS seq_deployments_id START 1")
        self._con.execute("CREATE SEQUENCE IF NOT EXISTS seq_inverse_trials_id START 1")
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS feature_trials (
                id              INTEGER PRIMARY KEY,
                trial_id        TEXT    NOT NULL,
                ndcg_at_3       DOUBLE  NOT NULL,
                is_active       BOOLEAN NOT NULL DEFAULT FALSE,
                feature_names   TEXT    NOT NULL,
                definition_json TEXT    NOT NULL DEFAULT '{}',
                created_at      TEXT    NOT NULL
            )
        """)
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS deployments (
                id            INTEGER PRIMARY KEY,
                ndcg_at_3     DOUBLE  NOT NULL,
                feature_count INTEGER NOT NULL,
                deployed_at   TEXT    NOT NULL
            )
        """)
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS inverse_trials (
                id                INTEGER PRIMARY KEY,
                original_trial_id TEXT    NOT NULL,
                inverse_name      TEXT    NOT NULL,
                approach_type     TEXT    NOT NULL,
                delta_pp_json     TEXT    NOT NULL DEFAULT '{}',
                decision          TEXT    NOT NULL DEFAULT 'PENDING',
                created_at        TEXT    NOT NULL,
                UNIQUE(original_trial_id, inverse_name)
            )
        """)
        # Sync sequences past existing row ids so pre-migration databases don't collide.
        self._sync_sequence_to_table("seq_feature_trials_id", "feature_trials")
        self._sync_sequence_to_table("seq_deployments_id", "deployments")
        self._sync_sequence_to_table("seq_inverse_trials_id", "inverse_trials")

    def _sync_sequence_to_table(self, seq_name: str, table_name: str) -> None:
        assert self._con is not None
        row = self._con.execute(
            f"SELECT COALESCE(MAX(id), 0) FROM {table_name}"
        ).fetchone()
        max_id = int(row[0]) if row else 0
        # DuckDB does not support ALTER SEQUENCE RESTART WITH, so drop and recreate.
        self._con.execute(f"DROP SEQUENCE IF EXISTS {seq_name}")
        self._con.execute(f"CREATE SEQUENCE {seq_name} START {max_id + 1}")

    def _next_id(self, table: str = "feature_trials") -> int:
        assert self._con is not None
        seq = {
            "feature_trials": "seq_feature_trials_id",
            "deployments": "seq_deployments_id",
            "inverse_trials": "seq_inverse_trials_id",
        }[table]
        row = self._con.execute(f"SELECT nextval('{seq}')").fetchone()
        assert row is not None
        return int(row[0])

    def get_best_ndcg(self) -> float:
        assert self._con is not None
        row = self._con.execute("SELECT MAX(ndcg_at_3) FROM feature_trials").fetchone()
        if row is None or row[0] is None:
            return 0.0
        return float(row[0])

    def get_active_entry(self) -> FeatureEntry | None:
        assert self._con is not None
        row = self._con.execute(
            "SELECT id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at "
            "FROM feature_trials WHERE is_active = TRUE ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return _row_to_entry(row)

    def record_trial(
        self,
        trial_id: str,
        ndcg_at_3: float,
        feature_names: list[str],
        definition_json: str = "{}",
    ) -> int:
        assert self._con is not None
        entry_id = self._next_id()
        self._con.execute(
            "INSERT INTO feature_trials "
            "(id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at) "
            "VALUES (?, ?, ?, FALSE, ?, ?, ?)",
            [
                entry_id,
                trial_id,
                ndcg_at_3,
                json.dumps(feature_names),
                definition_json,
                datetime.now(timezone.utc).isoformat(),
            ],
        )
        return entry_id

    def activate(self, entry_id: int) -> None:
        assert self._con is not None
        self._con.execute(
            "UPDATE feature_trials SET is_active = (id = ?)", [entry_id]
        )

    def maybe_promote(
        self,
        trial_id: str,
        ndcg_at_3: float,
        feature_names: list[str],
        definition_json: str = "{}",
        threshold: float = NDCG_IMPROVEMENT_THRESHOLD,
    ) -> bool:
        assert self._con is not None
        self._con.begin()
        try:
            active_entry = self.get_active_entry()
            active_ndcg = active_entry["ndcg_at_3"] if active_entry is not None else 0.0
            entry_id = self.record_trial(trial_id, ndcg_at_3, feature_names, definition_json)
            promoted = ndcg_at_3 > active_ndcg + threshold
            if promoted:
                self.activate(entry_id)
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise
        return promoted

    def _next_deployment_id(self) -> int:
        return self._next_id("deployments")

    def get_deployed_ndcg(self) -> float:
        assert self._con is not None
        row = self._con.execute(
            "SELECT ndcg_at_3 FROM deployments ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None or row[0] is None:
            return 0.0
        return float(row[0])

    def record_deployment(self, ndcg_at_3: float, feature_count: int) -> None:
        assert self._con is not None
        entry_id = self._next_deployment_id()
        self._con.execute(
            "INSERT INTO deployments (id, ndcg_at_3, feature_count, deployed_at) VALUES (?, ?, ?, ?)",
            [
                entry_id,
                ndcg_at_3,
                feature_count,
                datetime.now(timezone.utc).isoformat(),
            ],
        )

    def list_trials(self, limit: int = 20) -> list[FeatureEntry]:
        assert self._con is not None
        rows = self._con.execute(
            "SELECT id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at "
            "FROM feature_trials ORDER BY ndcg_at_3 DESC LIMIT ?",
            [limit],
        ).fetchall()
        return [_row_to_entry(row) for row in rows]

    def record_inverse_trial(
        self,
        original_trial_id: str,
        inverse_name: str,
        approach_type: str,
        delta_pp: dict[str, float],
        decision: str,
    ) -> int:
        assert self._con is not None
        entry_id = self._next_id("inverse_trials")
        self._con.execute(
            "INSERT INTO inverse_trials "
            "(id, original_trial_id, inverse_name, approach_type, delta_pp_json, decision, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                entry_id,
                original_trial_id,
                inverse_name,
                approach_type,
                json.dumps(delta_pp),
                decision,
                datetime.now(timezone.utc).isoformat(),
            ],
        )
        return entry_id

    def has_inverse_been_tried(self, original_trial_id: str, inverse_name: str) -> bool:
        assert self._con is not None
        row = self._con.execute(
            "SELECT COUNT(*) FROM inverse_trials "
            "WHERE original_trial_id = ? AND inverse_name = ?",
            [original_trial_id, inverse_name],
        ).fetchone()
        return row is not None and int(row[0]) > 0

    def list_strongly_negative_trials(
        self, threshold_pp: float = -1.0
    ) -> list[FeatureEntry]:
        assert self._con is not None
        rows = self._con.execute(
            "SELECT id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at "
            "FROM feature_trials ORDER BY id"
        ).fetchall()
        return [
            entry
            for entry in (_row_to_entry(row) for row in rows)
            if _min_delta_pp(entry["definition_json"]) <= threshold_pp
        ]

    def list_untried_inverses(self, original_trial_id: str) -> list[str]:
        assert self._con is not None
        rows = self._con.execute(
            "SELECT approach_type FROM inverse_trials WHERE original_trial_id = ?",
            [original_trial_id],
        ).fetchall()
        tried = {str(row[0]) for row in rows}
        return [approach for approach in INVERSE_APPROACH_TYPES if approach not in tried]


INVERSE_APPROACH_TYPES: Final[tuple[str, ...]] = (
    "feature_negate",
    "weight_invert",
    "window_invert",
    "anti_correlation",
)


def _min_delta_pp(definition_json: str) -> float:
    payload = json.loads(definition_json)
    if not isinstance(payload, dict):
        return 0.0
    raw = payload.get("delta_pp")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw)
    if isinstance(raw, dict):
        values = [float(v) for v in raw.values() if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return min(values) if values else 0.0
    return 0.0


def _row_to_entry(row: tuple[object, ...]) -> FeatureEntry:
    return FeatureEntry(
        id=int(cast(SupportsInt, row[0])),
        trial_id=str(row[1]),
        ndcg_at_3=float(cast(SupportsFloat, row[2])),
        is_active=bool(row[3]),
        feature_names=json.loads(str(row[4])),
        definition_json=str(row[5]),
        created_at=str(row[6]),
    )
