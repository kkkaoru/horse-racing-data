"""DuckDB-backed registry for Walk-Forward feature evaluation results."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, SupportsFloat, SupportsInt, TypedDict, cast

import duckdb

DEFAULT_DB_PATH: Final[Path] = Path("feature_registry.duckdb")
NDCG_IMPROVEMENT_THRESHOLD: Final[float] = 0.005
ENRICHMENT_SCORE_THRESHOLD: Final[float] = 0.3


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

    _SEQUENCES: Final[dict[str, str]] = {
        "feature_trials": "seq_feature_trials_id",
        "deployments": "seq_deployments_id",
        "inverse_trials": "seq_inverse_trials_id",
    }

    def _next_id(self, table: str = "feature_trials") -> int:
        assert self._con is not None
        seq = self._SEQUENCES[table]
        row = self._con.execute(f"SELECT nextval('{seq}')").fetchone()
        assert row is not None
        return int(row[0])

    def _next_ids(self, count: int, table: str = "feature_trials") -> list[int]:
        """Reserve ``count`` consecutive sequence ids in a single round-trip.

        ``bulk_record_trials`` would otherwise issue one ``nextval`` SELECT per row;
        generating the ids with a single ``range``-driven query collapses that N+1
        into one call while preserving ``nextval``'s monotonic allocation (the ids
        are identical to ``count`` successive :meth:`_next_id` calls).
        """
        assert self._con is not None
        seq = self._SEQUENCES[table]
        rows = self._con.execute(
            f"SELECT nextval('{seq}') FROM range(?)", [count]
        ).fetchall()
        return [int(row[0]) for row in rows]

    def get_best_ndcg(self) -> float:
        assert self._con is not None
        row = self._con.execute("SELECT MAX(ndcg_at_3) FROM feature_trials").fetchone()
        if row is None or row[0] is None:
            return 0.0
        return float(row[0])

    def get_best_ndcg_for_study(self, study_name: str) -> float | None:
        """Return the best ndcg_at_3 among trials produced by one study, or None.

        ``run_exploration`` records each trial as ``{study_name}_trial_{n}``, so a
        prefix match isolates a single study's trials from the global pool. This lets
        the caller measure the gain of one specific run instead of the all-time best.
        """
        assert self._con is not None
        row = self._con.execute(
            "SELECT MAX(ndcg_at_3) FROM feature_trials WHERE starts_with(trial_id, ?)",
            [f"{study_name}_trial_"],
        ).fetchone()
        if row is None or row[0] is None:
            return None
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

    def is_saturated(self, lookback: int = 50) -> bool:
        """True when none of the last `lookback` trials beat the active entry."""
        assert self._con is not None
        active = self.get_active_entry()
        if active is None:
            return False
        active_ndcg = active["ndcg_at_3"]
        row = self._con.execute(
            "SELECT COUNT(*) FROM (SELECT ndcg_at_3 FROM feature_trials ORDER BY id DESC LIMIT ?) "
            "WHERE ndcg_at_3 > ?",
            [lookback, active_ndcg],
        ).fetchone()
        return row is not None and int(row[0]) == 0

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

    def bulk_record_trials(self, trials: list[tuple[str, float, str, str]]) -> None:
        """Insert many non-promoting trial rows in a single ``executemany``.

        Each tuple is ``(trial_id, ndcg_at_3, feature_names_json, definition_json)``
        where ``feature_names_json`` is already a JSON string of the feature-name list
        (matching how :meth:`record_trial` stores ``feature_names``). Every row is
        assigned a fresh sequence id, ``is_active = FALSE``, and a fresh UTC
        ``created_at`` — byte-identical to what per-trial :meth:`record_trial` would
        have written, only deferred to one bulk flush. An empty list is a no-op so the
        DB is never touched when a round promotes every scored trial. The whole batch
        runs in one begin/commit with rollback-and-raise on failure, mirroring
        :meth:`maybe_promote`. Because these rows are never activated, a later flush
        cannot change which row :meth:`get_active_entry` returns (it selects on
        ``is_active = TRUE``, and only promoting trials set that flag).
        """
        assert self._con is not None
        if not trials:
            return
        created_at = datetime.now(timezone.utc).isoformat()
        ids = self._next_ids(len(trials))
        rows = [
            [
                entry_id,
                trial_id,
                ndcg_at_3,
                feature_names_json,
                definition_json,
                created_at,
            ]
            for entry_id, (trial_id, ndcg_at_3, feature_names_json, definition_json) in zip(
                ids, trials, strict=True
            )
        ]
        self._con.begin()
        try:
            self._con.executemany(
                "INSERT INTO feature_trials "
                "(id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at) "
                "VALUES (?, ?, ?, FALSE, ?, ?, ?)",
                rows,
            )
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise

    def activate(self, entry_id: int) -> None:
        assert self._con is not None
        self._con.execute(
            "UPDATE feature_trials SET is_active = (id = ?)", [entry_id]
        )

    def _current_active_ndcg(self) -> float:
        """NDCG of the active entry, or 0.0 when none is active."""
        active_entry = self.get_active_entry()
        return active_entry["ndcg_at_3"] if active_entry is not None else 0.0

    def maybe_promote(
        self,
        trial_id: str,
        ndcg_at_3: float,
        feature_names: list[str],
        definition_json: str = "{}",
        threshold: float = NDCG_IMPROVEMENT_THRESHOLD,
        active_ndcg: float | None = None,
    ) -> bool:
        """Record the trial and activate it when it beats the active entry by ``threshold``.

        Callers that already hold the active NDCG (e.g. the per-trial Optuna objective,
        which reads it to compute ``delta_pp``) can pass it as ``active_ndcg`` to skip the
        redundant ``get_active_entry()`` SELECT this method would otherwise run. Passing
        ``None`` queries it here, preserving the original single-argument behaviour. The
        value is read with no intervening writes from this connection, so a caller-supplied
        ``active_ndcg`` is identical to the one this method would fetch.
        """
        assert self._con is not None
        self._con.begin()
        try:
            resolved_active_ndcg = (
                self._current_active_ndcg() if active_ndcg is None else active_ndcg
            )
            entry_id = self.record_trial(trial_id, ndcg_at_3, feature_names, definition_json)
            promoted = ndcg_at_3 > resolved_active_ndcg + threshold
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
        """Trials whose ``_min_delta_pp(definition_json) <= threshold_pp``.

        Called once per learning round, so the table is not fully materialized:
        a DuckDB JSON pre-filter (:func:`_STRONGLY_NEGATIVE_SQL`) drops rows whose
        scalar ``delta_pp`` is unambiguously above ``threshold_pp`` and rows that
        evaluate to ``0.0`` when ``threshold_pp`` is below zero. The pre-filter is a
        SUPERSET of the qualifying rows (dict-deltas and every ambiguous case fall
        through), so the exact :func:`_min_delta_pp` check below stays authoritative
        and the output is byte-identical to scanning the whole table.
        """
        assert self._con is not None
        rows = self._con.execute(
            _STRONGLY_NEGATIVE_SQL, [threshold_pp, threshold_pp]
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

    def compute_feature_enrichment(
        self, top_k: int = 20, bottom_k: int = 20
    ) -> list[tuple[str, float]]:
        """Return features sorted by (top_k_freq - bottom_k_freq) enrichment score.

        Returns list of (feature_name, enrichment_score) where score is
        (fraction in top_k) - (fraction in bottom_k), range [-1.0, 1.0].
        Only returns features with |score| >= ENRICHMENT_SCORE_THRESHOLD.
        """
        assert self._con is not None
        rows = self._con.execute(
            "SELECT feature_names FROM feature_trials ORDER BY ndcg_at_3 DESC"
        ).fetchall()
        if not rows:
            return []
        feature_lists = [_feature_names_from_row(row) for row in rows]
        top_lists = feature_lists[:top_k]
        bottom_lists = feature_lists[-bottom_k:]
        top_counts = _count_distinct_features(top_lists)
        bottom_counts = _count_distinct_features(bottom_lists)
        top_denom = float(len(top_lists))
        bottom_denom = float(len(bottom_lists))
        scored = [
            (
                name,
                top_counts.get(name, 0) / top_denom
                - bottom_counts.get(name, 0) / bottom_denom,
            )
            for name in top_counts.keys() | bottom_counts.keys()
        ]
        filtered = [
            (name, score)
            for name, score in scored
            if abs(score) >= ENRICHMENT_SCORE_THRESHOLD
        ]
        return sorted(filtered, key=lambda pair: pair[1], reverse=True)


INVERSE_APPROACH_TYPES: Final[tuple[str, ...]] = (
    "feature_negate",
    "weight_invert",
    "window_invert",
    "anti_correlation",
)

# Pre-filter for ``list_strongly_negative_trials``. Keeps a SUPERSET of the rows
# that ``_min_delta_pp`` would qualify, so the Python check stays authoritative:
#   - ``$.delta_pp`` is a JSON object -> dict path, MIN decided in Python.
#   - ``$.delta_pp`` is a non-bool scalar number <= threshold -> scalar path.
#   - every other shape evaluates to 0.0 in Python and is kept iff 0.0 <= threshold.
# Both ``?`` placeholders bind ``threshold_pp``; ``ORDER BY id`` preserves order.
_STRONGLY_NEGATIVE_SQL: Final[str] = (
    "SELECT id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at "
    "FROM feature_trials "
    "WHERE json_type(json_extract(definition_json, '$.delta_pp')) = 'OBJECT' "
    "OR ("
    "json_type(json_extract(definition_json, '$.delta_pp')) <> 'BOOLEAN' "
    "AND TRY_CAST(json_extract(definition_json, '$.delta_pp') AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(json_extract(definition_json, '$.delta_pp') AS DOUBLE) <= ?"
    ") "
    "OR (0.0 <= ?) "
    "ORDER BY id"
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


def _feature_names_from_row(row: tuple[object, ...]) -> list[str]:
    parsed = json.loads(str(row[0]))
    return [str(name) for name in parsed] if isinstance(parsed, list) else []


def _count_distinct_features(feature_lists: list[list[str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for names in feature_lists:
        for name in set(names):
            counts[name] = counts.get(name, 0) + 1
    return counts


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
