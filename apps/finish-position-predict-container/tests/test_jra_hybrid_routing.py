from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import cast, final
from unittest.mock import patch

import predict_upcoming
from predict_lib.jra_hybrid_scorer import JraHybridScorer
from predict_lib.scorer import BoosterLike

_SCORE_ATTR = "_score_one_race_jra_dirt_hybrid"
_LOAD_ATTR = "_load_jra_dirt_hybrid"
_score_jra_hybrid = cast(Callable[..., list[list[object]]], getattr(predict_upcoming, _SCORE_ATTR))
_load_jra_hybrid = cast(
    Callable[[Path, Sequence[str]], JraHybridScorer | None],
    getattr(predict_upcoming, _LOAD_ATTR),
)


@final
class _Booster:
    def predict(self, matrix: Sequence[Sequence[float]]) -> list[float]:
        assert len(matrix) == 3
        return [0.9, 0.5, 0.1]


@final
class _Hybrid:
    companion_weight = 0.24

    def __init__(self, *, missing: bool = False, fail: bool = False) -> None:
        self.missing = missing
        self.fail = fail

    def missing_feature_keys(self, entries: Sequence[Mapping[str, object]]) -> set[str]:
        del entries
        return {"missing"} if self.missing else set()

    def companion_scores(self, entries: Sequence[Mapping[str, object]]) -> list[float]:
        if self.fail:
            raise RuntimeError("broken companion")
        assert len(entries) == 3
        return [0.1, 0.5, 0.9]


def _entries() -> list[dict[str, object]]:
    return [
        {"ketto_toroku_bango": "H1", "umaban": 1, "feat": 0.1},
        {"ketto_toroku_bango": "H2", "umaban": 2, "feat": 0.2},
        {"ketto_toroku_bango": "H3", "umaban": 3, "feat": 0.3},
    ]


def _score(hybrid: _Hybrid) -> list[list[object]]:
    return _score_jra_hybrid(
        cast(BoosterLike, _Booster()),
        hybrid,
        "jra:20260824:05:01:01",
        _entries(),
        ["feat"],
        "catboost",
        "jra-cb-v10-prior-corner274-2013",
    )


def test_hybrid_success_writes_hybrid_model_version() -> None:
    rows = _score(_Hybrid())
    assert all(row[0] == "jra-dirt-small-005-hybrid-v1" for row in rows)
    assert len(rows) == 3


def test_hybrid_missing_contract_falls_back() -> None:
    rows = _score(_Hybrid(missing=True))
    assert all(row[0] == "jra-cb-v10-prior-corner274-2013" for row in rows)


def test_hybrid_exception_falls_back() -> None:
    rows = _score(_Hybrid(fail=True))
    assert all(row[0] == "jra-cb-v10-prior-corner274-2013" for row in rows)


def test_load_hybrid_accepts_clean_artifact(tmp_path: Path) -> None:
    fake = cast(JraHybridScorer, type("Fake", (), {"feature_order": ("feat",), "seeds": ({},)})())
    with patch("predict_upcoming.JraHybridScorer.load", return_value=fake):
        assert _load_jra_hybrid(tmp_path, ["feat"]) is fake


def test_load_hybrid_fails_closed_for_missing_feature(tmp_path: Path) -> None:
    fake = cast(
        JraHybridScorer,
        type("Fake", (), {"feature_order": ("feat", "absent"), "seeds": ({},)})(),
    )
    with patch("predict_upcoming.JraHybridScorer.load", return_value=fake):
        assert _load_jra_hybrid(tmp_path, ["feat"]) is None


def test_load_hybrid_fails_closed_for_load_error(tmp_path: Path) -> None:
    with patch("predict_upcoming.JraHybridScorer.load", side_effect=OSError("missing")):
        assert _load_jra_hybrid(tmp_path, ["feat"]) is None
