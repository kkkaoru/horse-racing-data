from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest

from predict_lib import pytorch_mpx_blend as blend
from predict_lib.prophet_cell_policy import ProphetCellPolicy, parse_prophet_cell_policy
from predict_lib.pytorch_mpx_blend import (
    ARTIFACT_ENV,
    ARTIFACT_VERSION,
    ENABLED_ENV,
    WEIGHT_ENV,
    PytorchMpxArtifact,
    adjust_prediction_rows_with_pytorch_mpx,
    configured_pytorch_mpx_weight,
    load_pytorch_mpx_artifact,
    parse_pytorch_mpx_artifact,
    race_trends,
    reset_pytorch_mpx_caches,
    score_race,
)


def tiny_document(**overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "version": ARTIFACT_VERSION,
        "category": "jra",
        "feature_order": ["a", "b"],
        "mean": [0.0, 0.0],
        "scale": [1.0, 1.0],
        "onnx": "pytorch_mpx_jra.onnx",
        "signals": {"winner": "rank"},
    }
    document.update(overrides)
    return document


def tiny_artifact(tmp_path: Path, **overrides: object) -> PytorchMpxArtifact:
    onnx = tmp_path / "pytorch_mpx_jra.onnx"
    onnx.write_bytes(b"onnx")
    sidecar = tmp_path / "sidecar.json"
    sidecar.write_text(json.dumps(tiny_document(**overrides)), encoding="utf-8")
    return parse_pytorch_mpx_artifact(tiny_document(**overrides), base_dir=tmp_path)


def policy() -> ProphetCellPolicy:
    return parse_prophet_cell_policy(
        {
            "version": "pytorch-mpx-test-v1",
            "default_enabled": False,
            "default_weight": 0.05,
            "categories": {
                "jra": {"cells": {"winner": {"enabled": True, "weight": 0.03}}},
                "nar": {"cells": {"winner": {"enabled": False, "weight": 0.03}}},
            },
        }
    )


def rows() -> list[list[object]]:
    return [
        ["model", "jra", "2026", "0903", "06", "01", "h1", 1, 1.0, 1],
        ["model", "jra", "2026", "0903", "06", "01", "h2", 2, 0.8, 2],
        ["model", "jra", "2026", "0903", "06", "01", "h3", 3, 0.0, 3],
    ]


def entries() -> list[dict[str, object]]:
    return [
        {"ketto_toroku_bango": "h1", "a": 2.0, "b": 0.0},
        {"ketto_toroku_bango": "h2", "a": 1.0, "b": 0.0},
        {"ketto_toroku_bango": "h3", "a": 0.0, "b": 0.0},
    ]


def first_column(centered: np.ndarray) -> np.ndarray:
    return centered[:, 0]


def test_score_race_uses_injected_engine(tmp_path: Path) -> None:
    artifact = tiny_artifact(tmp_path)
    scores = score_race(
        artifact,
        np.asarray([[2.0, 0.0], [1.0, 0.0], [0.0, 0.0]], dtype=np.float32),
        engine=first_column,
    )
    assert scores.tolist() == [2.0, 1.0, 0.0]


def test_race_trends_rank_orders_higher_score_first() -> None:
    assert race_trends([0.2, 0.9, 0.1], signal="score") == [0.2, 0.9, 0.1]
    assert race_trends([0.2, 0.9, 0.1], signal="rank") == [-2.0, -1.0, -3.0]


def test_race_trends_rejects_unknown_signal() -> None:
    with pytest.raises(ValueError, match="score or rank"):
        race_trends([1.0], signal="odds")


def test_parse_rejects_malformed_documents(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="object"):
        parse_pytorch_mpx_artifact([], base_dir=tmp_path)
    with pytest.raises(ValueError, match="version"):
        parse_pytorch_mpx_artifact(tiny_document(version="other"), base_dir=tmp_path)
    with pytest.raises(ValueError, match="category"):
        parse_pytorch_mpx_artifact(tiny_document(category="usa"), base_dir=tmp_path)
    with pytest.raises(ValueError, match="feature_order"):
        parse_pytorch_mpx_artifact(tiny_document(feature_order=[]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="onnx"):
        parse_pytorch_mpx_artifact(tiny_document(onnx="weights.json"), base_dir=tmp_path)
    with pytest.raises(ValueError, match="length mismatch"):
        parse_pytorch_mpx_artifact(tiny_document(mean=[0.0]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="positive"):
        parse_pytorch_mpx_artifact(tiny_document(scale=[1.0, 0.0]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="signals"):
        parse_pytorch_mpx_artifact(tiny_document(signals=[]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="signals"):
        parse_pytorch_mpx_artifact(tiny_document(signals={"sim": "odds"}), base_dir=tmp_path)
    with pytest.raises(ValueError, match="mean"):
        parse_pytorch_mpx_artifact(tiny_document(mean=["x", 0.0]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="untrained_features"):
        parse_pytorch_mpx_artifact(tiny_document(untrained_features="a"), base_dir=tmp_path)
    with pytest.raises(ValueError, match="untrained_features"):
        parse_pytorch_mpx_artifact(tiny_document(untrained_features=["z"]), base_dir=tmp_path)


def test_score_race_pins_untrained_features_to_zero(tmp_path: Path) -> None:
    artifact = tiny_artifact(tmp_path, mean=[1.0, 0.0], untrained_features=["a"])
    scores = score_race(
        artifact,
        np.asarray([[21.5, 3.0], [9.0, 1.0]], dtype=np.float32),
        engine=lambda centered: centered[:, 0] + centered[:, 1],
    )
    assert scores.tolist() == [3.0, 1.0]


def test_production_sidecar_pins_the_venue_weather_columns() -> None:
    artifact = parse_pytorch_mpx_artifact(
        json.loads(blend.DEFAULT_ARTIFACT_PATH.read_text(encoding="utf-8")),
        base_dir=blend.DEFAULT_ARTIFACT_PATH.parent,
    )
    assert len(artifact.untrained_features) == 25
    assert "venue_temperature" in artifact.untrained_features
    assert "speed_index_avg_5" not in artifact.untrained_features


def test_load_artifact_returns_none_for_missing_and_invalid_files(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.json"
    assert load_pytorch_mpx_artifact(missing) is None
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    assert load_pytorch_mpx_artifact(bad) is None
    invalid = tmp_path / "invalid.json"
    invalid.write_text(json.dumps({"version": "nope"}), encoding="utf-8")
    assert load_pytorch_mpx_artifact(invalid) is None
    sidecar = tmp_path / "sidecar.json"
    sidecar.write_text(json.dumps(tiny_document()), encoding="utf-8")
    assert load_pytorch_mpx_artifact(sidecar) is None


def test_configured_weight_respects_policy_and_env() -> None:
    reset_pytorch_mpx_caches()
    assert configured_pytorch_mpx_weight("jra", cell_variant="winner", policy=policy()) == 0.03
    assert configured_pytorch_mpx_weight("jra", cell_variant="other", policy=policy()) is None
    assert (
        configured_pytorch_mpx_weight(
            "jra",
            {ENABLED_ENV: "off"},
            cell_variant="winner",
            policy=policy(),
        )
        is None
    )
    assert (
        configured_pytorch_mpx_weight(
            "jra",
            {WEIGHT_ENV: "0.2"},
            cell_variant="winner",
            policy=policy(),
        )
        == 0.2
    )
    assert (
        configured_pytorch_mpx_weight(
            "jra",
            {WEIGHT_ENV: "nope"},
            cell_variant="winner",
            policy=policy(),
        )
        is None
    )
    assert (
        configured_pytorch_mpx_weight(
            "jra",
            {WEIGHT_ENV: "2.0"},
            cell_variant="winner",
            policy=policy(),
        )
        is None
    )


def test_adjust_applies_rank_blend_when_enabled(tmp_path: Path) -> None:
    artifact = tiny_artifact(tmp_path)
    result = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=artifact,
        engine=first_column,
    )
    assert result.applied is True
    assert result.reason == "applied"


def test_adjust_skips_disabled_and_mismatched_rows(tmp_path: Path) -> None:
    artifact = tiny_artifact(tmp_path)
    disabled = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        cell_variant="other",
        policy=policy(),
        artifact=artifact,
        engine=first_column,
    )
    assert disabled.applied is False
    assert disabled.reason == "disabled"
    mismatch = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries()[:1],
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=artifact,
        engine=first_column,
    )
    assert mismatch.applied is False
    assert mismatch.reason == "row-entry-mismatch"


def test_adjust_skips_missing_artifact_and_wrong_category(tmp_path: Path) -> None:
    reset_pytorch_mpx_caches()
    result = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=None,
        environment={ARTIFACT_ENV: "/tmp/does-not-exist-pytorch-mpx.json"},
    )
    assert result.applied is False
    assert result.reason == "artifact-unavailable"
    artifact = tiny_artifact(tmp_path, category="nar")
    wrong = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=artifact,
        engine=first_column,
    )
    assert wrong.applied is False
    assert wrong.reason == "artifact-unavailable"


def test_adjust_fails_closed_when_engine_raises(tmp_path: Path) -> None:
    artifact = tiny_artifact(tmp_path)

    def boom(_centered: np.ndarray) -> np.ndarray:
        raise RuntimeError("onnx exploded")

    result = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=artifact,
        engine=boom,
    )
    assert result.applied is False
    assert result.reason == "score-failed"
    garbage = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=artifact,
    )
    assert garbage.applied is False
    assert garbage.reason == "score-failed"


def test_score_race_rejects_bad_shapes(tmp_path: Path) -> None:
    artifact = tiny_artifact(tmp_path)
    with pytest.raises(ValueError, match="width"):
        score_race(artifact, np.zeros((2, 3), dtype=np.float32), engine=first_column)
    with pytest.raises(ValueError, match="runners"):
        score_race(artifact, np.zeros((0, 2), dtype=np.float32), engine=first_column)


def test_load_artifact_succeeds_when_onnx_exists(tmp_path: Path) -> None:
    onnx = tmp_path / "pytorch_mpx_jra.onnx"
    onnx.write_bytes(b"onnx")
    sidecar = tmp_path / "sidecar.json"
    sidecar.write_text(json.dumps(tiny_document()), encoding="utf-8")
    loaded = load_pytorch_mpx_artifact(sidecar)
    assert loaded is not None
    assert loaded.onnx_path == onnx


def test_onnx_score_uses_session_and_rejects_bad_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tiny_artifact(tmp_path)
    numeric = np.asarray([[1.0, 0.0], [0.5, 0.0]], dtype=np.float32)

    class EmptySession:
        def run(
            self,
            output_names: list[str] | None,
            input_feed: dict[str, np.ndarray],
        ) -> list[np.ndarray]:
            return []

    monkeypatch.setattr(blend, "_cached_session", lambda _path: EmptySession())
    with pytest.raises(ValueError, match="no scores"):
        score_race(artifact, numeric)

    class ShortSession:
        def run(
            self,
            output_names: list[str] | None,
            input_feed: dict[str, np.ndarray],
        ) -> list[np.ndarray]:
            return [np.zeros((1, 1), dtype=np.float32)]

    monkeypatch.setattr(blend, "_cached_session", lambda _path: ShortSession())
    with pytest.raises(ValueError, match="shape"):
        score_race(artifact, numeric)

    class OkSession:
        def run(
            self,
            output_names: list[str] | None,
            input_feed: dict[str, np.ndarray],
        ) -> list[np.ndarray]:
            return [input_feed["features"][:, :, 0]]

    monkeypatch.setattr(blend, "_cached_session", lambda _path: OkSession())
    scores = score_race(artifact, numeric)
    assert scores.tolist() == [1.0, 0.5]
    result = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        [
            {"ketto_toroku_bango": "", "a": 2.0, "b": 0.0},
            {"ketto_toroku_bango": "h2", "a": 1.0, "b": 0.0},
            {"ketto_toroku_bango": "h3", "a": 0.0, "b": 0.0},
        ],
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=artifact,
        engine=first_column,
    )
    assert result.applied is True


def test_onnx_score_converts_sequence_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class SequenceSession:
        def run(
            self, _output_names: Sequence[str] | None, _input_feed: Mapping[str, np.ndarray]
        ) -> Sequence[object]:
            return [[[1.0, 0.5]]]

    monkeypatch.setattr(blend, "_cached_session", lambda _path: SequenceSession())
    scores = score_race(tiny_artifact(tmp_path), np.zeros((2, 2), dtype=np.float32))
    assert scores.tolist() == [1.0, 0.5]
    assert scores.dtype == np.dtype("float32")


def test_missing_numeric_feature_is_zero_at_the_scoring_boundary(tmp_path: Path) -> None:
    def check_numeric(centered: np.ndarray) -> np.ndarray:
        assert centered.tolist() == [[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]]
        return centered[:, 0]

    result = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        [
            {"ketto_toroku_bango": "h1", "a": None, "b": 0.0},
            {"ketto_toroku_bango": "h2", "a": 1.0, "b": 0.0},
            {"ketto_toroku_bango": "h3", "a": 0.0, "b": 0.0},
        ],
        "jra",
        cell_variant="winner",
        policy=policy(),
        artifact=tiny_artifact(tmp_path),
        engine=check_numeric,
    )
    assert result.applied is True
    assert result.reason == "applied"


def test_cached_session_reuses_loaded_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reset_pytorch_mpx_caches()
    created: list[str] = []

    class FakeSession:
        def run(
            self,
            output_names: list[str] | None,
            input_feed: dict[str, np.ndarray],
        ) -> list[np.ndarray]:
            return [np.zeros((1, 1), dtype=np.float32)]

    def make_session(path: str, providers: list[str]) -> FakeSession:
        assert providers == ["CPUExecutionProvider"]
        created.append(path)
        return FakeSession()

    monkeypatch.setitem(sys.modules, "onnxruntime", SimpleNamespace(InferenceSession=make_session))
    artifact = tiny_artifact(tmp_path)
    numeric = np.zeros((1, 2), dtype=np.float32)
    assert score_race(artifact, numeric).tolist() == [0.0]
    assert score_race(artifact, numeric).tolist() == [0.0]
    assert created == [str(tmp_path / "pytorch_mpx_jra.onnx")]
    reset_pytorch_mpx_caches()


def test_finite_helpers_and_default_policy_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ValueError, match="finite numbers"):
        parse_pytorch_mpx_artifact(tiny_document(mean=[True, 0.0]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="finite numbers"):
        parse_pytorch_mpx_artifact(tiny_document(mean=[None, 0.0]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="finite numbers"):
        parse_pytorch_mpx_artifact(tiny_document(mean=[[1], 0.0]), base_dir=tmp_path)
    with pytest.raises(ValueError, match="non-empty list"):
        parse_pytorch_mpx_artifact(tiny_document(mean=[]), base_dir=tmp_path)
    reset_pytorch_mpx_caches()
    present = tmp_path / "present-policy.json"
    present.write_text(
        json.dumps(
            {
                "version": "pytorch-mpx-test-v1",
                "default_enabled": False,
                "default_weight": 0.05,
                "categories": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(blend.POLICY_ENV, str(present))
    policy_loader = Mock(wraps=blend.load_prophet_cell_policy)
    monkeypatch.setattr(blend, "load_prophet_cell_policy", policy_loader)
    assert configured_pytorch_mpx_weight("nar", {}) is None
    assert configured_pytorch_mpx_weight("nar", {}) is None
    policy_loader.assert_called_once_with(present)
    reset_pytorch_mpx_caches()
    policy_loader.reset_mock()
    monkeypatch.setenv(blend.POLICY_ENV, str(tmp_path / "missing-policy.json"))
    assert configured_pytorch_mpx_weight("nar", {}) is None
    assert configured_pytorch_mpx_weight("nar", {}) is None
    policy_loader.assert_called_once_with(blend.DEFAULT_POLICY_PATH)
    reset_pytorch_mpx_caches()
    sidecar = tmp_path / "sidecar.json"
    sidecar.write_text("{", encoding="utf-8")
    artifact_loader = Mock(wraps=load_pytorch_mpx_artifact)
    monkeypatch.setattr(blend, "load_pytorch_mpx_artifact", artifact_loader)
    first = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        {ARTIFACT_ENV: str(sidecar)},
        cell_variant="winner",
        policy=policy(),
    )
    second = adjust_prediction_rows_with_pytorch_mpx(
        rows(),
        entries(),
        "jra",
        {ARTIFACT_ENV: str(sidecar)},
        cell_variant="winner",
        policy=policy(),
    )
    assert first.reason == "artifact-unavailable"
    assert second.reason == "artifact-unavailable"
    artifact_loader.assert_called_once_with(sidecar)
    reset_pytorch_mpx_caches()
    monkeypatch.setattr(
        blend, "load_prophet_cell_policy", Mock(side_effect=RuntimeError("bad policy"))
    )
    assert configured_pytorch_mpx_weight("jra", {}) is None
    reset_pytorch_mpx_caches()
