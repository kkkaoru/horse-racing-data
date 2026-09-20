from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

import timesfm_finish_position.model_comparison as subject
from timesfm_finish_position.model_comparison import (
    benchmark_cpu_inference,
    compare_to_baseline,
    evaluate_member_diversity,
)
from timesfm_finish_position.model_interface import ModelPrediction


def _prediction(name: str, probabilities: list[float]) -> ModelPrediction:
    return ModelPrediction(
        race_ids=np.asarray(["r1", "r1", "r2", "r2"], dtype=np.str_),
        horse_ids=np.asarray(["h1", "h2", "h3", "h4"], dtype=np.str_),
        prediction=np.asarray(probabilities, dtype=np.float64),
        probability=np.asarray(probabilities, dtype=np.float64),
        model_name=name,
        model_version="v1",
    )


def test_compare_to_baseline_measures_predictions_and_signed_errors() -> None:
    baseline = _prediction("baseline", [0.8, 0.2, 0.3, 0.7])
    candidate = _prediction("candidate", [0.7, 0.3, 0.4, 0.6])
    relationship = compare_to_baseline(
        baseline,
        candidate,
        np.asarray([1, 2, 2, 1], dtype=np.int64),
    )
    assert relationship.prediction_correlation == pytest.approx(0.9922778767)
    assert relationship.error_correlation == pytest.approx(0.9984603532)


def test_compare_to_baseline_rejects_identity_mismatch() -> None:
    baseline = _prediction("baseline", [0.8, 0.2, 0.3, 0.7])
    candidate = ModelPrediction(
        race_ids=baseline.race_ids,
        horse_ids=np.asarray(["other", "h2", "h3", "h4"], dtype=np.str_),
        prediction=baseline.prediction,
        probability=baseline.probability,
        model_name="candidate",
        model_version="v1",
    )
    with pytest.raises(ValueError, match="runner identities"):
        compare_to_baseline(
            baseline,
            candidate,
            np.asarray([1, 2, 2, 1], dtype=np.int64),
        )


def test_member_diversity_reports_probability_and_top1_difference() -> None:
    race_ids = np.asarray(["r1", "r1", "r2", "r2"], dtype=np.str_)
    member_probabilities = np.asarray(
        [[0.8, 0.2], [0.2, 0.8], [0.7, 0.6], [0.3, 0.4]], dtype=np.float64
    )
    diversity = evaluate_member_diversity(race_ids, member_probabilities)
    assert diversity.members == 2
    assert diversity.mean_pairwise_correlation < 0.0
    assert diversity.mean_symmetric_kl > 0.0
    assert diversity.top1_disagreement_rate == pytest.approx(0.5)
    with pytest.raises(ValueError, match="at least two"):
        evaluate_member_diversity(race_ids, member_probabilities[:, :1])


def test_cpu_benchmark_records_latency_memory_and_artifact_size(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "weights.npz").write_bytes(b"1234")
    timestamps = iter((1.0, 1.25, 2.0, 2.5))
    usages = iter((SimpleNamespace(ru_maxrss=1024), SimpleNamespace(ru_maxrss=2048)))

    def next_usage(_who: int) -> SimpleNamespace:
        return next(usages)

    monkeypatch.setattr(subject.time, "perf_counter", lambda: next(timestamps))
    monkeypatch.setattr(subject.resource, "getrusage", next_usage)
    monkeypatch.setattr(subject, "platform_peak_rss_is_bytes", lambda: False)
    performance = benchmark_cpu_inference(
        infer=lambda: _prediction("mlp", [0.8, 0.2, 0.3, 0.7]),
        cold_load=lambda: object(),
        artifact_path=tmp_path,
    )
    assert performance.runners == 4
    assert performance.races == 2
    assert performance.cold_start_seconds == pytest.approx(0.25)
    assert performance.elapsed_seconds == pytest.approx(0.5)
    assert performance.latency_per_horse_ms == pytest.approx(125.0)
    assert performance.peak_rss_mib == pytest.approx(2.0)
    assert performance.peak_rss_delta_mib == pytest.approx(1.0)
    assert performance.model_size_bytes == 4
