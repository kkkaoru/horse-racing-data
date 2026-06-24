from __future__ import annotations

import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import walk_forward_common as subject


def test_should_skip_fold_returns_false_when_ndcg_passes():
    skip, reason = subject.should_skip_fold(
        val_ndcg=0.50,
        val_top1=0.10,
        val_place3=0.10,
        baseline_ndcg=0.50,
        baseline_top1=0.40,
        baseline_place3=0.30,
    )
    assert skip is False
    assert reason == "ndcg-pass"


def test_should_skip_fold_skips_when_ndcg_and_top1_drop():
    skip, reason = subject.should_skip_fold(
        val_ndcg=0.40,
        val_top1=0.20,
        val_place3=0.30,
        baseline_ndcg=0.50,
        baseline_top1=0.40,
        baseline_place3=0.30,
    )
    assert skip is True
    assert "top1" in reason


def test_should_skip_fold_skips_when_ndcg_and_place3_drop():
    skip, reason = subject.should_skip_fold(
        val_ndcg=0.40,
        val_top1=0.40,
        val_place3=0.20,
        baseline_ndcg=0.50,
        baseline_top1=0.40,
        baseline_place3=0.30,
    )
    assert skip is True
    assert "place3" in reason


def test_should_skip_fold_passes_when_only_ndcg_dropped_but_secondary_ok():
    skip, reason = subject.should_skip_fold(
        val_ndcg=0.40,
        val_top1=0.40,
        val_place3=0.30,
        baseline_ndcg=0.50,
        baseline_top1=0.40,
        baseline_place3=0.30,
    )
    assert skip is False
    assert reason == "secondary-pass"


def test_assert_memory_available_returns_when_first_attempt_ok():
    reader = MagicMock(return_value=int(16 * subject.BYTES_PER_GIB))
    sleeper = MagicMock()
    subject.assert_memory_available(
        min_gb=8.0,
        retries=3,
        retry_sleep_s=10,
        memory_reader=reader,
        sleeper=sleeper,
    )
    assert reader.call_count == 1
    sleeper.assert_not_called()


def test_assert_memory_available_retries_then_passes():
    values = [
        int(2 * subject.BYTES_PER_GIB),
        int(2 * subject.BYTES_PER_GIB),
        int(10 * subject.BYTES_PER_GIB),
    ]
    reader = MagicMock(side_effect=values)
    sleeper = MagicMock()
    subject.assert_memory_available(
        min_gb=8.0,
        retries=3,
        retry_sleep_s=5,
        memory_reader=reader,
        sleeper=sleeper,
    )
    assert reader.call_count == 3
    assert sleeper.call_count == 2


def test_assert_memory_available_raises_when_all_retries_low():
    reader = MagicMock(return_value=int(2 * subject.BYTES_PER_GIB))
    sleeper = MagicMock()
    with pytest.raises(MemoryError) as info:
        subject.assert_memory_available(
            min_gb=8.0,
            retries=2,
            retry_sleep_s=5,
            memory_reader=reader,
            sleeper=sleeper,
        )
    assert "2 attempts" in str(info.value)
    assert reader.call_count == 2
    assert sleeper.call_count == 1


def test_assert_memory_available_rejects_retries_below_one():
    with pytest.raises(ValueError):
        subject.assert_memory_available(retries=0)


def test_assert_memory_available_uses_default_psutil_when_reader_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    fake_psutil = MagicMock()
    fake_psutil.virtual_memory.return_value = MagicMock(
        available=int(16 * subject.BYTES_PER_GIB),
    )
    monkeypatch.setattr(subject, "psutil", fake_psutil)
    subject.assert_memory_available(min_gb=8.0, retries=1)
    fake_psutil.virtual_memory.assert_called_once()


def test_assert_memory_available_uses_default_sleeper_when_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    import time as time_module

    reader = MagicMock(
        side_effect=[
            int(1 * subject.BYTES_PER_GIB),
            int(16 * subject.BYTES_PER_GIB),
        ],
    )
    sleep_calls: list[float] = []
    monkeypatch.setattr(time_module, "sleep", lambda s: sleep_calls.append(s))
    subject.assert_memory_available(
        min_gb=8.0,
        retries=2,
        retry_sleep_s=1,
        memory_reader=reader,
    )
    assert sleep_calls == [1.0]


def test_compute_time_decay_weights_empty_returns_empty():
    weights = subject.compute_time_decay_weights(np.array([], dtype=np.int64))
    assert weights.size == 0


def test_compute_time_decay_weights_single_year_returns_ones():
    weights = subject.compute_time_decay_weights(
        np.array([2020, 2020, 2020], dtype=np.int64),
    )
    assert weights.tolist() == [1.0, 1.0, 1.0]


def test_compute_time_decay_weights_linear_spans_half_to_one():
    weights = subject.compute_time_decay_weights(
        np.array([2010, 2015, 2020], dtype=np.int64),
    )
    assert weights.tolist() == [0.5, 0.75, 1.0]


def test_compute_bucket_aware_weights_alpha_zero_returns_time_weights():
    time_weights = np.array([1.0, 0.8, 0.5], dtype=np.float64)
    weak = np.array([0.0, 0.5, 1.0], dtype=np.float64)
    out = subject.compute_bucket_aware_weights(time_weights, weak, alpha=0.0)
    assert out.tolist() == [1.0, 0.8, 0.5]


def test_compute_bucket_aware_weights_alpha_half_boosts_weak():
    time_weights = np.array([1.0, 1.0, 1.0], dtype=np.float64)
    weak = np.array([0.0, 0.5, 1.0], dtype=np.float64)
    out = subject.compute_bucket_aware_weights(time_weights, weak, alpha=0.5)
    assert out[0] == 1.0
    assert out[1] == 1.25
    assert out[2] == 1.5


def test_compute_bucket_aware_weights_alpha_at_max_75():
    time_weights = np.array([1.0, 1.0], dtype=np.float64)
    weak = np.array([0.0, 1.0], dtype=np.float64)
    out = subject.compute_bucket_aware_weights(time_weights, weak, alpha=0.75)
    assert out.tolist() == [1.0, 1.75]


def test_compute_bucket_aware_weights_rejects_alpha_above_75():
    time_weights = np.array([1.0], dtype=np.float64)
    weak = np.array([1.0], dtype=np.float64)
    with pytest.raises(ValueError) as info:
        subject.compute_bucket_aware_weights(time_weights, weak, alpha=0.8)
    assert "<= 0.75" in str(info.value)


def test_compute_bucket_aware_weights_rejects_negative_alpha():
    time_weights = np.array([1.0], dtype=np.float64)
    weak = np.array([1.0], dtype=np.float64)
    with pytest.raises(ValueError) as info:
        subject.compute_bucket_aware_weights(time_weights, weak, alpha=-0.1)
    assert "non-negative" in str(info.value)


def test_compute_bucket_aware_weights_rejects_shape_mismatch():
    time_weights = np.array([1.0, 1.0], dtype=np.float64)
    weak = np.array([1.0], dtype=np.float64)
    with pytest.raises(ValueError) as info:
        subject.compute_bucket_aware_weights(time_weights, weak, alpha=0.5)
    assert "shape" in str(info.value)


def test_compute_bucket_aware_weights_clips_upper_bound():
    time_weights = np.array([1.5], dtype=np.float64)
    weak = np.array([1.0], dtype=np.float64)
    out = subject.compute_bucket_aware_weights(time_weights, weak, alpha=0.75)
    assert out.tolist() == [1.75]


def test_compute_bucket_aware_weights_clips_lower_bound():
    time_weights = np.array([0.1], dtype=np.float64)
    weak = np.array([0.0], dtype=np.float64)
    out = subject.compute_bucket_aware_weights(time_weights, weak, alpha=0.5)
    assert out.tolist() == [0.5]


def test_atomic_write_metadata_writes_new_file(tmp_path: Path):
    path = tmp_path / "fold-2025" / "metadata.json"
    subject.atomic_write_metadata(path, {"fold_year": 2025, "status": "completed"})
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "fold_year": 2025,
        "status": "completed",
    }


def test_atomic_write_metadata_overwrites_existing(tmp_path: Path):
    path = tmp_path / "metadata.json"
    path.write_text("{\"old\":true}", encoding="utf-8")
    subject.atomic_write_metadata(path, {"new": True})
    assert json.loads(path.read_text(encoding="utf-8")) == {"new": True}


def test_atomic_write_metadata_uses_os_replace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import os as os_module

    captured: list[tuple[Path, Path]] = []

    def fake_replace(src: str | Path, dst: str | Path) -> None:
        captured.append((Path(src), Path(dst)))
        Path(dst).write_text(Path(src).read_text(encoding="utf-8"), encoding="utf-8")
        Path(src).unlink()

    monkeypatch.setattr(os_module, "replace", fake_replace)
    path = tmp_path / "metadata.json"
    subject.atomic_write_metadata(path, {"status": "completed"})
    assert len(captured) == 1
    assert captured[0][1] == path


def test_detect_completed_fold_returns_false_when_no_metadata(tmp_path: Path):
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is False


def test_detect_completed_fold_returns_true_for_matching_completed_metadata(
    tmp_path: Path,
):
    (tmp_path / "metadata.json").write_text(
        json.dumps({"status": "completed", "fold_year": 2025}),
        encoding="utf-8",
    )
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is True


def test_detect_completed_fold_returns_false_when_in_progress(tmp_path: Path):
    (tmp_path / "metadata.json").write_text(
        json.dumps({"status": "in_progress", "fold_year": 2025}),
        encoding="utf-8",
    )
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is False


def test_detect_completed_fold_returns_false_when_year_mismatch(tmp_path: Path):
    (tmp_path / "metadata.json").write_text(
        json.dumps({"status": "completed", "fold_year": 2024}),
        encoding="utf-8",
    )
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is False


def test_detect_completed_fold_returns_false_for_invalid_json(tmp_path: Path):
    (tmp_path / "metadata.json").write_text("{not json", encoding="utf-8")
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is False


def test_detect_completed_fold_returns_false_for_non_object_root(tmp_path: Path):
    (tmp_path / "metadata.json").write_text("[1, 2, 3]", encoding="utf-8")
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is False


def test_detect_completed_fold_returns_false_for_missing_year_key(tmp_path: Path):
    (tmp_path / "metadata.json").write_text(
        json.dumps({"status": "completed"}),
        encoding="utf-8",
    )
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is False


def test_detect_completed_fold_returns_false_for_non_int_year(tmp_path: Path):
    (tmp_path / "metadata.json").write_text(
        json.dumps({"status": "completed", "fold_year": "not-a-number"}),
        encoding="utf-8",
    )
    assert subject.detect_completed_fold(tmp_path, fold_year=2025) is False


def test_compute_per_bucket_val_ndcg_returns_empty_dict_for_no_dims():
    out = subject.compute_per_bucket_val_ndcg(
        predictions=[0.9, 0.4],
        labels=[3, 1],
        bucket_dim_values={},
    )
    assert out == {}


def test_compute_per_bucket_val_ndcg_returns_one_score_per_dim():
    out = subject.compute_per_bucket_val_ndcg(
        predictions=[0.9, 0.5, 0.1],
        labels=[3, 2, 1],
        bucket_dim_values={"distance": "1200m", "grade": "G3"},
    )
    assert set(out.keys()) == {"distance", "grade"}
    assert out["distance"] == 1.0
    assert out["grade"] == 1.0


def test_compute_per_bucket_val_ndcg_handles_reversed_ranking():
    out = subject.compute_per_bucket_val_ndcg(
        predictions=[0.1, 0.5, 0.9],
        labels=[3, 2, 1],
        bucket_dim_values={"distance": "1200m"},
    )
    assert out["distance"] < 1.0
    assert out["distance"] > 0.0


def test_compute_per_bucket_val_ndcg_returns_zero_for_empty_inputs():
    out = subject.compute_per_bucket_val_ndcg(
        predictions=[],
        labels=[],
        bucket_dim_values={"distance": "1200m"},
    )
    assert out == {"distance": 0.0}


def test_compute_per_bucket_val_ndcg_raises_on_misaligned_inputs():
    with pytest.raises(ValueError) as info:
        subject.compute_per_bucket_val_ndcg(
            predictions=[0.9],
            labels=[3, 2],
            bucket_dim_values={"distance": "1200m"},
        )
    assert "align" in str(info.value)


def test_compute_per_bucket_val_ndcg_returns_zero_when_idcg_zero():
    out = subject.compute_per_bucket_val_ndcg(
        predictions=[0.9, 0.4],
        labels=[0, 0],
        bucket_dim_values={"distance": "1200m"},
    )
    assert out == {"distance": 0.0}


def _build_hpo_df(rows: int = 36) -> pd.DataFrame:
    race_ids = [f"r{i // 3}" for i in range(rows)]
    keibajo = [
        "A" if (i // 3) % 2 == 0 else "B" for i in range(rows)
    ]
    grade = [
        "G1" if (i // 3) % 4 == 0 else "G3" for i in range(rows)
    ]
    return pd.DataFrame({
        "race_id": race_ids,
        "keibajo_code": keibajo,
        "grade_code": grade,
        "finish_position": [(i % 3) + 1 for i in range(rows)],
    })


def test_stratified_kfold_indices_returns_n_folds():
    df = _build_hpo_df(rows=36)
    folds = subject.stratified_kfold_indices(
        df, strata_cols=["keibajo_code"], n_folds=3, seed=42,
    )
    assert len(folds) == 3


def test_stratified_kfold_indices_produces_disjoint_val_sets():
    df = _build_hpo_df(rows=36)
    folds = subject.stratified_kfold_indices(
        df, strata_cols=["keibajo_code"], n_folds=3, seed=42,
    )
    seen: set[int] = set()
    for _, val_idx in folds:
        for index_value in val_idx.tolist():
            assert cast(int, index_value) not in seen
            seen.add(cast(int, index_value))


def test_stratified_kfold_indices_supports_multiple_strata_cols():
    df = _build_hpo_df(rows=48)
    folds = subject.stratified_kfold_indices(
        df, strata_cols=["keibajo_code", "grade_code"], n_folds=2, seed=7,
    )
    assert len(folds) == 2


def test_stratified_kfold_indices_rejects_n_folds_below_two():
    df = _build_hpo_df()
    with pytest.raises(ValueError) as info:
        subject.stratified_kfold_indices(df, strata_cols=["keibajo_code"], n_folds=1, seed=1)
    assert "n_folds" in str(info.value)


def test_stratified_kfold_indices_raises_when_race_id_missing():
    df = pd.DataFrame({"keibajo_code": ["A", "B", "A", "B"]})
    with pytest.raises(ValueError) as info:
        subject.stratified_kfold_indices(df, strata_cols=["keibajo_code"], n_folds=2, seed=1)
    assert "race_id" in str(info.value)


def test_stratified_kfold_indices_raises_when_strata_missing():
    df = pd.DataFrame({"race_id": ["a", "b", "c", "d"]})
    with pytest.raises(ValueError) as info:
        subject.stratified_kfold_indices(df, strata_cols=["bogus"], n_folds=2, seed=1)
    assert "bogus" in str(info.value)


def test_stratified_kfold_indices_raises_when_strata_cols_empty():
    df = _build_hpo_df()
    with pytest.raises(ValueError) as info:
        subject.stratified_kfold_indices(df, strata_cols=[], n_folds=2, seed=1)
    assert "empty" in str(info.value).lower() or "strata_cols" in str(info.value).lower()


def test_stratified_kfold_indices_raises_when_race_id_overlaps(
    monkeypatch: pytest.MonkeyPatch,
):
    df = _build_hpo_df(rows=36)

    class CollidingSplitter:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def split(self, base: np.ndarray, _strata: np.ndarray):
            half = max(1, len(base) // 2)
            yield np.arange(0, half + 1), np.arange(half - 1, len(base))

    monkeypatch.setattr(subject, "StratifiedKFold", CollidingSplitter)
    with pytest.raises(AssertionError) as info:
        subject.stratified_kfold_indices(
            df, strata_cols=["keibajo_code"], n_folds=2, seed=1,
        )
    assert "overlap" in str(info.value)


def test_sort_full_dataset_orders_by_race_id_and_umaban():
    df = pd.DataFrame({
        "race_id": ["r2", "r1", "r2", "r1"],
        "umaban": [2, 1, 1, 2],
        "value": [10, 20, 30, 40],
    })
    out = subject.sort_full_dataset(df)
    assert out["race_id"].tolist() == ["r1", "r1", "r2", "r2"]
    assert out["umaban"].tolist() == [1, 2, 1, 2]
    assert out.index.tolist() == [0, 1, 2, 3]


def test_sort_full_dataset_mask_of_sorted_frame_stays_sorted():
    """The whole optimization hinges on this: a boolean mask of a sorted frame
    must remain sorted by (race_id, umaban) with contiguous race groups, so each
    walk-forward fold slice needs no further sorting."""
    df = pd.DataFrame({
        "race_id": ["r3", "r1", "r2", "r1", "r3", "r2"],
        "umaban": [1, 2, 1, 1, 2, 2],
        "keep": [True, True, False, True, True, False],
    })
    out = subject.sort_full_dataset(df)
    sliced = out[out["keep"]]
    assert sliced["race_id"].tolist() == ["r1", "r1", "r3", "r3"]
    assert sliced["umaban"].tolist() == [1, 2, 1, 2]


def test_sort_full_dataset_falls_back_when_sort_keys_missing():
    df = pd.DataFrame({"other": [3, 1, 2]}, index=[7, 8, 9])
    out = subject.sort_full_dataset(df)
    assert out["other"].tolist() == [3, 1, 2]
    assert out.index.tolist() == [0, 1, 2]


def test_sort_full_dataset_sorts_by_race_id_only_when_umaban_missing():
    df = pd.DataFrame({"race_id": ["r2", "r1", "r2"], "value": [1, 2, 3]})
    out = subject.sort_full_dataset(df)
    assert out["race_id"].tolist() == ["r1", "r2", "r2"]
