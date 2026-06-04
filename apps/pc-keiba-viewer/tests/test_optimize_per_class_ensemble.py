from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

import optimize_per_class_ensemble as subject

if TYPE_CHECKING:
    from collections.abc import Callable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_predictions(
    parquet_root: Path,
    year: int,
    race_id: str,
    horses: list[str],
    scores: list[float],
    actuals: list[int],
) -> None:
    year_dir = parquet_root / f"race_year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "race_id": [race_id] * len(horses),
        "ketto_toroku_bango": horses,
        "predicted_score": scores,
        "actual_finish_position": actuals,
        "umaban": list(range(1, len(horses) + 1)),
    })
    df.to_parquet(year_dir / "predictions.parquet", index=False)


def _make_pg_map(race_class_pairs: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": [p[0] for p in race_class_pairs],
        "kyoso_joken_code": [p[1] for p in race_class_pairs],
    })


def _build_argv(**overrides: str) -> list[str]:
    defaults: dict[str, str] = {
        "--class-code": "005",
        "--baseline-parquet-dir": "/tmp/baseline",
        "--pg-url": "postgresql://u:p@h/db",
        "--output-summary": "/tmp/summary.json",
    }
    defaults.update(overrides)
    argv: list[str] = []
    for key, value in defaults.items():
        argv.append(key)
        argv.append(value)
    return argv


def _sample_args(
    *,
    class_code: str = "005",
    baseline_parquet_dir: Path = Path("/tmp/baseline"),
    candidate_parquet_dirs: list[Path] | None = None,
    validation_years: list[int] | None = None,
    holdout_years: list[int] | None = None,
    min_baseline_weight: float = 0.2,
    n_trials: int = 10,
    seed: int = 42,
    pg_url: str = "postgresql://u:p@h/db",
    output_summary: Path = Path("/tmp/summary.json"),
    output_manifest_dir: Path | None = None,
    min_holdout_races: int = 200,
    delta_pp_floor: float = 0.0,
) -> subject.OptimizeArgs:
    return subject.OptimizeArgs(
        class_code=class_code,
        baseline_parquet_dir=baseline_parquet_dir,
        candidate_parquet_dirs=candidate_parquet_dirs or [],
        validation_years=validation_years or [2018, 2019],
        holdout_years=holdout_years or [2023, 2024],
        min_baseline_weight=min_baseline_weight,
        n_trials=n_trials,
        seed=seed,
        pg_url=pg_url,
        output_summary=output_summary,
        output_manifest_dir=output_manifest_dir,
        min_holdout_races=min_holdout_races,
        delta_pp_floor=delta_pp_floor,
    )


def _make_stub_study(best_logits: list[float], best_value: float) -> MagicMock:
    """Return a Mock matching ``StudyLike`` that exposes preset best params."""
    study = MagicMock()
    study.optimize = MagicMock()
    study.best_params = {f"z_{i}": v for i, v in enumerate(best_logits)}
    study.best_value = best_value
    return study


def _make_stub_create_study(
    best_logits: list[float], best_value: float,
) -> "Callable[[int], MagicMock]":
    def factory(_seed: int) -> MagicMock:
        return _make_stub_study(best_logits, best_value)
    return factory


def _make_pg_loader(map_df: pd.DataFrame) -> "Callable[[str], pd.DataFrame]":
    def loader(_pg_url: str) -> pd.DataFrame:
        return map_df
    return loader


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def test_build_arg_parser_accepts_required_flags_only():
    parsed = subject.build_arg_parser().parse_args(_build_argv())
    assert parsed.class_code == "005"
    assert parsed.validation_years == "2018,2019,2020,2021,2022"


def test_build_arg_parser_collects_multiple_candidates():
    argv = _build_argv()
    argv.extend(["--candidate-parquet-dir", "/tmp/cand1"])
    argv.extend(["--candidate-parquet-dir", "/tmp/cand2"])
    parsed = subject.build_arg_parser().parse_args(argv)
    assert [str(p) for p in parsed.candidate_parquet_dir] == [
        "/tmp/cand1", "/tmp/cand2",
    ]


def test_normalize_args_parses_year_lists_and_paths():
    argv = _build_argv(**{
        "--validation-years": "2018,2019",
        "--holdout-years": "2023",
        "--n-trials": "5",
    })
    parsed = subject.build_arg_parser().parse_args(argv)
    normalized = subject.normalize_args(parsed)
    assert normalized.validation_years == [2018, 2019]
    assert normalized.holdout_years == [2023]
    assert normalized.n_trials == 5


def test_normalize_args_keeps_manifest_dir_none_when_absent():
    argv = _build_argv()
    normalized = subject.normalize_args(subject.build_arg_parser().parse_args(argv))
    assert normalized.output_manifest_dir is None


def test_normalize_args_keeps_manifest_dir_when_provided():
    argv = _build_argv(**{"--output-manifest-dir": "/tmp/manifest"})
    normalized = subject.normalize_args(subject.build_arg_parser().parse_args(argv))
    assert normalized.output_manifest_dir == Path("/tmp/manifest")


def test_parse_year_list_rejects_empty_spec():
    parser = subject.build_arg_parser()
    with pytest.raises(ValueError):
        subject.normalize_args(parser.parse_args(
            _build_argv(**{"--validation-years": ""}),
        ))


# ---------------------------------------------------------------------------
# derive_model_version
# ---------------------------------------------------------------------------


def test_derive_model_version_returns_parent_of_predictions(tmp_path: Path):
    root = tmp_path / "iter14-jra-cb-pacestyle-course-v8"
    (root / "predictions").mkdir(parents=True)
    assert subject.derive_model_version(root) == "iter14-jra-cb-pacestyle-course-v8"


def test_derive_model_version_fallback_to_leaf_name(tmp_path: Path):
    parquet_dir = tmp_path / "synthetic-flat-dir"
    parquet_dir.mkdir()
    assert subject.derive_model_version(parquet_dir) == "synthetic-flat-dir"


# ---------------------------------------------------------------------------
# load_normalized_member + _resolve_predictions_root
# ---------------------------------------------------------------------------


def test_load_normalized_member_uses_hive_layout_when_present(tmp_path: Path):
    cat_dir = tmp_path / "predictions" / "category=jra"
    _write_predictions(
        cat_dir, 2018, "jra:2018:0101:01:01", ["a", "b"], [2.0, 1.0], [1, 2],
    )
    pg_map = _make_pg_map([("jra:2018:0101:01:01", "005")])
    out = subject.load_normalized_member(tmp_path, "005", [2018], pg_map)
    assert "normalized_score" in out.columns
    assert out.shape[0] == 2


def test_load_normalized_member_uses_flat_layout_when_hive_missing(tmp_path: Path):
    _write_predictions(
        tmp_path, 2018, "jra:2018:0101:01:01", ["a", "b"], [2.0, 1.0], [1, 2],
    )
    pg_map = _make_pg_map([("jra:2018:0101:01:01", "005")])
    out = subject.load_normalized_member(tmp_path, "005", [2018], pg_map)
    assert out.shape[0] == 2


# ---------------------------------------------------------------------------
# build_bundles
# ---------------------------------------------------------------------------


def test_build_bundles_baseline_first_and_relabeled(tmp_path: Path):
    baseline_dir = tmp_path / "baseline"
    cand_dir = tmp_path / "cand"
    _write_predictions(
        baseline_dir, 2018, "jra:2018:0101:01:01", ["a", "b"], [2.0, 1.0], [1, 2],
    )
    _write_predictions(
        baseline_dir, 2023, "jra:2023:0101:01:01", ["c", "d"], [2.0, 1.0], [1, 2],
    )
    _write_predictions(
        cand_dir, 2018, "jra:2018:0101:01:01", ["a", "b"], [1.0, 2.0], [1, 2],
    )
    _write_predictions(
        cand_dir, 2023, "jra:2023:0101:01:01", ["c", "d"], [1.0, 2.0], [1, 2],
    )
    pg_map = _make_pg_map([
        ("jra:2018:0101:01:01", "005"),
        ("jra:2023:0101:01:01", "005"),
    ])
    args = _sample_args(
        baseline_parquet_dir=baseline_dir,
        candidate_parquet_dirs=[cand_dir],
    )
    bundles = subject.build_bundles(args, pg_map)
    assert bundles[0].model_version == subject.BASELINE_VERSION
    assert bundles[0].validation_df.shape[0] == 2
    assert bundles[1].model_version == "cand"


def test_build_bundles_skips_candidate_with_empty_validation(tmp_path: Path):
    baseline_dir = tmp_path / "baseline"
    empty_cand_dir = tmp_path / "empty_cand"
    _write_predictions(
        baseline_dir, 2018, "jra:2018:0101:01:01", ["a", "b"], [2.0, 1.0], [1, 2],
    )
    _write_predictions(
        baseline_dir, 2023, "jra:2023:0101:01:01", ["c", "d"], [2.0, 1.0], [1, 2],
    )
    # empty candidate: no year-2018/2023 races match class 005
    _write_predictions(
        empty_cand_dir, 2018, "jra:2018:0101:01:02", ["x"], [1.0], [1],
    )
    pg_map = _make_pg_map([
        ("jra:2018:0101:01:01", "005"),
        ("jra:2023:0101:01:01", "005"),
        ("jra:2018:0101:01:02", "010"),
    ])
    args = _sample_args(
        baseline_parquet_dir=baseline_dir,
        candidate_parquet_dirs=[empty_cand_dir],
    )
    bundles = subject.build_bundles(args, pg_map)
    assert len(bundles) == 1
    assert bundles[0].model_version == subject.BASELINE_VERSION


def test_build_bundles_raises_when_baseline_empty(tmp_path: Path):
    baseline_dir = tmp_path / "baseline"
    _write_predictions(
        baseline_dir, 2018, "jra:2018:0101:01:01", ["a"], [1.0], [1],
    )
    pg_map = _make_pg_map([("jra:2018:0101:01:01", "010")])
    args = _sample_args(baseline_parquet_dir=baseline_dir)
    with pytest.raises(RuntimeError):
        subject.build_bundles(args, pg_map)


# ---------------------------------------------------------------------------
# run_optuna_search
# ---------------------------------------------------------------------------


def _make_normalized_bundle(model_version: str) -> subject.CandidateBundle:
    val = pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_score": [1.0, 0.0, 1.0, 0.0],
        "normalized_score": [1.0, 0.0, 1.0, 0.0],
        "actual_finish_position": [1, 2, 1, 2],
    })
    hold = pd.DataFrame({
        "race_id": ["r3", "r3", "r4", "r4"],
        "ketto_toroku_bango": ["e", "f", "g", "h"],
        "predicted_score": [1.0, 0.0, 1.0, 0.0],
        "normalized_score": [1.0, 0.0, 1.0, 0.0],
        "actual_finish_position": [1, 2, 1, 2],
    })
    return subject.CandidateBundle(
        model_version=model_version,
        parquet_dir=Path("/tmp/x"),
        validation_df=val,
        holdout_df=hold,
    )


def test_run_optuna_search_returns_weights_and_value_from_stubbed_study():
    bundles = [
        _make_normalized_bundle(subject.BASELINE_VERSION),
        _make_normalized_bundle("cand-A"),
    ]
    args = _sample_args(n_trials=5)
    factory = _make_stub_create_study([0.0, 0.0], 0.75)
    weights, val_top1 = subject.run_optuna_search(bundles, args, factory)
    assert sum(weights) == pytest.approx(1.0, abs=1e-9)
    assert val_top1 == 0.75


# ---------------------------------------------------------------------------
# _evaluate_weights edge cases
# ---------------------------------------------------------------------------


def test_evaluate_weights_returns_zero_when_any_member_empty():
    full = _make_normalized_bundle(subject.BASELINE_VERSION)
    empty_val = pd.DataFrame({
        "race_id": pd.Series([], dtype=str),
        "ketto_toroku_bango": pd.Series([], dtype=str),
        "normalized_score": pd.Series([], dtype=float),
        "actual_finish_position": pd.Series([], dtype=int),
    })
    empty_bundle = subject.CandidateBundle(
        model_version="empty",
        parquet_dir=Path("/tmp/x"),
        validation_df=empty_val,
        holdout_df=empty_val,
    )
    score, n_races = subject.evaluate_weights([full, empty_bundle], [0.5, 0.5], True)
    assert score == 0.0
    assert n_races == 0


def test_evaluate_weights_blend_empty_when_no_join(tmp_path: Path):
    # Two members with disjoint (race_id, horse) keys → inner join yields empty.
    left = pd.DataFrame({
        "race_id": ["r1"], "ketto_toroku_bango": ["a"],
        "predicted_score": [1.0],
        "normalized_score": [1.0],
        "actual_finish_position": [1],
    })
    right = pd.DataFrame({
        "race_id": ["r9"], "ketto_toroku_bango": ["x"],
        "predicted_score": [1.0],
        "normalized_score": [1.0],
        "actual_finish_position": [1],
    })
    left_bundle = subject.CandidateBundle(
        model_version="L", parquet_dir=Path("/tmp/x"),
        validation_df=left, holdout_df=left,
    )
    right_bundle = subject.CandidateBundle(
        model_version="R", parquet_dir=Path("/tmp/x"),
        validation_df=right, holdout_df=right,
    )
    score, n_races = subject.evaluate_weights(
        [left_bundle, right_bundle], [0.5, 0.5], True,
    )
    assert score == 0.0
    assert n_races == 0


# ---------------------------------------------------------------------------
# compute_holdout_metrics + accept gate
# ---------------------------------------------------------------------------


def _make_bundles_with_holdout_top1(top1: float) -> list[subject.CandidateBundle]:
    """Construct two bundles whose blended holdout produces an exact ``top1``."""
    # 4 holdout races; ``top1=1.0`` → all 4 races have actual==1 on top.
    # ``top1=0.5`` → 2 of 4 races have actual==1 on top.
    hits_needed = int(round(top1 * 4))
    actuals_pos1 = [1] * hits_needed + [2] * (4 - hits_needed)
    rows: list[dict[str, object]] = []
    for race_idx in range(4):
        rows.append({
            "race_id": f"r{race_idx}",
            "ketto_toroku_bango": "a",
            "predicted_score": 1.0,
            "normalized_score": 1.0,
            "actual_finish_position": actuals_pos1[race_idx],
        })
        rows.append({
            "race_id": f"r{race_idx}",
            "ketto_toroku_bango": "b",
            "predicted_score": 0.0,
            "normalized_score": 0.0,
            "actual_finish_position": 3 - actuals_pos1[race_idx],
        })
    holdout_df = pd.DataFrame(rows)
    val_df = holdout_df.copy()
    baseline = subject.CandidateBundle(
        model_version=subject.BASELINE_VERSION, parquet_dir=Path("/tmp/x"),
        validation_df=val_df, holdout_df=holdout_df,
    )
    candidate = subject.CandidateBundle(
        model_version="cand-A", parquet_dir=Path("/tmp/x"),
        validation_df=val_df, holdout_df=holdout_df,
    )
    return [baseline, candidate]


def test_compute_holdout_metrics_full_hit_returns_delta_zero():
    bundles = _make_bundles_with_holdout_top1(1.0)
    metrics = subject.compute_holdout_metrics(bundles, [0.5, 0.5])
    assert metrics.top1 == 1.0
    assert metrics.iter14_top1 == 1.0
    assert metrics.delta_pp == 0.0
    assert metrics.n_races == 4


def test_make_decision_accept_when_delta_positive_and_enough_races():
    metrics = subject.HoldoutMetrics(
        top1=0.55, iter14_top1=0.50, delta_pp=0.5,
        n_races=400, wilson_lower_delta=0.01,
    )
    decision = subject.make_decision(metrics, 200, 0.0)
    assert decision.decision == "accept"
    assert decision.rejected_reason is None


def test_make_decision_reject_when_delta_negative():
    metrics = subject.HoldoutMetrics(
        top1=0.49, iter14_top1=0.50, delta_pp=-0.1,
        n_races=400, wilson_lower_delta=-0.05,
    )
    decision = subject.make_decision(metrics, 200, 0.0)
    assert decision.decision == "reject"
    assert decision.rejected_reason == "delta_pp<0"


def test_make_decision_reject_when_holdout_races_insufficient():
    metrics = subject.HoldoutMetrics(
        top1=0.55, iter14_top1=0.50, delta_pp=0.5,
        n_races=150, wilson_lower_delta=0.01,
    )
    decision = subject.make_decision(metrics, 200, 0.0)
    assert decision.decision == "reject"
    assert decision.rejected_reason == "insufficient_holdout_races"


# ---------------------------------------------------------------------------
# Spearman pairwise correlations
# ---------------------------------------------------------------------------


def test_compute_pairwise_correlations_perfect_match():
    bundles = [
        _make_normalized_bundle(subject.BASELINE_VERSION),
        _make_normalized_bundle("cand-A"),
    ]
    corr = subject.compute_pairwise_correlations(bundles)
    key = f"{subject.BASELINE_VERSION}__cand-A"
    assert corr[key] == pytest.approx(1.0, abs=1e-9)


def test_compute_pairwise_correlations_empty_join_returns_nan():
    left = pd.DataFrame({
        "race_id": ["r1"], "ketto_toroku_bango": ["a"],
        "predicted_score": [1.0],
        "normalized_score": [1.0],
        "actual_finish_position": [1],
    })
    right = pd.DataFrame({
        "race_id": ["r9"], "ketto_toroku_bango": ["x"],
        "predicted_score": [1.0],
        "normalized_score": [1.0],
        "actual_finish_position": [1],
    })
    bundles = [
        subject.CandidateBundle(
            model_version="L", parquet_dir=Path("/tmp/x"),
            validation_df=left, holdout_df=left,
        ),
        subject.CandidateBundle(
            model_version="R", parquet_dir=Path("/tmp/x"),
            validation_df=right, holdout_df=right,
        ),
    ]
    corr = subject.compute_pairwise_correlations(bundles)
    assert corr["L__R"] != corr["L__R"]  # NaN check


def test_compute_pairwise_correlations_single_row_join_returns_nan():
    # Only one common (race_id, horse) → spearman cannot compute → NaN.
    left = pd.DataFrame({
        "race_id": ["r1"], "ketto_toroku_bango": ["a"],
        "predicted_score": [1.0],
        "normalized_score": [1.0],
        "actual_finish_position": [1],
    })
    right = pd.DataFrame({
        "race_id": ["r1"], "ketto_toroku_bango": ["a"],
        "predicted_score": [0.5],
        "normalized_score": [0.5],
        "actual_finish_position": [1],
    })
    bundles = [
        subject.CandidateBundle(
            model_version="L", parquet_dir=Path("/tmp/x"),
            validation_df=left, holdout_df=left,
        ),
        subject.CandidateBundle(
            model_version="R", parquet_dir=Path("/tmp/x"),
            validation_df=right, holdout_df=right,
        ),
    ]
    corr = subject.compute_pairwise_correlations(bundles)
    assert corr["L__R"] != corr["L__R"]  # NaN check


# ---------------------------------------------------------------------------
# Manifest / summary writers
# ---------------------------------------------------------------------------


def test_build_manifest_includes_required_keys():
    bundles = [
        _make_normalized_bundle(subject.BASELINE_VERSION),
        _make_normalized_bundle("cand-A"),
    ]
    metrics = subject.HoldoutMetrics(
        top1=0.55, iter14_top1=0.50, delta_pp=0.5,
        n_races=400, wilson_lower_delta=0.01,
    )
    result = subject.OptimizationResult(
        best_weights=[0.4, 0.6], validation_top1=0.7, holdout=metrics,
    )
    args = _sample_args()
    manifest = subject.build_manifest(args, bundles, result)
    assert manifest["model_version"] == "iter23-jra-cb-ensemble-005-v8"
    assert manifest["category"] == "jra"
    assert manifest["kyoso_joken_code"] == "005"
    assert manifest["ensemble_type"] == "rank_blend"
    members = cast(list[dict[str, object]], manifest["members"])
    assert members[0]["is_baseline"] is True
    assert members[1]["is_baseline"] is False
    assert manifest["search_method"] == "optuna_tpe"


def test_build_summary_includes_decision_and_correlations():
    bundles = [
        _make_normalized_bundle(subject.BASELINE_VERSION),
        _make_normalized_bundle("cand-A"),
    ]
    metrics = subject.HoldoutMetrics(
        top1=0.55, iter14_top1=0.50, delta_pp=0.5,
        n_races=400, wilson_lower_delta=0.01,
    )
    result = subject.OptimizationResult(
        best_weights=[0.4, 0.6], validation_top1=0.7, holdout=metrics,
    )
    decision = subject.DecisionResult(decision="accept", rejected_reason=None)
    pairwise = {"a__b": 0.95}
    summary = subject.build_summary(
        _sample_args(), bundles, result, decision, pairwise,
    )
    assert summary["decision"] == "accept"
    assert summary["pairwise_correlations"] == {"a__b": 0.95}
    weights_payload = cast(dict[str, float], summary["best_weights"])
    assert weights_payload[subject.BASELINE_VERSION] == 0.4


def test_write_json_creates_parent_dir(tmp_path: Path):
    out_path = tmp_path / "nested" / "subdir" / "summary.json"
    subject.write_json(out_path, {"hello": "world"})
    assert json.loads(out_path.read_text()) == {"hello": "world"}


# ---------------------------------------------------------------------------
# End-to-end ``run`` orchestration
# ---------------------------------------------------------------------------


def _setup_e2e_dirs(
    tmp_path: Path,
) -> tuple[Path, Path, pd.DataFrame]:
    """Create baseline + candidate parquet trees that produce 4 holdout races."""
    baseline = tmp_path / "iter14-jra-cb-pacestyle-course-v8"
    cand = tmp_path / "cand-A"
    pg_pairs: list[tuple[str, str]] = []
    for year in (2018, 2019):
        rid = f"jra:{year}:0101:01:01"
        _write_predictions(baseline, year, rid, ["a", "b"], [2.0, 1.0], [1, 2])
        _write_predictions(cand, year, rid, ["a", "b"], [1.0, 2.0], [1, 2])
        pg_pairs.append((rid, "005"))
    for year in (2023, 2024):
        for race_idx in range(1, 3):
            rid = f"jra:{year}:0101:01:{race_idx:02d}"
            _write_predictions(baseline, year, rid, ["a", "b"], [2.0, 1.0], [1, 2])
            _write_predictions(cand, year, rid, ["a", "b"], [2.0, 1.0], [1, 2])
            pg_pairs.append((rid, "005"))
    return baseline, cand, _make_pg_map(pg_pairs)


def test_run_writes_summary_and_no_manifest_on_reject(tmp_path: Path):
    baseline, cand, pg_map = _setup_e2e_dirs(tmp_path)
    summary_path = tmp_path / "summary.json"
    manifest_dir = tmp_path / "manifest"
    args = _sample_args(
        baseline_parquet_dir=baseline,
        candidate_parquet_dirs=[cand],
        output_summary=summary_path,
        output_manifest_dir=manifest_dir,
        min_holdout_races=200,
        delta_pp_floor=0.0,
    )
    summary = subject.run(
        args,
        load_race_meta=_make_pg_loader(pg_map),
        create_study=_make_stub_create_study([0.0, 0.0], 1.0),
    )
    assert summary["decision"] == "reject"
    assert summary["rejected_reason"] == "insufficient_holdout_races"
    assert summary_path.exists()
    assert not (manifest_dir / "manifest.json").exists()


def test_run_writes_manifest_on_accept(tmp_path: Path):
    baseline, cand, pg_map = _setup_e2e_dirs(tmp_path)
    summary_path = tmp_path / "summary.json"
    manifest_dir = tmp_path / "manifest"
    args = _sample_args(
        baseline_parquet_dir=baseline,
        candidate_parquet_dirs=[cand],
        output_summary=summary_path,
        output_manifest_dir=manifest_dir,
        min_holdout_races=1,  # accept gate trivially satisfied
        delta_pp_floor=0.0,
    )
    summary = subject.run(
        args,
        load_race_meta=_make_pg_loader(pg_map),
        create_study=_make_stub_create_study([0.0, 0.0], 1.0),
    )
    assert summary["decision"] == "accept"
    manifest_path = manifest_dir / "manifest.json"
    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text())
    assert manifest_payload["kyoso_joken_code"] == "005"


def test_run_accept_without_manifest_dir_skips_manifest_write(tmp_path: Path):
    baseline, cand, pg_map = _setup_e2e_dirs(tmp_path)
    summary_path = tmp_path / "summary.json"
    args = _sample_args(
        baseline_parquet_dir=baseline,
        candidate_parquet_dirs=[cand],
        output_summary=summary_path,
        output_manifest_dir=None,
        min_holdout_races=1,
        delta_pp_floor=0.0,
    )
    summary = subject.run(
        args,
        load_race_meta=_make_pg_loader(pg_map),
        create_study=_make_stub_create_study([0.0, 0.0], 1.0),
    )
    assert summary["decision"] == "accept"


# ---------------------------------------------------------------------------
# default_load_race_meta + default_create_study (importlib monkey-patches)
# ---------------------------------------------------------------------------


def test_default_load_race_meta_invokes_duckdb_postgres_attach(
    monkeypatch: pytest.MonkeyPatch,
):
    duckdb_con = MagicMock()
    fetch_result = MagicMock()
    fetch_result.fetchdf.return_value = pd.DataFrame({
        "race_id": ["jra:2018:0101:01:01"],
        "kyoso_joken_code": ["005"],
    })
    duckdb_con.execute.return_value = fetch_result
    duckdb_module = MagicMock()
    duckdb_module.connect.return_value = duckdb_con
    monkeypatch.setattr(
        importlib,
        "import_module",
        MagicMock(return_value=duckdb_module),
    )
    df = subject.default_load_race_meta("postgresql://u:p@h/db")
    assert df.shape[0] == 1
    duckdb_con.close.assert_called_once()


def test_default_create_study_uses_optuna_tpe_sampler(
    monkeypatch: pytest.MonkeyPatch,
):
    optuna_module = MagicMock()
    fake_sampler = MagicMock()
    optuna_module.samplers.TPESampler.return_value = fake_sampler
    fake_study = MagicMock()
    optuna_module.create_study.return_value = fake_study
    monkeypatch.setattr(
        importlib,
        "import_module",
        MagicMock(return_value=optuna_module),
    )
    study = subject.default_create_study(42)
    optuna_module.samplers.TPESampler.assert_called_once_with(seed=42)
    optuna_module.create_study.assert_called_once_with(
        direction="maximize", sampler=fake_sampler,
    )
    assert study is fake_study


# ---------------------------------------------------------------------------
# main entry point
# ---------------------------------------------------------------------------


def test_main_invokes_run_and_prints_summary(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
    fake_summary = {"decision": "reject", "class_code": "005"}
    monkeypatch.setattr(subject, "run", MagicMock(return_value=fake_summary))
    exit_code = subject.main(_build_argv())
    assert exit_code == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["decision"] == "reject"


# ---------------------------------------------------------------------------
# Internal _make_objective: ensures the closure shape matches Optuna.
# ---------------------------------------------------------------------------


def test_make_objective_returns_callable_that_uses_trial(tmp_path: Path):
    bundles = [
        _make_normalized_bundle(subject.BASELINE_VERSION),
        _make_normalized_bundle("cand-A"),
    ]
    obj = subject.make_objective(bundles, 0.2)
    trial = MagicMock()
    trial.suggest_float.side_effect = [0.0, 0.0]
    score = obj(trial)
    assert 0.0 <= score <= 1.0
    assert trial.suggest_float.call_count == 2
