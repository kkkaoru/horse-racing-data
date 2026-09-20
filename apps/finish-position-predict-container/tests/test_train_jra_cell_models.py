"""Trainer contracts; native fitting is replaced by an explicit test-only model."""

import hashlib
import json
from argparse import Namespace
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from catboost import Pool

import train_jra_cell_models as trainer
from predict_lib.teacher_catalog import TeacherCatalog
from predict_lib.training_admission import RunnerOutcome
from predict_lib.training_roster_match import EvidenceReferences, TeacherRaceEvidence


class FakeRanker:
    parameters: dict[str, object]
    fitted_rows: list[int]
    fitted_labels: list[list[float]]

    def __init__(self, parameters: dict[str, object]) -> None:
        self.parameters = parameters
        self.fitted_rows = []
        self.fitted_labels = []

    def fit(
        self,
        pool: Pool,
        *,
        eval_set: Pool | None = None,
        early_stopping_rounds: int | None = None,
    ) -> None:
        self.fitted_rows.append(pool.num_row())
        self.fitted_labels.append(np.asarray(pool.get_label(), dtype=np.float64).tolist())

    def predict(self, pool: Pool) -> npt.NDArray[np.float64]:
        return np.arange(pool.num_row(), dtype=np.float64)

    def get_best_iteration(self) -> int:
        return 3

    def save_model(self, path: Path, *, format: str) -> None:
        path.write_text(json.dumps({"test_only": True, "format": format}), encoding="utf-8")


@pytest.fixture
def native_models(monkeypatch: pytest.MonkeyPatch) -> list[FakeRanker]:
    created: list[FakeRanker] = []

    def construct(**parameters: object) -> FakeRanker:
        model = FakeRanker(parameters)
        created.append(model)
        return model

    monkeypatch.setattr(trainer, "CatBoostRanker", construct)
    return created


@pytest.fixture
def dataset() -> trainer.DatasetLike:
    frame = pd.DataFrame(
        {
            "race_id": np.concatenate(
                (np.repeat(np.arange(100).astype(str), 2), ["target", "target"])
            ),
            "race_date": np.concatenate(
                (
                    np.repeat(pd.date_range("2020-01-01", periods=100).strftime("%Y%m%d"), 2),
                    ["20210101", "20210101"],
                )
            ),
            "ketto_toroku_bango": np.tile(["a", "b"], 101),
            "umaban": np.tile([1, 2], 101),
            "finish_position": np.tile([1, 2], 101),
            "speed": np.arange(202, dtype=float),
        }
    )
    return ds.dataset(pa.Table.from_pandas(frame, preserve_index=False))


@pytest.fixture
def teacher_catalog() -> TeacherCatalog:
    return TeacherCatalog(
        EvidenceReferences("a" * 64, "b" * 64),
        {
            str(index): TeacherRaceEvidence(
                str(index),
                2,
                (RunnerOutcome("a", 1, "classified", 1), RunnerOutcome("b", 2, "classified", 2)),
            )
            for index in range(100)
        },
    )


@pytest.fixture
def teacher_cli_args(tmp_path: Path, teacher_catalog: TeacherCatalog) -> list[str]:
    declarations = tmp_path / "declared.json"
    outcomes = tmp_path / "outcomes.json"
    declarations.write_text(
        json.dumps(
            {
                "version": "independent-declarations-v1",
                "races": [
                    {"race_id": race.race_id, "declared_starters": 2}
                    for race in teacher_catalog.races.values()
                ],
            }
        ),
        encoding="utf-8",
    )
    outcomes.write_text(
        json.dumps(
            {
                "version": "runner-outcomes-v1",
                "races": [
                    {
                        "race_id": race.race_id,
                        "outcomes": [
                            {
                                "horse_id": "a",
                                "horse_number": 1,
                                "disposition": "classified",
                                "finish": 1,
                            },
                            {
                                "horse_id": "b",
                                "horse_number": 2,
                                "disposition": "classified",
                                "finish": 2,
                            },
                        ],
                    }
                    for race in teacher_catalog.races.values()
                ],
            }
        ),
        encoding="utf-8",
    )
    return [
        "--teacher-declarations",
        str(declarations),
        "--teacher-outcomes",
        str(outcomes),
        "--teacher-declarations-sha256",
        hashlib.sha256(declarations.read_bytes()).hexdigest(),
        "--teacher-outcomes-sha256",
        hashlib.sha256(outcomes.read_bytes()).hexdigest(),
    ]


@pytest.fixture
def cell() -> dict[str, object]:
    return {
        "model_version": "test-only-cell",
        "training_race_ids": np.arange(100).astype(str).tolist(),
        "target_race_ids": ["target"],
        "target_runner_count": 2,
        "training_scope_is_diverse": True,
        "training_cross_cell_race_count": 90,
        "training_target_cell_race_count": 10,
        "training_race_count": 100,
        "training_race_sha256": "test-only",
        "related_seed_race_count": 0,
        "cell_id": "cell",
        "canonical": "test-only",
        "condition_code": "005",
        "race_identity": None,
        "cutoff": "2021-01-01",
        "history_start": "2007-01-01",
    }


def test_teacher_columns_cannot_be_selected_as_numeric_features() -> None:
    assert trainer.numeric_feature_names(
        pa.schema(
            [
                pa.field("teacher_finish", pa.int64()),
                pa.field("teacher_disposition", pa.int64()),
                pa.field("teacher_relevance", pa.float64()),
                pa.field("speed", pa.float64()),
            ]
        )
    ) == ["speed"]


def test_training_cli_requires_explicit_evidence_files_and_hashes(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="2"):
        trainer.parse_args(
            [
                "--features-root",
                str(tmp_path),
                "--production-plan",
                str(tmp_path / "plan.json"),
                "--output-root",
                str(tmp_path / "out"),
            ]
        )


def test_training_label_preflight_preserves_every_row_and_missing_feature() -> None:
    source = pd.DataFrame(
        {
            "race_id": ["race", "race"],
            "ketto_toroku_bango": ["h2", "h1"],
            "umaban": [2, 1],
            "finish_position": ["2", "1.0"],
            "speed": [float("nan"), 3.0],
        },
        index=[20, 10],
    )
    result = trainer.require_classified_training_rows(source)
    assert result is not source
    assert result.index.tolist() == [20, 10]
    assert result["ketto_toroku_bango"].tolist() == ["h2", "h1"]
    assert result["umaban"].tolist() == [2, 1]
    assert result["finish_position"].tolist() == [2.0, 1.0]
    assert result["speed"].isna().tolist() == [True, False]
    assert result["speed"].iloc[1] == 3.0
    assert source["finish_position"].tolist() == ["2", "1.0"]


def test_training_label_preflight_preserves_empty_input() -> None:
    source = pd.DataFrame(columns=["race_id", "finish_position", "speed"])
    result = trainer.require_classified_training_rows(source)
    assert result is not source
    assert result.empty
    assert result.columns.tolist() == ["race_id", "finish_position", "speed"]


@pytest.mark.parametrize("finish", [np.bool_(True), False, pd.NA, float("nan")])
def test_training_label_preflight_rejects_additional_scalar_boundaries(finish: object) -> None:
    source = pd.DataFrame({"finish_position": pd.Series([finish], dtype=object)})
    with pytest.raises(ValueError, match="authoritative outcome evidence"):
        trainer.require_classified_training_rows(source)
    assert len(source) == 1


def test_split_never_shares_boundary_date() -> None:
    frame = pd.DataFrame(
        {
            "race_id": ["a", "b", "c", "d", "e"],
            "race_date": ["20200101", "20200101", "20200102", "20200102", "20200102"],
        }
    )
    training, validation = trainer.chronological_race_split(frame)
    assert training["race_id"].tolist() == ["a", "b"]
    assert validation["race_id"].tolist() == ["c", "d", "e"]


def test_split_small_cohort_has_no_validation() -> None:
    frame = pd.DataFrame({"race_id": ["a", "b"], "race_date": ["20200101", "20200102"]})
    training, validation = trainer.chronological_race_split(frame)
    assert training["race_id"].tolist() == ["a", "b"]
    assert validation.empty
    assert training is not frame


def test_split_empty_cohort() -> None:
    training, validation = trainer.chronological_race_split(
        pd.DataFrame(columns=["race_id", "race_date"])
    )
    assert training.empty
    assert validation.empty


def test_split_single_day_cannot_fake_temporal_validation() -> None:
    frame = pd.DataFrame({"race_id": ["a", "b", "c", "d"], "race_date": ["20200101"] * 4})
    training, validation = trainer.chronological_race_split(frame)
    assert training["race_id"].tolist() == ["a", "b", "c", "d"]
    assert validation.empty


def test_split_sparse_last_day_retains_nonempty_training() -> None:
    frame = pd.DataFrame(
        {
            "race_id": ["a", "b", "c", "d"],
            "race_date": ["20200101", "20200101", "20200101", "20200102"],
        }
    )
    training, validation = trainer.chronological_race_split(frame)
    assert training["race_id"].tolist() == ["a", "b", "c"]
    assert validation["race_id"].tolist() == ["d"]


def test_split_preserves_shuffled_runner_rows_and_missing_features() -> None:
    frame = pd.DataFrame(
        {
            "race_id": ["d", "a", "c", "b", "d"],
            "race_date": ["20200104", "20200101", "20200103", "20200102", "20200104"],
            "feature": [np.nan, 0, 1, 2, 3],
        }
    )
    training, validation = trainer.chronological_race_split(frame)
    assert training.index.tolist() == [1, 3]
    assert validation.index.tolist() == [0, 2, 4]
    assert pd.isna(validation.loc[0, "feature"])
    assert len(frame) == 5


def test_split_rejects_conflicting_race_dates() -> None:
    frame = pd.DataFrame({"race_id": ["a", "a"], "race_date": ["20200101", "20200102"]})
    with pytest.raises(ValueError, match="exactly one"):
        trainer.chronological_race_split(frame)


@pytest.mark.parametrize("column", ["race_id", "race_date"])
def test_split_rejects_missing_identity_or_date(column: str) -> None:
    frame = pd.DataFrame({"race_id": ["a"], "race_date": ["20200101"]})
    frame[column] = None
    with pytest.raises(ValueError, match="nonmissing"):
        trainer.chronological_race_split(frame)


def test_numeric_feature_schema_contract() -> None:
    schema = pa.schema(
        [
            ("race_id", pa.string()),
            ("finish_position", pa.int32()),
            ("text", pa.string()),
            ("integer", pa.int64()),
            ("float", pa.float32()),
            ("boolean", pa.bool_()),
            ("decimal", pa.decimal128(5, 2)),
        ]
    )
    assert trainer.numeric_feature_names(schema) == ["integer", "float", "boolean", "decimal"]
    assert trainer.numeric_feature_names(iter(schema)) == ["integer", "float", "boolean", "decimal"]


def test_legacy_top3_relevance_is_explicit() -> None:
    np.testing.assert_array_equal(
        trainer.relevance_labels(pd.Series([1, 2, 3, 4, None])), [3, 2, 1, 0, 0]
    )


def test_rank_prediction_ties_and_multiple_races() -> None:
    assert trainer.rank_predictions(["r", "s", "r"], ["b", "c", "a"], [1.0, 3.0, 1.0]) == [2, 1, 1]
    assert trainer.rank_predictions([], [], []) == []


@pytest.mark.parametrize("horses,scores", [(["a"], []), ([], [1.0])])
def test_rank_prediction_alignment(horses: list[str], scores: list[float]) -> None:
    with pytest.raises(ValueError, match="equal lengths"):
        trainer.rank_predictions(["r"], horses, scores)


def test_winner_metrics_do_not_misrepresent_exact_positions() -> None:
    frame = pd.DataFrame(
        {
            "race_id": ["r"] * 5,
            "finish_position": [1, 2, 3, 4, 5],
            "predicted_rank": [2, 1, 5, 3, 4],
        }
    )
    assert trainer.winner_topk_metrics(frame) == {
        "race_count": 1,
        "top1_hits": 0,
        "top1_accuracy": 0.0,
        "top2_hits": 1,
        "top2_accuracy": 1.0,
        "top3_hits": 1,
        "top3_accuracy": 1.0,
        "top4_hits": 1,
        "top4_accuracy": 1.0,
        "top5_hits": 1,
        "top5_accuracy": 1.0,
    }


def test_legacy_winner_metrics_skip_ties_and_undefined_winners() -> None:
    frame = pd.DataFrame(
        {"race_id": ["r", "r", "s"], "finish_position": [1, 1, None], "predicted_rank": [1, 2, 1]}
    )
    assert trainer.winner_topk_metrics(frame) == {
        "race_count": 0,
        "top1_hits": 0,
        "top1_accuracy": 0.0,
        "top2_hits": 0,
        "top2_accuracy": 0.0,
        "top3_hits": 0,
        "top3_accuracy": 0.0,
        "top4_hits": 0,
        "top4_accuracy": 0.0,
        "top5_hits": 0,
        "top5_accuracy": 0.0,
    }


def test_atomic_json_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "data.json"
    trainer.atomic_json(path, {"value": 3})
    assert json.loads(path.read_text(encoding="utf-8")) == {"value": 3}
    assert list(path.parent.glob(".data.json.*")) == []


def test_atomic_json_removes_temporary_file_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_replace(source: str, destination: Path) -> None:
        raise OSError("test replacement failure")

    monkeypatch.setattr(trainer.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replacement failure"):
        trainer.atomic_json(tmp_path / "data.json", {})
    assert list(tmp_path.iterdir()) == []


def test_empty_feature_load_has_schema(dataset: trainer.DatasetLike) -> None:
    assert trainer.load_feature_rows(dataset, [], ["speed"]).columns.tolist() == [
        "race_id",
        "race_date",
        "ketto_toroku_bango",
        "umaban",
        "finish_position",
        "speed",
    ]


def test_feature_matrix_preserves_existing_serving_contract() -> None:
    frame = pd.DataFrame({"speed": pd.Series(["2", None, "invalid"], dtype=object)})
    np.testing.assert_array_equal(trainer.feature_matrix(frame, ["speed"]), [[2], [0], [0]])
    assert frame["speed"].tolist() == ["2", None, "invalid"]


@pytest.mark.parametrize("ids,rows", [(["other"], 1), (["r"], 2)])
def test_target_coverage_rejects_mismatch(ids: list[str], rows: int) -> None:
    with pytest.raises(ValueError, match="coverage mismatch"):
        trainer.validate_target_feature_coverage(pd.DataFrame({"race_id": ["r"]}), ids, rows, "m")


def test_rank_pool_sorted_alignment() -> None:
    frame = pd.DataFrame(
        {"race_id": ["r", "r"], "umaban": [2, 1], "finish_position": [2, 1], "speed": [20, 10]}
    )
    pool = trainer.build_rank_pool(frame, ["speed"], with_labels=True)
    np.testing.assert_array_equal(pool.get_features(), [[10], [20]])
    np.testing.assert_array_equal(pool.get_label(), [3, 2])
    unlabeled = trainer.build_rank_pool(frame, ["speed"], with_labels=False)
    assert unlabeled.num_row() == 2


@pytest.mark.parametrize(
    "evidence", [{}, {"training_scope_is_diverse": True, "training_cross_cell_race_count": 0}]
)
def test_diversity_rejects_missing_or_cross_cell_zero(evidence: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="target-cell-only"):
        trainer.validate_diverse_training_scope(evidence, "m")


def test_complete_counter_rejects_insufficient_retained_races() -> None:
    with pytest.raises(ValueError, match="fewer than 100"):
        trainer.validate_complete_training_races(pd.DataFrame({"race_id": ["r"]}), "m")


@pytest.mark.parametrize(
    "parameters", [{"iterations": 0}, {"depth": 0}, {"learning_rate": 0.0}, {"learning_rate": 1.1}]
)
def test_invalid_training_parameters(parameters: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="invalid JRA training"):
        trainer.resolve_training_parameters(
            {"training_parameters": parameters},
            Namespace(iterations=10, depth=6, learning_rate=0.05),
            "m",
        )


def test_parameter_override() -> None:
    assert trainer.resolve_training_parameters(
        {"training_parameters": {"iterations": 8, "depth": 4, "learning_rate": 0.1}},
        Namespace(iterations=10, depth=6, learning_rate=0.05),
        "m",
    ) == (8, 4, 0.1)


def test_final_iteration_uses_best_zero_based_index() -> None:
    assert trainer.resolve_final_iterations(10, 3, fixed_iterations=False) == 4


def test_final_iteration_fixed_policy_ignores_best_index() -> None:
    assert trainer.resolve_final_iterations(10, 3, fixed_iterations=True) == 10


def test_final_iteration_without_best_index_keeps_requested() -> None:
    assert trainer.resolve_final_iterations(10, None, fixed_iterations=False) == 10


def test_final_iteration_without_valid_best_index_keeps_requested() -> None:
    assert trainer.resolve_final_iterations(10, -1, fixed_iterations=False) == 10


def test_relevance_override_and_default() -> None:
    assert trainer.resolve_cell_relevance_mode({}, Namespace()) == "top3"
    assert (
        trainer.resolve_cell_relevance_mode(
            {"training_parameters": {"relevance_mode": "reciprocal-rank"}}, Namespace()
        )
        == "reciprocal-rank"
    )


def test_invalid_relevance_parameters_object() -> None:
    with pytest.raises(ValueError, match="must be an object"):
        trainer.resolve_cell_relevance_mode({"training_parameters": []}, Namespace())


@pytest.mark.parametrize("override", [{"target_race_ids": ["absent"]}, {"target_runner_count": 3}])
def test_invalid_target_is_rejected_before_any_model_or_artifact(
    teacher_catalog: TeacherCatalog,
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    native_models: list[FakeRanker],
    tmp_path: Path,
    override: dict[str, object],
) -> None:
    cell.update(override)
    with pytest.raises(ValueError, match="target feature coverage mismatch"):
        trainer.train_cell(
            dataset,
            cell,
            ["speed"],
            tmp_path,
            Namespace(iterations=8, depth=2, learning_rate=0.1, thread_count=1),
            teacher_catalog=teacher_catalog,
        )
    assert native_models == []
    assert not (tmp_path / "finish-position").exists()


def test_invalid_target_pool_is_rejected_before_any_model_or_artifact(
    teacher_catalog: TeacherCatalog,
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    native_models: list[FakeRanker],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def rejected_pool(*_args: object, **kwargs: object) -> NoReturn:
        assert kwargs["with_labels"] is False
        raise ValueError("Invalid target pool")

    monkeypatch.setattr(trainer, "build_rank_pool", rejected_pool)
    with pytest.raises(ValueError, match="Invalid target pool"):
        trainer.train_cell(
            dataset,
            cell,
            ["speed"],
            tmp_path,
            Namespace(iterations=8, depth=2, learning_rate=0.1, thread_count=1),
            teacher_catalog=teacher_catalog,
        )
    assert native_models == []
    assert not (tmp_path / "finish-position").exists()


def test_train_cell_uses_date_block_validation_and_best_iteration(
    teacher_catalog: TeacherCatalog,
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    native_models: list[FakeRanker],
    tmp_path: Path,
) -> None:
    result = trainer.train_cell(
        dataset,
        cell,
        ["speed"],
        tmp_path,
        Namespace(iterations=10, depth=6, learning_rate=0.05, thread_count=1),
        teacher_catalog=teacher_catalog,
    )
    assert len(native_models) == 2
    assert native_models[0].fitted_rows == [180]
    assert native_models[1].fitted_rows == [200]
    assert native_models[1].parameters["iterations"] == 4
    assert result["training_rows"] == 200
    assert result["target_rows"] == 2
    artifact = tmp_path / "finish-position/jra/test-only-cell"
    metadata = json.loads((artifact / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["final_iterations"] == 4
    assert (
        metadata["training_finish_contract"] == "supplied-roster-evidence-explicit-status-gains-v1"
    )
    assert metadata["independent_training_roster_attested"] is False
    assert metadata["validation_metrics"]["race_count"] == 10
    assert json.loads((artifact / "predictions.json").read_text(encoding="utf-8")) == [
        {
            "race_id": "target",
            "ketto_toroku_bango": "a",
            "umaban": 1,
            "predicted_score": 0.0,
            "predicted_rank": 2,
        },
        {
            "race_id": "target",
            "ketto_toroku_bango": "b",
            "umaban": 2,
            "predicted_score": 1.0,
            "predicted_rank": 1,
        },
    ]


@pytest.mark.parametrize("finish", [None, 0, -1, float("inf"), float("-inf"), 1.5, True, "unknown"])
def test_unresolved_teacher_is_not_silently_removed_before_training(
    teacher_catalog: TeacherCatalog,
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    native_models: list[FakeRanker],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    finish: object,
) -> None:
    original_loader = trainer.load_feature_rows

    def load_rows(
        source: trainer.DatasetLike, race_ids: Sequence[str], features: Sequence[str]
    ) -> pd.DataFrame:
        loaded = original_loader(source, race_ids, features)
        if "target" not in race_ids:
            loaded["finish_position"] = loaded["finish_position"].astype(object)
            loaded.loc[0, "finish_position"] = finish
        return loaded

    monkeypatch.setattr(trainer, "load_feature_rows", load_rows)
    with pytest.raises(ValueError, match="Feature finish conflicts"):
        trainer.train_cell(
            dataset,
            cell,
            ["speed"],
            tmp_path,
            Namespace(iterations=8, depth=2, learning_rate=0.1, thread_count=1),
            teacher_catalog=teacher_catalog,
        )
    assert native_models == []
    assert not (tmp_path / "finish-position").exists()


def test_train_cell_without_inner_validation_keeps_requested_iterations(
    teacher_catalog: TeacherCatalog,
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    native_models: list[FakeRanker],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_validation(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        return frame.copy(), frame.iloc[0:0].copy()

    monkeypatch.setattr(trainer, "chronological_race_split", no_validation)
    result = trainer.train_cell(
        dataset,
        cell,
        ["speed"],
        tmp_path,
        Namespace(iterations=10, depth=6, learning_rate=0.05, thread_count=1),
        teacher_catalog=teacher_catalog,
    )
    assert result["validation_metrics"] is None
    assert native_models[0].fitted_rows == [200]
    assert native_models[1].parameters["iterations"] == 10


def test_missing_physical_teacher_is_rejected_before_any_native_model(
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    teacher_catalog: TeacherCatalog,
    native_models: list[FakeRanker],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_loader = trainer.load_feature_rows

    def missing_runner(
        source: trainer.DatasetLike,
        race_ids: Sequence[str],
        features: Sequence[str],
    ) -> pd.DataFrame:
        loaded = original_loader(source, race_ids, features)
        return loaded.iloc[1:].copy()

    monkeypatch.setattr(trainer, "load_feature_rows", missing_runner)
    with pytest.raises(ValueError, match="lack feature rows"):
        trainer.train_cell(
            dataset,
            cell,
            ["speed"],
            tmp_path,
            Namespace(iterations=8, depth=2, learning_rate=0.1, thread_count=1),
            teacher_catalog=teacher_catalog,
        )
    assert native_models == []
    assert not (tmp_path / "finish-position").exists()


def test_unknown_independent_declaration_blocks_actual_trainer(
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    teacher_catalog: TeacherCatalog,
    native_models: list[FakeRanker],
    tmp_path: Path,
) -> None:
    unknown = TeacherCatalog(
        teacher_catalog.references,
        {
            **teacher_catalog.races,
            "0": TeacherRaceEvidence("0", None, teacher_catalog.races["0"].outcomes),
        },
    )
    with pytest.raises(ValueError, match="starter_count_unverified"):
        trainer.train_cell(
            dataset,
            cell,
            ["speed"],
            tmp_path,
            Namespace(iterations=8, depth=2, learning_rate=0.1, thread_count=1),
            teacher_catalog=unknown,
        )
    assert native_models == []
    assert not (tmp_path / "finish-position").exists()


def test_actual_trainer_retains_explicit_dnf_with_zero_gain_not_zero_finish(
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    teacher_catalog: TeacherCatalog,
    native_models: list[FakeRanker],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    explicit = TeacherCatalog(
        teacher_catalog.references,
        {
            **teacher_catalog.races,
            "0": TeacherRaceEvidence(
                "0",
                2,
                (
                    RunnerOutcome("a", 1, "classified", 1),
                    RunnerOutcome("b", 2, "dnf", None),
                ),
            ),
        },
    )
    original_loader = trainer.load_feature_rows

    def nonfinish_rows(
        source: trainer.DatasetLike,
        race_ids: Sequence[str],
        features: Sequence[str],
    ) -> pd.DataFrame:
        loaded = original_loader(source, race_ids, features)
        loaded["finish_position"] = loaded["finish_position"].astype(object)
        loaded.loc[
            loaded.race_id.eq("0") & loaded.ketto_toroku_bango.eq("b"), "finish_position"
        ] = None
        return loaded

    monkeypatch.setattr(trainer, "load_feature_rows", nonfinish_rows)
    result = trainer.train_cell(
        dataset,
        cell,
        ["speed"],
        tmp_path,
        Namespace(iterations=8, depth=2, learning_rate=0.1, thread_count=1),
        teacher_catalog=explicit,
    )
    assert result["training_rows"] == 200
    assert native_models[1].fitted_rows == [200]
    assert native_models[1].fitted_labels[0][:2] == [3.0, 0.0]
    assert explicit.races["0"].outcomes[1].finish is None


def test_cli_main_writes_report(
    teacher_cli_args: list[str],
    dataset: trainer.DatasetLike,
    cell: dict[str, object],
    native_models: list[FakeRanker],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text(
        json.dumps({"version": "test-only", "target_date": "2021-01-01", "cells": [cell]}),
        encoding="utf-8",
    )

    def load_dataset(path: Path, *, format: str, partitioning: str) -> trainer.DatasetLike:
        return dataset

    monkeypatch.setattr(trainer.ds, "dataset", load_dataset)
    assert (
        trainer.main(
            [
                "--features-root",
                str(tmp_path),
                "--production-plan",
                str(plan),
                "--output-root",
                str(tmp_path / "out"),
                "--iterations",
                "10",
                "--thread-count",
                "1",
                "--condition-code",
                "005",
                *teacher_cli_args,
            ]
        )
        == 0
    )
    assert len(native_models) == 2
    report = json.loads((tmp_path / "out/training-20210101.json").read_text(encoding="utf-8"))
    assert report["model_count"] == 1


def test_cli_rejects_no_numeric_features(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    teacher_cli_args: list[str],
) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text(
        json.dumps({"version": "test-only", "target_date": "2021-01-01", "cells": []}),
        encoding="utf-8",
    )
    empty_dataset = ds.dataset(pa.table({"race_id": ["r"]}))

    def load_dataset(path: Path, *, format: str, partitioning: str) -> trainer.DatasetLike:
        return empty_dataset

    monkeypatch.setattr(trainer.ds, "dataset", load_dataset)
    with pytest.raises(ValueError, match="no numeric model features"):
        trainer.main(
            [
                "--features-root",
                str(tmp_path),
                "--production-plan",
                str(plan),
                "--output-root",
                str(tmp_path / "out"),
                *teacher_cli_args,
            ]
        )
