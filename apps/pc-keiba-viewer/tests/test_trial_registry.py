from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import trial_registry as subject


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    subject.ensure_schema(con)
    return con


def _cell(
    class_code: str = "G1",
    subgroup_dimension: str = "distance_band",
    subgroup_value: str = "sprint",
    season_band: str = "spring",
) -> subject.CellKey:
    return subject.CellKey(
        class_code=class_code,
        subgroup_dimension=subgroup_dimension,
        subgroup_value=subgroup_value,
        season_band=season_band,
    )


def _accuracies() -> dict[str, float]:
    return {
        "rank1_accuracy": 0.5,
        "rank2_accuracy": 0.4,
        "rank3_accuracy": 0.35,
        "rank4_accuracy": 0.3,
        "rank5_accuracy": 0.25,
        "rank6_accuracy": 0.2,
        "top1_accuracy": 0.5,
        "place2_accuracy": 0.6,
        "place3_accuracy": 0.7,
        "ndcg_at_3": 0.55,
        "race_count": 1000.0,
        "rank1_lb95": 0.45,
        "rank2_lb95": 0.35,
        "rank3_lb95": 0.3,
        "rank4_lb95": 0.25,
        "rank5_lb95": 0.2,
        "rank6_lb95": 0.15,
    }


def test_compute_feature_set_hash_is_order_independent() -> None:
    assert subject.compute_feature_set_hash(
        ["feat_b", "feat_a", "feat_c"]
    ) == subject.compute_feature_set_hash(["feat_a", "feat_c", "feat_b"])


def test_compute_feature_set_hash_differs_for_different_features() -> None:
    assert subject.compute_feature_set_hash(
        ["feat_a", "feat_b"]
    ) != subject.compute_feature_set_hash(["feat_a", "feat_c"])


def test_compute_feature_set_hash_length_is_sixteen() -> None:
    assert len(subject.compute_feature_set_hash(["feat_a"])) == 16


def test_compute_feature_set_hash_handles_unicode_names() -> None:
    first = subject.compute_feature_set_hash(["距離", "馬体重"])
    second = subject.compute_feature_set_hash(["馬体重", "距離"])
    assert first == second


def test_ensure_schema_creates_trials_table() -> None:
    con = duckdb.connect(":memory:")
    subject.ensure_schema(con)
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'trials'"
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 1


def test_ensure_schema_creates_dedup_index() -> None:
    con = duckdb.connect(":memory:")
    subject.ensure_schema(con)
    row = con.execute(
        "SELECT count(*) FROM duckdb_indexes() WHERE index_name = 'idx_trials_dedup'"
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 1


def test_ensure_schema_is_idempotent() -> None:
    con = duckdb.connect(":memory:")
    subject.ensure_schema(con)
    subject.ensure_schema(con)
    row = con.execute("SELECT count(*) FROM trials").fetchone()
    assert row is not None
    assert int(row[0]) == 0


def test_register_trial_then_trial_exists_returns_true() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a", "feat_b"],
        _accuracies(),
    )
    feature_hash = subject.compute_feature_set_hash(["feat_a", "feat_b"])
    assert (
        subject.trial_exists(
            con, "jra", "G1", "distance_band", "sprint", "spring", feature_hash
        )
        is True
    )


def test_trial_exists_returns_false_for_unknown_combo() -> None:
    con = _con()
    feature_hash = subject.compute_feature_set_hash(["feat_a"])
    assert (
        subject.trial_exists(
            con, "jra", "G1", "distance_band", "sprint", "spring", feature_hash
        )
        is False
    )


def test_register_trial_stored_feature_count() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a", "feat_b", "feat_c"],
        _accuracies(),
    )
    row = con.execute(
        "SELECT feature_count FROM trials WHERE trial_id = 'trial-1'"
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 3


def test_register_trial_persists_optional_metadata() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
        verdict="ADOPT",
        verdict_reason="strong gain",
        model_version="iter20",
        train_window_start="2013",
        train_window_end="2025",
        blind_year=2025,
        focus_features=["feat_a"],
        exclude_features=["feat_x"],
    )
    row = con.execute(
        "SELECT verdict, verdict_reason, model_version, train_window_start, "
        "train_window_end, blind_year, focus_features, exclude_features "
        "FROM trials WHERE trial_id = 'trial-1'"
    ).fetchone()
    assert row is not None
    assert row[0] == "ADOPT"
    assert row[1] == "strong gain"
    assert row[2] == "iter20"
    assert row[3] == "2013"
    assert row[4] == "2025"
    assert int(row[5]) == 2025
    assert row[6] == '["feat_a"]'
    assert row[7] == '["feat_x"]'


def test_register_trial_defaults_null_optional_lists() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    row = con.execute(
        "SELECT focus_features, exclude_features, blind_year "
        "FROM trials WHERE trial_id = 'trial-1'"
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] is None
    assert row[2] is None


def test_register_trial_duplicate_cell_and_hash_raises() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a", "feat_b"],
        _accuracies(),
    )
    with pytest.raises(ValueError):
        subject.register_trial(
            con,
            "trial-2",
            "jra",
            "G1",
            "distance_band",
            "sprint",
            "spring",
            ["feat_b", "feat_a"],
            _accuracies(),
        )


def test_register_trial_same_cell_different_features_allowed() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    subject.register_trial(
        con,
        "trial-2",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a", "feat_b"],
        _accuracies(),
    )
    row = con.execute("SELECT count(*) FROM trials").fetchone()
    assert row is not None
    assert int(row[0]) == 2


def test_find_duplicate_cells_returns_only_existing() -> None:
    con = _con()
    feature_hash = subject.compute_feature_set_hash(["feat_a"])
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    cells = [
        _cell(class_code="G1"),
        _cell(class_code="G2"),
    ]
    duplicates = subject.find_duplicate_cells(con, "jra", feature_hash, cells)
    assert duplicates == [_cell(class_code="G1")]


def test_find_duplicate_cells_empty_when_none_registered() -> None:
    con = _con()
    feature_hash = subject.compute_feature_set_hash(["feat_a"])
    duplicates = subject.find_duplicate_cells(
        con, "jra", feature_hash, [_cell(), _cell(class_code="G2")]
    )
    assert duplicates == []


def test_get_untested_cells_excludes_registered() -> None:
    con = _con()
    feature_hash = subject.compute_feature_set_hash(["feat_a"])
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    all_cells = [
        _cell(class_code="G1"),
        _cell(class_code="G2"),
        _cell(class_code="G3"),
    ]
    untested = subject.get_untested_cells(con, "jra", feature_hash, all_cells)
    assert untested == [_cell(class_code="G2"), _cell(class_code="G3")]


def test_get_untested_cells_returns_all_when_none_registered() -> None:
    con = _con()
    feature_hash = subject.compute_feature_set_hash(["feat_a"])
    all_cells = [_cell(class_code="G1"), _cell(class_code="G2")]
    untested = subject.get_untested_cells(con, "jra", feature_hash, all_cells)
    assert untested == all_cells


def test_search_similar_accuracy_orders_by_similarity() -> None:
    con = _con()
    near = _accuracies()
    far = _accuracies()
    far.update(
        {
            "rank1_accuracy": 0.01,
            "rank2_accuracy": 0.5,
            "rank3_accuracy": 0.02,
            "rank4_accuracy": 0.5,
            "rank5_accuracy": 0.01,
            "rank6_accuracy": 0.5,
        }
    )
    subject.register_trial(
        con,
        "trial-near",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        near,
    )
    subject.register_trial(
        con,
        "trial-far",
        "jra",
        "G2",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        far,
    )
    results = subject.search_similar_accuracy(
        con, "jra", [0.5, 0.4, 0.35, 0.3, 0.25, 0.2]
    )
    assert results[0]["trial_id"] == "trial-near"
    assert results[1]["trial_id"] == "trial-far"


def test_search_similar_accuracy_respects_top_k() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    subject.register_trial(
        con,
        "trial-2",
        "jra",
        "G2",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_similar_accuracy(
        con, "jra", [0.5, 0.4, 0.35, 0.3, 0.25, 0.2], top_k=1
    )
    assert len(results) == 1


def test_search_similar_accuracy_skips_null_vectors() -> None:
    con = _con()
    empty: dict[str, float] = {"top1_accuracy": 0.5}
    subject.register_trial(
        con,
        "trial-null",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        empty,
    )
    results = subject.search_similar_accuracy(
        con, "jra", [0.5, 0.4, 0.35, 0.3, 0.25, 0.2]
    )
    assert results == []


def test_search_similar_accuracy_returns_similarity_field() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_similar_accuracy(
        con, "jra", [0.5, 0.4, 0.35, 0.3, 0.25, 0.2]
    )
    assert results[0]["similarity"] == pytest.approx(1.0)
    assert results[0]["rank1_accuracy"] == 0.5
    assert results[0]["verdict"] == "PENDING"


def test_search_similar_accuracy_filters_by_category() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-jra",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    subject.register_trial(
        con,
        "trial-nar",
        "nar",
        "A",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_similar_accuracy(
        con, "nar", [0.5, 0.4, 0.35, 0.3, 0.25, 0.2]
    )
    assert len(results) == 1
    assert results[0]["trial_id"] == "trial-nar"


def test_search_by_cell_filters_class_code() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-g1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    subject.register_trial(
        con,
        "trial-g2",
        "jra",
        "G2",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_by_cell(con, "jra", class_code="G2")
    assert len(results) == 1
    assert results[0]["trial_id"] == "trial-g2"


def test_search_by_cell_wildcard_returns_all_in_category() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-g1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    subject.register_trial(
        con,
        "trial-g2",
        "jra",
        "G2",
        "distance_band",
        "mile",
        "summer",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_by_cell(con, "jra")
    assert len(results) == 2


def test_search_by_cell_filters_subgroup_value() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-sprint",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    subject.register_trial(
        con,
        "trial-mile",
        "jra",
        "G1",
        "distance_band",
        "mile",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_by_cell(con, "jra", subgroup_value="mile")
    assert len(results) == 1
    assert results[0]["subgroup_value"] == "mile"


def test_search_by_cell_filters_season_band() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-spring",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    subject.register_trial(
        con,
        "trial-summer",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "summer",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_by_cell(con, "jra", season_band="summer")
    assert len(results) == 1
    assert results[0]["season_band"] == "summer"


def test_search_by_cell_filters_verdict() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-adopt",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
        verdict="ADOPT",
    )
    subject.register_trial(
        con,
        "trial-reject",
        "jra",
        "G2",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
        verdict="REJECT",
    )
    results = subject.search_by_cell(con, "jra", verdict="ADOPT")
    assert len(results) == 1
    assert results[0]["verdict"] == "ADOPT"


def test_search_by_cell_empty_when_no_match() -> None:
    con = _con()
    subject.register_trial(
        con,
        "trial-1",
        "jra",
        "G1",
        "distance_band",
        "sprint",
        "spring",
        ["feat_a"],
        _accuracies(),
    )
    results = subject.search_by_cell(con, "jra", class_code="G9")
    assert results == []


def test_connect_creates_file_and_schema(tmp_path: Path) -> None:
    con = subject.connect("jra", base_dir=str(tmp_path))
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'trials'"
    ).fetchone()
    con.close()
    assert (tmp_path / "trial_registry_jra.duckdb").exists()
    assert row is not None
    assert int(row[0]) == 1


def test_connect_read_only_does_not_create_schema(tmp_path: Path) -> None:
    writer = subject.connect("nar", base_dir=str(tmp_path))
    writer.close()
    reader = subject.connect("nar", base_dir=str(tmp_path), read_only=True)
    row = reader.execute("SELECT count(*) FROM trials").fetchone()
    reader.close()
    assert row is not None
    assert int(row[0]) == 0


def test_round_trip_register_search_and_dedup(tmp_path: Path) -> None:
    con = subject.connect("ban-ei", base_dir=str(tmp_path))
    feature_names = ["futan", "grade_career"]
    subject.register_trial(
        con,
        "trial-a",
        "ban-ei",
        "A",
        "distance_band",
        "sprint",
        "spring",
        feature_names,
        _accuracies(),
        verdict="ADOPT",
    )
    subject.register_trial(
        con,
        "trial-b",
        "ban-ei",
        "B",
        "distance_band",
        "sprint",
        "spring",
        feature_names,
        _accuracies(),
        verdict="REJECT",
    )
    feature_hash = subject.compute_feature_set_hash(feature_names)
    all_cells = [
        _cell(class_code="A"),
        _cell(class_code="B"),
        _cell(class_code="C"),
    ]
    untested = subject.get_untested_cells(con, "ban-ei", feature_hash, all_cells)
    similar = subject.search_similar_accuracy(
        con, "ban-ei", [0.5, 0.4, 0.35, 0.3, 0.25, 0.2]
    )
    adopted = subject.search_by_cell(con, "ban-ei", verdict="ADOPT")
    con.close()
    assert untested == [_cell(class_code="C")]
    assert len(similar) == 2
    assert len(adopted) == 1
    assert adopted[0]["trial_id"] == "trial-a"
