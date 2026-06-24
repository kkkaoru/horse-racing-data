from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import learning.feature_registry as subject


def test_get_best_ndcg_on_empty_registry_returns_zero() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg.get_best_ndcg() == 0.0


def test_get_active_entry_on_empty_registry_returns_none() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg.get_active_entry() is None


def test_record_trial_first_returns_id_one() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        entry_id = reg.record_trial("trial-1", 0.5, ["feat_a"])
        assert entry_id == 1


def test_record_trial_second_returns_id_two() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        entry_id = reg.record_trial("trial-2", 0.6, ["feat_b"])
        assert entry_id == 2


def test_get_best_ndcg_after_two_trials_returns_max() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.record_trial("trial-2", 0.8, ["feat_b"])
        assert reg.get_best_ndcg() == 0.8


def test_activate_sets_is_active_true_and_clears_others() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        id1 = reg.record_trial("trial-1", 0.5, ["feat_a"])
        id2 = reg.record_trial("trial-2", 0.8, ["feat_b"])
        reg.activate(id1)
        reg.activate(id2)
        active = reg.get_active_entry()
        assert active is not None
        assert active["id"] == id2


def test_maybe_promote_when_ndcg_exceeds_threshold_promotes_and_returns_true() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        promoted = reg.maybe_promote("trial-2", 0.62, ["feat_b"], threshold=0.01)
        assert promoted is True
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-2"


def test_maybe_promote_when_ndcg_does_not_exceed_threshold_records_but_no_promote() -> (
    None
):
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        promoted = reg.maybe_promote("trial-2", 0.505, ["feat_b"], threshold=0.01)
        assert promoted is False
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-1"
        assert len(reg.list_trials(limit=10)) == 2


def test_list_trials_returns_sorted_by_ndcg_desc() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.3, ["feat_a"])
        reg.record_trial("trial-2", 0.9, ["feat_b"])
        reg.record_trial("trial-3", 0.6, ["feat_c"])
        trials = reg.list_trials(limit=10)
        assert len(trials) == 3
        assert trials[0]["ndcg_at_3"] == 0.9
        assert trials[1]["ndcg_at_3"] == 0.6
        assert trials[2]["ndcg_at_3"] == 0.3


def test_list_trials_respects_limit() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.3, ["feat_a"])
        reg.record_trial("trial-2", 0.9, ["feat_b"])
        reg.record_trial("trial-3", 0.6, ["feat_c"])
        trials = reg.list_trials(limit=2)
        assert len(trials) == 2


def test_get_active_entry_returns_activated_entry_with_correct_fields() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        entry_id = reg.record_trial("trial-x", 0.77, ["feat_x", "feat_y"], '{"k": "v"}')
        reg.activate(entry_id)
        active = reg.get_active_entry()
        assert active is not None
        assert active["id"] == entry_id
        assert active["trial_id"] == "trial-x"
        assert active["ndcg_at_3"] == 0.77
        assert active["is_active"] is True
        assert active["feature_names"] == ["feat_x", "feat_y"]
        assert active["definition_json"] == '{"k": "v"}'
        assert active["created_at"] != ""


def test_context_manager_enter_exit_works() -> None:
    reg = subject.FeatureRegistry(Path(":memory:"))
    with reg as r:
        r.record_trial("trial-1", 0.5, ["feat_a"])
        assert r.get_best_ndcg() == 0.5
    assert reg._con is None


def test_feature_names_roundtrip_list_to_json_to_list() -> None:
    names = ["feature_alpha", "feature_beta", "feature_gamma"]
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, names)
        trials = reg.list_trials(limit=1)
        assert trials[0]["feature_names"] == names


def test_row_to_entry_via_list_trials_has_correct_types() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.42, ["f1"])
        trials = reg.list_trials(limit=1)
        entry = trials[0]
        assert isinstance(entry["id"], int)
        assert isinstance(entry["trial_id"], str)
        assert isinstance(entry["ndcg_at_3"], float)
        assert isinstance(entry["is_active"], bool)
        assert isinstance(entry["feature_names"], list)
        assert isinstance(entry["definition_json"], str)
        assert isinstance(entry["created_at"], str)


def test_close_called_twice_is_safe() -> None:
    reg = subject.FeatureRegistry(Path(":memory:"))
    reg.open()
    reg.close()
    reg.close()
    assert reg._con is None


def test_maybe_promote_first_trial_always_promotes() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        promoted = reg.maybe_promote("trial-1", 0.05, ["feat_a"], threshold=0.01)
        assert promoted is True
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-1"


def test_maybe_promote_exactly_at_threshold_does_not_promote() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        promoted = reg.maybe_promote("trial-2", 0.51, ["feat_b"], threshold=0.01)
        assert promoted is False
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-1"


def test_activate_multiple_times_only_last_is_active() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        id1 = reg.record_trial("trial-1", 0.5, ["feat_a"])
        id2 = reg.record_trial("trial-2", 0.6, ["feat_b"])
        id3 = reg.record_trial("trial-3", 0.7, ["feat_c"])
        reg.activate(id1)
        reg.activate(id2)
        reg.activate(id3)
        active = reg.get_active_entry()
        assert active is not None
        assert active["id"] == id3
        # list_trials returns DESC by ndcg: 0.7(id3), 0.6(id2), 0.5(id1)
        trials = reg.list_trials(limit=10)
        assert trials[0]["is_active"] is True
        assert trials[1]["is_active"] is False
        assert trials[2]["is_active"] is False


def test_record_trial_default_definition_json_is_empty_object() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        trials = reg.list_trials(limit=1)
        assert trials[0]["definition_json"] == "{}"


def test_list_trials_empty_registry_returns_empty_list() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg.list_trials() == []


def test_get_deployed_ndcg_on_empty_registry_returns_zero() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg.get_deployed_ndcg() == 0.0


def test_record_deployment_stores_ndcg_and_feature_count() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_deployment(0.75, 50)
        assert reg.get_deployed_ndcg() == 0.75


def test_record_deployment_multiple_get_deployed_ndcg_returns_most_recent() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_deployment(0.60, 30)
        reg.record_deployment(0.80, 45)
        reg.record_deployment(0.70, 40)
        assert reg.get_deployed_ndcg() == 0.70


def test_next_deployment_id_increments_each_time() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_deployment(0.50, 10)
        reg.record_deployment(0.60, 20)
        assert reg.get_deployed_ndcg() == 0.60


def test_record_deployment_stores_timestamp() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_deployment(0.55, 25)
        assert reg._con is not None
        row = reg._con.execute("SELECT deployed_at FROM deployments").fetchone()
        assert row is not None
        assert row[0] != ""


def test_sync_sequence_skips_past_manually_inserted_ids() -> None:
    """_sync_sequence_to_table advances sequence past current max id."""
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg._con is not None
        # Bypass sequence by inserting a row with a high id (simulates old-code row)
        reg._con.execute(
            "INSERT INTO feature_trials VALUES (100, 't0', 0.9, FALSE, '[]', '{}', '2024-01-01')"
        )
        reg._sync_sequence_to_table("seq_feature_trials_id", "feature_trials")
        next_id = reg.record_trial("trial-new", 0.6, ["feat_a"])
        assert next_id == 101


def test_maybe_promote_compares_against_active_not_global_max() -> None:
    # A previously recorded but NEVER activated trial with high NDCG must not
    # block future promotions that beat the current active entry.
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        # Record a high-NDCG trial and leave it inactive.
        reg.record_trial("trial-high", 0.9, ["feat_h"])
        # Activate a lower-NDCG trial as the current production model.
        id_low = reg.record_trial("trial-low", 0.5, ["feat_l"])
        reg.activate(id_low)
        # Now promote with 0.62, which beats active (0.5+0.01) but not global max (0.9+0.01).
        promoted = reg.maybe_promote("trial-new", 0.62, ["feat_n"], threshold=0.01)
        assert promoted is True
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-new"


def test_maybe_promote_uses_supplied_active_ndcg_for_decision() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        promoted = reg.maybe_promote(
            "trial-2", 0.62, ["feat_b"], threshold=0.01, active_ndcg=0.6
        )
        assert promoted is True
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-2"


def test_maybe_promote_supplied_active_ndcg_overrides_db_active() -> None:
    # A supplied active_ndcg of 0.99 makes 0.62 fail the threshold even though the
    # real DB active is only 0.5; this proves the param is authoritative over the DB.
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        promoted = reg.maybe_promote(
            "trial-2", 0.62, ["feat_b"], threshold=0.01, active_ndcg=0.99
        )
        assert promoted is False
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-1"


def test_maybe_promote_supplied_active_ndcg_skips_internal_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        calls: list[int] = []
        real_current_active = reg._current_active_ndcg
        def counting_current_active() -> float:
            calls.append(1)
            return real_current_active()
        monkeypatch.setattr(reg, "_current_active_ndcg", counting_current_active)
        reg.maybe_promote(
            "trial-2", 0.62, ["feat_b"], threshold=0.01, active_ndcg=0.5
        )
        assert calls == []


def test_maybe_promote_without_active_ndcg_queries_internal_active() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        promoted = reg.maybe_promote("trial-2", 0.62, ["feat_b"], threshold=0.01)
        assert promoted is True
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-2"


def test_current_active_ndcg_returns_zero_on_empty_registry() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg._current_active_ndcg() == 0.0


def test_current_active_ndcg_returns_active_entry_ndcg() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.73, ["feat_a"])
        reg.activate(1)
        assert reg._current_active_ndcg() == 0.73


def test_maybe_promote_rolls_back_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        def broken_record_trial(*args: object, **kwargs: object) -> int:
            raise RuntimeError("injected failure")
        monkeypatch.setattr(reg, "record_trial", broken_record_trial)
        with pytest.raises(RuntimeError, match="injected failure"):
            reg.maybe_promote("t1", 0.9, ["f"])
        assert reg.get_best_ndcg() == 0.0


def test_maybe_promote_rolls_back_insert_when_activate_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        def broken_activate(*args: object, **kwargs: object) -> None:
            raise RuntimeError("injected activate failure")
        monkeypatch.setattr(reg, "activate", broken_activate)
        with pytest.raises(RuntimeError, match="injected activate failure"):
            reg.maybe_promote("t1", 0.9, ["f"])
        assert reg.get_best_ndcg() == 0.0


def test_inverse_trials_table_created() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg._con is not None
        rows = reg._con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'inverse_trials' ORDER BY ordinal_position"
        ).fetchall()
        columns = [row[0] for row in rows]
        assert columns == [
            "id",
            "original_trial_id",
            "inverse_name",
            "approach_type",
            "delta_pp_json",
            "decision",
            "created_at",
        ]


def test_record_inverse_trial_first_returns_id_one() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        entry_id = reg.record_inverse_trial(
            "orig-1", "orig-1__feature_negate", "feature_negate", {"top1": -1.2}, "REJECT"
        )
        assert entry_id == 1


def test_record_inverse_trial_stores_fields() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_inverse_trial(
            "orig-1", "orig-1__weight_invert", "weight_invert", {"top1": 0.4}, "ADOPT"
        )
        assert reg._con is not None
        row = reg._con.execute(
            "SELECT original_trial_id, inverse_name, approach_type, delta_pp_json, decision "
            "FROM inverse_trials"
        ).fetchone()
        assert row is not None
        assert row[0] == "orig-1"
        assert row[1] == "orig-1__weight_invert"
        assert row[2] == "weight_invert"
        assert row[3] == '{"top1": 0.4}'
        assert row[4] == "ADOPT"


def test_record_inverse_trial_stores_timestamp() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_inverse_trial(
            "orig-1", "orig-1__feature_negate", "feature_negate", {}, "REJECT"
        )
        assert reg._con is not None
        row = reg._con.execute("SELECT created_at FROM inverse_trials").fetchone()
        assert row is not None
        assert row[0] != ""


def test_has_inverse_been_tried_false_then_true() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg.has_inverse_been_tried("orig-1", "orig-1__feature_negate") is False
        reg.record_inverse_trial(
            "orig-1", "orig-1__feature_negate", "feature_negate", {}, "REJECT"
        )
        assert reg.has_inverse_been_tried("orig-1", "orig-1__feature_negate") is True


def test_has_inverse_been_tried_distinguishes_by_inverse_name() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_inverse_trial(
            "orig-1", "orig-1__feature_negate", "feature_negate", {}, "REJECT"
        )
        assert reg.has_inverse_been_tried("orig-1", "orig-1__weight_invert") is False


def test_record_inverse_trial_unique_constraint_raises() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_inverse_trial(
            "orig-1", "orig-1__feature_negate", "feature_negate", {}, "REJECT"
        )
        with pytest.raises(duckdb.ConstraintException):
            reg.record_inverse_trial(
                "orig-1", "orig-1__feature_negate", "feature_negate", {}, "ADOPT"
            )


def test_list_strongly_negative_trials_returns_scalar_delta_below_threshold() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("strong-neg", 0.4, ["f"], '{"delta_pp": -1.5}')
        reg.record_trial("mild-neg", 0.4, ["f"], '{"delta_pp": -0.3}')
        result = reg.list_strongly_negative_trials(-1.0)
        assert len(result) == 1
        assert result[0]["trial_id"] == "strong-neg"


def test_list_strongly_negative_trials_uses_min_of_dict_delta() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("dict-neg", 0.4, ["f"], '{"delta_pp": {"top1": 0.2, "place2": -1.3}}')
        result = reg.list_strongly_negative_trials(-1.0)
        assert len(result) == 1
        assert result[0]["trial_id"] == "dict-neg"


def test_list_strongly_negative_trials_excludes_when_no_delta_pp() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("no-delta", 0.4, ["f"], '{"features": ["f"]}')
        result = reg.list_strongly_negative_trials(-1.0)
        assert result == []


def test_list_strongly_negative_trials_includes_value_equal_to_threshold() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("at-threshold", 0.4, ["f"], '{"delta_pp": -1.0}')
        result = reg.list_strongly_negative_trials(-1.0)
        assert len(result) == 1
        assert result[0]["trial_id"] == "at-threshold"


def test_list_strongly_negative_trials_empty_when_none_negative() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("positive", 0.7, ["f"], '{"delta_pp": 0.5}')
        assert reg.list_strongly_negative_trials(-1.0) == []


def test_list_strongly_negative_trials_excludes_dict_min_above_threshold() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("dict-above", 0.4, ["f"], '{"delta_pp": {"a": -0.5, "b": 0.1}}')
        assert reg.list_strongly_negative_trials(-1.0) == []


def test_list_strongly_negative_trials_excludes_non_dict_root_for_negative_threshold() -> (
    None
):
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("array-root", 0.4, ["f"], "[1, 2, 3]")
        assert reg.list_strongly_negative_trials(-1.0) == []


def test_list_strongly_negative_trials_excludes_bool_scalar_for_negative_threshold() -> (
    None
):
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("bool-delta", 0.4, ["f"], '{"delta_pp": true}')
        assert reg.list_strongly_negative_trials(-1.0) == []


def test_list_strongly_negative_trials_includes_dict_with_bool_ignored() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("dict-bool", 0.4, ["f"], '{"delta_pp": {"flag": true, "score": -2.0}}')
        result = reg.list_strongly_negative_trials(-1.0)
        assert len(result) == 1
        assert result[0]["trial_id"] == "dict-bool"


def test_list_strongly_negative_trials_excludes_empty_dict_delta() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("dict-empty", 0.4, ["f"], '{"delta_pp": {}}')
        assert reg.list_strongly_negative_trials(-1.0) == []


def test_list_strongly_negative_trials_zero_fallback_kept_for_positive_threshold() -> (
    None
):
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("no-delta", 0.4, ["f"], '{"features": ["f"]}')
        result = reg.list_strongly_negative_trials(0.0)
        assert len(result) == 1
        assert result[0]["trial_id"] == "no-delta"


def test_list_strongly_negative_trials_mix_returns_ascending_ids_and_exact_set() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        id_scalar = reg.record_trial("scalar-neg", 0.4, ["f"], '{"delta_pp": -1.5}')
        reg.record_trial("scalar-mild", 0.4, ["f"], '{"delta_pp": -0.3}')
        id_dict = reg.record_trial(
            "dict-neg", 0.4, ["f"], '{"delta_pp": {"top1": 0.2, "place2": -1.3}}'
        )
        reg.record_trial("no-delta", 0.4, ["f"], '{"features": ["f"]}')
        id_at = reg.record_trial("at-threshold", 0.4, ["f"], '{"delta_pp": -1.0}')
        result = reg.list_strongly_negative_trials(-1.0)
        ids = [entry["id"] for entry in result]
        assert ids == [id_scalar, id_dict, id_at]
        assert ids == sorted(ids)


def test_list_strongly_negative_trials_equivalent_to_full_scan_reference() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("a", 0.4, ["f"], '{"delta_pp": -1.5}')
        reg.record_trial("b", 0.4, ["f"], '{"delta_pp": {"top1": 0.2, "place2": -1.3}}')
        reg.record_trial("c", 0.4, ["f"], '{"delta_pp": {"a": -0.5, "b": 0.1}}')
        reg.record_trial("d", 0.4, ["f"], '{"features": ["f"]}')
        reg.record_trial("e", 0.4, ["f"], "[1, 2, 3]")
        reg.record_trial("f", 0.4, ["f"], '{"delta_pp": -1.0}')
        reg.record_trial("g", 0.4, ["f"], '{"delta_pp": true}')
        assert reg._con is not None
        all_rows = reg._con.execute(
            "SELECT id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at "
            "FROM feature_trials ORDER BY id"
        ).fetchall()
        reference = [
            subject._row_to_entry(row)
            for row in all_rows
            if subject._min_delta_pp(str(row[5])) <= -1.0
        ]
        assert reg.list_strongly_negative_trials(-1.0) == reference


def test_list_untried_inverses_returns_all_when_none_tried() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        result = reg.list_untried_inverses("orig-1")
        assert result == [
            "feature_negate",
            "weight_invert",
            "window_invert",
            "anti_correlation",
        ]


def test_list_untried_inverses_excludes_tried_approaches() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_inverse_trial(
            "orig-1", "orig-1__feature_negate", "feature_negate", {}, "REJECT"
        )
        reg.record_inverse_trial(
            "orig-1", "orig-1__window_invert", "window_invert", {}, "REJECT"
        )
        result = reg.list_untried_inverses("orig-1")
        assert result == ["weight_invert", "anti_correlation"]


def test_list_untried_inverses_isolates_by_original_trial() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_inverse_trial(
            "orig-1", "orig-1__feature_negate", "feature_negate", {}, "REJECT"
        )
        result = reg.list_untried_inverses("orig-2")
        assert result == [
            "feature_negate",
            "weight_invert",
            "window_invert",
            "anti_correlation",
        ]


def test_min_delta_pp_returns_zero_for_non_dict_json() -> None:
    assert subject._min_delta_pp("[1, 2, 3]") == 0.0


def test_min_delta_pp_ignores_bool_scalar_delta() -> None:
    assert subject._min_delta_pp('{"delta_pp": true}') == 0.0


def test_min_delta_pp_ignores_bool_values_in_dict_delta() -> None:
    assert subject._min_delta_pp('{"delta_pp": {"flag": true, "score": -2.0}}') == -2.0


def test_min_delta_pp_returns_zero_for_empty_dict_delta() -> None:
    assert subject._min_delta_pp('{"delta_pp": {}}') == 0.0


def test_record_inverse_trial_sync_skips_past_manual_ids() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg._con is not None
        reg._con.execute(
            "INSERT INTO inverse_trials VALUES "
            "(50, 'orig-0', 'orig-0__feature_negate', 'feature_negate', '{}', 'REJECT', '2024-01-01')"
        )
        reg._sync_sequence_to_table("seq_inverse_trials_id", "inverse_trials")
        next_id = reg.record_inverse_trial(
            "orig-1", "orig-1__weight_invert", "weight_invert", {}, "ADOPT"
        )
        assert next_id == 51


def test_compute_feature_enrichment_basic() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("top-1", 0.90, ["feat_top", "feat_shared"])
        reg.record_trial("top-2", 0.80, ["feat_top", "feat_shared"])
        reg.record_trial("bot-1", 0.20, ["feat_bottom", "feat_shared"])
        reg.record_trial("bot-2", 0.10, ["feat_bottom", "feat_shared"])
        result = reg.compute_feature_enrichment(top_k=2, bottom_k=2)
        assert result == [("feat_top", 1.0), ("feat_bottom", -1.0)]


def test_compute_feature_enrichment_empty_registry() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg.compute_feature_enrichment() == []


def test_compute_feature_enrichment_threshold() -> None:
    # feat_strong:   4/4 top, 0/4 bottom => score +1.0  (kept).
    # feat_weakneg:  2/4 top, 4/4 bottom => score -0.5  (kept).
    # feat_meh:      2/4 top, 1/4 bottom => score +0.25 (dropped, < 0.3).
    # feat_balanced: 1/4 top, 1/4 bottom => score  0.0  (dropped).
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial(
            "top-1", 0.95, ["feat_strong", "feat_meh", "feat_balanced", "feat_weakneg"]
        )
        reg.record_trial("top-2", 0.90, ["feat_strong", "feat_meh", "feat_weakneg"])
        reg.record_trial("top-3", 0.85, ["feat_strong"])
        reg.record_trial("top-4", 0.80, ["feat_strong"])
        reg.record_trial("bot-1", 0.40, ["feat_meh", "feat_balanced", "feat_weakneg"])
        reg.record_trial("bot-2", 0.30, ["feat_weakneg"])
        reg.record_trial("bot-3", 0.20, ["feat_weakneg"])
        reg.record_trial("bot-4", 0.10, ["feat_weakneg"])
        result = reg.compute_feature_enrichment(top_k=4, bottom_k=4)
        names = [name for name, _ in result]
        assert names == ["feat_strong", "feat_weakneg"]


def test_compute_feature_enrichment_sorted_descending() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("top-1", 0.95, ["feat_always", "feat_sometimes"])
        reg.record_trial("top-2", 0.90, ["feat_always"])
        reg.record_trial("bot-1", 0.20, ["feat_never"])
        reg.record_trial("bot-2", 0.10, ["feat_never"])
        result = reg.compute_feature_enrichment(top_k=2, bottom_k=2)
        scores = [score for _, score in result]
        assert scores == sorted(scores, reverse=True)


def test_is_saturated_false_when_no_active_entry() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.40, ["feat_a"])
        reg.record_trial("t2", 0.50, ["feat_b"])
        assert reg.is_saturated(lookback=50) is False


def test_is_saturated_true_when_last_n_trials_all_below_active() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial("active", 0.80, ["feat_a"])
        reg.activate(active_id)
        reg.record_trial("t1", 0.40, ["feat_b"])
        reg.record_trial("t2", 0.50, ["feat_c"])
        reg.record_trial("t3", 0.79, ["feat_d"])
        assert reg.is_saturated(lookback=50) is True


def test_is_saturated_false_when_recent_trial_exceeds_active() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial("active", 0.60, ["feat_a"])
        reg.activate(active_id)
        reg.record_trial("t1", 0.40, ["feat_b"])
        reg.record_trial("t2", 0.85, ["feat_c"])
        assert reg.is_saturated(lookback=50) is False


def test_is_saturated_ignores_better_trial_outside_lookback_window() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        # A trial beating active sits older than the lookback window.
        reg.record_trial("old-winner", 0.95, ["feat_old"])
        active_id = reg.record_trial("active", 0.60, ["feat_a"])
        reg.activate(active_id)
        reg.record_trial("t1", 0.40, ["feat_b"])
        reg.record_trial("t2", 0.50, ["feat_c"])
        assert reg.is_saturated(lookback=2) is True


def test_is_saturated_equal_to_active_does_not_count_as_improvement() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial("active", 0.70, ["feat_a"])
        reg.activate(active_id)
        reg.record_trial("tie", 0.70, ["feat_b"])
        assert reg.is_saturated(lookback=50) is True


def test_compute_feature_enrichment_handles_non_list_feature_json() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg._con is not None
        reg._con.execute(
            "INSERT INTO feature_trials VALUES "
            "(100, 'corrupt', 0.90, FALSE, '\"not-a-list\"', '{}', '2024-01-01')"
        )
        reg._sync_sequence_to_table("seq_feature_trials_id", "feature_trials")
        reg.record_trial("normal", 0.10, ["feat_a", "feat_a"])
        result = reg.compute_feature_enrichment(top_k=1, bottom_k=1)
        assert result == [("feat_a", -1.0)]
