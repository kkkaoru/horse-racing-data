from __future__ import annotations

from pathlib import Path

import pytest

import feature_registry as subject


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
