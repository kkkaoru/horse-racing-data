from __future__ import annotations

from pathlib import Path

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
        trials = reg.list_trials(limit=10)
        active_ids = [t["id"] for t in trials if t["is_active"]]
        assert active_ids == [id2]


def test_maybe_promote_when_ndcg_exceeds_threshold_promotes_and_returns_true() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        reg.activate(1)
        promoted = reg.maybe_promote("trial-2", 0.62, ["feat_b"], threshold=0.01)
        assert promoted is True
        active = reg.get_active_entry()
        assert active is not None
        assert active["trial_id"] == "trial-2"


def test_maybe_promote_when_ndcg_does_not_exceed_threshold_records_but_no_promote() -> None:
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
        assert [t["ndcg_at_3"] for t in trials] == [0.9, 0.6, 0.3]


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
        trials = reg.list_trials(limit=10)
        active_count = sum(1 for t in trials if t["is_active"])
        assert active_count == 1


def test_record_trial_default_definition_json_is_empty_object() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("trial-1", 0.5, ["feat_a"])
        trials = reg.list_trials(limit=1)
        assert trials[0]["definition_json"] == "{}"


def test_list_trials_empty_registry_returns_empty_list() -> None:
    with subject.FeatureRegistry(Path(":memory:")) as reg:
        assert reg.list_trials() == []
