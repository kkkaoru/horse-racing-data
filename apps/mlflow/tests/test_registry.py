"""Tests for mlflow_tracking.registry."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from mlflow_tracking import registry


def test_normalize_category_passes_through_known_categories() -> None:
    assert registry.normalize_category("jra") == "jra"
    assert registry.normalize_category("nar") == "nar"
    assert registry.normalize_category("banei") == "banei"


def test_normalize_category_aliases_ban_ei_directory_name() -> None:
    assert registry.normalize_category("ban-ei") == "banei"


def test_normalize_category_raises_for_unsupported_value() -> None:
    with pytest.raises(ValueError, match="unsupported category"):
        registry.normalize_category("keirin")


def test_registered_model_name_combines_category_and_task() -> None:
    assert registry.registered_model_name("jra", "finish-position") == "jra-finish-position"
    assert registry.registered_model_name("nar", "running-style") == "nar-running-style"


def test_register_version_creates_registered_model_and_version(
    client: MlflowClient, tmp_path: Path
) -> None:
    source_uri = f"file://{tmp_path}"
    version = registry.register_version(client, "jra-finish-position", source_uri)
    assert version.name == "jra-finish-position"
    registered = client.get_registered_model("jra-finish-position")
    assert registered.name == "jra-finish-position"


def test_register_version_reuses_existing_registered_model(
    client: MlflowClient, tmp_path: Path
) -> None:
    source_uri = f"file://{tmp_path}"
    registry.register_version(client, "jra-finish-position", source_uri)
    second = registry.register_version(client, "jra-finish-position", source_uri)
    assert second.name == "jra-finish-position"
    assert int(second.version) == 2


def test_register_version_sets_tags(client: MlflowClient, tmp_path: Path) -> None:
    source_uri = f"file://{tmp_path}"
    version = registry.register_version(
        client, "jra-finish-position", source_uri, tags={"model_version": "v1"}
    )
    assert version.tags.get("model_version") == "v1"


def test_register_version_with_file_uri_sets_allow_env_flag(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(registry.FILE_URI_ALLOW_ENV, raising=False)
    source_uri = f"file://{tmp_path}"
    with pytest.warns(UserWarning, match=registry.FILE_URI_ALLOW_ENV):
        registry.register_version(client, "jra-finish-position", source_uri)
    assert os.environ[registry.FILE_URI_ALLOW_ENV] == "true"


def test_register_version_with_file_uri_does_not_warn_when_already_set(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(registry.FILE_URI_ALLOW_ENV, "true")
    source_uri = f"file://{tmp_path}"
    registry.register_version(client, "jra-finish-position", source_uri)


def test_register_version_with_non_file_uri_does_not_touch_env_flag(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(registry.FILE_URI_ALLOW_ENV, raising=False)
    registry.register_version(client, "nar-finish-position", "s3://bucket/nar/v1")
    assert registry.FILE_URI_ALLOW_ENV not in os.environ


def test_set_champion_sets_alias(client: MlflowClient, tmp_path: Path) -> None:
    source_uri = f"file://{tmp_path}"
    version = registry.register_version(client, "jra-finish-position", source_uri)
    registry.set_champion(client, "jra-finish-position", version.version)
    registered = client.get_registered_model("jra-finish-position")
    assert registered.aliases[registry.CHAMPION_ALIAS] == int(version.version)


def test_set_challenger_sets_alias(client: MlflowClient, tmp_path: Path) -> None:
    source_uri = f"file://{tmp_path}"
    version = registry.register_version(client, "jra-finish-position", source_uri)
    registry.set_challenger(client, "jra-finish-position", version.version)
    registered = client.get_registered_model("jra-finish-position")
    assert registered.aliases[registry.CHALLENGER_ALIAS] == int(version.version)


def test_find_version_by_label_returns_exact_match(client: MlflowClient, tmp_path: Path) -> None:
    source_uri = f"file://{tmp_path}"
    version = registry.register_version(
        client, "jra-finish-position", source_uri, tags={"model_version": "v1"}
    )
    assert registry.find_version_by_label(client, "jra-finish-position", "v1") == version.version


def test_find_version_by_label_falls_back_to_casefold_match(
    client: MlflowClient, tmp_path: Path
) -> None:
    source_uri = f"file://{tmp_path}"
    version = registry.register_version(
        client, "jra-finish-position", source_uri, tags={"model_version": "V1-CLEAN"}
    )
    with pytest.warns(UserWarning, match="case-insensitive"):
        found = registry.find_version_by_label(client, "jra-finish-position", "v1-clean")
    assert found == version.version


def test_find_version_by_label_prefers_exact_match_over_casefold_candidate(
    client: MlflowClient, tmp_path: Path
) -> None:
    source_uri = f"file://{tmp_path}"
    registry.register_version(
        client, "jra-finish-position", source_uri, tags={"model_version": "V1"}
    )
    exact = registry.register_version(
        client, "jra-finish-position", source_uri, tags={"model_version": "v1"}
    )
    assert registry.find_version_by_label(client, "jra-finish-position", "v1") == exact.version


def test_find_version_by_label_returns_none_when_nothing_matches(
    client: MlflowClient, tmp_path: Path
) -> None:
    registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "other"}
    )
    assert registry.find_version_by_label(client, "jra-finish-position", "missing") is None


def test_find_duplicate_version_returns_version_when_label_and_source_match(
    client: MlflowClient, tmp_path: Path
) -> None:
    source_uri = f"file://{tmp_path}"
    version = registry.register_version(
        client, "jra-running-style", source_uri, tags={"model_version": "jra-rs-v1"}
    )
    found = registry.find_duplicate_version(client, "jra-running-style", "jra-rs-v1", source_uri)
    assert found == version.version


def test_find_duplicate_version_returns_none_when_source_differs(
    client: MlflowClient, tmp_path: Path
) -> None:
    original_source = f"file://{tmp_path / 'v1'}"
    registry.register_version(
        client, "jra-running-style", original_source, tags={"model_version": "jra-rs-v1"}
    )
    changed_source = f"file://{tmp_path / 'v1-changed'}"
    found = registry.find_duplicate_version(
        client, "jra-running-style", "jra-rs-v1", changed_source
    )
    assert found is None


def test_find_duplicate_version_returns_none_when_label_has_no_match(
    client: MlflowClient, tmp_path: Path
) -> None:
    source_uri = f"file://{tmp_path}"
    registry.register_version(
        client, "jra-running-style", source_uri, tags={"model_version": "other-label"}
    )
    found = registry.find_duplicate_version(
        client, "jra-running-style", "missing-label", source_uri
    )
    assert found is None


def test_find_duplicate_version_returns_none_when_registered_model_does_not_exist(
    client: MlflowClient, tmp_path: Path
) -> None:
    found = registry.find_duplicate_version(
        client, "jra-running-style", "jra-rs-v1", f"file://{tmp_path}"
    )
    assert found is None


def test_find_duplicate_version_returns_none_when_get_model_version_raises(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers the fail-open branch: the label lookup succeeds but the
    immediate follow-up fetch fails (e.g. a version deleted between the two
    calls) -- same fail-open posture as `find_version_by_label` itself, a
    lookup failure must never be mistaken for "definitely not a duplicate".
    """
    source_uri = f"file://{tmp_path}"
    registry.register_version(
        client, "jra-running-style", source_uri, tags={"model_version": "jra-rs-v1"}
    )

    def _raise_lookup_failure(*_args: object, **_kwargs: object) -> None:
        raise MlflowException("simulated lookup failure")

    monkeypatch.setattr(client, "get_model_version", _raise_lookup_failure)
    found = registry.find_duplicate_version(client, "jra-running-style", "jra-rs-v1", source_uri)
    assert found is None


def test_version_tags_omits_unset_fields() -> None:
    assert registry.version_tags() == {}


def test_version_tags_includes_set_fields() -> None:
    result = registry.version_tags(
        architecture="catboost",
        feature_count=250,
        train_window="20130101-20251231",
        routing_scope="class:C",
        category="jra",
        task="finish-position",
    )
    assert result == {
        "architecture": "catboost",
        "feature_count": "250",
        "train_window": "20130101-20251231",
        "routing_scope": "class:C",
        "category": "jra",
        "task": "finish-position",
    }


def test_version_tags_includes_zero_feature_count_because_it_is_not_none() -> None:
    result = registry.version_tags(feature_count=0)
    assert result == {"feature_count": "0"}
