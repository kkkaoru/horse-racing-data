"""Tests for mlflow_tracking.export_production."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from mlflow_tracking import config, export_production, registry


def _dict_field(payload: dict[str, object], key: str) -> dict[str, object]:
    """Narrow a dict[str, object] value at `key` for test assertions.

    build_active_models_export/build_cell_routing_export return a generic
    dict[str, object] payload (it mixes plain strings, nested dicts, and
    lists), so drilling into it for assertions needs an explicit narrowing
    step at each level.
    """
    return cast(dict[str, object], payload[key])


def test_default_export_dir_honors_data_dir_env_override(tmp_path: Path) -> None:
    # conftest.py's isolate_data_dir autouse fixture already sets
    # HORSE_RACING_MLFLOW_DATA_DIR to tmp_path/"data" for every test.
    assert export_production.default_export_dir() == config.get_data_dir() / "exports"


def test_default_active_models_path_uses_default_export_dir_when_none_given() -> None:
    result = export_production.default_active_models_path()
    assert result == export_production.default_export_dir() / "active_models.json"


def test_default_active_models_path(tmp_path: Path) -> None:
    assert export_production.default_active_models_path(tmp_path) == tmp_path / "active_models.json"


def test_default_cell_routing_path(tmp_path: Path) -> None:
    result = export_production.default_cell_routing_path("jra", tmp_path)
    assert result == tmp_path / "cell_routing_jra.json"


def test_to_cell_routing_category_key_maps_banei_to_ban_ei() -> None:
    assert export_production.to_cell_routing_category_key("banei") == "ban-ei"


def test_to_cell_routing_category_key_passes_through_jra_nar() -> None:
    assert export_production.to_cell_routing_category_key("jra") == "jra"
    assert export_production.to_cell_routing_category_key("nar") == "nar"


def test_write_json_file_creates_parent_dirs(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "file.json"
    export_production.write_json_file(output, {"a": 1})
    assert json.loads(output.read_text(encoding="utf-8")) == {"a": 1}


def test_write_provenance_sidecar_writes_expected_shape(tmp_path: Path) -> None:
    output = tmp_path / "export.json"
    sidecar = export_production.write_provenance_sidecar(
        output, registry_versions_used=["jra-finish-position:1"], run_ids=["run1", "run1", ""]
    )
    assert sidecar == tmp_path / "export.json.provenance.json"
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["registry_versions_used"] == ["jra-finish-position:1"]
    assert payload["run_ids"] == ["run1"]
    assert payload["missing_categories"] == []
    assert "exported_at" in payload


def test_write_provenance_sidecar_includes_missing_categories_when_given(tmp_path: Path) -> None:
    output = tmp_path / "export.json"
    sidecar = export_production.write_provenance_sidecar(
        output,
        registry_versions_used=[],
        run_ids=[],
        missing_categories=["nar", "jra"],
    )
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["missing_categories"] == ["jra", "nar"]


class _StubPutObjectClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        return {}


def test_upload_to_r2_calls_put_object_with_parsed_bucket_and_key(tmp_path: Path) -> None:
    local_file = tmp_path / "export.json"
    local_file.write_text('{"a": 1}', encoding="utf-8")
    stub = _StubPutObjectClient()
    export_production.upload_to_r2(local_file, "s3://my-bucket/my/key.json", client=stub)
    assert len(stub.calls) == 1
    assert stub.calls[0]["Bucket"] == "my-bucket"
    assert stub.calls[0]["Key"] == "my/key.json"
    assert stub.calls[0]["Body"] == b'{"a": 1}'


def test_upload_to_r2_rejects_non_s3_uri(tmp_path: Path) -> None:
    local_file = tmp_path / "export.json"
    local_file.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="must be an s3:// URI"):
        export_production.upload_to_r2(
            local_file, "https://example.com/key", client=_StubPutObjectClient()
        )


def test_upload_to_r2_rejects_uri_missing_key(tmp_path: Path) -> None:
    local_file = tmp_path / "export.json"
    local_file.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="must be s3://bucket/key"):
        export_production.upload_to_r2(
            local_file, "s3://bucket-only", client=_StubPutObjectClient()
        )


def test_upload_to_r2_builds_boto3_client_when_none_provided(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_file = tmp_path / "export.json"
    local_file.write_text("{}", encoding="utf-8")
    stub = _StubPutObjectClient()

    def _fake_client(service_name: str) -> _StubPutObjectClient:
        assert service_name == "s3"
        return stub

    monkeypatch.setattr(export_production.boto3, "client", _fake_client)
    export_production.upload_to_r2(local_file, "s3://bucket/key")
    assert len(stub.calls) == 1


def test_build_active_models_export_empty_when_no_registered_models(client: MlflowClient) -> None:
    build_result = export_production.build_active_models_export(client)
    payload, versions_used, run_ids, missing_categories = build_result
    assert payload == {"model_versions": {}, "subclass_overrides": {}}
    assert versions_used == []
    assert run_ids == []
    assert sorted(missing_categories) == ["banei", "jra", "nar"]


def test_build_active_models_export_includes_champion_and_subclass_overrides(
    client: MlflowClient, tmp_path: Path
) -> None:
    base_version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "jra-base-v1"}
    )
    registry.set_champion(client, "jra-finish-position", base_version.version)
    registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        tags={"model_version": "jra-class-C-v1", "routing_scope": "class:C"},
    )

    payload, versions_used, run_ids, missing_categories = (
        export_production.build_active_models_export(client)
    )
    assert payload["model_versions"] == {"jra": "jra-base-v1"}
    assert payload["subclass_overrides"] == {"jra": {"C": "jra-class-C-v1"}}
    assert f"jra-finish-position:{base_version.version}" in versions_used
    assert isinstance(run_ids, list)
    assert "jra" not in missing_categories


def test_build_active_models_export_skips_registered_model_without_champion(
    client: MlflowClient, tmp_path: Path
) -> None:
    registry.register_version(client, "nar-finish-position", f"file://{tmp_path}")
    payload, _versions_used, _run_ids, missing_categories = (
        export_production.build_active_models_export(client)
    )
    assert "nar" not in _dict_field(payload, "model_versions")
    assert "nar" in missing_categories


def test_build_active_models_export_collects_run_ids_when_versions_have_runs(
    client: MlflowClient, tmp_path: Path
) -> None:
    experiment_id = client.create_experiment("run-id-collection-test")
    champion_run = client.create_run(experiment_id)
    base_version = registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        run_id=champion_run.info.run_id,
        tags={"model_version": "jra-base-v1"},
    )
    registry.set_champion(client, "jra-finish-position", base_version.version)
    class_run = client.create_run(experiment_id)
    registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        run_id=class_run.info.run_id,
        tags={"model_version": "jra-class-C-v1", "routing_scope": "class:C"},
    )

    _payload, _versions_used, run_ids, _missing = export_production.build_active_models_export(
        client
    )
    assert champion_run.info.run_id in run_ids
    assert class_run.info.run_id in run_ids


def test_champion_version_returns_none_when_get_model_version_raises(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    version = registry.register_version(client, "jra-finish-position", f"file://{tmp_path}")
    registry.set_champion(client, "jra-finish-position", version.version)

    def _raise_get_model_version(name: str, version_str: str) -> None:
        raise MlflowException("boom")

    monkeypatch.setattr(client, "get_model_version", _raise_get_model_version)
    payload, _versions_used, _run_ids, missing_categories = (
        export_production.build_active_models_export(client)
    )
    assert "jra" not in _dict_field(payload, "model_versions")
    assert "jra" in missing_categories


def test_search_versions_returns_empty_when_search_model_versions_raises(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    version = registry.register_version(client, "jra-finish-position", f"file://{tmp_path}")
    registry.set_champion(client, "jra-finish-position", version.version)

    def _raise_search_model_versions(filter_string: str) -> list[object]:
        raise MlflowException("boom")

    monkeypatch.setattr(client, "search_model_versions", _raise_search_model_versions)
    payload, _versions_used, _run_ids, _missing = export_production.build_active_models_export(
        client
    )
    assert payload["subclass_overrides"] == {}


def test_export_active_models_writes_file_and_provenance(
    client: MlflowClient, tmp_path: Path
) -> None:
    output_path = tmp_path / "active.json"
    result_path = export_production.export_active_models(client, output_path, allow_missing=True)
    assert result_path == output_path
    assert output_path.is_file()
    assert output_path.with_name("active.json.provenance.json").is_file()


def test_export_active_models_uses_default_path_when_none_given(client: MlflowClient) -> None:
    # default_export_dir() resolves via config.get_data_dir(), which the
    # conftest.py `isolate_data_dir` autouse fixture already points at
    # tmp_path for every test, so this exercises the real default-path
    # resolution without touching the actual repo directory.
    result_path = export_production.export_active_models(client, allow_missing=True)
    assert result_path == export_production.default_export_dir() / "active_models.json"
    assert result_path.is_file()


def test_export_active_models_uploads_to_r2_when_requested(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[Path, str]] = []
    monkeypatch.setattr(
        export_production, "upload_to_r2", lambda path, uri, **kwargs: calls.append((path, uri))
    )
    output_path = tmp_path / "active.json"
    export_production.export_active_models(
        client, output_path, upload_r2="s3://bucket/key", allow_missing=True
    )
    assert calls == [(output_path, "s3://bucket/key")]


def test_export_active_models_succeeds_by_default_when_every_category_has_a_champion(
    client: MlflowClient, tmp_path: Path
) -> None:
    for category in export_production.ACTIVE_MODELS_CATEGORIES:
        registered_name = registry.registered_model_name(category, "finish-position")
        version = registry.register_version(
            client, registered_name, f"file://{tmp_path}", tags={"model_version": f"{category}-v1"}
        )
        registry.set_champion(client, registered_name, version.version)
    output_path = tmp_path / "active.json"

    result_path = export_production.export_active_models(client, output_path)

    assert result_path == output_path
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert set(payload["model_versions"]) == set(export_production.ACTIVE_MODELS_CATEGORIES)
    provenance = json.loads(
        output_path.with_name("active.json.provenance.json").read_text(encoding="utf-8")
    )
    assert provenance["missing_categories"] == []


def test_export_active_models_raises_when_categories_missing_champion_by_default(
    client: MlflowClient, tmp_path: Path
) -> None:
    output_path = tmp_path / "active.json"
    with pytest.raises(ValueError, match="missing champion alias for categories"):
        export_production.export_active_models(client, output_path)
    assert not output_path.is_file()


def test_export_active_models_writes_partial_file_and_lists_missing_in_provenance(
    client: MlflowClient, tmp_path: Path
) -> None:
    registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "jra-base-v1"}
    )
    champion = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "jra-base-v2"}
    )
    registry.set_champion(client, "jra-finish-position", champion.version)
    output_path = tmp_path / "active.json"

    result_path = export_production.export_active_models(client, output_path, allow_missing=True)

    assert result_path == output_path
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["model_versions"] == {"jra": "jra-base-v2"}
    provenance = json.loads(
        output_path.with_name("active.json.provenance.json").read_text(encoding="utf-8")
    )
    assert provenance["missing_categories"] == ["banei", "nar"]


def test_build_cell_routing_export_raises_when_no_champion(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="has no champion alias set"):
        export_production.build_cell_routing_export(client, "jra")


def test_build_cell_routing_export_champion_only(client: MlflowClient, tmp_path: Path) -> None:
    version = registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        tags={"model_version": "jra-base-v1", "feature_count": "250", "architecture": "catboost"},
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    payload, versions_used, _run_ids = export_production.build_cell_routing_export(client, "jra")
    routing = _dict_field(payload, "jra")
    variants = _dict_field(routing, "variants")
    champion_variant = _dict_field(variants, "champion")
    assert routing["default_variant"] == "champion"
    assert champion_variant["model_version"] == "jra-base-v1"
    assert champion_variant["feature_count"] == 250
    assert routing["rules"] == []
    assert f"jra-finish-position:{version.version}" in versions_used


def test_build_cell_routing_export_adds_class_rule(client: MlflowClient, tmp_path: Path) -> None:
    champion = registry.register_version(
        client, "nar-finish-position", f"file://{tmp_path}", tags={"model_version": "nar-base-v1"}
    )
    registry.set_champion(client, "nar-finish-position", champion.version)
    registry.register_version(
        client,
        "nar-finish-position",
        f"file://{tmp_path}",
        tags={"model_version": "nar-class-C-v1", "routing_scope": "class:C"},
    )

    payload, _versions_used, _run_ids = export_production.build_cell_routing_export(client, "nar")
    routing = _dict_field(payload, "nar")
    variants = _dict_field(routing, "variants")
    assert "class-C" in variants
    assert _dict_field(variants, "class-C")["model_version"] == "nar-class-C-v1"
    assert routing["rules"] == [
        {"conditions": [{"dimension": "kyoso_joken_code", "values": ["C"]}], "variant": "class-C"}
    ]


def test_build_cell_routing_export_non_class_routing_scope_has_no_rule(
    client: MlflowClient, tmp_path: Path
) -> None:
    champion = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "jra-base-v1"}
    )
    registry.set_champion(client, "jra-finish-position", champion.version)
    registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        tags={"model_version": "jra-jockey-pedigree-v1", "routing_scope": "jockey_pedigree_703"},
    )

    payload, _versions_used, _run_ids = export_production.build_cell_routing_export(client, "jra")
    routing = _dict_field(payload, "jra")
    assert "jockey_pedigree_703" in _dict_field(routing, "variants")
    assert routing["rules"] == []


def test_build_cell_routing_export_uses_ban_ei_key_for_banei_category(
    client: MlflowClient, tmp_path: Path
) -> None:
    champion = registry.register_version(
        client,
        "banei-finish-position",
        f"file://{tmp_path}",
        tags={"model_version": "banei-base-v1"},
    )
    registry.set_champion(client, "banei-finish-position", champion.version)
    payload, _versions_used, _run_ids = export_production.build_cell_routing_export(client, "banei")
    assert "ban-ei" in payload
    assert "banei" not in payload


def test_build_cell_routing_export_skips_version_with_no_routing_scope_tag(
    client: MlflowClient, tmp_path: Path
) -> None:
    champion = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "jra-base-v1"}
    )
    registry.set_champion(client, "jra-finish-position", champion.version)
    # An extra registered version with no routing_scope tag at all -- should
    # be ignored entirely, not added as a variant or a rule.
    registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        tags={"model_version": "jra-untagged-v1"},
    )
    payload, _versions_used, _run_ids = export_production.build_cell_routing_export(client, "jra")
    routing = _dict_field(payload, "jra")
    assert list(_dict_field(routing, "variants").keys()) == ["champion"]
    assert routing["rules"] == []


def test_build_cell_routing_export_collects_run_ids_for_champion_and_class_variant(
    client: MlflowClient, tmp_path: Path
) -> None:
    experiment_id = client.create_experiment("cell-routing-run-id-test")
    champion_run = client.create_run(experiment_id)
    champion = registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        run_id=champion_run.info.run_id,
        tags={"model_version": "jra-base-v1"},
    )
    registry.set_champion(client, "jra-finish-position", champion.version)
    class_run = client.create_run(experiment_id)
    registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        run_id=class_run.info.run_id,
        tags={"model_version": "jra-class-C-v1", "routing_scope": "class:C"},
    )
    _payload, _versions_used, run_ids = export_production.build_cell_routing_export(client, "jra")
    assert champion_run.info.run_id in run_ids
    assert class_run.info.run_id in run_ids


def test_variant_spec_falls_back_to_zero_for_non_numeric_feature_count(
    client: MlflowClient, tmp_path: Path
) -> None:
    champion = registry.register_version(
        client,
        "jra-finish-position",
        f"file://{tmp_path}",
        tags={"model_version": "jra-base-v1", "feature_count": "not-a-number"},
    )
    registry.set_champion(client, "jra-finish-position", champion.version)
    payload, _versions_used, _run_ids = export_production.build_cell_routing_export(client, "jra")
    routing = _dict_field(payload, "jra")
    champion_variant = _dict_field(routing, "variants")
    assert _dict_field(champion_variant, "champion")["feature_count"] == 0


def test_export_cell_routing_writes_file_and_provenance(
    client: MlflowClient, tmp_path: Path
) -> None:
    champion = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "jra-base-v1"}
    )
    registry.set_champion(client, "jra-finish-position", champion.version)
    output_path = tmp_path / "routing.json"
    result_path = export_production.export_cell_routing(client, "jra", output_path)
    assert result_path == output_path
    assert output_path.is_file()
    assert output_path.with_name("routing.json.provenance.json").is_file()


def test_export_cell_routing_uses_default_path_when_none_given(
    client: MlflowClient, tmp_path: Path
) -> None:
    champion = registry.register_version(
        client, "nar-finish-position", f"file://{tmp_path}", tags={"model_version": "nar-base-v1"}
    )
    registry.set_champion(client, "nar-finish-position", champion.version)
    result_path = export_production.export_cell_routing(client, "nar")
    assert result_path == export_production.default_export_dir() / "cell_routing_nar.json"
    assert result_path.is_file()


def test_export_cell_routing_uploads_to_r2_when_requested(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    champion = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "jra-base-v1"}
    )
    registry.set_champion(client, "jra-finish-position", champion.version)
    calls: list[tuple[Path, str]] = []
    monkeypatch.setattr(
        export_production, "upload_to_r2", lambda path, uri, **kwargs: calls.append((path, uri))
    )
    output_path = tmp_path / "routing.json"
    export_production.export_cell_routing(client, "jra", output_path, upload_r2="s3://bucket/key")
    assert calls == [(output_path, "s3://bucket/key")]
