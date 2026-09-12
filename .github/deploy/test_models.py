"""Verify digest failures never publish or replace a production model."""

import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from predict_lib.artifact_integrity import ArtifactSpec

import models
from models import check_bytes, main, object_key, transfer


@pytest.fixture
def artifact() -> ArtifactSpec:
    return ArtifactSpec(
        id="model",
        system="finish-position",
        category="jra",
        model_version="test",
        release_id="test",
        bundle_id="test",
        role="model",
        sha256="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        size_bytes=3,
        source_ref="test",
        serving_bucket="models",
        serving_key="finish-position/jra/test/model.json",
        container_path="finish-position/jra/test/model.json",
        activation="required",
        activation_environment=None,
    )


def test_content_address_is_independent_of_mutable_serving_path(artifact: ArtifactSpec) -> None:
    assert (
        object_key(artifact)
        == "ci-artifacts/sha256/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_size_mismatch_is_rejected(tmp_path: Path, artifact: ArtifactSpec) -> None:
    path = tmp_path / "model"
    path.write_bytes(b"bad size")
    with pytest.raises(ValueError, match="size mismatch"):
        check_bytes(path, artifact)


def test_digest_mismatch_is_rejected(tmp_path: Path, artifact: ArtifactSpec) -> None:
    path = tmp_path / "model"
    path.write_bytes(b"xyz")
    with pytest.raises(ValueError, match="digest mismatch"):
        check_bytes(path, artifact)


def test_publish_checks_bytes_before_upload(tmp_path: Path, artifact: ArtifactSpec) -> None:
    path = tmp_path / "finish-position/jra/test/model.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"abc")
    store = Mock()
    transfer(store=store, artifact=artifact, root=tmp_path, publish=True, bucket="artifacts")
    store.upload_file.assert_called_once()
    store.download_file.assert_not_called()


def test_corrupt_source_is_not_published(tmp_path: Path, artifact: ArtifactSpec) -> None:
    path = tmp_path / "finish-position/jra/test/model.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"xyz")
    store = Mock()
    with pytest.raises(ValueError, match="digest mismatch"):
        transfer(store=store, artifact=artifact, root=tmp_path, publish=True, bucket="artifacts")
    store.upload_file.assert_not_called()


def test_restore_replaces_only_verified_bytes(tmp_path: Path, artifact: ArtifactSpec) -> None:
    store = Mock()
    store.download_file.side_effect = lambda bucket, key, filename: Path(filename).write_bytes(
        b"abc"
    )
    transfer(store=store, artifact=artifact, root=tmp_path, publish=False, bucket="artifacts")
    assert (tmp_path / "finish-position/jra/test/model.json").read_bytes() == b"abc"
    assert (tmp_path / "finish-position/jra/test/model.json.download").exists() is False


def test_bad_download_preserves_previous_model(tmp_path: Path, artifact: ArtifactSpec) -> None:
    path = tmp_path / "finish-position/jra/test/model.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"previous")
    store = Mock()
    store.download_file.side_effect = lambda bucket, key, filename: Path(filename).write_bytes(
        b"xyz"
    )
    with pytest.raises(ValueError, match="digest mismatch"):
        transfer(store=store, artifact=artifact, root=tmp_path, publish=False, bucket="artifacts")
    assert path.read_bytes() == b"previous"
    assert path.with_suffix(".json.download").exists() is False


def test_network_failure_is_not_a_successful_restore(
    tmp_path: Path, artifact: ArtifactSpec
) -> None:
    store = Mock()
    store.download_file.side_effect = OSError("unavailable")
    with pytest.raises(OSError, match="unavailable"):
        transfer(store=store, artifact=artifact, root=tmp_path, publish=False, bucket="artifacts")


def test_missing_container_path_is_rejected(tmp_path: Path, artifact: ArtifactSpec) -> None:
    with pytest.raises(ValueError, match="no container_path"):
        transfer(
            store=Mock(),
            artifact=replace(artifact, container_path=None),
            root=tmp_path,
            publish=False,
            bucket="artifacts",
        )


def test_cli_rejects_unmanifested_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["models.py", "--root", "models"])
    monkeypatch.setattr(models, "load_manifest", Mock())
    monkeypatch.setattr(models, "derive_selected_artifact_keys", Mock())
    monkeypatch.setattr(
        models,
        "verify_selector_closure",
        Mock(return_value=SimpleNamespace(status="INTEGRITY_FAILURE")),
    )
    with pytest.raises(ValueError, match="not covered"):
        main()


@pytest.mark.parametrize("publish", [False, True])
@pytest.mark.parametrize("extra", [False, True])
def test_cli_transfers_selected_container_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    artifact: ArtifactSpec,
    capsys: pytest.CaptureFixture[str],
    publish: bool,
    extra: bool,
) -> None:
    argv = ["models.py", "--root", "models"] + (["--publish"] if publish else [])
    if extra:
        argv.extend(["--extra-manifest", "test-artifacts.json"])
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setenv("PRODUCTION_ARTIFACT_BUCKET", "artifacts")
    monkeypatch.setenv("R2_ENDPOINT_URL", "https://example.invalid")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test")
    monkeypatch.setattr(
        models,
        "load_manifest",
        Mock(
            return_value=SimpleNamespace(
                artifacts=[
                    artifact,
                    replace(artifact, system="running-style"),
                    replace(artifact, serving_key="unselected"),
                ]
            )
        ),
    )
    monkeypatch.setattr(
        models, "derive_selected_artifact_keys", Mock(return_value={artifact.serving_key})
    )
    monkeypatch.setattr(
        models, "verify_selector_closure", Mock(return_value=SimpleNamespace(status="MATCH"))
    )
    monkeypatch.setattr(models.boto3, "client", Mock())
    operation = Mock()
    monkeypatch.setattr(models, "transfer", operation)
    main()
    if extra:
        assert operation.call_count == 4
        assert "model artifacts: 4" in capsys.readouterr().out
    else:
        operation.assert_called_once()
        assert "model artifacts: 1" in capsys.readouterr().out


def test_regression_manifest_is_valid_and_contains_only_nonselected_inputs() -> None:
    manifest = models.load_manifest(Path(__file__).with_name("test-artifacts.json"))
    selected = models.derive_selected_artifact_keys()
    assert len(manifest.artifacts) == 6
    assert {artifact.serving_key for artifact in manifest.artifacts}.isdisjoint(selected)
