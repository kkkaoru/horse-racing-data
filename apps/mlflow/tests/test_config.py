"""Tests for mlflow_tracking.config."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from mlflow_tracking import config

_ENV_KEYS = (
    config.ENV_DATA_DIR,
    config.ENV_BACKEND_URI,
    config.ENV_TRACKING_URI,
    config.ENV_ARTIFACTS_MODE,
    config.ENV_R2_BUCKET,
    config.ENV_R2_PREFIX,
    config.ENV_R2_ACCOUNT_ID,
    config.ENV_CLOUDFLARE_ACCOUNT_ID,
    config.ENV_R2_ACCESS_KEY_ID,
    config.ENV_R2_SECRET_ACCESS_KEY,
    config.ENV_MLFLOW_S3_ENDPOINT_URL,
    config.ENV_AWS_ACCESS_KEY_ID,
    config.ENV_AWS_SECRET_ACCESS_KEY,
    config.ENV_AWS_DEFAULT_REGION,
)


@pytest.fixture(autouse=True)
def clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_default_data_dir_is_under_repo_root_apps_mlflow(tmp_path: Path) -> None:
    result = config.default_data_dir(tmp_path)
    assert result == tmp_path / "apps" / "mlflow" / "data"


def test_get_data_dir_uses_default_when_env_unset(tmp_path: Path) -> None:
    result = config.get_data_dir(tmp_path)
    assert result == tmp_path / "apps" / "mlflow" / "data"


def test_get_data_dir_honors_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    override = tmp_path / "custom-data"
    monkeypatch.setenv(config.ENV_DATA_DIR, str(override))
    result = config.get_data_dir(tmp_path)
    assert result == override


def test_db_path_for_appends_mlflow_db(tmp_path: Path) -> None:
    result = config.db_path_for(tmp_path)
    assert result == tmp_path / "mlflow.db"


def test_default_tracking_uri_has_four_slashes(tmp_path: Path) -> None:
    result = config.default_tracking_uri(tmp_path)
    assert result.startswith("sqlite:////")
    assert result.endswith("mlflow.db")


def test_get_tracking_uri_uses_default_when_env_unset(tmp_path: Path) -> None:
    result = config.get_tracking_uri(tmp_path)
    assert result == config.default_tracking_uri(tmp_path)


def test_get_tracking_uri_honors_generic_env_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(config.ENV_TRACKING_URI, "sqlite:///:memory:")
    result = config.get_tracking_uri(tmp_path)
    assert result == "sqlite:///:memory:"


def test_get_tracking_uri_honors_backend_uri_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(config.ENV_BACKEND_URI, "sqlite:///backend-only.db")
    result = config.get_tracking_uri(tmp_path)
    assert result == "sqlite:///backend-only.db"


def test_get_tracking_uri_backend_uri_wins_over_generic_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(config.ENV_TRACKING_URI, "sqlite:///generic.db")
    monkeypatch.setenv(config.ENV_BACKEND_URI, "sqlite:///repo-scoped.db")
    result = config.get_tracking_uri(tmp_path)
    assert result == "sqlite:///repo-scoped.db"


def test_default_artifact_root_is_file_uri(tmp_path: Path) -> None:
    result = config.default_artifact_root(tmp_path)
    assert result.startswith("file://")
    assert result.endswith("mlartifacts")


def test_get_artifacts_mode_defaults_to_local() -> None:
    assert config.get_artifacts_mode() == "local"


def test_get_artifacts_mode_reads_r2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_ARTIFACTS_MODE, "r2")
    assert config.get_artifacts_mode() == "r2"


def test_get_artifacts_mode_is_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_ARTIFACTS_MODE, "R2")
    assert config.get_artifacts_mode() == "r2"


def test_get_artifacts_mode_rejects_invalid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_ARTIFACTS_MODE, "bogus")
    with pytest.raises(ValueError, match="HORSE_RACING_MLFLOW_ARTIFACTS_MODE"):
        config.get_artifacts_mode()


def test_get_r2_bucket_raises_when_unset() -> None:
    with pytest.raises(ValueError, match=config.ENV_R2_BUCKET):
        config.get_r2_bucket()


def test_get_r2_bucket_returns_env_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_R2_BUCKET, "my-bucket")
    assert config.get_r2_bucket() == "my-bucket"


def test_get_r2_prefix_defaults_to_mlflow() -> None:
    assert config.get_r2_prefix() == "mlflow"


def test_get_r2_prefix_honors_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_R2_PREFIX, "custom-prefix")
    assert config.get_r2_prefix() == "custom-prefix"


def test_get_r2_prefix_falls_back_when_env_is_blank(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_R2_PREFIX, "   ")
    assert config.get_r2_prefix() == "mlflow"


def test_build_r2_artifact_location() -> None:
    assert config.build_r2_artifact_location("bucket", "prefix") == "s3://bucket/prefix"


def test_resolve_artifact_location_local_mode(tmp_path: Path) -> None:
    result = config.resolve_artifact_location(tmp_path)
    assert result == config.default_artifact_root(tmp_path)


def test_resolve_artifact_location_local_mode_uses_default_data_dir_when_unset() -> None:
    result = config.resolve_artifact_location()
    assert result == config.default_artifact_root(config.get_data_dir())


def test_resolve_artifact_location_r2_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_ARTIFACTS_MODE, "r2")
    monkeypatch.setenv(config.ENV_R2_BUCKET, "my-bucket")
    result = config.resolve_artifact_location()
    assert result == "s3://my-bucket/mlflow"


def test_ensure_wal_creates_parent_dir_and_sets_wal_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "nested" / "mlflow.db"
    config.ensure_wal(db_path)
    assert db_path.parent.is_dir()
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("PRAGMA journal_mode").fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "wal"


def test_apply_r2_env_sets_default_region_only_when_nothing_else_set() -> None:
    config.apply_r2_env()
    assert os.environ[config.ENV_AWS_DEFAULT_REGION] == "auto"
    assert config.ENV_MLFLOW_S3_ENDPOINT_URL not in os.environ
    assert config.ENV_AWS_ACCESS_KEY_ID not in os.environ


def test_apply_r2_env_maps_r2_account_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_R2_ACCOUNT_ID, "abc123")
    config.apply_r2_env()
    assert (
        os.environ[config.ENV_MLFLOW_S3_ENDPOINT_URL] == "https://abc123.r2.cloudflarestorage.com"
    )


def test_apply_r2_env_falls_back_to_cloudflare_account_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_CLOUDFLARE_ACCOUNT_ID, "cf123")
    config.apply_r2_env()
    assert os.environ[config.ENV_MLFLOW_S3_ENDPOINT_URL] == "https://cf123.r2.cloudflarestorage.com"


def test_apply_r2_env_prefers_r2_account_id_over_cloudflare(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_R2_ACCOUNT_ID, "primary")
    monkeypatch.setenv(config.ENV_CLOUDFLARE_ACCOUNT_ID, "fallback")
    config.apply_r2_env()
    assert (
        os.environ[config.ENV_MLFLOW_S3_ENDPOINT_URL] == "https://primary.r2.cloudflarestorage.com"
    )


def test_apply_r2_env_does_not_overwrite_existing_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_R2_ACCOUNT_ID, "abc123")
    monkeypatch.setenv(config.ENV_MLFLOW_S3_ENDPOINT_URL, "https://custom-endpoint.example.com")
    config.apply_r2_env()
    assert os.environ[config.ENV_MLFLOW_S3_ENDPOINT_URL] == "https://custom-endpoint.example.com"


def test_apply_r2_env_maps_access_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_R2_ACCESS_KEY_ID, "key-id")
    monkeypatch.setenv(config.ENV_R2_SECRET_ACCESS_KEY, "secret")
    config.apply_r2_env()
    assert os.environ[config.ENV_AWS_ACCESS_KEY_ID] == "key-id"
    assert os.environ[config.ENV_AWS_SECRET_ACCESS_KEY] == "secret"


def test_apply_r2_env_does_not_overwrite_existing_access_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_R2_ACCESS_KEY_ID, "key-id")
    monkeypatch.setenv(config.ENV_AWS_ACCESS_KEY_ID, "existing-key")
    config.apply_r2_env()
    assert os.environ[config.ENV_AWS_ACCESS_KEY_ID] == "existing-key"


def test_apply_r2_env_does_not_overwrite_existing_secret_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_R2_SECRET_ACCESS_KEY, "secret")
    monkeypatch.setenv(config.ENV_AWS_SECRET_ACCESS_KEY, "existing-secret")
    config.apply_r2_env()
    assert os.environ[config.ENV_AWS_SECRET_ACCESS_KEY] == "existing-secret"


def test_apply_r2_env_does_not_overwrite_existing_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_AWS_DEFAULT_REGION, "us-east-1")
    config.apply_r2_env()
    assert os.environ[config.ENV_AWS_DEFAULT_REGION] == "us-east-1"


class _StubBucketClient:
    def __init__(self, *, error: Exception | None = None) -> None:
        self._error: Exception | None = error

    def head_bucket(self, **kwargs: str) -> object:
        if self._error is not None:
            raise self._error
        return {"kwargs": kwargs}


def test_check_r2_bucket_reachable_returns_true_on_success() -> None:
    assert config.check_r2_bucket_reachable("bucket", client=_StubBucketClient()) is True


def test_check_r2_bucket_reachable_returns_false_on_client_error() -> None:
    error = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")
    assert (
        config.check_r2_bucket_reachable("bucket", client=_StubBucketClient(error=error)) is False
    )


def test_check_r2_bucket_reachable_returns_false_on_botocore_error() -> None:
    client = _StubBucketClient(error=BotoCoreError())
    assert config.check_r2_bucket_reachable("bucket", client=client) is False


def test_check_r2_bucket_reachable_builds_boto3_client_when_none_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _fake_client(service_name: str) -> _StubBucketClient:
        calls.append(service_name)
        return _StubBucketClient()

    monkeypatch.setattr(config.boto3, "client", _fake_client)
    assert config.check_r2_bucket_reachable("bucket") is True
    assert calls == ["s3"]
