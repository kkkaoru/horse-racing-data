"""Tests for mlflow_tracking.config."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import conftest
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
    config.ENV_NEON_RACING_URL,
    config.ENV_LOCAL_REPLICA_URL,
)


@pytest.fixture(autouse=True)
def clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_load_dotenv_local_sets_unset_vars_from_simple_lines(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text("MLFLOW_TEST_ENV_LOAD_SIMPLE=value-one\n", encoding="utf-8")
    config.load_dotenv_local(env_file)
    assert os.environ["MLFLOW_TEST_ENV_LOAD_SIMPLE"] == "value-one"


def test_load_dotenv_local_does_not_override_existing_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MLFLOW_TEST_ENV_LOAD_PRECEDENCE", "already-set")
    env_file = tmp_path / ".env.local"
    env_file.write_text("MLFLOW_TEST_ENV_LOAD_PRECEDENCE=from-file\n", encoding="utf-8")
    config.load_dotenv_local(env_file)
    assert os.environ["MLFLOW_TEST_ENV_LOAD_PRECEDENCE"] == "already-set"


def test_load_dotenv_local_missing_file_is_a_noop(tmp_path: Path) -> None:
    config.load_dotenv_local(tmp_path / "does-not-exist.env.local")


def test_load_dotenv_local_skips_blank_and_comment_lines(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text(
        "\n# a comment line\nMLFLOW_TEST_ENV_LOAD_AFTER_COMMENT=value-two\n", encoding="utf-8"
    )
    config.load_dotenv_local(env_file)
    assert os.environ["MLFLOW_TEST_ENV_LOAD_AFTER_COMMENT"] == "value-two"


def test_load_dotenv_local_skips_malformed_line_without_equals(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text(
        "NOT_A_VALID_LINE_NO_EQUALS\nMLFLOW_TEST_ENV_LOAD_AFTER_MALFORMED=value-three\n",
        encoding="utf-8",
    )
    config.load_dotenv_local(env_file)
    assert os.environ["MLFLOW_TEST_ENV_LOAD_AFTER_MALFORMED"] == "value-three"
    assert "NOT_A_VALID_LINE_NO_EQUALS" not in os.environ


def test_load_dotenv_local_strips_double_quotes(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text('MLFLOW_TEST_ENV_LOAD_DQUOTE="quoted-value"\n', encoding="utf-8")
    config.load_dotenv_local(env_file)
    assert os.environ["MLFLOW_TEST_ENV_LOAD_DQUOTE"] == "quoted-value"


def test_load_dotenv_local_strips_single_quotes(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text("MLFLOW_TEST_ENV_LOAD_SQUOTE='quoted-value'\n", encoding="utf-8")
    config.load_dotenv_local(env_file)
    assert os.environ["MLFLOW_TEST_ENV_LOAD_SQUOTE"] == "quoted-value"


def test_load_dotenv_local_default_path_resolves_under_repo_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    default_dir = tmp_path / "apps" / "mlflow"
    default_dir.mkdir(parents=True)
    (default_dir / ".env.local").write_text(
        "MLFLOW_TEST_ENV_LOAD_DEFAULT_PATH=from-default-path\n", encoding="utf-8"
    )
    monkeypatch.setattr(config, "REPO_ROOT", tmp_path)
    # The conftest.py isolate_env_file_loaders autouse fixture forces this env
    # var to a nonexistent path for every test; clear it here so this test can
    # exercise the actual REPO_ROOT-based default it's named after.
    monkeypatch.delenv(config.ENV_ENV_FILE, raising=False)
    config.load_dotenv_local()
    assert os.environ["MLFLOW_TEST_ENV_LOAD_DEFAULT_PATH"] == "from-default-path"


def test_load_dotenv_local_honors_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "custom-location.env"
    override_file.write_text("MLFLOW_TEST_ENV_LOAD_OVERRIDE=from-override\n", encoding="utf-8")
    monkeypatch.setenv(config.ENV_ENV_FILE, str(override_file))
    config.load_dotenv_local()
    assert os.environ["MLFLOW_TEST_ENV_LOAD_OVERRIDE"] == "from-override"


def test_load_dotenv_local_explicit_arg_wins_over_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "should-not-be-used.env"
    override_file.write_text("MLFLOW_TEST_SHOULD_NOT_LOAD=bad\n", encoding="utf-8")
    monkeypatch.setenv(config.ENV_ENV_FILE, str(override_file))
    explicit_file = tmp_path / "explicit.env"
    explicit_file.write_text("MLFLOW_TEST_EXPLICIT_WINS=good\n", encoding="utf-8")
    config.load_dotenv_local(explicit_file)
    assert os.environ["MLFLOW_TEST_EXPLICIT_WINS"] == "good"
    assert "MLFLOW_TEST_SHOULD_NOT_LOAD" not in os.environ


def test_load_dotenv_local_env_file_override_pointing_at_missing_file_is_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(config.ENV_ENV_FILE, str(tmp_path / "does-not-exist.env"))
    config.load_dotenv_local()  # must not raise
    assert "MLFLOW_TEST_ENV_LOAD_MISSING_OVERRIDE_VAR" not in os.environ


def test_load_repo_root_env_fallback_sets_allowed_prefix_keys(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "HORSE_RACING_MLFLOW_TEST_ROOT_FALLBACK_A=value-a\n"
        "MLFLOW_TEST_ROOT_FALLBACK_B=value-b\n"
        "R2_TEST_ROOT_FALLBACK_C=value-c\n",
        encoding="utf-8",
    )
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["HORSE_RACING_MLFLOW_TEST_ROOT_FALLBACK_A"] == "value-a"
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_B"] == "value-b"
    assert os.environ["R2_TEST_ROOT_FALLBACK_C"] == "value-c"


def test_load_repo_root_env_fallback_sets_exact_cloudflare_account_id(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("CLOUDFLARE_ACCOUNT_ID=account-id-value\n", encoding="utf-8")
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["CLOUDFLARE_ACCOUNT_ID"] == "account-id-value"


def test_load_repo_root_env_fallback_sets_exact_neon_primary_url(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "NEON_PRIMARY_URL=postgresql://user:pass@ep-example.neon.tech/racing\n",
        encoding="utf-8",
    )
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["NEON_PRIMARY_URL"] == "postgresql://user:pass@ep-example.neon.tech/racing"


def test_load_repo_root_env_fallback_ignores_unrelated_keys(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABASE_URL=postgres://should-not-be-imported\n"
        "PC_KEIBA_INTERNAL_TOKEN=super-secret-should-not-be-imported\n",
        encoding="utf-8",
    )
    config.load_repo_root_env_fallback(env_file)
    assert "DATABASE_URL" not in os.environ
    assert "PC_KEIBA_INTERNAL_TOKEN" not in os.environ


def test_load_repo_root_env_fallback_does_not_override_existing_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MLFLOW_TEST_ROOT_FALLBACK_PRECEDENCE", "already-set")
    env_file = tmp_path / ".env"
    env_file.write_text("MLFLOW_TEST_ROOT_FALLBACK_PRECEDENCE=from-root-env\n", encoding="utf-8")
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_PRECEDENCE"] == "already-set"


def test_load_repo_root_env_fallback_missing_file_is_a_noop(tmp_path: Path) -> None:
    config.load_repo_root_env_fallback(tmp_path / "does-not-exist.env")


def test_load_repo_root_env_fallback_handles_export_prefix(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "export MLFLOW_TEST_ROOT_FALLBACK_EXPORT=value-exported\n"
        "export DATABASE_URL=should-not-be-imported\n",
        encoding="utf-8",
    )
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_EXPORT"] == "value-exported"
    assert "DATABASE_URL" not in os.environ


def test_load_repo_root_env_fallback_skips_blank_and_comment_lines(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n# a comment line\nMLFLOW_TEST_ROOT_FALLBACK_AFTER_COMMENT=value-after-comment\n",
        encoding="utf-8",
    )
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_AFTER_COMMENT"] == "value-after-comment"


def test_load_repo_root_env_fallback_skips_malformed_line_without_equals(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "NOT_A_VALID_LINE_NO_EQUALS\n"
        "MLFLOW_TEST_ROOT_FALLBACK_AFTER_MALFORMED=value-after-malformed\n",
        encoding="utf-8",
    )
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_AFTER_MALFORMED"] == "value-after-malformed"
    assert "NOT_A_VALID_LINE_NO_EQUALS" not in os.environ


def test_load_repo_root_env_fallback_strips_quotes(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        'MLFLOW_TEST_ROOT_FALLBACK_DQUOTE="quoted-value"\n'
        "MLFLOW_TEST_ROOT_FALLBACK_SQUOTE='quoted-value'\n",
        encoding="utf-8",
    )
    config.load_repo_root_env_fallback(env_file)
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_DQUOTE"] == "quoted-value"
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_SQUOTE"] == "quoted-value"


def test_load_repo_root_env_fallback_default_path_resolves_under_repo_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / ".env").write_text(
        "MLFLOW_TEST_ROOT_FALLBACK_DEFAULT_PATH=from-default-path\n", encoding="utf-8"
    )
    monkeypatch.setattr(config, "REPO_ROOT", tmp_path)
    # See the analogous delenv in test_load_dotenv_local_default_path_resolves_
    # under_repo_root above: conftest.py's isolate_env_file_loaders fixture
    # forces this env var to a nonexistent path for every test.
    monkeypatch.delenv(config.ENV_ROOT_ENV_FILE, raising=False)
    config.load_repo_root_env_fallback()
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_DEFAULT_PATH"] == "from-default-path"


def test_load_repo_root_env_fallback_honors_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "custom-root.env"
    override_file.write_text(
        "MLFLOW_TEST_ROOT_FALLBACK_OVERRIDE=from-override\n", encoding="utf-8"
    )
    monkeypatch.setenv(config.ENV_ROOT_ENV_FILE, str(override_file))
    config.load_repo_root_env_fallback()
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_OVERRIDE"] == "from-override"


def test_load_repo_root_env_fallback_explicit_arg_wins_over_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "should-not-be-used.env"
    override_file.write_text(
        "MLFLOW_TEST_ROOT_FALLBACK_SHOULD_NOT_LOAD=bad\n", encoding="utf-8"
    )
    monkeypatch.setenv(config.ENV_ROOT_ENV_FILE, str(override_file))
    explicit_file = tmp_path / "explicit-root.env"
    explicit_file.write_text(
        "MLFLOW_TEST_ROOT_FALLBACK_EXPLICIT_WINS=good\n", encoding="utf-8"
    )
    config.load_repo_root_env_fallback(explicit_file)
    assert os.environ["MLFLOW_TEST_ROOT_FALLBACK_EXPLICIT_WINS"] == "good"
    assert "MLFLOW_TEST_ROOT_FALLBACK_SHOULD_NOT_LOAD" not in os.environ


def test_load_repo_root_env_fallback_env_file_override_pointing_at_missing_file_is_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(config.ENV_ROOT_ENV_FILE, str(tmp_path / "does-not-exist.env"))
    config.load_repo_root_env_fallback()  # must not raise
    assert "MLFLOW_TEST_ROOT_FALLBACK_MISSING_OVERRIDE_VAR" not in os.environ


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


def test_get_racing_neon_dsn_raises_when_unset() -> None:
    with pytest.raises(ValueError, match=config.ENV_NEON_RACING_URL):
        config.get_racing_neon_dsn()


def test_get_racing_neon_dsn_returns_env_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_NEON_RACING_URL, "postgresql://user:pass@neon.example/racing")
    assert config.get_racing_neon_dsn() == "postgresql://user:pass@neon.example/racing"


def test_get_racing_neon_dsn_raises_when_env_is_blank_after_strip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_NEON_RACING_URL, "   ")
    with pytest.raises(ValueError, match=config.ENV_NEON_RACING_URL):
        config.get_racing_neon_dsn()


def test_get_local_replica_dsn_uses_default_when_env_unset() -> None:
    assert config.get_local_replica_dsn() == config.DEFAULT_LOCAL_REPLICA_URL


def test_get_local_replica_dsn_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.ENV_LOCAL_REPLICA_URL, "postgresql://custom:custom@127.0.0.1:5432/db")
    assert config.get_local_replica_dsn() == "postgresql://custom:custom@127.0.0.1:5432/db"


def test_get_local_replica_dsn_falls_back_when_env_is_blank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_LOCAL_REPLICA_URL, "   ")
    assert config.get_local_replica_dsn() == config.DEFAULT_LOCAL_REPLICA_URL


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


# --- Regression coverage for the 2026-07-08 production-write incident -----
#
# apps/mlflow/tests/conftest.py's autouse `isolate_data_dir` fixture used to
# clear only HORSE_RACING_MLFLOW_DATA_DIR. Because config.get_tracking_uri()
# checks HORSE_RACING_MLFLOW_BACKEND_URI first, a stray copy of that var in
# the ambient process environment (e.g. exported for interactive use of the
# real Neon-backed CLI) made every test in this package that builds an
# MlflowClient via get_tracking_uri()/build_client() -- including ones that
# go through cli.main() -- silently target the real production tracking
# store. Two registered-model champion aliases were overwritten with fake
# test data before this was caught. conftest.isolate_data_dir now also calls
# conftest.clear_ambient_backend_uri(); the tests below prove that fix.
#
# The generic MLFLOW_TRACKING_URI is a second, independent leak vector for
# the same underlying incident class: it is the standard mlflow env var that
# mlflow.tracking.MlflowClient()/mlflow.set_tracking_uri() consult directly
# whenever code constructs a client without an explicit tracking_uri
# argument, and get_tracking_uri() itself falls back to it (see config.py's
# docstring) before computing the isolated sqlite default. It was originally
# cleared only by test_cli.py's own local fixture, not package-wide;
# clear_ambient_backend_uri now clears it here too.


def test_backend_uri_env_var_is_absent_during_every_test() -> None:
    """isolate_data_dir (autouse, every test in this package) must have
    already cleared HORSE_RACING_MLFLOW_BACKEND_URI by the time any test body
    runs, so get_tracking_uri() can never resolve to a stray ambient value.
    """
    assert config.ENV_BACKEND_URI not in os.environ


def test_tracking_uri_env_var_is_absent_during_every_test() -> None:
    """isolate_data_dir (autouse, every test in this package) must have
    already cleared the generic MLFLOW_TRACKING_URI by the time any test body
    runs, so get_tracking_uri() -- and any code that builds an MlflowClient
    or calls mlflow.set_tracking_uri() without an explicit URI -- can never
    resolve to a stray ambient value.
    """
    assert config.ENV_TRACKING_URI not in os.environ


def test_clear_ambient_backend_uri_overrides_preset_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulates the exact incident precondition: HORSE_RACING_MLFLOW_BACKEND_URI
    already set to a realistic Neon-style postgres URI, as if exported in the
    ambient shell environment before pytest ever started (i.e. before any
    fixture, including isolate_data_dir itself, has had a chance to run).
    Invokes conftest.clear_ambient_backend_uri -- the exact function
    isolate_data_dir calls -- directly against that preset value, and proves
    it unconditionally wins: the var is gone afterward and get_tracking_uri()
    can no longer resolve to the fake production URI, regardless of fixture
    ordering.
    """
    fake_production_uri = "postgresql://mlflow_app:s3cr3t@ep-fake-prod.neon.tech/mlflow"
    monkeypatch.setenv(config.ENV_BACKEND_URI, fake_production_uri)
    assert os.environ[config.ENV_BACKEND_URI] == fake_production_uri

    conftest.clear_ambient_backend_uri(monkeypatch)

    assert config.ENV_BACKEND_URI not in os.environ
    assert config.get_tracking_uri() != fake_production_uri


def test_experiment_timelines_is_included_in_all_experiment_names() -> None:
    assert config.EXPERIMENT_TIMELINES in config.ALL_EXPERIMENT_NAMES


def test_new_production_experiment_names_are_included_in_all_experiment_names() -> None:
    assert config.EXPERIMENT_FP_PRODUCTION_USAGE in config.ALL_EXPERIMENT_NAMES
    assert config.EXPERIMENT_RS_PRODUCTION_USAGE in config.ALL_EXPERIMENT_NAMES
    assert config.EXPERIMENT_FP_CHAMPION_EVAL in config.ALL_EXPERIMENT_NAMES
    assert config.EXPERIMENT_RS_CHAMPION_EVAL in config.ALL_EXPERIMENT_NAMES


def test_cell_eval_experiment_names_are_included_in_all_experiment_names() -> None:
    assert config.EXPERIMENT_FP_CELL_EVAL == "finish-position/cell-eval"
    assert config.EXPERIMENT_RS_CELL_EVAL == "running-style/cell-eval"
    assert config.EXPERIMENT_FP_CELL_EVAL in config.ALL_EXPERIMENT_NAMES
    assert config.EXPERIMENT_RS_CELL_EVAL in config.ALL_EXPERIMENT_NAMES


def test_smoke_tests_experiment_name_is_included_in_all_experiment_names() -> None:
    assert config.EXPERIMENT_SMOKE_TESTS == "internal/smoke-tests"
    assert config.EXPERIMENT_SMOKE_TESTS in config.ALL_EXPERIMENT_NAMES


def test_clear_ambient_backend_uri_overrides_preset_generic_tracking_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same precondition as the test above, but for the generic
    MLFLOW_TRACKING_URI rather than the repo-scoped HORSE_RACING_MLFLOW_
    BACKEND_URI: preset to a realistic Neon-style postgres URI as if exported
    ambiently before pytest started. Proves clear_ambient_backend_uri clears
    this var too, so get_tracking_uri() cannot fall back to it either.
    """
    fake_production_uri = "postgresql://mlflow_app:s3cr3t@ep-fake-prod.neon.tech/mlflow"
    monkeypatch.setenv(config.ENV_TRACKING_URI, fake_production_uri)
    assert os.environ[config.ENV_TRACKING_URI] == fake_production_uri

    conftest.clear_ambient_backend_uri(monkeypatch)

    assert config.ENV_TRACKING_URI not in os.environ
    assert config.get_tracking_uri() != fake_production_uri
