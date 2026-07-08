"""Tests for mlflow_ui.config."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from mlflow_ui import config


def test_repo_root_points_at_actual_repo_root() -> None:
    # apps/mlflow-ui lives two levels under the repo root.
    assert (config.REPO_ROOT / "apps" / "mlflow-ui").is_dir()
    assert (config.REPO_ROOT / "apps" / "mlflow-ui" / "pyproject.toml").is_file()


def test_default_config_values() -> None:
    cfg = config.load_config()

    assert cfg.data_dir == config.DEFAULT_DATA_DIR
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 5252
    assert cfg.artifacts_mode == "local"
    assert cfg.r2_bucket is None
    assert cfg.r2_prefix == "mlflow"


def test_default_sqlite_uri_has_exactly_four_slashes() -> None:
    cfg = config.load_config()

    assert cfg.backend_store_uri.startswith("sqlite:")
    remainder = cfg.backend_store_uri[len("sqlite:") :]
    slash_run = len(remainder) - len(remainder.lstrip("/"))
    assert slash_run == 4
    assert cfg.backend_store_uri == f"sqlite:///{cfg.data_dir / 'mlflow.db'}"


def test_default_local_artifacts_uri_has_exactly_three_slashes() -> None:
    cfg = config.load_config()

    assert cfg.artifacts_destination.startswith("file:")
    remainder = cfg.artifacts_destination[len("file:") :]
    slash_run = len(remainder) - len(remainder.lstrip("/"))
    assert slash_run == 3
    assert cfg.artifacts_destination == f"file://{cfg.data_dir / 'mlartifacts'}"


def test_data_dir_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    override = tmp_path / "custom-data"
    monkeypatch.setenv("HORSE_RACING_MLFLOW_DATA_DIR", str(override))

    cfg = config.load_config()

    assert cfg.data_dir == override
    assert cfg.backend_store_uri == f"sqlite:///{override / 'mlflow.db'}"
    assert cfg.artifacts_destination == f"file://{override / 'mlartifacts'}"


def test_backend_uri_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_BACKEND_URI", "sqlite:////custom/path/mlflow.db")

    cfg = config.load_config()

    assert cfg.backend_store_uri == "sqlite:////custom/path/mlflow.db"


def test_host_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_UI_HOST", "0.0.0.0")

    cfg = config.load_config()

    assert cfg.host == "0.0.0.0"


def test_port_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_UI_PORT", "9999")

    cfg = config.load_config()

    assert cfg.port == 9999


def test_invalid_port_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_UI_PORT", "not-a-port")

    with pytest.raises(ValueError, match="not-a-port"):
        config.load_config()


def test_invalid_artifacts_mode_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "gcs")

    with pytest.raises(ValueError, match="gcs"):
        config.load_config()


def test_explicit_local_mode_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "local")

    cfg = config.load_config()

    assert cfg.artifacts_mode == "local"


def test_r2_prefix_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_BUCKET", "my-bucket")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_PREFIX", "custom-prefix")
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")

    cfg = config.load_config()

    assert cfg.r2_prefix == "custom-prefix"
    assert cfg.artifacts_destination == "s3://my-bucket/custom-prefix"


def _set_full_r2_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_BUCKET", "my-bucket")
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")


def test_r2_mode_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_full_r2_env(monkeypatch)

    cfg = config.load_config()

    assert cfg.artifacts_mode == "r2"
    assert cfg.artifacts_destination == "s3://my-bucket/mlflow"
    assert cfg.r2_account_id == "acct123"
    assert cfg.r2_access_key_id == "key123"
    assert cfg.r2_secret_access_key == "secret123"
    assert cfg.r2_bucket == "my-bucket"


def test_r2_mode_cloudflare_account_id_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_BUCKET", "my-bucket")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "fallback-acct")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")

    cfg = config.load_config()

    assert cfg.r2_account_id == "fallback-acct"


def test_r2_mode_prefers_r2_account_id_over_cloudflare_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_full_r2_env(monkeypatch)
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "should-not-be-used")

    cfg = config.load_config()

    assert cfg.r2_account_id == "acct123"


def test_r2_mode_missing_everything_raises_naming_all_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")

    with pytest.raises(ValueError) as excinfo:
        config.load_config()

    message = str(excinfo.value)
    assert "R2_ACCOUNT_ID" in message
    assert "R2_ACCESS_KEY_ID" in message
    assert "R2_SECRET_ACCESS_KEY" in message
    assert "HORSE_RACING_MLFLOW_R2_BUCKET" in message


def test_r2_mode_missing_bucket_only_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")

    with pytest.raises(ValueError) as excinfo:
        config.load_config()

    message = str(excinfo.value)
    assert "HORSE_RACING_MLFLOW_R2_BUCKET" in message
    assert "R2_ACCESS_KEY_ID" not in message


def test_server_env_local_mode_is_empty() -> None:
    # Not actually empty any more: the job-execution scheduler kill switch is
    # unconditional (local and r2 modes alike), since it is a backend-store
    # concern unrelated to artifacts. Name kept for git-blame continuity;
    # the assertion below reflects the current (non-empty) behavior.
    cfg = config.load_config()

    assert config.server_env(cfg) == {"MLFLOW_SERVER_ENABLE_JOB_EXECUTION": "false"}


def test_server_env_r2_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_full_r2_env(monkeypatch)
    cfg = config.load_config()

    env = config.server_env(cfg)

    assert env["MLFLOW_S3_ENDPOINT_URL"] == "https://acct123.r2.cloudflarestorage.com"
    assert env["AWS_ACCESS_KEY_ID"] == "key123"
    assert env["AWS_SECRET_ACCESS_KEY"] == "secret123"
    assert env["AWS_DEFAULT_REGION"] == "auto"
    assert env["MLFLOW_SERVER_ENABLE_JOB_EXECUTION"] == "false"


def test_server_env_does_not_clobber_existing_aws_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_full_r2_env(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "operator-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "operator-secret")
    cfg = config.load_config()

    env = config.server_env(cfg)

    assert "AWS_ACCESS_KEY_ID" not in env
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert env["MLFLOW_S3_ENDPOINT_URL"] == "https://acct123.r2.cloudflarestorage.com"


def test_server_env_does_not_clobber_operator_set_job_execution_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MLFLOW_SERVER_ENABLE_JOB_EXECUTION", "true")
    cfg = config.load_config()

    env = config.server_env(cfg)

    assert "MLFLOW_SERVER_ENABLE_JOB_EXECUTION" not in env


def test_load_dotenv_local_default_path_points_at_shared_sibling_file() -> None:
    assert config.DEFAULT_ENV_LOCAL_FILE == (
        config.REPO_ROOT / "apps" / "mlflow" / ".env.local"
    )


def test_load_dotenv_local_missing_file_is_noop(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.env"

    config.load_dotenv_local(missing)  # must not raise

    assert "SOME_VAR_THAT_SHOULD_NOT_EXIST" not in os.environ


def test_load_dotenv_local_sets_unset_vars(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HRD_TEST_NEW_VAR", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text("HRD_TEST_NEW_VAR=hello\n")

    config.load_dotenv_local(env_file)

    assert os.environ["HRD_TEST_NEW_VAR"] == "hello"


def test_load_dotenv_local_does_not_override_existing_env_var(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("HRD_TEST_EXISTING_VAR", "process-value")
    env_file = tmp_path / ".env.local"
    env_file.write_text("HRD_TEST_EXISTING_VAR=file-value\n")

    config.load_dotenv_local(env_file)

    assert os.environ["HRD_TEST_EXISTING_VAR"] == "process-value"


def test_load_dotenv_local_skips_blank_lines_and_comments(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HRD_TEST_COMMENT_VAR", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text(
        "\n"
        "# a comment\n"
        "   \n"
        "# HRD_TEST_COMMENT_VAR=should-not-be-set\n"
        "HRD_TEST_COMMENT_VAR=actual-value\n"
    )

    config.load_dotenv_local(env_file)

    assert os.environ["HRD_TEST_COMMENT_VAR"] == "actual-value"


def test_load_dotenv_local_skips_malformed_lines_without_raising(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HRD_TEST_MALFORMED_VAR", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text("this-line-has-no-equals-sign\nHRD_TEST_MALFORMED_VAR=ok\n")

    config.load_dotenv_local(env_file)  # must not raise

    assert os.environ["HRD_TEST_MALFORMED_VAR"] == "ok"


def test_load_dotenv_local_skips_line_with_empty_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HRD_TEST_EMPTY_KEY_VAR", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text("=value-with-no-key\nHRD_TEST_EMPTY_KEY_VAR=ok\n")

    config.load_dotenv_local(env_file)  # must not raise

    assert os.environ["HRD_TEST_EMPTY_KEY_VAR"] == "ok"


def test_load_dotenv_local_strips_double_quotes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HRD_TEST_DQUOTE_VAR", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text('HRD_TEST_DQUOTE_VAR="quoted-value"\n')

    config.load_dotenv_local(env_file)

    assert os.environ["HRD_TEST_DQUOTE_VAR"] == "quoted-value"


def test_load_dotenv_local_strips_single_quotes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HRD_TEST_SQUOTE_VAR", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text("HRD_TEST_SQUOTE_VAR='quoted-value'\n")

    config.load_dotenv_local(env_file)

    assert os.environ["HRD_TEST_SQUOTE_VAR"] == "quoted-value"


def test_load_dotenv_local_falls_back_to_default_when_env_file_override_unset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HORSE_RACING_MLFLOW_ENV_FILE", raising=False)
    fake_default = tmp_path / "fake-default.env.local"
    fake_default.write_text("HRD_TEST_DEFAULT_FALLBACK_VAR=from-fake-default\n")
    monkeypatch.setattr(config, "DEFAULT_ENV_LOCAL_FILE", fake_default)

    config.load_dotenv_local()

    assert os.environ["HRD_TEST_DEFAULT_FALLBACK_VAR"] == "from-fake-default"


def test_load_dotenv_local_honors_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "custom-location.env"
    override_file.write_text("HRD_TEST_OVERRIDE_VAR=from-override\n")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENV_FILE", str(override_file))

    config.load_dotenv_local()

    assert os.environ["HRD_TEST_OVERRIDE_VAR"] == "from-override"


def test_load_dotenv_local_explicit_arg_wins_over_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "should-not-be-used.env"
    override_file.write_text("HRD_TEST_SHOULD_NOT_LOAD=bad\n")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENV_FILE", str(override_file))
    explicit_file = tmp_path / "explicit.env"
    explicit_file.write_text("HRD_TEST_EXPLICIT_WINS=good\n")

    config.load_dotenv_local(explicit_file)

    assert os.environ["HRD_TEST_EXPLICIT_WINS"] == "good"
    assert "HRD_TEST_SHOULD_NOT_LOAD" not in os.environ


def test_load_dotenv_local_env_file_override_pointing_at_missing_file_is_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENV_FILE", str(tmp_path / "does-not-exist.env"))

    config.load_dotenv_local()  # must not raise

    assert "HRD_TEST_ENV_FILE_OVERRIDE_MISSING_VAR" not in os.environ


def test_load_repo_root_env_fallback_default_path_points_at_repo_root() -> None:
    assert config.DEFAULT_ROOT_ENV_FILE == config.REPO_ROOT / ".env"


def test_load_repo_root_env_fallback_missing_file_is_noop(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.env"

    config.load_repo_root_env_fallback(missing)  # must not raise

    assert "SOME_VAR_THAT_SHOULD_NOT_EXIST" not in os.environ


def test_load_repo_root_env_fallback_imports_allowed_prefix_and_exact_keys(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HORSE_RACING_MLFLOW_TEST_ROOT_VAR", raising=False)
    monkeypatch.delenv("MLFLOW_TEST_ROOT_VAR", raising=False)
    monkeypatch.delenv("R2_TEST_ROOT_VAR", raising=False)
    monkeypatch.delenv("CLOUDFLARE_ACCOUNT_ID", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "HORSE_RACING_MLFLOW_TEST_ROOT_VAR=hrmv\n"
        "MLFLOW_TEST_ROOT_VAR=mfv\n"
        "R2_TEST_ROOT_VAR=r2v\n"
        "CLOUDFLARE_ACCOUNT_ID=acct-fallback\n"
    )

    config.load_repo_root_env_fallback(env_file)

    assert os.environ["HORSE_RACING_MLFLOW_TEST_ROOT_VAR"] == "hrmv"
    assert os.environ["MLFLOW_TEST_ROOT_VAR"] == "mfv"
    assert os.environ["R2_TEST_ROOT_VAR"] == "r2v"
    assert os.environ["CLOUDFLARE_ACCOUNT_ID"] == "acct-fallback"


def test_load_repo_root_env_fallback_does_not_import_non_matching_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("PC_KEIBA_INTERNAL_TOKEN", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("PC_KEIBA_INTERNAL_TOKEN=super-secret\n")

    config.load_repo_root_env_fallback(env_file)

    assert "PC_KEIBA_INTERNAL_TOKEN" not in os.environ


def test_load_repo_root_env_fallback_does_not_override_existing_env_var(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("R2_TEST_EXISTING_VAR", "process-value")
    env_file = tmp_path / ".env"
    env_file.write_text("R2_TEST_EXISTING_VAR=file-value\n")

    config.load_repo_root_env_fallback(env_file)

    assert os.environ["R2_TEST_EXISTING_VAR"] == "process-value"


def test_load_repo_root_env_fallback_handles_export_prefix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("MLFLOW_TEST_EXPORT_VAR", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("export MLFLOW_TEST_EXPORT_VAR=exported-value\n")

    config.load_repo_root_env_fallback(env_file)

    assert os.environ["MLFLOW_TEST_EXPORT_VAR"] == "exported-value"


def test_load_repo_root_env_fallback_skips_blank_comment_and_malformed_lines(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("MLFLOW_TEST_MALFORMED_VAR", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n"
        "# a comment\n"
        "   \n"
        "this-line-has-no-equals-sign\n"
        "# MLFLOW_TEST_MALFORMED_VAR=should-not-be-set\n"
        "MLFLOW_TEST_MALFORMED_VAR=actual-value\n"
    )

    config.load_repo_root_env_fallback(env_file)  # must not raise

    assert os.environ["MLFLOW_TEST_MALFORMED_VAR"] == "actual-value"


def test_load_repo_root_env_fallback_strips_quotes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("MLFLOW_TEST_QUOTED_VAR", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text('MLFLOW_TEST_QUOTED_VAR="quoted-value"\n')

    config.load_repo_root_env_fallback(env_file)

    assert os.environ["MLFLOW_TEST_QUOTED_VAR"] == "quoted-value"


def test_load_repo_root_env_fallback_falls_back_to_default_when_env_file_override_unset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("HORSE_RACING_MLFLOW_ROOT_ENV_FILE", raising=False)
    fake_default = tmp_path / "fake-default.env"
    fake_default.write_text("MLFLOW_TEST_ROOT_DEFAULT_FALLBACK_VAR=from-fake-default\n")
    monkeypatch.setattr(config, "DEFAULT_ROOT_ENV_FILE", fake_default)

    config.load_repo_root_env_fallback()

    assert os.environ["MLFLOW_TEST_ROOT_DEFAULT_FALLBACK_VAR"] == "from-fake-default"


def test_load_repo_root_env_fallback_honors_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "custom-root.env"
    override_file.write_text("MLFLOW_TEST_ROOT_OVERRIDE_VAR=from-override\n")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ROOT_ENV_FILE", str(override_file))

    config.load_repo_root_env_fallback()

    assert os.environ["MLFLOW_TEST_ROOT_OVERRIDE_VAR"] == "from-override"


def test_load_repo_root_env_fallback_explicit_arg_wins_over_env_file_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    override_file = tmp_path / "should-not-be-used.env"
    override_file.write_text("MLFLOW_TEST_ROOT_SHOULD_NOT_LOAD=bad\n")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ROOT_ENV_FILE", str(override_file))
    explicit_file = tmp_path / "explicit-root.env"
    explicit_file.write_text("MLFLOW_TEST_ROOT_EXPLICIT_WINS=good\n")

    config.load_repo_root_env_fallback(explicit_file)

    assert os.environ["MLFLOW_TEST_ROOT_EXPLICIT_WINS"] == "good"
    assert "MLFLOW_TEST_ROOT_SHOULD_NOT_LOAD" not in os.environ


def test_load_repo_root_env_fallback_env_file_override_pointing_at_missing_file_is_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(
        "HORSE_RACING_MLFLOW_ROOT_ENV_FILE", str(tmp_path / "does-not-exist.env")
    )

    config.load_repo_root_env_fallback()  # must not raise

    assert "MLFLOW_TEST_ROOT_ENV_FILE_OVERRIDE_MISSING_VAR" not in os.environ


def test_ensure_wal_sets_wal_mode_and_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "nested" / "mlflow.db"

    config.ensure_wal(db_path)
    config.ensure_wal(db_path)

    conn = sqlite3.connect(db_path)
    try:
        (mode,) = conn.execute("PRAGMA journal_mode;").fetchone()
    finally:
        conn.close()
    assert mode.lower() == "wal"
