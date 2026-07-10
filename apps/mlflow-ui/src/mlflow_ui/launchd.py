"""Generate (but never install) a launchd LaunchAgent plist for mlflow-ui.

This module only produces plist text. Installing it under
``~/Library/LaunchAgents`` is a separate, explicit operator action -- this
package never writes there on its own.
"""

from __future__ import annotations

import os
import plistlib

from mlflow_ui.config import REPO_ROOT, Config

LABEL: str = "com.horse-racing.mlflow-ui"

# Deliberately NON-secret operational/config vars only. Nothing in this
# tuple may be a credential, connection string, or account identifier --
# this plist's EnvironmentVariables dict lands in a world-readable-by-
# default file under ~/Library/LaunchAgents, and (per three separate past
# incidents) other agents doing unrelated investigation have read that file
# incidentally and ingested whatever secrets it embedded into their own
# transcripts. The supervised process (`mlflow_ui.cli foreground`, invoked
# by ProgramArguments below) never needs any of that carried in: `cli.main()`
# unconditionally calls `load_dotenv_local()` then
# `load_repo_root_env_fallback()` *before* dispatching to any subcommand,
# so it resolves HORSE_RACING_MLFLOW_BACKEND_URI/R2 credentials itself from
# apps/mlflow/.env.local (or the repo-root .env allow-listed fallback) at
# process startup -- with zero help from the plist. See generate_plist()'s
# docstring for the full contract.
#
# Explicitly EXCLUDED, and must stay excluded:
#   - HORSE_RACING_MLFLOW_BACKEND_URI: full Postgres/Neon DSN, plaintext
#     password included.
#   - R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY: R2 credential pair.
#   - R2_ACCOUNT_ID / CLOUDFLARE_ACCOUNT_ID: not credentials in the same
#     sense (an account id alone doesn't authenticate anything), but kept
#     out anyway for defense-in-depth -- they're redundant here (resolved
#     by the same loader chain as everything else) and there's no reason to
#     let a leaked plist narrow down which cloud account a leaked R2 key
#     (from some other leak) would belong to.
_CARRIED_ENV_VARS: tuple[str, ...] = (
    "HORSE_RACING_MLFLOW_DATA_DIR",
    "HORSE_RACING_MLFLOW_ARTIFACTS_MODE",
    "HORSE_RACING_MLFLOW_UI_HOST",
    "HORSE_RACING_MLFLOW_UI_PORT",
    "HORSE_RACING_MLFLOW_R2_BUCKET",
    "HORSE_RACING_MLFLOW_R2_PREFIX",
)


def _package_dir() -> str:
    return str(REPO_ROOT / "apps" / "mlflow-ui")


def generate_plist(cfg: Config, uv_path: str = "uv") -> str:
    """Return LaunchAgent plist XML text for supervising mlflow-ui.

    ``KeepAlive``/``RunAtLoad`` are both true so launchd restarts the server
    on crash and at login.

    Security contract (this is deliberate, not an oversight): the plist's
    ``EnvironmentVariables`` dict is a snapshot of only the non-secret
    ``_CARRIED_ENV_VARS`` that are set in the current process at generation
    time -- NOT the backend store URI or any R2 credential. This plist is
    intentionally *not* self-contained for secrets. Instead, the spawned
    process (``mlflow_ui.cli foreground``) resolves
    ``HORSE_RACING_MLFLOW_BACKEND_URI``/``R2_ACCESS_KEY_ID``/
    ``R2_SECRET_ACCESS_KEY`` (and any other credential) itself at startup,
    via the same 3-tier env resolution every other entrypoint in this
    package uses: explicit process env > ``apps/mlflow/.env.local``
    (``load_dotenv_local()``) > repo-root ``.env`` allow-listed fallback
    (``load_repo_root_env_fallback()``) -- both called unconditionally by
    ``cli.main()`` before any subcommand dispatch, including ``foreground``.
    A plist installed under ``~/Library/LaunchAgents`` is a plausible
    world-readable file (LaunchAgents plists are typically 644), so keeping
    credentials out of it entirely -- rather than relying on the operator
    remembering to ``chmod 600`` it -- is a real security improvement, not
    just a style preference. Do not re-add any credential-bearing var to
    ``_CARRIED_ENV_VARS``; see the comment there for the full rationale.
    """
    environment_variables = {
        name: os.environ[name] for name in _CARRIED_ENV_VARS if name in os.environ
    }

    plist: dict[str, object] = {
        "Label": LABEL,
        "ProgramArguments": [
            uv_path,
            "run",
            "--project",
            _package_dir(),
            "python",
            "-m",
            "mlflow_ui.cli",
            "foreground",
        ],
        "KeepAlive": True,
        "RunAtLoad": True,
        "StandardOutPath": str(cfg.data_dir / "mlflow-ui-launchd.out.log"),
        "StandardErrorPath": str(cfg.data_dir / "mlflow-ui-launchd.err.log"),
        "EnvironmentVariables": environment_variables,
    }

    return plistlib.dumps(plist, fmt=plistlib.FMT_XML).decode()
