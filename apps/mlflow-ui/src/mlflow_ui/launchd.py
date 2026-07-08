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

_CARRIED_ENV_VARS: tuple[str, ...] = (
    "HORSE_RACING_MLFLOW_DATA_DIR",
    "HORSE_RACING_MLFLOW_BACKEND_URI",
    "HORSE_RACING_MLFLOW_ARTIFACTS_MODE",
    "HORSE_RACING_MLFLOW_UI_HOST",
    "HORSE_RACING_MLFLOW_UI_PORT",
    "HORSE_RACING_MLFLOW_R2_BUCKET",
    "HORSE_RACING_MLFLOW_R2_PREFIX",
    "R2_ACCOUNT_ID",
    "CLOUDFLARE_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
)


def _package_dir() -> str:
    return str(REPO_ROOT / "apps" / "mlflow-ui")


def generate_plist(cfg: Config, uv_path: str = "uv") -> str:
    """Return LaunchAgent plist XML text for supervising mlflow-ui.

    ``KeepAlive``/``RunAtLoad`` are both true so launchd restarts the server
    on crash and at login. The environment carried into the plist is a
    snapshot of whichever ``HORSE_RACING_MLFLOW_*``/R2 vars are set in the
    current process at generation time, so the plist is self-contained.
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
