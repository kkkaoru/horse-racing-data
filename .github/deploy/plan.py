"""Select production services from a complete Git diff, preserving deploy order."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Final

TARGETS: Final[tuple[str, ...]] = (
    "pc-keiba-r2-catalog",
    "jra-van-datalab-worker-only-probe",
    "umacon-worker",
    "daily-keiba-sync",
    "sync-realtime-data-features",
    "sync-realtime-data-hot",
    "sync-realtime-data",
    "finish-position-cron",
    "pc-keiba-viewer",
    "pipeline-health-monitor",
    "venue-weather",
    "mlflow-ui-proxy",
    "jra-van-datalab-cloudflare-demo",
)
GLOBAL_FILES: Final[frozenset[str]] = frozenset({"bun.lock", "package.json", "tsconfig.json"})
EXTRA_INPUTS: Final[dict[str, tuple[str, ...]]] = {
    "finish-position-cron": (
        "apps/finish-position-predict-container/",
        "apps/pc-keiba-viewer/src/scripts/",
        "apps/pc-keiba-viewer/finish-position/lookups/",
        "scripts/ensure-docker-compat.sh",
    ),
    "pc-keiba-viewer": ("apps/sync-realtime-data-hot/",),
    "sync-realtime-data": (
        "apps/pc-keiba-viewer/src/lib/jra-url.ts",
        "apps/pc-keiba-viewer/src/lib/win5/",
    ),
    "sync-realtime-data-hot": ("apps/pc-keiba-viewer/src/lib/jra-url.ts",),
    "mlflow-ui-proxy": ("apps/mlflow/", "scripts/ensure-docker-compat.sh"),
    "jra-van-datalab-cloudflare-demo": ("scripts/ensure-docker-compat.sh",),
}


def is_runtime_input(path: str) -> bool:
    """Test fixtures and documentation do not alter deployed runtime behavior."""
    parts = PurePosixPath(path).parts
    return not (
        path.endswith((".md", ".test.ts", ".test.tsx"))
        or any(part in {"test", "tests", "__tests__", "docs"} for part in parts)
        or (PurePosixPath(path).name.startswith("test_") and path.endswith(".py"))
    )


def select_targets(paths: Iterable[str], requested: str) -> list[str]:
    """An explicit dispatch selects one service/all; pushes select affected inputs."""
    if requested == "all":
        return list(TARGETS)
    if requested != "changed":
        if requested not in TARGETS:
            raise ValueError(f"Unknown deployment target: {requested}")
        return [requested]
    changed = tuple(path for path in paths if is_runtime_input(path))
    if any(path in GLOBAL_FILES or path.startswith("packages/") for path in changed):
        return list(TARGETS)
    return [
        target
        for target in TARGETS
        if any(
            path.startswith((f"apps/{target}/", *EXTRA_INPUTS.get(target, ()))) for path in changed
        )
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", required=True, type=Path)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    paths = args.paths.read_text(encoding="utf-8").split("\0")
    print(json.dumps(select_targets(paths, args.target)))


if __name__ == "__main__":
    main()
