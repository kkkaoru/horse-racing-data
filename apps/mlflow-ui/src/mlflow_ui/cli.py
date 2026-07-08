"""Command-line entrypoint for the mlflow-ui launcher/operator."""

from __future__ import annotations

import argparse
from pathlib import Path

from mlflow_ui import server
from mlflow_ui.config import load_config
from mlflow_ui.launchd import generate_plist

EXIT_OK: int = 0
EXIT_ERROR: int = 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mlflow_ui",
        description="Launcher/operator for the MLflow tracking UI server.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("start", help="Start mlflow server in the background")
    subparsers.add_parser("stop", help="Stop a background mlflow server")
    subparsers.add_parser("status", help="Report whether mlflow server is running")
    subparsers.add_parser("foreground", help="Run mlflow server in the foreground")

    plist_parser = subparsers.add_parser(
        "plist", help="Generate a launchd LaunchAgent plist (prints to stdout by default)"
    )
    plist_parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write the generated plist to this path instead of stdout",
    )

    return parser


def _run_start() -> int:
    cfg = load_config()
    result = server.start(cfg)
    print(result.message)
    return EXIT_OK if result.ok else EXIT_ERROR


def _run_stop() -> int:
    cfg = load_config()
    result = server.stop(cfg)
    print(result.message)
    return EXIT_OK if result.ok else EXIT_ERROR


def _run_status() -> int:
    cfg = load_config()
    result = server.status(cfg)
    if result.running:
        print(f"mlflow-ui is running (pid {result.pid}) at {result.url}")
    else:
        print(f"mlflow-ui is not running (would serve at {result.url})")
    return EXIT_OK


def _run_foreground() -> int:
    cfg = load_config()
    server.run_foreground(cfg)
    return EXIT_OK


def _run_plist(output: Path | None) -> int:
    cfg = load_config()
    text = generate_plist(cfg)
    if output is None:
        print(text)
    else:
        output.write_text(text)
        print(f"wrote plist to {output}")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "start":
            return _run_start()
        if args.command == "stop":
            return _run_stop()
        if args.command == "status":
            return _run_status()
        if args.command == "foreground":
            return _run_foreground()
        return _run_plist(args.output)
    except (ValueError, RuntimeError) as exc:
        print(f"error: {exc}")
        return EXIT_ERROR


if __name__ == "__main__":
    # Allows `python -m mlflow_ui.cli ...` (documented in README.md and used
    # by the generated launchd ProgramArguments) to work directly, in
    # addition to `python -m mlflow_ui ...` via __main__.py.
    raise SystemExit(main())
