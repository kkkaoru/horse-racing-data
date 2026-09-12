from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "prod.sh"
FAKE_PROGRAM = """import json
import os
import sys
print(json.dumps(sys.argv[1:]))
sys.exit(int(os.environ.get('TEST_EXIT_CODE', '0')))
"""


@pytest.fixture
def cli(tmp_path: Path) -> Callable[..., subprocess.CompletedProcess[str]]:
    tools = tmp_path / "bin"
    tools.mkdir()
    bun = tools / "bun"
    bun.write_text(f"#!{sys.executable}\n{FAKE_PROGRAM}", encoding="utf-8")
    bun.chmod(0o700)
    cloudflared = tools / "cloudflared"
    cloudflared.write_text(f"#!{sys.executable}\n{FAKE_PROGRAM}", encoding="utf-8")
    cloudflared.chmod(0o700)

    def run(
        *args: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        environment = {
            "PATH": f"{tools}:/usr/bin:/bin",
            "HOME": str(tmp_path),
            "PC_KEIBA_VIEWER_ORIGIN": "https://viewer.example",
        }
        environment.update(env or {})
        return subprocess.run(
            ["/bin/bash", str(SCRIPT), *args],
            cwd=tmp_path,
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )

    return run


def test_help(cli: Callable[..., subprocess.CompletedProcess[str]]) -> None:
    result = cli()
    assert result.returncode == 0
    assert result.stdout.splitlines()[0] == "Usage: bun run prod <command> [arguments]"


def test_status_reuses_wrangler(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("status")
    assert result.returncode == 0
    assert json.loads(result.stdout) == ["x", "wrangler", "whoami"]


def test_login_does_not_print_jwt(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("login")
    assert result.returncode == 0
    assert json.loads(result.stdout) == [
        "access",
        "login",
        "--quiet",
        "https://viewer.example/",
    ]


def test_url(cli: Callable[..., subprocess.CompletedProcess[str]]) -> None:
    result = cli("url", "/races/2026/09/12/83/01")
    assert result.returncode == 0
    assert result.stdout == "https://viewer.example/races/2026/09/12/83/01\n"


def test_trailing_origin_slash_and_separators(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "--", "url", "--", env={"PC_KEIBA_VIEWER_ORIGIN": "https://viewer.example/"}
    )
    assert result.returncode == 0
    assert result.stdout == "https://viewer.example/\n"


def test_get_is_read_only_and_does_not_follow_redirects(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("get", "/api/example?date=20260912")
    assert result.returncode == 0
    assert json.loads(result.stdout) == [
        "access",
        "curl",
        "https://viewer.example/api/example?date=20260912",
        "--request",
        "GET",
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--max-redirs",
        "0",
        "--connect-timeout",
        "10",
        "--max-time",
        "30",
    ]


@pytest.mark.parametrize(
    "path",
    [
        "https://other.example/",
        "//other.example/",
        "/\\other.example/",
        "/x\ny",
        "/x\ry",
        "/x\ty",
    ],
)
def test_rejects_unsafe_paths(
    cli: Callable[..., subprocess.CompletedProcess[str]], path: str
) -> None:
    result = cli("get", path)
    assert result.returncode == 1
    assert result.stdout == ""
    assert (
        result.stderr
        == "Use a site-relative path beginning with one /; URLs and control characters are rejected.\n"
    )


@pytest.mark.parametrize(
    "origin",
    [
        "",
        "http://viewer.example",
        "https://user:secret@viewer.example",
        "https://viewer.example/path",
        "https://viewer.example?x=1",
    ],
)
def test_rejects_unsafe_origins(
    cli: Callable[..., subprocess.CompletedProcess[str]], origin: str
) -> None:
    result = cli("url", env={"PC_KEIBA_VIEWER_ORIGIN": origin})
    assert result.returncode == 1
    assert result.stdout == ""
    assert (
        result.stderr
        == "Set PC_KEIBA_VIEWER_ORIGIN to an HTTPS origin (no credentials, path, or query).\n"
    )


def test_kv_is_remote_read_with_existing_binding(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("kv", "pred:fp:v1:20260912:83:01")
    args = json.loads(result.stdout)
    config = args.pop(7)
    assert result.returncode == 0
    assert Path(config).parts[-3:] == ("apps", "pc-keiba-viewer", "wrangler.jsonc")
    assert args == [
        "x",
        "wrangler",
        "kv",
        "key",
        "get",
        "pred:fp:v1:20260912:83:01",
        "--config",
        "--binding",
        "DETAIL_SECTION_CACHE_KV",
        "--remote",
        "--text",
    ]


def test_r2_is_remote_download(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("r2", "features/example.parquet", "download.parquet")
    args = json.loads(result.stdout)
    config = args.pop(7)
    assert result.returncode == 0
    assert Path(config).parts[-3:] == ("apps", "pc-keiba-viewer", "wrangler.jsonc")
    assert args == [
        "x",
        "wrangler",
        "r2",
        "object",
        "get",
        "features/example.parquet",
        "--config",
        "--remote",
        "--file",
        "download.parquet",
    ]


def test_r2_refuses_overwrite(
    cli: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    (tmp_path / "download.parquet").write_bytes(b"existing")
    result = cli("r2", "features/example.parquet", "download.parquet")
    assert result.returncode == 1
    assert result.stderr == "Output already exists; refusing to overwrite it.\n"
    assert (tmp_path / "download.parquet").read_bytes() == b"existing"


def test_r2_refuses_dangling_symlink(
    cli: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    (tmp_path / "download.parquet").symlink_to(tmp_path / "absent")
    result = cli("r2", "features/example.parquet", "download.parquet")
    assert result.returncode == 1
    assert result.stderr == "Output already exists; refusing to overwrite it.\n"


@pytest.mark.parametrize(
    "args",
    [
        ("get", "/", "-XPOST"),
        ("login", "extra"),
        ("status", "extra"),
        ("kv",),
        ("kv", ""),
        ("kv", "--remote"),
        ("r2",),
        ("r2", "no-bucket", "file"),
        ("r2", "--bucket/key", "file"),
        ("r2", "bucket/key", "--file"),
        ("r2", "bucket/key", ""),
    ],
)
def test_rejects_bad_arguments(
    cli: Callable[..., subprocess.CompletedProcess[str]], args: tuple[str, ...]
) -> None:
    result = cli(*args)
    assert result.returncode == 1
    assert result.stdout == ""
    assert bool(result.stderr) is True


def test_unknown_command(cli: Callable[..., subprocess.CompletedProcess[str]]) -> None:
    result = cli("deploy")
    assert result.returncode == 1
    assert result.stderr.splitlines()[-1] == "Unknown production command: deploy"


def test_missing_cloudflared(
    cli: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    (tmp_path / "bin/cloudflared").unlink()
    result = cli("get")
    assert result.returncode == 1
    assert result.stderr == "Required command not found: cloudflared\n"


def test_missing_bun(
    cli: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    (tmp_path / "bin/bun").unlink()
    result = cli("status")
    assert result.returncode == 1
    assert result.stderr == "Required command not found: bun\n"


def test_command_failure_is_preserved(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("get", env={"TEST_EXIT_CODE": "23"})
    assert result.returncode == 23
