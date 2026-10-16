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
    curl = tools / "curl"
    curl.write_text(f"#!{sys.executable}\n{FAKE_PROGRAM}", encoding="utf-8")
    curl.chmod(0o700)

    def run(
        *args: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        environment = {
            "PATH": f"{tools}:/usr/bin:/bin",
            "HOME": str(tmp_path),
            "PROD_SKIP_ENV_FILE": "1",
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
        "--fail-with-body",
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


def test_get_uses_access_service_token_without_cloudflared(
    cli: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    (tmp_path / "bin/cloudflared").unlink()
    result = cli(
        "get",
        "/",
        env={
            "PC_KEIBA_ACCESS_CLIENT_ID": "client-id",
            "PC_KEIBA_ACCESS_CLIENT_SECRET": "client-secret",
        },
    )
    assert result.returncode == 0
    assert json.loads(result.stdout) == [
        "https://viewer.example/",
        "--request",
        "GET",
        "--fail-with-body",
        "--silent",
        "--show-error",
        "--location",
        "--max-redirs",
        "0",
        "--connect-timeout",
        "10",
        "--max-time",
        "30",
        "--header",
        "CF-Access-Client-Id: client-id",
        "--header",
        "CF-Access-Client-Secret: client-secret",
    ]


def test_get_http_error_remains_failure_with_service_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "get",
        env={
            "PC_KEIBA_ACCESS_CLIENT_ID": "client-id",
            "PC_KEIBA_ACCESS_CLIENT_SECRET": "client-secret",
            "TEST_EXIT_CODE": "22",
        },
    )
    assert result.returncode == 22
    assert "--fail-with-body" in json.loads(result.stdout)
    assert "--fail" not in json.loads(result.stdout)


def test_get_http_error_remains_failure_with_cloudflared(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("get", env={"TEST_EXIT_CODE": "22"})
    assert result.returncode == 22
    assert "--fail-with-body" in json.loads(result.stdout)
    assert "--fail" not in json.loads(result.stdout)


def test_logs_queries_observability_for_named_worker(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "logs",
        "pc-keiba-viewer",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "POST", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1].endswith("/accounts/acct123/workers/observability/telemetry/query")
    body = json.loads(args[args.index("--data") + 1])
    assert body["view"] == "events"
    assert body["parameters"]["filters"][0]["value"] == "pc-keiba-viewer"
    timeframe = body["timeframe"]
    assert isinstance(timeframe["from"], int) and isinstance(timeframe["to"], int)
    assert timeframe["to"] - timeframe["from"] == 15 * 60 * 1000


def test_logs_queries_a_bounded_historical_utc_range(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "logs",
        "finish-position-cron",
        "2026-09-22T18:55:00Z",
        "2026-09-22T19:30:00Z",
        "100",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    body = json.loads(args[args.index("--data") + 1])
    assert body["limit"] == 100
    assert body["timeframe"] == {
        "from": 1790103300000,
        "to": 1790105400000,
    }
    assert body["parameters"]["filters"][0]["value"] == "finish-position-cron"


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (
            ("logs", "worker", "2026-09-22T18:55:00", "2026-09-22T19:30:00Z"),
            "logs timestamps must use UTC (Z or +00:00).\n",
        ),
        (
            ("logs", "worker", "2026-09-22T19:30:00Z", "2026-09-22T18:55:00Z"),
            "logs UTC range must be ordered, not future-dated, and at most 7 days.\n",
        ),
        (
            ("logs", "worker", "2026-09-10T00:00:00Z", "2026-09-18T00:00:00Z"),
            "logs UTC range must be ordered, not future-dated, and at most 7 days.\n",
        ),
        (
            ("logs", "worker", "2026-09-22T18:55:00Z", "2026-09-22T19:30:00Z", "2001"),
            "logs event limit must be an integer from 1 to 2000.\n",
        ),
    ],
)
def test_logs_rejects_invalid_historical_ranges(
    cli: Callable[..., subprocess.CompletedProcess[str]],
    args: tuple[str, ...],
    message: str,
) -> None:
    result = cli(
        *args,
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 1
    assert result.stderr == message


def test_queues_lists_read_only_page_with_queue_ids(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "queues",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "GET", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert (
        args[-1]
        == "https://api.cloudflare.com/client/v4/accounts/acct123/queues?page=1"
    )


def test_workflow_instance_fetches_details_read_only(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "workflow-instance",
        "finish-position-race-day",
        "race-day-nar-20260923-mud1i9dh",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "GET", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == (
        "https://api.cloudflare.com/client/v4/accounts/acct123/workflows/"
        "finish-position-race-day/instances/race-day-nar-20260923-mud1i9dh"
    )


def test_workflow_instance_rejects_path_injection(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("workflow-instance", "finish-position-race-day", "../status")
    assert result.returncode == 1
    assert result.stderr == (
        "workflow-instance requires an instance ID of letters, digits, hyphens, or underscores.\n"
    )


def test_workflows_lists_running_instances_read_only(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "workflows",
        "finish-position-race-day",
        "running",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "GET", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == (
        "https://api.cloudflare.com/client/v4/accounts/acct123/workflows/"
        "finish-position-race-day/instances?status=running"
    )


def test_workflows_rejects_unsupported_status(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("workflows", "finish-position-race-day", "deleted")
    assert result.returncode == 1
    assert (
        result.stderr
        == "workflows status must be a documented Workflow instance status.\n"
    )


def test_prediction_readiness_is_a_bounded_authenticated_get(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "prediction-readiness",
        "20260925",
        env={"FINISH_POSITION_CRON_TRIGGER_TOKEN": "trigger-token"},
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "GET", "--fail"]
    assert "Authorization: Bearer trigger-token" in args
    assert args[-1] == (
        "https://finish-position-cron.kaoru.workers.dev/"
        "api/internal/prediction-readiness?runYmd=20260925"
    )


def test_prediction_readiness_scopes_category_without_starting_containers(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "prediction-readiness",
        "20260926",
        "nar",
        env={"FINISH_POSITION_CRON_TRIGGER_TOKEN": "trigger-token"},
    )
    assert result.returncode == 0
    assert json.loads(result.stdout)[-1] == (
        "https://finish-position-cron.kaoru.workers.dev/"
        "api/internal/prediction-readiness?runYmd=20260926&category=nar"
    )
    invalid = cli("prediction-readiness", "20260926", "nar;delete")
    assert invalid.returncode == 1
    assert (
        invalid.stderr == "prediction-readiness category must be jra, nar or ban-ei.\n"
    )


def test_prediction_readiness_rejects_unsafe_date_and_missing_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    invalid = cli("prediction-readiness", "20260925;delete")
    assert invalid.returncode == 1
    assert (
        invalid.stderr
        == "prediction-readiness requires YYYYMMDD and optional jra|nar|ban-ei.\n"
    )
    missing = cli("prediction-readiness", "20260925")
    assert missing.returncode == 1
    assert (
        missing.stderr
        == "Set FINISH_POSITION_CRON_TRIGGER_TOKEN for the internal read-only endpoint.\n"
    )


def test_worker_deployments_reads_only_the_prediction_worker(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "worker-deployments",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "GET", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == (
        "https://api.cloudflare.com/client/v4/accounts/acct123/workers/"
        "scripts/finish-position-cron/deployments"
    )


def test_worker_version_validates_uuid_and_reads_metadata(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    rejected = cli("worker-version", "uuid/../../../bad")
    assert rejected.returncode == 1
    assert rejected.stderr == "worker-version requires one UUID.\n"
    result = cli(
        "worker-version",
        "7e6b224f-b723-4aad-888b-7d6d307af3b8",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "GET", "--fail"]
    assert args[-1] == (
        "https://api.cloudflare.com/client/v4/accounts/acct123/workers/"
        "scripts/finish-position-cron/versions/7e6b224f-b723-4aad-888b-7d6d307af3b8"
    )


def test_queue_metrics_uses_read_only_queue_endpoint(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "queue-metrics",
        "queue123",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "GET", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == (
        "https://api.cloudflare.com/client/v4/accounts/acct123/queues/queue123/metrics"
    )


def test_d1_insights_reads_query_timing_and_rows_without_executing_sql(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "d1-insights",
        "9475ef83-d1da-4b47-9251-0cb431176aa8",
        "2026-09-16",
        "2026-09-22",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "POST", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == "https://api.cloudflare.com/client/v4/graphql"
    body = json.loads(args[args.index("--data") + 1])
    assert body["variables"] == {
        "accountTag": "acct123",
        "databaseId": "9475ef83-d1da-4b47-9251-0cb431176aa8",
        "start": "2026-09-16",
        "end": "2026-09-22",
    }
    assert "d1QueriesAdaptiveGroups" in body["query"]
    assert "sum_queryDurationMs_DESC" in body["query"]
    assert "queryDurationMs rowsRead rowsWritten" in body["query"]


def test_d1_insights_requires_database_and_date_arguments(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("d1-insights")
    assert result.returncode == 1
    assert result.stderr == (
        "d1-insights requires a D1 database UUID and inclusive UTC start/end dates.\n"
    )


def test_d1_insights_rejects_invalid_database_id(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("d1-insights", "../database", "2026-09-16", "2026-09-22")
    assert result.returncode == 1
    assert result.stderr == "d1-insights requires a valid D1 database UUID.\n"


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (
            "2026-09-16",
            "09-22",
            "d1-insights dates must use YYYY-MM-DD.\n",
        ),
        (
            "20260916",
            "2026-09-22",
            "d1-insights dates must use YYYY-MM-DD.\n",
        ),
        (
            "2026-09-22",
            "2026-09-16",
            "d1-insights date range must be ordered, not future-dated, and at most 31 days inclusive.\n",
        ),
        (
            "2026-08-01",
            "2026-09-15",
            "d1-insights date range must be ordered, not future-dated, and at most 31 days inclusive.\n",
        ),
        (
            "2099-09-22",
            "2099-09-23",
            "d1-insights date range must be ordered, not future-dated, and at most 31 days inclusive.\n",
        ),
    ],
)
def test_d1_insights_rejects_invalid_date_ranges(
    cli: Callable[..., subprocess.CompletedProcess[str]],
    start: str,
    end: str,
    message: str,
) -> None:
    result = cli(
        "d1-insights",
        "9475ef83-d1da-4b47-9251-0cb431176aa8",
        start,
        end,
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 1
    assert result.stderr == message


def test_d1_insights_requires_account_id(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "d1-insights",
        "9475ef83-d1da-4b47-9251-0cb431176aa8",
        "2026-09-16",
        "2026-09-22",
        env={"CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 1
    assert result.stderr == "Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.\n"


def test_d1_insights_requires_analytics_read_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "d1-insights",
        "9475ef83-d1da-4b47-9251-0cb431176aa8",
        "2026-09-16",
        "2026-09-22",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123"},
    )
    assert result.returncode == 1
    assert result.stderr == "Set CLOUDFLARE_API_TOKEN with Analytics Read permission.\n"


def test_day_base_owner_reads_only_the_current_repair_reservation(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("day-base-owner", "nar", "20260924")
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[:6] == [
        "x",
        "wrangler",
        "d1",
        "execute",
        "finish-position-cron-db",
        "--remote",
    ]
    assert (
        Path(args[7])
        == Path(__file__).resolve().parents[2]
        / "apps/finish-position-cron/wrangler.jsonc"
    )
    sql = args[args.index("--command") + 1]
    assert sql == (
        "select category, run_ymd, requested_at, repair_generation_id from "
        "finish_position_day_base_repair_requests where category = 'nar' and run_ymd = '20260924'"
    )
    assert "update " not in sql and "delete " not in sql


def test_day_base_owner_rejects_untrusted_category_and_date(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    category = cli("day-base-owner", "nar;drop", "20260924")
    assert category.returncode == 1
    assert category.stderr == "day-base-owner category must be jra, nar, or ban-ei.\n"

    date = cli("day-base-owner", "nar", "20260924;delete")
    assert date.returncode == 1
    assert date.stderr == "day-base-owner date must use YYYYMMDD.\n"


def test_day_base_manifest_reads_only_publication_state(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("day-base-manifest", "nar", "20260925")
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[:6] == [
        "x",
        "wrangler",
        "d1",
        "execute",
        "finish-position-cron-db",
        "--remote",
    ]
    assert (
        Path(args[7])
        == Path(__file__).resolve().parents[2]
        / "apps/finish-position-cron/wrangler.jsonc"
    )
    assert args[args.index("--command") + 1] == (
        "select category, run_ymd, status, repair_generation_id, object_key, object_etag, updated_at "
        "from finish_position_day_base_manifests where category = 'nar' and run_ymd = '20260925'"
    )
    assert args[-1] == "--json"


def test_day_base_manifest_rejects_untrusted_category_and_date(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    category = cli("day-base-manifest", "nar;drop", "20260925")
    assert category.returncode == 1
    assert (
        category.stderr == "day-base-manifest category must be jra, nar, or ban-ei.\n"
    )

    date = cli("day-base-manifest", "nar", "20260925;delete")
    assert date.returncode == 1
    assert date.stderr == "day-base-manifest date must use YYYYMMDD.\n"


def test_race_source_count_executes_a_fixed_read_only_remote_query(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("race-source-count", "nar", "20260925")
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[:6] == [
        "x",
        "wrangler",
        "d1",
        "execute",
        "sync-realtime-data",
        "--remote",
    ]
    assert (
        Path(args[7])
        == Path(__file__).resolve().parents[2]
        / "apps/finish-position-cron/wrangler.jsonc"
    )
    sql = args[args.index("--command") + 1]
    assert "count(*) as race_count" in sql
    assert "kaisai_nen = '2026' and kaisai_tsukihi = '0925'" in sql
    assert "source = 'nar'" in sql and "<> '83'" in sql
    assert args[-1] == "--json"


@pytest.mark.parametrize(
    ("category", "predicate"),
    [
        ("jra", "source = 'jra'"),
        ("nar", "source = 'nar'"),
        ("ban-ei", "= '83'"),
    ],
)
def test_race_source_count_uses_allowlisted_categories(
    cli: Callable[..., subprocess.CompletedProcess[str]], category: str, predicate: str
) -> None:
    result = cli("race-source-count", category, "20260925")
    assert result.returncode == 0
    args = json.loads(result.stdout)
    sql = args[args.index("--command") + 1]
    assert predicate in sql


def test_race_source_count_rejects_untrusted_category_and_date(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    category = cli("race-source-count", "nar;drop", "20260925")
    assert category.returncode == 1
    assert (
        category.stderr == "race-source-count category must be jra, nar, or ban-ei.\n"
    )

    date = cli("race-source-count", "nar", "20260925;delete")
    assert date.returncode == 1
    assert date.stderr == "race-source-count date must use YYYYMMDD.\n"


def test_container_usage_reads_runtime_and_billing_aggregates(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "container-usage",
        "2026-09-22T19:00:00Z",
        "2026-09-22T19:22:00Z",
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "POST", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == "https://api.cloudflare.com/client/v4/graphql"
    body = json.loads(args[args.index("--data") + 1])
    assert body["variables"] == {
        "accountTag": "acct123",
        "datetimeStart": "2026-09-22T19:00:00Z",
        "datetimeEnd": "2026-09-22T19:22:00Z",
    }
    assert "containersMetricsAdaptiveGroups" in body["query"]
    assert "containersUsageAdaptiveGroups" in body["query"]


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (
            "2026-09-22T19:00:00",
            "2026-09-22T19:22:00Z",
            "container-usage timestamps must use UTC (Z or +00:00).\n",
        ),
        (
            "2026-09-22T19:22:00Z",
            "2026-09-22T19:00:00Z",
            "container-usage UTC range must be ordered, not future-dated, and at most 7 days.\n",
        ),
        (
            "2026-09-10T00:00:00Z",
            "2026-09-18T00:00:00Z",
            "container-usage UTC range must be ordered, not future-dated, and at most 7 days.\n",
        ),
    ],
)
def test_container_usage_rejects_invalid_ranges(
    cli: Callable[..., subprocess.CompletedProcess[str]],
    start: str,
    end: str,
    message: str,
) -> None:
    result = cli(
        "container-usage",
        start,
        end,
        env={
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "CLOUDFLARE_API_TOKEN": "cf-token",
        },
    )
    assert result.returncode == 1
    assert result.stderr == message


def test_container_usage_requires_analytics_read_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "container-usage",
        "2026-09-22T19:00:00Z",
        "2026-09-22T19:22:00Z",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123"},
    )
    assert result.returncode == 1
    assert result.stderr == "Set CLOUDFLARE_API_TOKEN with Analytics Read permission.\n"


def test_browser_usage_queries_worker_events_and_session_duration(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "browser-usage",
        "2026-09-22T00:00:00Z",
        "2026-09-23T00:00:00Z",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "POST", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == "https://api.cloudflare.com/client/v4/graphql"
    body = json.loads(args[args.index("--data") + 1])
    assert body["variables"] == {
        "accountTag": "acct123",
        "datetimeStart": "2026-09-22T00:00:00Z",
        "datetimeEnd": "2026-09-23T00:00:00Z",
    }
    assert "browserRenderingEventsAdaptiveGroups" in body["query"]
    assert "dimensions { scriptName clientLibrary browserCloseReason }" in body["query"]
    assert "browserRenderingBrowserTimeUsageAdaptiveGroups" in body["query"]
    assert "sum { totalSessionDurationMs }" in body["query"]
    assert "limit: 50" in body["query"]
    assert "limit: 1" in body["query"]


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (
            "2026-09-22T00:00:00",
            "2026-09-23T00:00:00Z",
            "browser-usage timestamps must use UTC (Z or +00:00).\n",
        ),
        (
            "2026-09-23T00:00:00Z",
            "2026-09-22T00:00:00Z",
            "browser-usage UTC range must be ordered, not future-dated, and at most 1 hour with --sessions (otherwise 1 day).\n",
        ),
        (
            "2026-09-20T00:00:00Z",
            "2026-09-22T00:00:00Z",
            "browser-usage UTC range must be ordered, not future-dated, and at most 1 hour with --sessions (otherwise 1 day).\n",
        ),
        (
            "invalid",
            "2026-09-22T00:00:00Z",
            "browser-usage timestamps must be valid ISO-8601 values.\n",
        ),
    ],
)
def test_browser_usage_rejects_invalid_ranges(
    cli: Callable[..., subprocess.CompletedProcess[str]],
    start: str,
    end: str,
    message: str,
) -> None:
    result = cli(
        "browser-usage",
        start,
        end,
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 1
    assert result.stderr == message


def test_browser_usage_session_details_bounded_by_one_hour(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "browser-usage",
        "2026-09-22T12:00:00Z",
        "2026-09-22T13:00:00Z",
        "--sessions",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    query = json.loads(args[args.index("--data") + 1])["query"]
    assert "dimensions { sessionId clientLibrary browserCloseReason }" in query
    assert "dimensions { sessionId }" in query
    assert "limit: 10000" in query
    assert "limit: 5000" in query

    too_long = cli(
        "browser-usage",
        "2026-09-22T12:00:00Z",
        "2026-09-22T14:00:00Z",
        "--sessions",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert too_long.returncode == 1
    assert "at most 1 hour with --sessions" in too_long.stderr


def test_browser_usage_requires_analytics_read_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "browser-usage",
        "2026-09-22T00:00:00Z",
        "2026-09-23T00:00:00Z",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123"},
    )
    assert result.returncode == 1
    assert result.stderr == "Set CLOUDFLARE_API_TOKEN with Analytics Read permission.\n"


def test_kv_usage_queries_bounded_namespace_operations(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "kv-usage",
        "2026-09-18",
        "2026-09-24",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 0
    args = json.loads(result.stdout)
    assert args[0:3] == ["--request", "POST", "--fail"]
    assert "Authorization: Bearer cf-token" in args
    assert args[-1] == "https://api.cloudflare.com/client/v4/graphql"
    body = json.loads(args[args.index("--data") + 1])
    assert body["variables"] == {
        "accountTag": "acct123",
        "datetimeStart": "2026-09-18T00:00:00Z",
        "datetimeEnd": "2026-09-24T00:00:00Z",
    }
    assert "kvOperationsAdaptiveGroups" in body["query"]
    assert "dimensions { namespaceId actionType date }" in body["query"]
    assert "sum { requests }" in body["query"]
    assert "limit: 5000" in body["query"]


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (
            "2026-09-18",
            "2026-09-18",
            "kv-usage date range must be ordered, completed, and at most 7 days.\n",
        ),
        (
            "2026-09-17",
            "2026-09-25",
            "kv-usage date range must be ordered, completed, and at most 7 days.\n",
        ),
        ("2026-09-18oops", "2026-09-24", "kv-usage dates must use YYYY-MM-DD.\n"),
        (
            "9999-12-30",
            "9999-12-31",
            "kv-usage date range must be ordered, completed, and at most 7 days.\n",
        ),
    ],
)
def test_kv_usage_rejects_invalid_dates(
    cli: Callable[..., subprocess.CompletedProcess[str]],
    start: str,
    end: str,
    message: str,
) -> None:
    result = cli(
        "kv-usage",
        start,
        end,
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 1
    assert result.stderr == message


def test_kv_usage_requires_analytics_read_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "kv-usage",
        "2026-09-18",
        "2026-09-24",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123"},
    )
    assert result.returncode == 1
    assert result.stderr == "Set CLOUDFLARE_API_TOKEN with Analytics Read permission.\n"


def test_queue_metrics_requires_queue_read_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("queue-metrics", "queue123", env={"CLOUDFLARE_ACCOUNT_ID": "acct123"})
    assert result.returncode == 1
    assert result.stderr == "Set CLOUDFLARE_API_TOKEN with Queues Read permission.\n"


def test_queue_metrics_rejects_unsafe_queue_id(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli(
        "queue-metrics",
        "../queues",
        env={"CLOUDFLARE_ACCOUNT_ID": "acct123", "CLOUDFLARE_API_TOKEN": "cf-token"},
    )
    assert result.returncode == 1
    assert (
        result.stderr
        == "queue-metrics requires one Queue ID (letters and digits only).\n"
    )


def test_logs_rejects_unsafe_worker_names(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("logs", "../etc/passwd", env={"CLOUDFLARE_API_TOKEN": "cf-token"})
    assert result.returncode == 1
    assert result.stdout == ""
    assert (
        result.stderr
        == "logs requires a Worker name of letters, digits, and hyphens.\n"
    )


def test_logs_requires_account_id(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("logs", env={"CLOUDFLARE_API_TOKEN": "cf-token"})
    assert result.returncode == 1
    assert result.stderr == "Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.\n"


def test_logs_requires_api_token(
    cli: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = cli("logs", env={"CLOUDFLARE_ACCOUNT_ID": "acct123"})
    assert result.returncode == 1
    assert (
        result.stderr == "Set CLOUDFLARE_API_TOKEN to query production Worker logs.\n"
    )


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
        ("queues", "0"),
        ("queues", "1", "2"),
        ("workflows",),
        ("workflow-instance",),
        ("workflow-instance", "name", "id", "extra"),
        ("workflows", "name", "running", "extra"),
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
