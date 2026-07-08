// Run with bun.
import { expect, it, vi } from "vitest";

import {
  buildDefaultConfig,
  buildHotOddsUrl,
  summarizeLatestCounts,
  validateHotOddsPayload,
  verifyCloudflareResource,
  verifyD1OddsWritesStopped,
  verifyHotApiRace,
  verifyOddsR2Cutover,
  verifyR2SqlNamespace,
  type CommandResult,
  type OddsR2CutoverConfig,
} from "./verify-odds-r2-cutover";

const buildCommandResult = (overrides: Partial<CommandResult> = {}): CommandResult => ({
  code: 0,
  stderr: "",
  stdout: "",
  ...overrides,
});

const buildConfig = (overrides: Partial<OddsR2CutoverConfig> = {}): OddsR2CutoverConfig => ({
  commandImpl: vi.fn(async () => buildCommandResult()),
  d1DatabaseName: "sync-realtime-data-hot-v2",
  fetchImpl: vi.fn(),
  hotApiBaseUrl: "https://hot.example.com",
  raceKeys: ["nar:2026:0708:30:11"],
  r2SqlAuthToken: "token",
  sinceFetchedAt: "2026-07-08T18:52:00+09:00",
  warehouse: "warehouse",
  ...overrides,
});

const buildResponse = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status: init?.status ?? 200,
  });

const buildFullOddsPayload = () => ({
  fetchedAt: "2026-07-08T19:15:18+09:00",
  latest: {
    "3renpuku": [{ combination: "1-2-3" }],
    "3rentan": [{ combination: "1-2-3" }],
    fukusho: [{ combination: "1" }],
    tansho: [{ combination: "1" }],
    umaren: [{ combination: "1-2" }],
    umatan: [{ combination: "1-2" }],
    wakuren: [{ combination: "1-2" }],
    wide: [{ combination: "1-2" }],
  },
  raceKey: "nar:2026:0708:30:11",
});

it("buildDefaultConfig reads env overrides and comma separated race keys", () => {
  const config = buildDefaultConfig(vi.fn(), vi.fn(), {
    ODDS_HOT_API_BASE_URL: "https://override.example.com",
    ODDS_D1_DATABASE_NAME: "custom-d1",
    ODDS_R2_SQL_WAREHOUSE: "warehouse2",
    ODDS_R2_VERIFY_D1_CUTOFF: "2026-07-08T19:00:00+09:00",
    ODDS_R2_VERIFY_RACE_KEYS: " a , b ,, ",
    WRANGLER_R2_SQL_AUTH_TOKEN: "sql-token",
  });
  expect(config.hotApiBaseUrl).toBe("https://override.example.com");
  expect(config.d1DatabaseName).toBe("custom-d1");
  expect(config.raceKeys).toStrictEqual(["a", "b"]);
  expect(config.r2SqlAuthToken).toBe("sql-token");
  expect(config.sinceFetchedAt).toBe("2026-07-08T19:00:00+09:00");
  expect(config.warehouse).toBe("warehouse2");
});

it("buildDefaultConfig falls back to R2_API_TOKEN and defaults", () => {
  const config = buildDefaultConfig(vi.fn(), vi.fn(), { R2_API_TOKEN: "r2-token" });
  expect(config.hotApiBaseUrl).toBe("https://sync-realtime-data-hot.kkk4oru.com");
  expect(config.d1DatabaseName).toBe("sync-realtime-data-hot-v2");
  expect(config.r2SqlAuthToken).toBe("r2-token");
  expect(config.raceKeys).toStrictEqual([]);
});

it("buildHotOddsUrl encodes race key and sets fresh=1", () => {
  expect(buildHotOddsUrl("https://hot.example.com", "nar:2026:0708:30:11")).toBe(
    "https://hot.example.com/api/odds/nar%3A2026%3A0708%3A30%3A11?fresh=1",
  );
});

it("summarizeLatestCounts returns counts only for array values", () => {
  expect(summarizeLatestCounts({ latest: { tansho: [{ x: 1 }], wide: "bad" } })).toStrictEqual({
    tansho: 1,
    wide: 0,
  });
});

it("summarizeLatestCounts returns empty object when latest is absent", () => {
  expect(summarizeLatestCounts({})).toStrictEqual({});
});

it("validateHotOddsPayload returns all ok for complete payload", () => {
  const checks = validateHotOddsPayload("nar:2026:0708:30:11", buildFullOddsPayload());
  expect(checks.every((check) => check.ok)).toBe(true);
});

it("validateHotOddsPayload reports mismatched race key and missing odds types", () => {
  const checks = validateHotOddsPayload("nar:2026:0708:30:11", {
    fetchedAt: "",
    latest: { tansho: [] },
    raceKey: "other",
  });
  expect(checks.map((check) => check.ok)).toStrictEqual([false, false, false]);
});

it("verifyHotApiRace fetches fresh payload and validates it", async () => {
  const fetchImpl = vi.fn(async () => buildResponse(buildFullOddsPayload()));
  const checks = await verifyHotApiRace(buildConfig({ fetchImpl }), "nar:2026:0708:30:11");
  expect(fetchImpl).toHaveBeenCalledWith(
    "https://hot.example.com/api/odds/nar%3A2026%3A0708%3A30%3A11?fresh=1",
  );
  expect(checks.every((check) => check.ok)).toBe(true);
});

it("verifyHotApiRace returns failed check for non-ok response", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ error: "bad" }, { status: 503 }));
  const checks = await verifyHotApiRace(buildConfig({ fetchImpl }), "nar:2026:0708:30:11");
  expect(checks).toStrictEqual([
    { detail: "status=503", name: "hot-api fetch nar:2026:0708:30:11", ok: false },
  ]);
});

it("verifyD1OddsWritesStopped passes when D1 count is zero", async () => {
  const commandImpl = vi
    .fn()
    .mockResolvedValueOnce(
      buildCommandResult({ stdout: JSON.stringify([{ results: [{ name: "odds_snapshots" }] }]) }),
    )
    .mockResolvedValueOnce(
      buildCommandResult({ stdout: JSON.stringify([{ results: [{ rows: 0 }] }]) }),
    );
  const check = await verifyD1OddsWritesStopped(buildConfig({ commandImpl }));
  expect(check.ok).toBe(true);
  expect(commandImpl).toHaveBeenNthCalledWith(1, [
    "bunx",
    "wrangler",
    "d1",
    "execute",
    "sync-realtime-data-hot-v2",
    "--remote",
    "--command",
    "select name from sqlite_master where type = 'table' and name = 'odds_snapshots'",
  ]);
  expect(commandImpl).toHaveBeenNthCalledWith(2, [
    "bunx",
    "wrangler",
    "d1",
    "execute",
    "sync-realtime-data-hot-v2",
    "--remote",
    "--command",
    "select count(*) as rows from odds_snapshots where fetched_at >= '2026-07-08T18:52:00+09:00'",
  ]);
});

it("verifyD1OddsWritesStopped passes when the legacy table has been dropped", async () => {
  const commandImpl = vi.fn(async () =>
    buildCommandResult({ stdout: JSON.stringify([{ results: [] }]) }),
  );
  const check = await verifyD1OddsWritesStopped(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "table dropped",
    name: "d1 odds_snapshots stopped",
    ok: true,
  });
  expect(commandImpl).toHaveBeenCalledTimes(1);
});

it("verifyD1OddsWritesStopped escapes single quotes in cutoff", async () => {
  const commandImpl = vi
    .fn()
    .mockResolvedValueOnce(
      buildCommandResult({ stdout: JSON.stringify([{ results: [{ name: "odds_snapshots" }] }]) }),
    )
    .mockResolvedValueOnce(
      buildCommandResult({ stdout: JSON.stringify([{ results: [{ rows: 1 }] }]) }),
    );
  const check = await verifyD1OddsWritesStopped(
    buildConfig({ commandImpl, sinceFetchedAt: "x'y" }),
  );
  expect(check.ok).toBe(false);
  expect(commandImpl).toHaveBeenNthCalledWith(2, [
    "bunx",
    "wrangler",
    "d1",
    "execute",
    "sync-realtime-data-hot-v2",
    "--remote",
    "--command",
    "select count(*) as rows from odds_snapshots where fetched_at >= 'x''y'",
  ]);
});

it("verifyD1OddsWritesStopped reports stderr when command fails", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ code: 1, stderr: "boom" }));
  const check = await verifyD1OddsWritesStopped(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "boom",
    name: "d1 odds_snapshots stopped",
    ok: false,
  });
});

it("verifyCloudflareResource checks command output for expected text", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "resource-name" }));
  const check = await verifyCloudflareResource(commandImpl, ["cmd"], "resource-name", "resource");
  expect(check).toStrictEqual({
    detail: "expected=resource-name",
    name: "resource",
    ok: true,
  });
});

it("verifyCloudflareResource returns failure details from stderr", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ code: 1, stderr: "nope" }));
  const check = await verifyCloudflareResource(commandImpl, ["cmd"], "missing", "resource");
  expect(check).toStrictEqual({ detail: "nope", name: "resource", ok: false });
});

it("verifyR2SqlNamespace fails when token is missing", async () => {
  const check = await verifyR2SqlNamespace(buildConfig({ r2SqlAuthToken: undefined }));
  expect(check).toStrictEqual({
    detail: "missing WRANGLER_R2_SQL_AUTH_TOKEN",
    name: "r2-sql namespace odds",
    ok: false,
  });
});

it("verifyR2SqlNamespace passes when SHOW NAMESPACES contains odds", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "default\nodds\n" }));
  const check = await verifyR2SqlNamespace(buildConfig({ commandImpl }));
  expect(check.ok).toBe(true);
  expect(commandImpl).toHaveBeenCalledWith(
    ["bunx", "wrangler", "r2", "sql", "query", "warehouse", "SHOW NAMESPACES"],
    { WRANGLER_R2_SQL_AUTH_TOKEN: "token" },
  );
});

it("verifyR2SqlNamespace includes command output when unauthorized", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ code: 1, stderr: "Unauthorized" }));
  const check = await verifyR2SqlNamespace(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "Unauthorized",
    name: "r2-sql namespace odds",
    ok: false,
  });
});

it("verifyR2SqlNamespace treats unauthorized output as failure even when command exits zero", async () => {
  const commandImpl = vi.fn(async () =>
    buildCommandResult({ stdout: "ERROR\n80013: Unauthorized\nodds" }),
  );
  const check = await verifyR2SqlNamespace(buildConfig({ commandImpl }));
  expect(check).toStrictEqual({
    detail: "ERROR\n80013: Unauthorized\nodds",
    name: "r2-sql namespace odds",
    ok: false,
  });
});

it("verifyOddsR2Cutover aggregates all checks and fails when sink is absent", async () => {
  const fetchImpl = vi.fn(async () => buildResponse(buildFullOddsPayload()));
  const commandImpl = vi.fn(async (args: string[]) => {
    const joined = args.join(" ");
    if (joined.includes("d1 execute")) {
      return buildCommandResult({ stdout: JSON.stringify([{ results: [{ rows: 0 }] }]) });
    }
    if (joined.includes("catalog get")) {
      return buildCommandResult({ stdout: "Status:       active" });
    }
    if (joined.includes("streams list")) {
      return buildCommandResult({ stdout: "odds_snapshots_hot_stream" });
    }
    if (joined.includes("sinks list")) {
      return buildCommandResult({ stdout: "other_sink" });
    }
    if (joined.includes("pipelines list")) {
      return buildCommandResult({ stdout: "odds_snapshots_hot_pipeline" });
    }
    if (joined.includes("r2 sql query")) {
      return buildCommandResult({ stdout: "odds" });
    }
    return buildCommandResult();
  });
  const result = await verifyOddsR2Cutover(buildConfig({ commandImpl, fetchImpl }));
  expect(result.ok).toBe(false);
  expect(result.checks.some((check) => check.name === "pipeline sink exists" && !check.ok)).toBe(
    true,
  );
});

it("verifyOddsR2Cutover reports missing race keys", async () => {
  const commandImpl = vi.fn(async () => buildCommandResult({ stdout: "no resources" }));
  const result = await verifyOddsR2Cutover(buildConfig({ commandImpl, raceKeys: [] }));
  expect(result.ok).toBe(false);
  expect(result.checks[0]).toStrictEqual({
    detail: "missing ODDS_R2_VERIFY_RACE_KEYS",
    name: "hot-api race keys",
    ok: false,
  });
});
