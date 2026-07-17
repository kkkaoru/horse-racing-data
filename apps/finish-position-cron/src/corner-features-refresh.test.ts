// Run with bun. Tests for the corner-features Neon-direct refresh (§4.4).

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const { neonMock, queryMock } = vi.hoisted(() => {
  const query = vi.fn(async () => undefined);
  return { neonMock: vi.fn(() => ({ query })), queryMock: query };
});

vi.mock("@neondatabase/serverless", () => ({ neon: neonMock }));

import {
  CORNER_FEATURES_REFRESH_CRON_EVENING,
  CORNER_FEATURES_REFRESH_CRON_MORNING,
  refreshCornerFeatures,
  shouldRunCornerFeaturesRefreshCron,
} from "./corner-features-refresh";

const makeEnv = (): Env => ({ NEON_DATABASE_URL: "postgres://example" }) as Env;

// CREATE EXTENSION + CREATE TABLE + 17 ALTER + 1 upsert + 5 CREATE INDEX. Split
// into two separate statements/calls (not one semicolon-joined string) since
// 2026-07-17: neon()'s serverless HTTP driver rejects a single sql.query() call
// containing more than one command ("cannot insert multiple commands into a
// prepared statement"), confirmed against real production Neon while
// backfilling the NAR zero-row gap (corner-features-settlement-backfill-heal-
// 2026-07-17.md) -- the mocked driver here never exercised that real
// constraint, so the previous single combined statement passed every test
// while never actually succeeding against real Neon.
const EXPECTED_STATEMENT_COUNT = 25;

beforeEach(() => {
  neonMock.mockClear();
  queryMock.mockClear();
  queryMock.mockResolvedValue(undefined);
});

test("connects to Neon via env.NEON_DATABASE_URL", async () => {
  const env = makeEnv();
  await refreshCornerFeatures({ daysAhead: 2, env, runYmd: "20260712" });
  expect(neonMock).toHaveBeenCalledWith("postgres://example");
});

test("logs an unconditional start line before any work, so an uncaught throw still leaves evidence", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  neonMock.mockImplementationOnce(() => {
    throw new Error("neon() constructor blew up");
  });
  await expect(
    refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" }),
  ).resolves.toBeUndefined();
  expect(logSpy).toHaveBeenCalledWith(
    "[corner-features-refresh] start runYmd=20260712 daysAhead=2 lookbackDays=0",
  );
  logSpy.mockRestore();
});

test("logs the configured lookbackDays in the start line when set", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await refreshCornerFeatures({
    daysAhead: 2,
    env: makeEnv(),
    lookbackDays: 7,
    runYmd: "20260712",
  });
  expect(logSpy).toHaveBeenCalledWith(
    "[corner-features-refresh] start runYmd=20260712 daysAhead=2 lookbackDays=7",
  );
  logSpy.mockRestore();
});

test("runs the extension DDL first, the table DDL second, and the horse-history index last", async () => {
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledTimes(EXPECTED_STATEMENT_COUNT);
  expect(queryMock).toHaveBeenNthCalledWith(1, "create extension if not exists vector");
  expect(queryMock).toHaveBeenNthCalledWith(
    2,
    expect.stringContaining("create table if not exists race_entry_corner_features"),
  );
  expect(queryMock).toHaveBeenNthCalledWith(
    EXPECTED_STATEMENT_COUNT,
    expect.stringContaining("race_entry_corner_features_horse_history_idx"),
  );
});

test("a CREATE EXTENSION permission failure does not block the rest of the refresh", async () => {
  const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  queryMock.mockRejectedValueOnce(
    new Error("cannot execute CREATE EXTENSION in a read-only transaction"),
  );
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledTimes(EXPECTED_STATEMENT_COUNT);
  expect(consoleWarn).toHaveBeenCalledWith(
    expect.stringContaining("create extension vector skipped"),
  );
  consoleWarn.mockRestore();
});

test("the upsert statement filters on a forward-looking date window (runYmd to runYmd+daysAhead)", async () => {
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi >= '20260712'"),
  );
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi <= '20260714'"),
  );
});

test("the upsert statement covers both jra and nar source tables", async () => {
  await refreshCornerFeatures({ daysAhead: 0, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledWith(expect.stringContaining("from jvd_se se"));
  expect(queryMock).toHaveBeenCalledWith(expect.stringContaining("from nvd_se se"));
});

test("date window correctly rolls over a month boundary", async () => {
  await refreshCornerFeatures({ daysAhead: 3, env: makeEnv(), runYmd: "20260129" });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi <= '20260201'"),
  );
});

test("date window correctly rolls over a year boundary", async () => {
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20261230" });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi <= '20270101'"),
  );
});

test("swallows and logs a query failure instead of throwing", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  // First call is the extension DDL, which ensureVectorExtension swallows
  // internally (see the dedicated non-fatal test above) -- reject the second
  // call (table DDL) to exercise the outer catch this test targets.
  queryMock.mockResolvedValueOnce(undefined); // extension DDL
  queryMock.mockRejectedValueOnce(new Error("neon unreachable")); // table DDL
  await expect(
    refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" }),
  ).resolves.toBeUndefined();
  expect(consoleError).toHaveBeenCalledWith(
    expect.stringContaining(
      "failed runYmd=20260712 daysAhead=2 lookbackDays=0: Error: neon unreachable",
    ),
  );
  consoleError.mockRestore();
});

test("logs the resolved fromDate/toDate window in the ok line", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await refreshCornerFeatures({
    daysAhead: 2,
    env: makeEnv(),
    lookbackDays: 7,
    runYmd: "20260712",
  });
  expect(logSpy).toHaveBeenCalledWith(
    "[corner-features-refresh] ok runYmd=20260712 fromDate=20260705 toDate=20260714",
  );
  logSpy.mockRestore();
});

test("the upsert statement widens the date window backward when lookbackDays is set", async () => {
  await refreshCornerFeatures({
    daysAhead: 2,
    env: makeEnv(),
    lookbackDays: 7,
    runYmd: "20260712",
  });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi >= '20260705'"),
  );
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi <= '20260714'"),
  );
});

test("lookbackDays 0 behaves identically to omitting it (forward-only window)", async () => {
  await refreshCornerFeatures({
    daysAhead: 2,
    env: makeEnv(),
    lookbackDays: 0,
    runYmd: "20260712",
  });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi >= '20260712'"),
  );
});

test("lookbackDays correctly rolls backward over a month boundary", async () => {
  await refreshCornerFeatures({
    daysAhead: 0,
    env: makeEnv(),
    lookbackDays: 5,
    runYmd: "20260703",
  });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("se.kaisai_nen || se.kaisai_tsukihi >= '20260628'"),
  );
});

test("the time_sa normalization accepts sign-prefixed values (jvd_se/nvd_se's actual format)", async () => {
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledWith(expect.stringContaining("time_sa ~ '^[+-]?[0-9]+$'"));
});

test("shouldRunCornerFeaturesRefreshCron matches the morning cron", () => {
  expect(shouldRunCornerFeaturesRefreshCron(CORNER_FEATURES_REFRESH_CRON_MORNING)).toBe(true);
});

test("shouldRunCornerFeaturesRefreshCron matches the evening cron", () => {
  expect(shouldRunCornerFeaturesRefreshCron(CORNER_FEATURES_REFRESH_CRON_EVENING)).toBe(true);
});

test("shouldRunCornerFeaturesRefreshCron rejects an unrelated cron string", () => {
  expect(shouldRunCornerFeaturesRefreshCron("*/10 1-11 * * *")).toBe(false);
});

test("stops issuing further statements once one query call rejects", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  queryMock.mockResolvedValueOnce(undefined); // extension DDL (non-fatal path, but succeeds here)
  queryMock.mockResolvedValueOnce(undefined); // table DDL
  queryMock.mockRejectedValueOnce(new Error("alter failed")); // first ALTER
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledTimes(3);
  consoleError.mockRestore();
});

test("the JRA raw_rows select does not duplicate source/kaisai_nen/kaisai_tsukihi/keibajo_code/race_bango as bare unqualified names", async () => {
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining(
      "select 'jra' source, ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,\n    \n  se.ketto_toroku_bango",
    ),
  );
});

test("the NAR raw_rows select does not duplicate source/kaisai_nen/kaisai_tsukihi/keibajo_code/race_bango as bare unqualified names", async () => {
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining(
      "select 'nar' source, ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,\n    \n  se.ketto_toroku_bango",
    ),
  );
});
