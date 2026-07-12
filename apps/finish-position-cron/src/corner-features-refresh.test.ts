// Run with bun. Tests for the corner-features Neon-direct refresh (§4.4).

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const { neonMock, queryMock } = vi.hoisted(() => {
  const query = vi.fn(async () => undefined);
  return { neonMock: vi.fn(() => ({ query })), queryMock: query };
});

vi.mock("@neondatabase/serverless", () => ({ neon: neonMock }));

import { refreshCornerFeatures } from "./corner-features-refresh";

const makeEnv = (): Env => ({ NEON_DATABASE_URL: "postgres://example" }) as Env;

// CREATE TABLE + 17 ALTER + 1 upsert + 5 CREATE INDEX.
const EXPECTED_STATEMENT_COUNT = 24;

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
    "[corner-features-refresh] start runYmd=20260712 daysAhead=2",
  );
  logSpy.mockRestore();
});

test("runs the table DDL first and the horse-history index last", async () => {
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledTimes(EXPECTED_STATEMENT_COUNT);
  expect(queryMock).toHaveBeenNthCalledWith(
    1,
    expect.stringContaining("create table if not exists race_entry_corner_features"),
  );
  expect(queryMock).toHaveBeenNthCalledWith(
    EXPECTED_STATEMENT_COUNT,
    expect.stringContaining("race_entry_corner_features_horse_history_idx"),
  );
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
  queryMock.mockRejectedValueOnce(new Error("neon unreachable"));
  await expect(
    refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" }),
  ).resolves.toBeUndefined();
  expect(consoleError).toHaveBeenCalledWith(
    expect.stringContaining("failed runYmd=20260712 daysAhead=2: Error: neon unreachable"),
  );
  consoleError.mockRestore();
});

test("stops issuing further statements once one query call rejects", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  queryMock.mockResolvedValueOnce(undefined); // table DDL
  queryMock.mockRejectedValueOnce(new Error("alter failed")); // first ALTER
  await refreshCornerFeatures({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(queryMock).toHaveBeenCalledTimes(2);
  consoleError.mockRestore();
});
