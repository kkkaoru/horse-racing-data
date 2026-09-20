// Runs with bun through Vitest; all provider I/O is mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { handleRaceYearsRead } from "./race-years-service";
import type { R2SqlCatalogConfig } from "./types";

const mocks = vi.hoisted(() => ({ query: vi.fn<typeof import("./r2-sql").executeR2Sql>() }));
vi.mock("./r2-sql", () => ({ executeR2Sql: mocks.query }));
const env: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "catalog",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "test-provider-token",
};
beforeEach(() => {
  mocks.query.mockReset().mockResolvedValue([]);
});

it.each(["POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])(
  "rejects %s before I/O",
  async (method) => {
    const response: Response = await handleRaceYearsRead(
      new Request("https://catalog.internal/v1/race-years", { method }),
      env,
    );
    expect(response.status).toBe(405);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.query).not.toHaveBeenCalled();
  },
);

it.each(["?year=2026", "?namespace=other", "?sql=SELECT%201", "?x=", "?year=2026&year=2025"])(
  "rejects unexpected parameters %s",
  async (query) => {
    const response: Response = await handleRaceYearsRead(
      new Request(`https://catalog.internal/v1/race-years${query}`),
      env,
    );
    expect(response.status).toBe(400);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toStrictEqual({ error: "Invalid race years request" });
    expect(mocks.query).not.toHaveBeenCalled();
  },
);

it("returns an empty source without inventing years", async () => {
  const response: Response = await handleRaceYearsRead(
    new Request("https://catalog.internal/v1/race-years"),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ years: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
});

it("returns validated counts using the fixed source union", async () => {
  mocks.query.mockResolvedValue([{ year: "2026", race_count: "500", day_count: "260" }]);
  const response: Response = await handleRaceYearsRead(
    new Request("https://catalog.internal/v1/race-years"),
    env,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    years: [{ year: "2026", raceCount: 500, dayCount: 260 }],
  });
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("COUNT(DISTINCT kaisai_tsukihi)");
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("FROM pc_keiba.jvd_ra");
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("FROM pc_keiba.nvd_ra");
});

it("sanitizes provider errors without retry or fallback", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new Error("private provider detail"));
  try {
    const response: Response = await handleRaceYearsRead(
      new Request("https://catalog.internal/v1/race-years"),
      env,
    );
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toStrictEqual({ error: "Catalog race years unavailable" });
    expect(log).toHaveBeenCalledWith('{"event":"race_years_read_failed"}');
    expect(mocks.query).toHaveBeenCalledTimes(1);
  } finally {
    log.mockRestore();
  }
});

it("fails closed on invalid rows rather than reporting successful absence", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockResolvedValue([{ year: "2026", race_count: 1, day_count: 2 }]);
  try {
    const response: Response = await handleRaceYearsRead(
      new Request("https://catalog.internal/v1/race-years"),
      env,
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toStrictEqual({ error: "Catalog race years unavailable" });
  } finally {
    log.mockRestore();
  }
});

it("rejects invalid configured namespaces before provider I/O", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  try {
    const response: Response = await handleRaceYearsRead(
      new Request("https://catalog.internal/v1/race-years"),
      { ...env, R2_SQL_NAMESPACE: "bad.namespace" },
    );
    expect(response.status).toBe(503);
    expect(mocks.query).not.toHaveBeenCalled();
  } finally {
    log.mockRestore();
  }
});
