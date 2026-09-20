// Runs with bun through Vitest; provider I/O is mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { handleRaceCalendarRead } from "./race-calendar-service";
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

it.each(["POST", "PUT", "PATCH", "DELETE", "HEAD"])(
  "rejects %s before provider I/O",
  async (method) => {
    const response: Response = await handleRaceCalendarRead(
      new Request("https://catalog.internal/v1/race-calendar?year=2026", { method }),
      env,
    );
    expect(response.status).toBe(405);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.query).not.toHaveBeenCalled();
  },
);

it.each([
  "",
  "?year=",
  "?year=2026&year=2025",
  "?year=2026&namespace=other",
  "?year=2026&sql=SELECT%201",
  "?year=2026%27--",
  "?year=26",
])("rejects invalid query %s without I/O", async (query) => {
  const response: Response = await handleRaceCalendarRead(
    new Request(`https://catalog.internal/v1/race-calendar${query}`),
    env,
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "Invalid race calendar request" });
  expect(mocks.query).not.toHaveBeenCalled();
});

it("returns an empty year without inventing days", async () => {
  const response: Response = await handleRaceCalendarRead(
    new Request("https://catalog.internal/v1/race-calendar?year=2026"),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ days: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
});

it("returns validated day counts and partition-restricts both source tables", async () => {
  mocks.query.mockResolvedValue([
    { kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: 0, nar_count: "24" },
  ]);
  const response: Response = await handleRaceCalendarRead(
    new Request("https://catalog.internal/v1/race-calendar?year=2026"),
    env,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    days: [{ year: "2026", month: "09", day: "17", jraCount: 0, narCount: 24 }],
  });
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("FROM pc_keiba.jvd_ra WHERE kaisai_nen = '2026'");
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("FROM pc_keiba.nvd_ra WHERE kaisai_nen = '2026'");
});

it("sanitizes an upstream error and does not retry or fall back", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new Error("private provider detail"));
  const response: Response = await handleRaceCalendarRead(
    new Request("https://catalog.internal/v1/race-calendar?year=2026"),
    env,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ error: "Catalog race calendar unavailable" });
  expect(log).toHaveBeenCalledWith('{"event":"race_calendar_read_failed"}');
  expect(mocks.query).toHaveBeenCalledTimes(1);
  log.mockRestore();
});

it("treats invalid provider rows as failure rather than successful absence", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockResolvedValue([
    { kaisai_nen: "2025", kaisai_tsukihi: "0917", jra_count: 1, nar_count: 0 },
  ]);
  const response: Response = await handleRaceCalendarRead(
    new Request("https://catalog.internal/v1/race-calendar?year=2026"),
    env,
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race calendar unavailable" });
  log.mockRestore();
});
