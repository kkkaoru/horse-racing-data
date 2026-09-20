// Runs with bun through Vitest; upstream I/O is mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { handleRaceHistoryRead } from "./race-history-service";
import type { R2SqlCatalogConfig } from "./types";
const mocks = vi.hoisted(() => ({
  query: vi.fn<typeof import("./r2-sql").executeR2Sql>(),
  alertSend: vi.fn<(message: unknown) => Promise<void>>(),
}));
vi.mock("./r2-sql", () => ({ executeR2Sql: mocks.query }));
const env: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "catalog",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "test-provider-token",
  INGESTION_ALERTS: { send: mocks.alertSend },
};
const url: string =
  "https://catalog.internal/v1/race-history?horseIds=2021106753,2024101291&beforeDate=20260920&minDate=20230920";
beforeEach(() => {
  mocks.query.mockReset().mockResolvedValue([]);
  mocks.alertSend.mockReset().mockResolvedValue(undefined);
});

it.each(["POST", "PUT", "PATCH", "DELETE", "HEAD"])("rejects %s before I/O", async (method) => {
  const response: Response = await handleRaceHistoryRead(new Request(url, { method }), env);
  expect(response.status).toBe(405);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(mocks.query).not.toHaveBeenCalled();
});
it.each([
  "",
  "?horseIds=2021106753",
  "?horseIds=2021106753&beforeDate=20260920&limit=1&limit=2",
  "?horseIds=2021106753&beforeDate=20260920&sql=SELECT%201",
  "?horseIds=&beforeDate=20260920",
  "?horseIds=2021106753&beforeDate=20260229",
  "?horseIds=2021106753&beforeDate=20260920&limit=0",
  "?horseIds=2021106753&beforeDate=20260920&limit=abc",
  "?horseIds=2021106753&beforeDate=20260920&minDate=2023-09-20",
])("rejects invalid query %s", async (query) => {
  const response: Response = await handleRaceHistoryRead(
    new Request(`https://catalog.internal/v1/race-history${query}`),
    env,
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "Invalid race history request" });
  expect(mocks.query).not.toHaveBeenCalled();
});
it("returns validated history rows with a default limit", async () => {
  const historyRow: Record<string, unknown> = {
    ketto_toroku_bango: "2021106753",
    kaisai_nen: "2023",
    kaisai_tsukihi: "1112",
    keibajo_code: "08",
    race_bango: "07",
    umaban: "05",
    kyori: "1200",
    soha_time: "1150",
    kohan_3f: "375",
    bataiju: "512",
    futan_juryo: "560",
    time_sa: "023",
    kakutei_chakujun: "02",
  };
  mocks.query.mockResolvedValue([historyRow]);
  const response: Response = await handleRaceHistoryRead(
    new Request("https://catalog.internal/v1/race-history?horseIds=2021106753&beforeDate=20260920"),
    env,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ rows: [historyRow] });
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("LIMIT 2001");
  expect(mocks.alertSend).not.toHaveBeenCalled();
});
it("sanitizes provider failures and alerts", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new Error("private provider detail"));
  const response: Response = await handleRaceHistoryRead(new Request(url), env);
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race history unavailable" });
  expect(log).toHaveBeenCalledWith('{"event":"race_history_read_failed"}');
  expect(JSON.stringify(log.mock.calls)).not.toContain("private provider detail");
  expect(mocks.alertSend).toHaveBeenCalledWith(
    expect.objectContaining({
      checkName: "catalog-read-failure",
      fields: [{ name: "event", value: "race_history_read_failed" }],
    }),
  );
  log.mockRestore();
});
it("serves the overseas history shape for source=overseas", async () => {
  mocks.query.mockResolvedValue([
    { source_horse_id: "2021106753", race_date: "2026-04-26", distance_metres: "1600" },
  ]);
  const response: Response = await handleRaceHistoryRead(
    new Request(
      "https://catalog.internal/v1/race-history?horseIds=2021106753&beforeDate=20260920&source=overseas",
    ),
    env,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    rows: [{ sourceHorseId: "2021106753", raceDate: "2026-04-26", distanceMetres: "1600" }],
  });
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("FROM pc_keiba.oversea_horse_race_history");
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("race_date < '2026-09-20'");
});
