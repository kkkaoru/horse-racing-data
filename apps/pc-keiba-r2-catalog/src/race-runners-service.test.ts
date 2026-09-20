// Runs with bun through Vitest; upstream I/O is mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { handleRaceRunnersRead } from "./race-runners-service";
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
const runnerRow = (): Record<string, unknown> => ({
  wakuban: "1",
  umaban: "01",
  ketto_toroku_bango: "2024101291",
  bamei: "バビット",
  moshoku_code: "1",
  seibetsu_code: "1",
  barei: "3",
  futan_juryo: "550",
  kishumei_ryakusho: "丹内祐次",
  chokyoshimei_ryakusho: "武市康男",
  banushimei: "　",
  bataiju: "480",
  zogen_fugo: " ",
  zogen_sa: "   ",
  kakutei_chakujun: "01",
  tansho_odds: "0123",
  tansho_ninkijun: "02",
  soha_time: "1234",
  time_sa: "0005",
  corner_1: "03",
  corner_2: "02",
  corner_3: "01",
  corner_4: "01",
  kohan_3f: "345",
  blinker_shiyo_kubun: "0",
  sire_name: "Nicobar                             ",
  sire_sire_name: null,
  dam_sire_name: "Kaldounevees                        ",
});
beforeEach(() => {
  mocks.query.mockReset().mockResolvedValue([]);
  mocks.alertSend.mockReset().mockResolvedValue(undefined);
});

it.each(["POST", "PUT", "PATCH", "DELETE", "HEAD"])("rejects %s before I/O", async (method) => {
  const response: Response = await handleRaceRunnersRead(
    new Request(
      "https://catalog.internal/v1/race-runners?source=jra&date=20260920&keibajoCode=06&raceBango=01",
      { method },
    ),
    env,
  );
  expect(response.status).toBe(405);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(mocks.query).not.toHaveBeenCalled();
});
it.each([
  "",
  "?source=jra&date=20260920&keibajoCode=06",
  "?source=jra&date=20260920&keibajoCode=06&raceBango=01&raceBango=02",
  "?source=jra&date=20260920&keibajoCode=06&raceBango=01&sql=SELECT%201",
  "?source=all&date=20260920&keibajoCode=06&raceBango=01",
  "?source=jra&date=2026092&keibajoCode=06&raceBango=01",
  "?source=jra&date=20260229&keibajoCode=06&raceBango=01",
  "?source=jra&date=20260920&keibajoCode=6&raceBango=01",
  "?source=jra&date=20260920&keibajoCode=06&raceBango=1",
])("rejects invalid query %s", async (query) => {
  const response: Response = await handleRaceRunnersRead(
    new Request(`https://catalog.internal/v1/race-runners${query}`),
    env,
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "Invalid race runners request" });
  expect(mocks.query).not.toHaveBeenCalled();
});
it("returns genuine empty runner lists without a fallback source", async () => {
  const response: Response = await handleRaceRunnersRead(
    new Request(
      "https://catalog.internal/v1/race-runners?source=nar&date=20260920&keibajoCode=54&raceBango=08",
    ),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ runners: [], identities: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
  expect(mocks.query.mock.calls[0]?.[1]).toMatch("FROM pc_keiba.nvd_se se");
});
it("returns JRA runners with their overseas identities", async () => {
  mocks.query.mockImplementation(async (_env, sql) =>
    sql.includes("oversea_runner_identity")
      ? [
          {
            umaban: "05",
            source: "netkeiba",
            source_horse_id: "2021100675",
            source_url: "https://example.test/horse/2021100675",
            horse_name_full: "Horse Name",
            jockey_name_full: null,
            trainer_name_full: null,
            owner_name_full: null,
          },
        ]
      : [runnerRow()],
  );
  const response: Response = await handleRaceRunnersRead(
    new Request(
      "https://catalog.internal/v1/race-runners?source=jra&date=20260920&keibajoCode=06&raceBango=01",
    ),
    env,
  );
  expect(response.status).toBe(200);
  const body: { runners: Record<string, unknown>[]; identities: unknown[] } = await response.json();
  expect(body.runners).toHaveLength(1);
  expect(body.runners[0]?.sireName).toBe("Nicobar                             ");
  expect(body.runners[0]?.sireSireName).toBeNull();
  expect(body.identities).toStrictEqual([
    {
      umaban: "05",
      identitySource: "netkeiba",
      sourceHorseId: "2021100675",
      sourceUrl: "https://example.test/horse/2021100675",
      horseNameFull: "Horse Name",
      jockeyNameFull: null,
      trainerNameFull: null,
      ownerNameFull: null,
    },
  ]);
  expect(mocks.query).toHaveBeenCalledTimes(2);
});
it("sanitizes provider failures and invalid rows", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new Error("private provider detail"));
  const failed: Response = await handleRaceRunnersRead(
    new Request(
      "https://catalog.internal/v1/race-runners?source=nar&date=20260920&keibajoCode=54&raceBango=08",
    ),
    env,
  );
  expect(failed.status).toBe(503);
  expect(await failed.json()).toStrictEqual({ error: "Catalog race runners unavailable" });
  expect(log).toHaveBeenCalledWith('{"event":"race_runners_read_failed"}');
  expect(JSON.stringify(log.mock.calls)).not.toContain("private provider detail");
  expect(mocks.alertSend).toHaveBeenCalledWith(
    expect.objectContaining({
      checkName: "catalog-read-failure",
      fields: [{ name: "event", value: "race_runners_read_failed" }],
    }),
  );
  mocks.query.mockReset().mockResolvedValue([{ umaban: "99" }]);
  const malformed: Response = await handleRaceRunnersRead(
    new Request(
      "https://catalog.internal/v1/race-runners?source=nar&date=20260920&keibajoCode=54&raceBango=08",
    ),
    env,
  );
  expect(malformed.status).toBe(503);
  log.mockRestore();
});
