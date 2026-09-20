// Runs with bun through Vitest; upstream I/O is mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { handleRaceDayListRead } from "./race-day-list-service";
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

it.each(["POST", "PUT", "PATCH", "DELETE", "HEAD"])("rejects %s before I/O", async (method) => {
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229", { method }),
    env,
  );
  expect(response.status).toBe(405);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(mocks.query).not.toHaveBeenCalled();
});
it.each([
  "",
  "?date=",
  "?date=20240229&date=20240228",
  "?date=20240229&namespace=other",
  "?date=20240229&sql=SELECT%201",
  "?date=20240229%27--",
  "?date=20230229",
  "?date=20241301",
])("rejects invalid query %s", async (query) => {
  const response: Response = await handleRaceDayListRead(
    new Request(`https://catalog.internal/v1/race-day-list${query}`),
    env,
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "Invalid race day list request" });
  expect(mocks.query).not.toHaveBeenCalled();
});
it("returns genuine empty days without querying fallback sources", async () => {
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ races: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
  expect(mocks.query.mock.calls[0]?.[1]).toMatch(
    "FROM pc_keiba.jvd_ra WHERE kaisai_nen = '2024' AND kaisai_tsukihi = '0229'",
  );
  expect(mocks.query.mock.calls[0]?.[1]).toMatch(
    "FROM pc_keiba.nvd_ra WHERE kaisai_nen = '2024' AND kaisai_tsukihi = '0229'",
  );
});
it("returns validated race fields", async () => {
  mocks.query.mockResolvedValue([
    {
      source: "nar",
      kaisai_nen: "2024",
      kaisai_tsukihi: "0229",
      keibajo_code: "36",
      race_bango: "01",
      kyosomei_hondai: "競走",
      kyosomei_fukudai: null,
      grade_code: "",
      kyoso_shubetsu_code: "11",
      kyoso_kigo_code: null,
      juryo_shubetsu_code: "1",
      kyoso_joken_code: "005",
      kyoso_joken_meisho: null,
      kyori: "1600",
      track_code: "11",
      hasso_jikoku: "1000",
      shusso_tosu: "18",
    },
  ]);
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
    env,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    races: [
      {
        source: "nar",
        kaisaiNen: "2024",
        kaisaiTsukihi: "0229",
        keibajoCode: "36",
        raceBango: "01",
        kyosomeiHondai: "競走",
        kyosomeiFukudai: null,
        gradeCode: "",
        kyosoShubetsuCode: "11",
        kyosoKigoCode: null,
        juryoShubetsuCode: "1",
        jockeyNames: [],
        kyosoJokenCode: "005",
        kyosoJokenMeisho: null,
        kyori: "1600",
        trackCode: "11",
        hassoJikoku: "1000",
        shussoTosu: "18",
      },
    ],
  });
});
it("sanitizes upstream failures without retry", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new Error("private provider detail"));
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
    env,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ error: "Catalog race day list unavailable" });
  expect(log).toHaveBeenCalledWith('{"event":"race_day_list_read_failed","errorName":"Error"}');
  expect(JSON.stringify(log.mock.calls)).not.toContain("private provider detail");
  expect(mocks.query).toHaveBeenCalledTimes(1);
  log.mockRestore();
});
it("retries a transient provider 503 once and returns the recovered rows", async () => {
  const transient = Object.assign(new Error("R2 SQL HTTP 503"), { status: 503 });
  mocks.query.mockRejectedValueOnce(transient).mockResolvedValueOnce([
    {
      source: "nar",
      kaisai_nen: "2024",
      kaisai_tsukihi: "0229",
      keibajo_code: "36",
      race_bango: "01",
      kyosomei_hondai: "競走",
      kyosomei_fukudai: null,
      grade_code: "",
      kyoso_shubetsu_code: "11",
      kyoso_kigo_code: null,
      juryo_shubetsu_code: "1",
      kyoso_joken_code: "005",
      kyoso_joken_meisho: null,
      kyori: "1600",
      track_code: "11",
      hasso_jikoku: "1000",
      shusso_tosu: "18",
    },
  ]);
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
    env,
  );
  expect(response.status).toBe(200);
  expect(mocks.query).toHaveBeenCalledTimes(2);
});
it("does not retry a non-transient provider 400", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(Object.assign(new Error("R2 SQL HTTP 400"), { status: 400 }));
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
    env,
  );
  expect(response.status).toBe(503);
  expect(mocks.query).toHaveBeenCalledTimes(1);
  expect(log).toHaveBeenCalledWith(
    '{"event":"race_day_list_read_failed","errorName":"Error","status":400}',
  );
  log.mockRestore();
});
it("retries an aborted provider read once", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new DOMException("aborted", "AbortError"));
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
    env,
  );
  expect(response.status).toBe(503);
  expect(mocks.query).toHaveBeenCalledTimes(2);
  log.mockRestore();
});
it("treats malformed provider data as failure, not empty success", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockResolvedValue([{ source: "nar", kaisai_nen: "2023" }]);
  const response: Response = await handleRaceDayListRead(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
    env,
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race day list unavailable" });
  log.mockRestore();
});
