// Runs with bun through Vitest. No live provider I/O.
import { beforeEach, expect, it, vi } from "vitest";
import { handleRaceDayListWithJockeysRead } from "./race-day-list-service";
import type { R2SqlCatalogConfig } from "./types";
const mocks = vi.hoisted(() => ({ query: vi.fn<typeof import("./r2-sql").executeR2Sql>() }));
vi.mock("./r2-sql", () => ({ executeR2Sql: mocks.query }));
const env: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "catalog",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "test-token",
};
beforeEach(() => {
  mocks.query.mockReset().mockResolvedValue([]);
});
it.each(["POST", "PUT", "PATCH", "DELETE", "HEAD"])("rejects %s before I/O", async (method) => {
  const response = await handleRaceDayListWithJockeysRead(
    new Request("https://catalog.internal/v1/race-day-list-with-jockeys?date=20240229", { method }),
    env,
  );
  expect(response.status).toBe(405);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(mocks.query).not.toHaveBeenCalled();
});
it.each([
  "",
  "?date=",
  "?date=20230229",
  "?date=20240229&date=20240228",
  "?date=20240229&sql=SELECT%201",
  "?date=20240229&namespace=other",
])("rejects invalid request %s", async (query) => {
  const response = await handleRaceDayListWithJockeysRead(
    new Request(`https://catalog.internal/v1/race-day-list-with-jockeys${query}`),
    env,
  );
  expect(response.status).toBe(400);
  expect(mocks.query).not.toHaveBeenCalled();
});
it("returns empty success using one bounded query", async () => {
  const response = await handleRaceDayListWithJockeysRead(
    new Request("https://catalog.internal/v1/race-day-list-with-jockeys?date=20240229"),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ races: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
  expect(mocks.query.mock.calls[0]?.[1]).toMatch(/^WITH races AS /u);
});
it("returns names in verified UTF-8 order", async () => {
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
      names: ["武豊", "A"],
    },
  ]);
  const response = await handleRaceDayListWithJockeysRead(
    new Request("https://catalog.internal/v1/race-day-list-with-jockeys?date=20240229"),
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
        jockeyNames: ["A", "武豊"],
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
it("sanitizes failures without retrying or falling back", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new Error("private detail"));
  const response = await handleRaceDayListWithJockeysRead(
    new Request("https://catalog.internal/v1/race-day-list-with-jockeys?date=20240229"),
    env,
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race day list unavailable" });
  expect(log).toHaveBeenCalledWith('{"event":"race_day_list_with_jockeys_read_failed"}');
  expect(mocks.query).toHaveBeenCalledTimes(1);
  log.mockRestore();
});
it("does not return malformed data as empty success", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockResolvedValue([{}]);
  const response = await handleRaceDayListWithJockeysRead(
    new Request("https://catalog.internal/v1/race-day-list-with-jockeys?date=20240229"),
    env,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  log.mockRestore();
});
