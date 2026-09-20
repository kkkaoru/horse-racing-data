// Runs with bun; all SQL I/O is mocked.
import { expect, it, vi } from "vitest";
import {
  buildRaceDayListWithJockeysReadSql,
  readRaceDayListWithJockeys,
} from "./race-day-jockeys-read";
import type { RaceDayListInput } from "./race-day-list-read";

const input: RaceDayListInput = { namespace: "keiba", date: "20240229" };
const race: Record<string, unknown> = {
  source: "jra",
  kaisai_nen: "2024",
  kaisai_tsukihi: "0229",
  keibajo_code: "05",
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
  names: null,
};
it("uses a race-driven join with all identity fields and only an outer overflow sentinel", () => {
  const sql: string = buildRaceDayListWithJockeysReadSql(input);
  expect(sql.match(/LIMIT \d+/gu)).toStrictEqual(["LIMIT 4097"]);
  expect(sql).toMatch(
    /SELECT races\.\*, jockeys\.names FROM races LEFT JOIN jockeys\nON races\.source=jockeys\.source AND races\.kaisai_nen=jockeys\.kaisai_nen AND races\.kaisai_tsukihi=jockeys\.kaisai_tsukihi AND races\.keibajo_code=jockeys\.keibajo_code AND races\.race_bango=jockeys\.race_bango/u,
  );
});
it("preserves race order and separate sources using exactly one remote statement", async () => {
  const query = vi.fn().mockResolvedValue([
    { ...race, source: "nar", names: ["地方"] },
    { ...race, race_bango: "02", hasso_jikoku: "1100" },
    { ...race, names: ["武豊", "A"] },
  ]);
  const rows = await readRaceDayListWithJockeys({ input, query });
  expect(
    rows.map((row) => ({ source: row.source, race: row.raceBango, names: row.jockeyNames })),
  ).toStrictEqual([
    { source: "jra", race: "01", names: ["A", "武豊"] },
    { source: "nar", race: "01", names: ["地方"] },
    { source: "jra", race: "02", names: [] },
  ]);
  expect(rows[0]?.kyosomeiHondai).toBe("競走");
  expect(query).toHaveBeenCalledTimes(1);
});
it("returns genuine empty days", async () => {
  expect(
    await readRaceDayListWithJockeys({ input, query: vi.fn().mockResolvedValue([]) }),
  ).toStrictEqual([]);
});
it("does not convert remote failure into empty success", async () => {
  await expect(
    readRaceDayListWithJockeys({
      input,
      query: vi.fn().mockRejectedValue(new Error("provider failed")),
    }),
  ).rejects.toThrow("provider failed");
});
it("rejects overflow rather than silently truncating", async () => {
  await expect(
    readRaceDayListWithJockeys({
      input,
      query: vi.fn().mockResolvedValue(Array.from({ length: 4097 }, () => race)),
    }),
  ).rejects.toThrow("Race day list exceeds row limit");
});
it.each([undefined, [], [""], [null], ["武豊", "武豊"]])(
  "rejects malformed joined names %j",
  async (names) => {
    await expect(
      readRaceDayListWithJockeys({ input, query: vi.fn().mockResolvedValue([{ ...race, names }]) }),
    ).rejects.toThrow("Invalid day jockey names");
  },
);
it.each([null, 12, [], "row"])("rejects malformed joined row %j", async (value) => {
  await expect(
    readRaceDayListWithJockeys({ input, query: vi.fn().mockResolvedValue([value]) }),
  ).rejects.toThrow("Invalid race day list row");
});
it("rejects duplicated race identities even when no jockey names exist", async () => {
  await expect(
    readRaceDayListWithJockeys({ input, query: vi.fn().mockResolvedValue([race, race]) }),
  ).rejects.toThrow("Duplicate race day list identity");
});
it("rejects invalid input before remote I/O", async () => {
  const query = vi.fn<() => Promise<unknown[]>>();
  await expect(
    readRaceDayListWithJockeys({ input: { namespace: "keiba", date: "20230229" }, query }),
  ).rejects.toThrow("Invalid race day list input");
  expect(query).not.toHaveBeenCalled();
});
