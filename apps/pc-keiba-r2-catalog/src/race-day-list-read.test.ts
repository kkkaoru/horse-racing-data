// Runs with bun; pure projection tests without network or filesystem access.
import { expect, it, vi } from "vitest";
import {
  buildRaceDayListReadSql,
  readRaceDayList,
  validateRaceDayListInput,
} from "./race-day-list-read";

const input = { namespace: "keiba", date: "20240229" };
const row = {
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
};

it("shares input validation with the jockey reader without changing the day-list contract", () => {
  expect(validateRaceDayListInput(input)).toBeUndefined();
  expect(() => validateRaceDayListInput({ namespace: "keiba", date: "20230229" })).toThrow(
    "Invalid race day list input",
  );
});

it("builds both validated partitions without a truncating SQL limit", () => {
  expect(buildRaceDayListReadSql(input)).toBe(
    "SELECT 'jra' AS source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyosomei_hondai, kyosomei_fukudai, grade_code, kyoso_shubetsu_code, kyoso_kigo_code, juryo_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, kyori, track_code, hasso_jikoku, shusso_tosu\nFROM keiba.jvd_ra WHERE kaisai_nen = '2024' AND kaisai_tsukihi = '0229'\nUNION\nSELECT 'nar' AS source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyosomei_hondai, kyosomei_fukudai, grade_code, kyoso_shubetsu_code, kyoso_kigo_code, juryo_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, kyori, track_code, hasso_jikoku, shusso_tosu\nFROM keiba.nvd_ra WHERE kaisai_nen = '2024' AND kaisai_tsukihi = '0229'\nORDER BY hasso_jikoku ASC NULLS LAST, keibajo_code ASC, race_bango ASC, source ASC",
  );
});

it.each(["20230229", "20241301", "20240431", "2024-02-29", "00000229", "20240229'", "20240100"])(
  "rejects invalid date %s before querying",
  async (date) => {
    const query = vi.fn<() => Promise<unknown[]>>();
    await expect(readRaceDayList({ input: { ...input, date }, query })).rejects.toThrow(
      "Invalid race day list input",
    );
    expect(query).not.toHaveBeenCalled();
  },
);
it("rejects namespace injection", () => {
  expect(() => buildRaceDayListReadSql({ ...input, namespace: "keiba;DROP TABLE x" })).toThrow(
    "Invalid race day list input",
  );
});
it("preserves nullable strings and produces explicitly empty jockey names", async () => {
  expect(await readRaceDayList({ input, query: vi.fn().mockResolvedValue([row]) })).toStrictEqual([
    {
      source: "jra",
      kaisaiNen: "2024",
      kaisaiTsukihi: "0229",
      keibajoCode: "05",
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
  ]);
});
it("returns genuine empty days", async () => {
  expect(await readRaceDayList({ input, query: vi.fn().mockResolvedValue([]) })).toStrictEqual([]);
});
it("does not convert query failure to empty success", async () => {
  await expect(
    readRaceDayList({ input, query: vi.fn().mockRejectedValue(new Error("unavailable")) }),
  ).rejects.toThrow("unavailable");
});
it.each([null, [], 12, "row"])("rejects malformed provider row %j", async (value) => {
  await expect(
    readRaceDayList({ input, query: vi.fn().mockResolvedValue([value]) }),
  ).rejects.toThrow("Invalid race day list row");
});
it.each([
  { source: "unknown" },
  { kaisai_nen: "2025" },
  { kaisai_tsukihi: "0228" },
  { keibajo_code: null },
  { keibajo_code: "005" },
  { race_bango: null },
  { race_bango: "1" },
])("rejects mismatched identity %j", async (change) => {
  await expect(
    readRaceDayList({ input, query: vi.fn().mockResolvedValue([{ ...row, ...change }]) }),
  ).rejects.toThrow("Invalid race day list identity");
});
it.each([undefined, 1000, {}, []])(
  "rejects missing or malformed scalar %j",
  async (hasso_jikoku) => {
    await expect(
      readRaceDayList({ input, query: vi.fn().mockResolvedValue([{ ...row, hasso_jikoku }]) }),
    ).rejects.toThrow("Invalid race day list field");
  },
);
it("rejects duplicates rather than concealing ambiguity", async () => {
  await expect(
    readRaceDayList({ input, query: vi.fn().mockResolvedValue([row, row]) }),
  ).rejects.toThrow("Duplicate race day list identity");
});
it("rejects oversized responses instead of truncating", async () => {
  await expect(
    readRaceDayList({
      input,
      query: vi.fn().mockResolvedValue(Array.from({ length: 4097 }, () => row)),
    }),
  ).rejects.toThrow("Race day list exceeds row limit");
});
it("sorts complete day output by start nulls last, venue, race and source", async () => {
  const results = await readRaceDayList({
    input,
    query: vi
      .fn()
      .mockResolvedValue([
        { ...row, source: "nar", hasso_jikoku: null, race_bango: "08" },
        { ...row, hasso_jikoku: "1100", race_bango: "07" },
        { ...row, hasso_jikoku: "0900", race_bango: "06" },
        { ...row, source: "nar" },
        { ...row, keibajo_code: "06" },
        { ...row, race_bango: "02" },
        row,
        { ...row, hasso_jikoku: null, race_bango: "09" },
        { ...row, hasso_jikoku: "", race_bango: "10" },
      ]),
  });
  expect(
    results.map(
      (race) => `${race.hassoJikoku}/${race.keibajoCode}/${race.raceBango}/${race.source}`,
    ),
  ).toStrictEqual([
    "/05/10/jra",
    "0900/05/06/jra",
    "1000/05/01/jra",
    "1000/05/01/nar",
    "1000/05/02/jra",
    "1000/06/01/jra",
    "1100/05/07/jra",
    "null/05/08/nar",
    "null/05/09/jra",
  ]);
});
