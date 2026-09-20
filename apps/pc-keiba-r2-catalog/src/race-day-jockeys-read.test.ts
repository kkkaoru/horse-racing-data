// Runs with bun; all query I/O is mocked.
import { expect, it, vi } from "vitest";
import { buildRaceDayJockeysReadSql, readRaceDayJockeys } from "./race-day-jockeys-read";
import type { RaceDayListInput } from "./race-day-list-read";

const input: RaceDayListInput = { namespace: "keiba", date: "20240229" };
const row: Record<string, unknown> = {
  source: "jra",
  kaisai_nen: "2024",
  kaisai_tsukihi: "0229",
  keibajo_code: "05",
  race_bango: "01",
  names: ["武豊"],
};

it("bounds both source partitions and keeps exact distinct names with ASCII-space trimming", () => {
  expect(buildRaceDayJockeysReadSql(input)).toBe(`WITH entries AS (
SELECT 'jra' AS source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kishumei_ryakusho FROM keiba.jvd_se WHERE kaisai_nen = '2024' AND kaisai_tsukihi = '0229'
UNION ALL
SELECT 'nar' AS source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kishumei_ryakusho FROM keiba.nvd_se WHERE kaisai_nen = '2024' AND kaisai_tsukihi = '0229'
), cleaned AS (SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, btrim(kishumei_ryakusho, ' ') AS jockey_name FROM entries)
SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, array_agg(DISTINCT jockey_name) AS names FROM cleaned
WHERE jockey_name IS NOT NULL AND jockey_name <> '' GROUP BY source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango LIMIT 4097`);
});
it.each([
  { namespace: "bad;sql", date: "20240229" },
  { namespace: "keiba", date: "20230229" },
])("rejects invalid input before query: %j", async (invalid) => {
  const query = vi.fn<() => Promise<unknown[]>>();
  await expect(readRaceDayJockeys({ input: invalid, query })).rejects.toThrow(
    "Invalid race day list input",
  );
  expect(query).not.toHaveBeenCalled();
});
it("sorts Unicode names by UTF-8, not locale or UTF-16, without mutating the response", async () => {
  const names: string[] = ["𐀀", "Ｚ", "é", "A", "Ａ", "　", "\t"];
  const result = await readRaceDayJockeys({
    input,
    query: vi.fn().mockResolvedValue([{ ...row, names }]),
  });
  expect(result).toStrictEqual([
    {
      source: "jra",
      kaisaiNen: "2024",
      kaisaiTsukihi: "0229",
      keibajoCode: "05",
      raceBango: "01",
      jockeyNames: ["\t", "A", "é", "　", "Ａ", "Ｚ", "𐀀"],
    },
  ]);
  expect(names).toStrictEqual(["𐀀", "Ｚ", "é", "A", "Ａ", "　", "\t"]);
});
it("keeps separate source identities and sorts their groups", async () => {
  const result = await readRaceDayJockeys({
    input,
    query: vi.fn().mockResolvedValue([{ ...row, source: "nar" }, row]),
  });
  expect(result.map((value) => value.source)).toStrictEqual(["jra", "nar"]);
});
it("allows empty days without inventing jockey groups", async () => {
  expect(await readRaceDayJockeys({ input, query: vi.fn().mockResolvedValue([]) })).toStrictEqual(
    [],
  );
});
it("propagates provider failure rather than treating it as an empty day", async () => {
  await expect(
    readRaceDayJockeys({ input, query: vi.fn().mockRejectedValue(new Error("unavailable")) }),
  ).rejects.toThrow("unavailable");
});
it.each([null, [], 12, "row"])("rejects malformed row %j", async (value) => {
  await expect(
    readRaceDayJockeys({ input, query: vi.fn().mockResolvedValue([value]) }),
  ).rejects.toThrow("Invalid day jockey row");
});
it.each([
  { source: "other" },
  { kaisai_nen: "2023" },
  { kaisai_tsukihi: "0301" },
  { keibajo_code: 5 },
  { keibajo_code: "!5" },
  { race_bango: 1 },
  { race_bango: "1" },
])("rejects malformed identity %j", async (patch) => {
  await expect(
    readRaceDayJockeys({ input, query: vi.fn().mockResolvedValue([{ ...row, ...patch }]) }),
  ).rejects.toThrow("Invalid day jockey identity");
});
it.each([null, undefined, "武豊", [], [null], [1], [""], [" 武豊"], ["武豊 "], ["武豊", "武豊"]])(
  "rejects malformed or non-distinct names %j",
  async (names) => {
    await expect(
      readRaceDayJockeys({ input, query: vi.fn().mockResolvedValue([{ ...row, names }]) }),
    ).rejects.toThrow("Invalid day jockey names");
  },
);
it("rejects duplicate groups", async () => {
  await expect(
    readRaceDayJockeys({ input, query: vi.fn().mockResolvedValue([row, row]) }),
  ).rejects.toThrow("Duplicate day jockey identity");
});
it("detects the sentinel row instead of returning a silently truncated day", async () => {
  await expect(
    readRaceDayJockeys({
      input,
      query: vi.fn().mockResolvedValue(Array.from({ length: 4097 }, () => row)),
    }),
  ).rejects.toThrow("Day jockey groups exceed row limit");
});
