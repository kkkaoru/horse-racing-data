// Runs with bun through Vitest; no provider I/O.
import { expect, it } from "vitest";
import {
  buildRaceHistoryReadSql,
  readRaceHistory,
  type RaceHistoryReadInput,
} from "./race-history-read";

const input: RaceHistoryReadInput = {
  namespace: "pc_keiba",
  horseIds: ["2021106753", "2024101291"],
  beforeDate: "20260920",
  minDate: "20230920",
  limit: 2000,
};
const row = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
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
  ...overrides,
});

it("builds a bounded JRA history read over the requested horses", () => {
  const sql: string = buildRaceHistoryReadSql(input);
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.jvd_ra ra");
  expect(sql).toMatch("se.ketto_toroku_bango IN ('2021106753', '2024101291')");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) < '20260920'");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '20230920'");
  expect(sql).toMatch("ORDER BY se.ketto_toroku_bango ASC");
  expect(sql).toMatch("LIMIT 2001");
  expect(sql).not.toMatch("regexp_replace");
});

it("omits the lower bound when no year window is requested", () => {
  const sql: string = buildRaceHistoryReadSql({ ...input, minDate: null });
  expect(sql).not.toMatch(">= '");
  expect(sql).toMatch("< '20260920'");
});

it.each([
  { ...input, namespace: "pc-keiba" },
  { ...input, horseIds: [] },
  { ...input, horseIds: ["20211067"] },
  { ...input, horseIds: ["2021106753", "2021106753"] },
  {
    ...input,
    horseIds: Array.from(
      { length: 41 },
      (_unused, index) => `20211067${String(index).padStart(2, "0")}`,
    ),
  },
  { ...input, beforeDate: "20260229" },
  { ...input, beforeDate: "2026092" },
  { ...input, minDate: "2023-09-20" },
  { ...input, limit: 0 },
  { ...input, limit: 4001 },
  { ...input, limit: 1.5 },
])("rejects invalid history input %j", (value) => {
  expect(() => buildRaceHistoryReadSql(value)).toThrow("Invalid race history input");
});

it("returns parsed raw rows and rejects foreign or malformed rows", async () => {
  await expect(readRaceHistory({ input, query: async () => [row()] })).resolves.toStrictEqual([
    row(),
  ]);
  await expect(
    readRaceHistory({ input, query: async () => [row({ ketto_toroku_bango: "2021999999" })] }),
  ).rejects.toThrow("Race history horse identity mismatch");
  await expect(
    readRaceHistory({ input, query: async () => [row({ ketto_toroku_bango: "0" })] }),
  ).rejects.toThrow("Invalid race history horse identity");
  await expect(
    readRaceHistory({ input, query: async () => [row({ kyori: 1200 })] }),
  ).rejects.toThrow("Missing or invalid race history field");
  await expect(
    readRaceHistory({ input, query: async () => [row({ extra: "1" })] }),
  ).rejects.toThrow("Missing or invalid race history field");
  await expect(
    readRaceHistory({ input, query: async () => Array.from({ length: 2001 }, () => row()) }),
  ).rejects.toThrow("Too many race history rows");
});
