// Runs with bun through Vitest; no provider I/O.
import { expect, it } from "vitest";
import {
  buildRaceHistoryReadSql,
  earliestRaceYear,
  readRaceHistory,
  type RaceHistoryReadInput,
} from "./race-history-read";

const input: RaceHistoryReadInput = {
  namespace: "pc_keiba",
  source: "jra",
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
  // jvd_se/nvd_se have no kyori column: R2 SQL rejects the whole query when
  // distance is read from the runner mirror instead of the race row.
  expect(sql).toMatch("ra.kyori");
  expect(sql).not.toMatch("se.kyori");
});

it("omits the lower bound when no year window is requested and a ban-ei ID has no foal year", () => {
  const sql: string = buildRaceHistoryReadSql({
    ...input,
    source: "nar",
    horseIds: ["2021106753", "2120190323"],
    minDate: null,
  });
  expect(sql).not.toMatch(">= '");
  expect(sql).toMatch(
    "< '20260920'\n  AND se.kaisai_nen <= '2026'\n  AND ra.kaisai_nen <= '2026'\nORDER BY",
  );
});

it("prunes year partitions from the oldest horse's foal year when no window is requested", () => {
  const sql: string = buildRaceHistoryReadSql({ ...input, minDate: null });
  expect(sql).toMatch(
    "AND se.kaisai_nen >= '2023' AND se.kaisai_nen <= '2026'\n  AND ra.kaisai_nen >= '2023' AND ra.kaisai_nen <= '2026'",
  );
});

it("keeps the later of the window start and the foal-year bound", () => {
  expect(buildRaceHistoryReadSql({ ...input, horseIds: ["2015100001"] })).toMatch(
    "se.kaisai_nen >= '2023' AND se.kaisai_nen <= '2026'",
  );
  expect(buildRaceHistoryReadSql({ ...input, minDate: "20200101" })).toMatch(
    "ra.kaisai_nen >= '2023' AND ra.kaisai_nen <= '2026'",
  );
  expect(
    buildRaceHistoryReadSql({ ...input, horseIds: ["2120190323"], minDate: "20200101" }),
  ).toMatch("ra.kaisai_nen >= '2020' AND ra.kaisai_nen <= '2026'");
});

it("derives the earliest race year only from plausible foal-year prefixes", () => {
  expect(earliestRaceYear(["2021106753", "2019100001"], "20261024")).toBe(2021);
  expect(earliestRaceYear(["2026100001"], "20261024")).toBe(2028);
  expect(earliestRaceYear(["2021106753", "2120190323"], "20261024")).toBeNull();
  expect(earliestRaceYear(["1899100001"], "20261024")).toBeNull();
  expect(earliestRaceYear([" 021106753"], "20261024")).toBeNull();
  expect(earliestRaceYear([], "20261024")).toBeNull();
});

it("builds a NAR history read over the nvd pair", () => {
  const sql: string = buildRaceHistoryReadSql({ ...input, source: "nar" });
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.nvd_ra ra");
  expect(sql).not.toMatch("jvd_");
});

it.each([
  { ...input, namespace: "pc-keiba" },
  { ...input, source: "ban-ei" as "jra" },
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

it("allows provider alphanumeric IDs only on the overseas reader", () => {
  expect(
    buildRaceHistoryReadSql({ ...input, source: "overseas", horseIds: ["000a029d42"] }),
  ).toMatch("source_horse_id IN ('000a029d42')");
  expect(() => buildRaceHistoryReadSql({ ...input, horseIds: ["000a029d42"] })).toThrow(
    "Invalid race history input",
  );
  expect(() =>
    buildRaceHistoryReadSql({ ...input, source: "nar", horseIds: ["000a029d42"] }),
  ).toThrow("Invalid race history input");
});
it.each(["000a'29d42", "0000000000", "", "000a029d42 OR 1=1"])(
  "rejects malformed provider ID %s",
  (id) => {
    expect(() => buildRaceHistoryReadSql({ ...input, source: "overseas", horseIds: [id] })).toThrow(
      "Invalid race history input",
    );
  },
);

it("uses the published provider alphanumeric contract without imposing domestic ID length", () => {
  expect(buildRaceHistoryReadSql({ ...input, source: "overseas", horseIds: ["horse1"] })).toMatch(
    "source_horse_id IN ('horse1')",
  );
});
