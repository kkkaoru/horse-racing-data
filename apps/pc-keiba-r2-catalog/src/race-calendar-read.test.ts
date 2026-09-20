// Runs with bun (bunx vitest); all query I/O is mocked.
import { expect, it, vi } from "vitest";
import { buildRaceCalendarReadSql, readRaceCalendar } from "./race-calendar-read";

it("restricts both raw race tables to the requested year without dropping either source", () => {
  const sql: string = buildRaceCalendarReadSql({ namespace: "pc_keiba", year: "2026" });
  expect(sql).toMatch("FROM pc_keiba.jvd_ra WHERE kaisai_nen = '2026'");
  expect(sql).toMatch("FROM pc_keiba.nvd_ra WHERE kaisai_nen = '2026'");
  expect(sql).toMatch("UNION ALL");
  expect(sql).toMatch("SUM(jra_count) AS jra_count, SUM(nar_count) AS nar_count");
});

it.each(["2026'--", "26", "0000", "0200", "20260", "2026\n"])(
  "rejects invalid year %s before I/O",
  async (year) => {
    const query = vi.fn();
    await expect(
      readRaceCalendar({ input: { namespace: "pc_keiba", year }, query }),
    ).rejects.toThrow("Invalid race calendar input");
    expect(query).not.toHaveBeenCalled();
  },
);

it.each(["pc-keiba", "pc_keiba;DROP", "", "1schema", "schema.name"])(
  "rejects unsafe namespace %s",
  (namespace) => {
    expect(() => buildRaceCalendarReadSql({ namespace, year: "2026" })).toThrow(
      "Invalid race calendar input",
    );
  },
);

it("normalizes integer representations and sorts actual dates descending", async () => {
  const query = vi.fn().mockResolvedValue([
    { kaisai_nen: "2024", kaisai_tsukihi: "0229", jra_count: "12", nar_count: 0 },
    { kaisai_nen: "2024", kaisai_tsukihi: "0917", jra_count: 0, nar_count: "24" },
    { kaisai_nen: "2024", kaisai_tsukihi: "0901", jra_count: 24, nar_count: 12 },
  ]);
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2024" }, query }),
  ).resolves.toStrictEqual([
    { year: "2024", month: "09", day: "17", jraCount: 0, narCount: 24 },
    { year: "2024", month: "09", day: "01", jraCount: 24, narCount: 12 },
    { year: "2024", month: "02", day: "29", jraCount: 12, narCount: 0 },
  ]);
  expect(query).toHaveBeenCalledTimes(1);
});

it("preserves an empty year as an empty result without a fallback", async () => {
  const query = vi.fn().mockResolvedValue([]);
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).resolves.toStrictEqual([]);
  expect(query).toHaveBeenCalledTimes(1);
});

it("propagates upstream failure without retrying or inventing an empty year", async () => {
  const query = vi.fn().mockRejectedValue(new Error("Upstream unavailable"));
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).rejects.toThrow("Upstream unavailable");
  expect(query).toHaveBeenCalledTimes(1);
});

it.each(["0229", "0001", "1301", "0931", "0100"])(
  "rejects impossible 2026 date %s",
  async (date) => {
    const query = vi
      .fn()
      .mockResolvedValue([
        { kaisai_nen: "2026", kaisai_tsukihi: date, jra_count: 1, nar_count: 0 },
      ]);
    await expect(
      readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
    ).rejects.toThrow("Invalid race calendar date");
  },
);

it.each([null, 917, "917", "09-17", "0917\n"])("rejects malformed date %s", async (date) => {
  const query = vi
    .fn()
    .mockResolvedValue([{ kaisai_nen: "2026", kaisai_tsukihi: date, jra_count: 1, nar_count: 0 }]);
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).rejects.toThrow("Invalid race calendar identity");
});

it("rejects a response belonging to another year", async () => {
  const query = vi
    .fn()
    .mockResolvedValue([
      { kaisai_nen: "2025", kaisai_tsukihi: "0917", jra_count: 1, nar_count: 0 },
    ]);
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).rejects.toThrow("Invalid race calendar identity");
});

it.each([
  null,
  undefined,
  -1,
  1.5,
  "-1",
  "1.5",
  "",
  " ",
  "NaN",
  "1e2",
  Infinity,
  NaN,
  9007199254740992,
  "9007199254740992",
])("rejects malformed or unsafe count %s", async (count) => {
  const query = vi
    .fn()
    .mockResolvedValue([
      { kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: count, nar_count: 1 },
    ]);
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).rejects.toThrow("Invalid race calendar count");
});

it("rejects an aggregated day with neither source present", async () => {
  const query = vi
    .fn()
    .mockResolvedValue([
      { kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: 0, nar_count: "0" },
    ]);
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).rejects.toThrow("Invalid empty race calendar day");
});

it("rejects duplicate dates rather than silently merging them", async () => {
  const query = vi.fn().mockResolvedValue([
    { kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: 1, nar_count: 0 },
    { kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: 0, nar_count: 1 },
  ]);
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).rejects.toThrow("Duplicate race calendar day");
});

it("rejects responses exceeding the single-year bound", async () => {
  const query = vi.fn().mockResolvedValue(Array.from({ length: 367 }, () => ({})));
  await expect(
    readRaceCalendar({ input: { namespace: "pc_keiba", year: "2026" }, query }),
  ).rejects.toThrow("Race calendar exceeds day limit");
});
