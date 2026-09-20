// Runs with bun via Vitest; no network or filesystem access.
import { expect, it, vi } from "vitest";
import { buildRaceYearsReadSql, readRaceYears } from "./race-years-read";

it("preserves the cross-source distinct-day aggregation without truncation", () => {
  expect(buildRaceYearsReadSql("pc_keiba"))
    .toBe(`SELECT kaisai_nen AS year, SUM(race_count) AS race_count,
  COUNT(DISTINCT kaisai_tsukihi) AS day_count
FROM (
  SELECT kaisai_nen, kaisai_tsukihi, COUNT(*) AS race_count
  FROM pc_keiba.jvd_ra GROUP BY kaisai_nen, kaisai_tsukihi
  UNION ALL
  SELECT kaisai_nen, kaisai_tsukihi, COUNT(*) AS race_count
  FROM pc_keiba.nvd_ra GROUP BY kaisai_nen, kaisai_tsukihi
) race_days
GROUP BY kaisai_nen
ORDER BY kaisai_nen DESC`);
});

it.each(["", "bad.name", "1schema", "x;DROP TABLE x", "a-b"])(
  "rejects namespace %s before querying",
  async (namespace) => {
    const query = vi.fn<(sql: string) => Promise<Record<string, unknown>[]>>();
    await expect(readRaceYears({ namespace, query })).rejects.toThrow(
      "Invalid race years namespace",
    );
    expect(query).not.toHaveBeenCalled();
  },
);

it("normalizes integer strings and sorts years descending", async () => {
  const query = vi.fn<(sql: string) => Promise<Record<string, unknown>[]>>().mockResolvedValue([
    { year: "2024", race_count: "1000", day_count: "366" },
    { year: "2026", race_count: 500, day_count: 260 },
  ]);
  await expect(readRaceYears({ namespace: "pc_keiba", query })).resolves.toStrictEqual([
    { year: "2026", raceCount: 500, dayCount: 260 },
    { year: "2024", raceCount: 1000, dayCount: 366 },
  ]);
  expect(query).toHaveBeenCalledTimes(1);
});

it("preserves an empty source result", async () => {
  const query = vi.fn<(sql: string) => Promise<Record<string, unknown>[]>>().mockResolvedValue([]);
  await expect(readRaceYears({ namespace: "pc_keiba", query })).resolves.toStrictEqual([]);
});

it.each([null, undefined, 2026, "", "0000", "999", "10000", "2026x"])(
  "rejects malformed year %s",
  async (year) => {
    const query = vi
      .fn<(sql: string) => Promise<Record<string, unknown>[]>>()
      .mockResolvedValue([{ year, race_count: 500, day_count: 260 }]);
    await expect(readRaceYears({ namespace: "pc_keiba", query })).rejects.toThrow(
      "Invalid race years identity",
    );
  },
);

it.each([
  null,
  undefined,
  false,
  {},
  "",
  "1.5",
  "-1",
  "1e2",
  "Infinity",
  "9007199254740992",
  0,
  -1,
  1.5,
  Number.NaN,
  Number.POSITIVE_INFINITY,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects malformed race count %s", async (raceCount) => {
  const query = vi
    .fn<(sql: string) => Promise<Record<string, unknown>[]>>()
    .mockResolvedValue([{ year: "2026", race_count: raceCount, day_count: 1 }]);
  await expect(readRaceYears({ namespace: "pc_keiba", query })).rejects.toThrow(
    "Invalid race years count",
  );
});

it.each([
  { year: "2025", race_count: 500, day_count: 366 },
  { year: "1900", race_count: 500, day_count: 366 },
  { year: "2024", race_count: 500, day_count: 367 },
  { year: "2026", race_count: 1, day_count: 2 },
])("rejects impossible counts %j", async (row) => {
  const query = vi
    .fn<(sql: string) => Promise<Record<string, unknown>[]>>()
    .mockResolvedValue([row]);
  await expect(readRaceYears({ namespace: "pc_keiba", query })).rejects.toThrow(
    "Inconsistent race years counts",
  );
});

it("accepts Gregorian century leap years", async () => {
  const query = vi
    .fn<(sql: string) => Promise<Record<string, unknown>[]>>()
    .mockResolvedValue([{ year: "2000", race_count: 366, day_count: 366 }]);
  await expect(readRaceYears({ namespace: "pc_keiba", query })).resolves.toStrictEqual([
    { year: "2000", raceCount: 366, dayCount: 366 },
  ]);
});

it("rejects invalid day counts", async () => {
  const query = vi
    .fn<(sql: string) => Promise<Record<string, unknown>[]>>()
    .mockResolvedValue([{ year: "2026", race_count: 20, day_count: "0" }]);
  await expect(readRaceYears({ namespace: "pc_keiba", query })).rejects.toThrow(
    "Invalid race years count",
  );
});

it("rejects duplicate years", async () => {
  const query = vi.fn<(sql: string) => Promise<Record<string, unknown>[]>>().mockResolvedValue([
    { year: "2026", race_count: 20, day_count: 1 },
    { year: "2026", race_count: 30, day_count: 2 },
  ]);
  await expect(readRaceYears({ namespace: "pc_keiba", query })).rejects.toThrow(
    "Duplicate race year",
  );
});

it("rejects oversized results rather than truncating", async () => {
  const query = vi.fn<(sql: string) => Promise<Record<string, unknown>[]>>().mockResolvedValue(
    Array.from({ length: 257 }, (_, index) => ({
      year: String(1700 + index),
      race_count: 1,
      day_count: 1,
    })),
  );
  await expect(readRaceYears({ namespace: "pc_keiba", query })).rejects.toThrow(
    "Race years exceeds result limit",
  );
});

it("propagates query errors without a fallback", async () => {
  const query = vi
    .fn<(sql: string) => Promise<Record<string, unknown>[]>>()
    .mockRejectedValue(new Error("Unavailable"));
  await expect(readRaceYears({ namespace: "pc_keiba", query })).rejects.toThrow("Unavailable");
});
