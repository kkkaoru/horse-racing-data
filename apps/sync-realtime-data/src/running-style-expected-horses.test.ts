// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("./storage", () => ({
  getLatestRaceEntries: vi.fn(),
}));

const RACE_KEY = "jra:20260512:08:01";

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("listRunningStyleExpectedHorseCounts keeps the Catalog count over a partial snapshot", async () => {
  const { listRunningStyleExpectedHorseCounts } = await import("./running-style-expected-horses");
  const { getLatestRaceEntries } = await import("./storage");
  vi.mocked(getLatestRaceEntries).mockResolvedValue({
    fetchedAt: "2026-05-12T11:00:00+09:00",
    horses: [
      { horseNumber: "1", status: null },
      { horseNumber: "2", status: null },
    ],
  } as never);
  const db = {} as unknown as D1Database;
  const counts = await listRunningStyleExpectedHorseCounts(
    db,
    [RACE_KEY],
    new Map([[RACE_KEY, 9]]),
  );
  expect(counts.get(RACE_KEY)).toBe(9);
});

it("listRunningStyleExpectedHorseCounts subtracts an explicit scratch present in Catalog", async () => {
  const { listRunningStyleExpectedHorseCounts } = await import("./running-style-expected-horses");
  const { getLatestRaceEntries } = await import("./storage");
  vi.mocked(getLatestRaceEntries).mockResolvedValue({
    fetchedAt: "2026-05-12T11:00:00+09:00",
    horses: [
      { horseNumber: "1", status: null },
      { horseNumber: "2", status: "出走取消" },
    ],
  } as never);

  const counts = await listRunningStyleExpectedHorseCounts(
    {} as unknown as D1Database,
    [RACE_KEY],
    new Map([[RACE_KEY, 9]]),
    new Map([[RACE_KEY, "01:horse-1|02:horse-2|03:horse-3"]]),
  );

  expect(counts.get(RACE_KEY)).toBe(8);
});

it("listRunningStyleExpectedHorseCounts does not subtract a scratch absent from Catalog", async () => {
  const { listRunningStyleExpectedHorseCounts } = await import("./running-style-expected-horses");
  const { getLatestRaceEntries } = await import("./storage");
  vi.mocked(getLatestRaceEntries).mockResolvedValue({
    fetchedAt: "2026-05-12T11:00:00+09:00",
    horses: [{ horseNumber: "2", status: "出走取消" }],
  } as never);

  const counts = await listRunningStyleExpectedHorseCounts(
    {} as unknown as D1Database,
    [RACE_KEY],
    new Map([[RACE_KEY, 9]]),
    new Map([[RACE_KEY, "invalid|01:horse-1|03:horse-3"]]),
  );

  expect(counts.get(RACE_KEY)).toBe(9);
});

it("listRunningStyleExpectedHorseCounts falls back to snapshot count without Catalog coverage", async () => {
  const { listRunningStyleExpectedHorseCounts } = await import("./running-style-expected-horses");
  const { getLatestRaceEntries } = await import("./storage");
  vi.mocked(getLatestRaceEntries).mockResolvedValue({
    fetchedAt: "2026-05-12T11:00:00+09:00",
    horses: [
      { horseNumber: "1", status: null },
      { horseNumber: "2", status: null },
    ],
  } as never);

  const counts = await listRunningStyleExpectedHorseCounts(
    {} as unknown as D1Database,
    [RACE_KEY],
    new Map(),
  );

  expect(counts.get(RACE_KEY)).toBe(2);
});

it("listRunningStyleExpectedHorseCounts falls back to featureCount when entries missing", async () => {
  const { listRunningStyleExpectedHorseCounts } = await import("./running-style-expected-horses");
  const { getLatestRaceEntries } = await import("./storage");
  vi.mocked(getLatestRaceEntries).mockResolvedValue(null);
  const db = {} as unknown as D1Database;
  const counts = await listRunningStyleExpectedHorseCounts(
    db,
    [RACE_KEY],
    new Map([[RACE_KEY, 9]]),
  );
  expect(counts.get(RACE_KEY)).toBe(9);
});

it("listRunningStyleExpectedHorseCounts skips storage lookup for unparseable race key", async () => {
  const { listRunningStyleExpectedHorseCounts } = await import("./running-style-expected-horses");
  const { getLatestRaceEntries } = await import("./storage");
  const db = {} as unknown as D1Database;
  const counts = await listRunningStyleExpectedHorseCounts(
    db,
    ["malformed-key"],
    new Map([["malformed-key", 7]]),
  );
  expect(counts.get("malformed-key")).toBe(7);
  expect(getLatestRaceEntries).not.toHaveBeenCalled();
});

it("listRunningStyleExpectedHorseCounts defaults missing featureCount to 0", async () => {
  const { listRunningStyleExpectedHorseCounts } = await import("./running-style-expected-horses");
  const { getLatestRaceEntries } = await import("./storage");
  vi.mocked(getLatestRaceEntries).mockResolvedValue(null);
  const db = {} as unknown as D1Database;
  const counts = await listRunningStyleExpectedHorseCounts(db, [RACE_KEY], new Map());
  expect(counts.get(RACE_KEY)).toBe(0);
});

it("filterRunningStyleFeatureRowsByActiveEntries returns a copy of all rows when entries is null", async () => {
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const rows = [{ umaban: 1 }, { umaban: 7 }];
  const result = filterRunningStyleFeatureRowsByActiveEntries(rows, null);
  expect(result).toStrictEqual([{ umaban: 1 }, { umaban: 7 }]);
});

it("filterRunningStyleFeatureRowsByActiveEntries returns a copy of all rows when entries.horses is empty", async () => {
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const rows = [{ umaban: 3 }];
  const result = filterRunningStyleFeatureRowsByActiveEntries(rows, { horses: [] });
  expect(result).toStrictEqual([{ umaban: 3 }]);
});

it("filterRunningStyleFeatureRowsByActiveEntries removes explicitly scratched rows", async () => {
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const rows = [{ umaban: 4 }, { umaban: 9 }];
  const result = filterRunningStyleFeatureRowsByActiveEntries(rows, {
    horses: [
      { horseNumber: "4", status: "取消" },
      { horseNumber: "9", status: "出走取消" },
    ],
  });
  expect(result).toStrictEqual([]);
});

it("filterRunningStyleFeatureRowsByActiveEntries preserves Catalog rows missing from a partial snapshot", async () => {
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const rows = [{ umaban: 1 }, { umaban: 2 }, { umaban: 5 }];
  const result = filterRunningStyleFeatureRowsByActiveEntries(rows, {
    horses: [
      { horseNumber: "1", status: null },
      { horseNumber: "5", status: null },
    ],
  });
  expect(result).toStrictEqual([{ umaban: 1 }, { umaban: 2 }, { umaban: 5 }]);
});

it("filterRunningStyleFeatureRowsByActiveEntries removes a scratch from a mixed snapshot", async () => {
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const rows = [{ umaban: 1 }, { umaban: 2 }, { umaban: 5 }];
  const result = filterRunningStyleFeatureRowsByActiveEntries(rows, {
    horses: [
      { horseNumber: "1", status: null },
      { horseNumber: "2", status: "取消" },
    ],
  });
  expect(result).toStrictEqual([{ umaban: 1 }, { umaban: 5 }]);
});
