// Run with bun.
import { afterEach, expect, it, vi } from "vitest";

import {
  claimOddsFetch,
  completeOddsFetch,
  countOddsFetchStateForDate,
  countOddsRows,
  failOddsFetch,
  filterChangedOdds,
  getOddsFetchState,
  hasOddsRowChanged,
  listClosingBackfillCandidates,
  listOddsFetchStateForDate,
  listRaceKeysForDate,
  logFetch,
  markOddsFetchStateDiscardedForRaceKeys,
  markOddsFetchQueued,
  runD1Batches,
  toHorseTrends,
  toOddsTrendsByType,
  updateOddsLinks,
  upsertOddsFetchState,
} from "./storage";

afterEach(() => {
  vi.restoreAllMocks();
});

it("runD1Batches returns early when statements list is empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch } as unknown as D1Database;
  await runD1Batches(db, []);
  expect(batch).not.toHaveBeenCalled();
});

it("runD1Batches splits over batch boundary", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch } as unknown as D1Database;
  const stmt = { bind: vi.fn() } as unknown as D1PreparedStatement;
  const statements = Array.from({ length: 250 }, () => stmt);
  await runD1Batches(db, statements);
  expect(batch).toHaveBeenCalledTimes(3);
});

it("listRaceKeysForDate returns ordered race keys for the target date", async () => {
  const all = vi.fn(async () => ({
    results: [{ race_key: "nar:2026:0707:30:01" }, { race_key: "nar:2026:0707:35:01" }],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn((_sql: string) => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const raceKeys = await listRaceKeysForDate(db, "2026", "0707");
  expect(raceKeys).toStrictEqual(["nar:2026:0707:30:01", "nar:2026:0707:35:01"]);
  expect(bind).toHaveBeenCalledWith("2026", "0707");
});

it("markOddsFetchStateDiscardedForRaceKeys returns early when raceKeys is empty", async () => {
  const db = { batch: vi.fn(async () => []), prepare: vi.fn() } as unknown as D1Database;
  await markOddsFetchStateDiscardedForRaceKeys(db, [], "2026-07-07T15:58:00+09:00");
  expect(db.prepare).not.toHaveBeenCalled();
});

it("markOddsFetchStateDiscardedForRaceKeys records the discard time for each race", async () => {
  const batch = vi.fn(async () => []);
  const bind = vi.fn(() => ({ bind }));
  const prepare = vi.fn((_sql: string) => ({ bind }));
  const db = { batch, prepare } as unknown as D1Database;
  await markOddsFetchStateDiscardedForRaceKeys(
    db,
    ["nar:2026:0707:30:01", "nar:2026:0707:35:01"],
    "2026-07-07T15:58:00+09:00",
  );
  expect(prepare).toHaveBeenCalledTimes(2);
  expect(bind).toHaveBeenNthCalledWith(
    1,
    "2026-07-07T15:58:00+09:00",
    expect.any(String),
    "nar:2026:0707:30:01",
  );
  expect(bind).toHaveBeenNthCalledWith(
    2,
    "2026-07-07T15:58:00+09:00",
    expect.any(String),
    "nar:2026:0707:35:01",
  );
});

it("toHorseTrends groups history points by horseNumber", () => {
  const result = toHorseTrends([
    { fetchedAt: "t1", horseNumber: "01", odds: 2.5, popularity: 1 },
    { fetchedAt: "t2", horseNumber: "01", odds: 2.4, popularity: 1 },
    { fetchedAt: "t1", horseNumber: "02", odds: 5.0, popularity: 2 },
  ]);
  expect(result.length).toBe(2);
});

it("toOddsTrendsByType groups trend points by combination per odds_type", () => {
  const result = toOddsTrendsByType({
    tansho: [
      { combination: "01", fetchedAt: "t1", odds: 2.5, rank: 1 },
      { combination: "01", fetchedAt: "t2", odds: 2.4, rank: 1 },
      { combination: "02", fetchedAt: "t1", odds: 5.0, rank: 2 },
    ],
  });
  expect(result.tansho?.length).toBe(2);
});

it("upsertOddsFetchState binds all columns", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertOddsFetchState(db, {
    debaUrl: "https://example.com",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0528",
    keibajoCode: "42",
    oddsLinksJson: "{}",
    raceBango: "01",
    raceKey: "nar:20260528:42:01",
    raceStartAtJst: "2026-05-28T10:00:00+09:00",
    source: "nar",
  });
  expect(prepare).toHaveBeenCalledTimes(1);
  expect(run).toHaveBeenCalledTimes(1);
});

it("getOddsFetchState returns null when row missing", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getOddsFetchState(db, "nar:20260528:42:01")).toBeNull();
});

it("getOddsFetchState maps D1 row to typed row", async () => {
  const first = vi.fn(async () => ({
    deba_url: "https://example.com",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0528",
    keibajo_code: "42",
    last_odds_fetch_at: null,
    last_odds_queued_at: null,
    odds_fetch_lock_until: null,
    odds_links_json: "{}",
    race_bango: "01",
    race_key: "nar:20260528:42:01",
    race_start_at_jst: "2026-05-28T10:00:00+09:00",
    source: "nar",
    updated_at: "2026-05-28T09:00:00+09:00",
  }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getOddsFetchState(db, "nar:20260528:42:01");
  expect(result).toStrictEqual({
    debaUrl: "https://example.com",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0528",
    keibajoCode: "42",
    lastOddsFetchAt: null,
    lastOddsQueuedAt: null,
    oddsFetchLockUntil: null,
    oddsLinksJson: "{}",
    raceBango: "01",
    raceKey: "nar:20260528:42:01",
    raceStartAtJst: "2026-05-28T10:00:00+09:00",
    source: "nar",
    updatedAt: "2026-05-28T09:00:00+09:00",
  });
});

it("listOddsFetchStateForDate returns array of race list entries", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        last_odds_fetch_at: null,
        race_key: "nar:20260528:42:01",
        race_start_at_jst: "2026-05-28T10:00:00+09:00",
        source: "nar",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listOddsFetchStateForDate(db, "nar", "2026", "0528");
  expect(result).toStrictEqual([
    {
      lastOddsFetchAt: null,
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T10:00:00+09:00",
      source: "nar",
    },
  ]);
});

it("updateOddsLinks binds JSON and timestamp", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updateOddsLinks(db, "nar:20260528:42:01", { tansho: "https://x/y" });
  expect(prepare).toHaveBeenCalledTimes(1);
});

it("markOddsFetchQueued returns early when empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  await markOddsFetchQueued(db, [], "2026-05-28T10:00:00+09:00");
  expect(batch).not.toHaveBeenCalled();
});

it("markOddsFetchQueued issues batched updates", async () => {
  const batch = vi.fn(async () => []);
  const bind = vi.fn(() => ({}));
  const prepare = vi.fn(() => ({ bind }));
  const db = { batch, prepare } as unknown as D1Database;
  await markOddsFetchQueued(
    db,
    ["nar:20260528:42:01", "nar:20260528:42:02"],
    "2026-05-28T10:00:00+09:00",
  );
  expect(batch).toHaveBeenCalledTimes(1);
});

it("claimOddsFetch returns true when changes > 0", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(
    await claimOddsFetch(
      db,
      "nar:20260528:42:01",
      "2026-05-28T10:05:00+09:00",
      "2026-05-28T10:00:00+09:00",
    ),
  ).toBe(true);
});

it("claimOddsFetch returns false when no rows changed", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(
    await claimOddsFetch(
      db,
      "nar:20260528:42:01",
      "2026-05-28T10:05:00+09:00",
      "2026-05-28T10:00:00+09:00",
    ),
  ).toBe(false);
});

it("completeOddsFetch issues an update", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await completeOddsFetch(db, "nar:20260528:42:01", "2026-05-28T10:00:00+09:00");
  expect(prepare).toHaveBeenCalledTimes(1);
});

it("failOddsFetch issues an update", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await failOddsFetch(db, "nar:20260528:42:01");
  expect(prepare).toHaveBeenCalledTimes(1);
});

it("logFetch writes a row", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await logFetch(db, "fetch-odds", "ok", "nar:20260528:42:01", null);
  expect(prepare).toHaveBeenCalledTimes(1);
});

it("getNarVenueLastRaceStartAtJst returns the last race start when present", async () => {
  const { getNarVenueLastRaceStartAtJst } = await import("./storage");
  const first = vi.fn(async () => ({ last_race_start_at_jst: "2026-05-28T20:00:00+09:00" }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getNarVenueLastRaceStartAtJst(db, "2026", "0528", "42");
  expect(result).toBe("2026-05-28T20:00:00+09:00");
});

it("getNarVenueLastRaceStartAtJst returns null when no rows", async () => {
  const { getNarVenueLastRaceStartAtJst } = await import("./storage");
  const first = vi.fn(async () => null);
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getNarVenueLastRaceStartAtJst(db, "2026", "0528", "42")).toBeNull();
});

it("getNarVenueLastRaceStartAtJst returns null when row column is null", async () => {
  const { getNarVenueLastRaceStartAtJst } = await import("./storage");
  const first = vi.fn(async () => ({ last_race_start_at_jst: null }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getNarVenueLastRaceStartAtJst(db, "2026", "0528", "42")).toBeNull();
});

it("countOddsFetchStateForDate returns the count from D1", async () => {
  const first = vi.fn(async () => ({ count: 7 }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await countOddsFetchStateForDate(db, "2026", "0529")).toBe(7);
});

it("countOddsFetchStateForDate returns 0 when first() yields null", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await countOddsFetchStateForDate(db, "2026", "0529")).toBe(0);
});

it("listClosingBackfillCandidates returns race keys with last_odds_fetch_at null", async () => {
  const all = vi.fn(async () => ({
    results: [{ race_key: "nar:20260622:42:01" }],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn((_sql: string) => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listClosingBackfillCandidates(db, "2026", "0622");
  const sql = String(prepare.mock.calls[0]?.[0] ?? "");
  expect(sql).not.toContain("odds_snapshots");
  expect(result).toStrictEqual(["nar:20260622:42:01"]);
});

it("listClosingBackfillCandidates returns race keys whose last poll predates close window", async () => {
  const all = vi.fn(async () => ({
    results: [{ race_key: "nar:20260622:42:02" }, { race_key: "nar:20260622:42:03" }],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listClosingBackfillCandidates(db, "2026", "0622");
  expect(result).toStrictEqual(["nar:20260622:42:02", "nar:20260622:42:03"]);
});

it("listClosingBackfillCandidates excludes races whose last poll caught the close window", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listClosingBackfillCandidates(db, "2026", "0622");
  expect(result).toStrictEqual([]);
});

it("listClosingBackfillCandidates returns empty array when no rows exist for the date", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listClosingBackfillCandidates(db, "2026", "0625");
  expect(result).toStrictEqual([]);
});

it("hasOddsRowChanged returns true when there is no stored row", () => {
  expect(hasOddsRowChanged({ combination: "01", odds: 2.5, rank: 1 }, undefined)).toBe(true);
});

it("hasOddsRowChanged returns false when every key field matches", () => {
  expect(
    hasOddsRowChanged(
      { averageOdds: 3.0, combination: "01", maxOdds: 3.5, minOdds: 2.5, odds: 2.8, rank: 1 },
      { averageOdds: 3.0, combination: "01", maxOdds: 3.5, minOdds: 2.5, odds: 2.8, rank: 1 },
    ),
  ).toBe(false);
});

it("hasOddsRowChanged treats undefined-vs-undefined odds as unchanged", () => {
  expect(hasOddsRowChanged({ combination: "01", rank: 1 }, { combination: "01", rank: 1 })).toBe(
    false,
  );
});

it("hasOddsRowChanged returns true when odds differ", () => {
  expect(
    hasOddsRowChanged(
      { combination: "01", odds: 2.5, rank: 1 },
      { combination: "01", odds: 2.6, rank: 1 },
    ),
  ).toBe(true);
});

it("hasOddsRowChanged returns true when rank differs", () => {
  expect(
    hasOddsRowChanged(
      { combination: "01", odds: 2.5, rank: 1 },
      { combination: "01", odds: 2.5, rank: 2 },
    ),
  ).toBe(true);
});

it("hasOddsRowChanged returns true when minOdds differs", () => {
  expect(
    hasOddsRowChanged(
      { combination: "01", minOdds: 2.4, odds: 2.5 },
      { combination: "01", minOdds: 2.3, odds: 2.5 },
    ),
  ).toBe(true);
});

it("hasOddsRowChanged returns true when maxOdds differs", () => {
  expect(
    hasOddsRowChanged(
      { combination: "01", maxOdds: 3.5, odds: 2.5 },
      { combination: "01", maxOdds: 3.6, odds: 2.5 },
    ),
  ).toBe(true);
});

it("hasOddsRowChanged returns true when averageOdds differs", () => {
  expect(
    hasOddsRowChanged(
      { averageOdds: 3.0, combination: "01", odds: 2.5 },
      { averageOdds: 3.1, combination: "01", odds: 2.5 },
    ),
  ).toBe(true);
});

it("filterChangedOdds returns an empty object when all rows are unchanged", () => {
  expect(
    filterChangedOdds(
      { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
    ),
  ).toStrictEqual({});
});

it("filterChangedOdds keeps only the changed rows for a partially changed type", () => {
  expect(
    filterChangedOdds(
      {
        tansho: [
          { combination: "01", odds: 2.4, rank: 1 },
          { combination: "02", odds: 5.0, rank: 2 },
        ],
      },
      {
        tansho: [
          { combination: "01", odds: 2.5, rank: 1 },
          { combination: "02", odds: 5.0, rank: 2 },
        ],
      },
    ),
  ).toStrictEqual({ tansho: [{ combination: "01", odds: 2.4, rank: 1 }] });
});

it("filterChangedOdds omits a type whose rows are all unchanged while keeping a changed type", () => {
  expect(
    filterChangedOdds(
      {
        fukusho: [{ combination: "01", odds: 1.5, rank: 1 }],
        tansho: [{ combination: "01", odds: 2.4, rank: 1 }],
      },
      {
        fukusho: [{ combination: "01", odds: 1.5, rank: 1 }],
        tansho: [{ combination: "01", odds: 2.5, rank: 1 }],
      },
    ),
  ).toStrictEqual({ tansho: [{ combination: "01", odds: 2.4, rank: 1 }] });
});

it("filterChangedOdds includes a new combination absent from the stored snapshot", () => {
  expect(
    filterChangedOdds(
      {
        tansho: [
          { combination: "01", odds: 2.5, rank: 1 },
          { combination: "03", odds: 9.0, rank: 3 },
        ],
      },
      { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
    ),
  ).toStrictEqual({ tansho: [{ combination: "03", odds: 9.0, rank: 3 }] });
});

it("filterChangedOdds includes all rows when the stored type is missing", () => {
  expect(
    filterChangedOdds({ tansho: [{ combination: "01", odds: 2.5, rank: 1 }] }, {}),
  ).toStrictEqual({ tansho: [{ combination: "01", odds: 2.5, rank: 1 }] });
});

it("countOddsRows returns 0 for an empty snapshot", () => {
  expect(countOddsRows({})).toBe(0);
});

it("countOddsRows ignores undefined arrays and sums the rest", () => {
  expect(
    countOddsRows({
      fukusho: undefined,
      tansho: [{ combination: "01", odds: 2.5 }, { combination: "02" }],
      umaren: [{ combination: "01-02", odds: 12.3 }],
    }),
  ).toBe(3);
});
