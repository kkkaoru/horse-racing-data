// Run with bun.
import { afterEach, expect, it, vi } from "vitest";

import {
  claimOddsFetch,
  completeOddsFetch,
  countOddsFetchStateForDate,
  failOddsFetch,
  getLatestOddsFromD1,
  getOddsFetchState,
  insertOddsSnapshot,
  listArchiveCandidatesBeforeCutoff,
  listOddsFetchStateForDate,
  listOddsHistoryByType,
  listOddsSnapshotsBeforeCutoff,
  listTanshoHistory,
  logFetch,
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

it("insertOddsSnapshot returns 0 when odds is empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  const count = await insertOddsSnapshot(db, "nar:20260528:42:01", "2026-05-28T10:00:00+09:00", {});
  expect(count).toBe(0);
});

it("insertOddsSnapshot binds all rows including null fallbacks", async () => {
  const batch = vi.fn(async () => []);
  const bind = vi.fn(() => ({ bind }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { batch, prepare } as unknown as D1Database;
  const count = await insertOddsSnapshot(db, "nar:20260528:42:01", "2026-05-28T10:00:00+09:00", {
    tansho: [
      {
        averageOdds: 3.0,
        combination: "01",
        maxOdds: 3.5,
        minOdds: 2.5,
        odds: 2.8,
        rank: 1,
      },
      { combination: "02" },
    ],
  });
  expect(count).toBe(2);
  expect(bind).toHaveBeenNthCalledWith(
    1,
    "nar:20260528:42:01",
    "2026-05-28T10:00:00+09:00",
    "tansho",
    "01",
    2.8,
    2.5,
    3.5,
    3.0,
    1,
  );
  expect(bind).toHaveBeenNthCalledWith(
    2,
    "nar:20260528:42:01",
    "2026-05-28T10:00:00+09:00",
    "tansho",
    "02",
    null,
    null,
    null,
    null,
    null,
  );
});

it("insertOddsSnapshot skips undefined arrays", async () => {
  const batch = vi.fn(async () => []);
  const prepare = vi.fn();
  const db = { batch, prepare } as unknown as D1Database;
  const count = await insertOddsSnapshot(db, "nar:20260528:42:01", "2026-05-28T10:00:00+09:00", {
    tansho: undefined,
  });
  expect(count).toBe(0);
});

it("insertOddsSnapshot uses ON CONFLICT DO UPDATE so re-fetch does not duplicate rows", async () => {
  const batch = vi.fn(async () => []);
  const bind = vi.fn(() => ({ bind }));
  const prepare = vi.fn((_sql: string) => ({ bind }));
  const db = { batch, prepare } as unknown as D1Database;
  await insertOddsSnapshot(db, "nar:20260528:42:01", "2026-05-28T10:00:00+09:00", {
    tansho: [{ combination: "01", odds: 2.8 }],
  });
  const sql = String(prepare.mock.calls[0]?.[0] ?? "");
  expect(/on conflict\(race_key, fetched_at, odds_type, combination\)/u.test(sql)).toBe(true);
  expect(/do update set odds = excluded\.odds/u.test(sql)).toBe(true);
});

it("bulkInsertOddsSnapshotRows uses ON CONFLICT DO UPDATE for idempotent backfill", async () => {
  const { bulkInsertOddsSnapshotRows } = await import("./storage");
  const batch = vi.fn(async () => []);
  const bind = vi.fn(() => ({ bind }));
  const prepare = vi.fn((_sql: string) => ({ bind }));
  const db = { batch, prepare } as unknown as D1Database;
  await bulkInsertOddsSnapshotRows(db, [
    {
      average_odds: null,
      combination: "01",
      fetched_at: "2026-05-28T10:00:00+09:00",
      max_odds: null,
      min_odds: null,
      odds: 2.5,
      odds_type: "tansho",
      race_key: "nar:20260528:42:01",
      rank: 1,
    },
  ]);
  const sql = String(prepare.mock.calls[0]?.[0] ?? "");
  expect(/on conflict\(race_key, fetched_at, odds_type, combination\)/u.test(sql)).toBe(true);
});

it("bulkInsertOddsSnapshotRows returns 0 when rows array is empty", async () => {
  const { bulkInsertOddsSnapshotRows } = await import("./storage");
  const batch = vi.fn(async () => []);
  const prepare = vi.fn();
  const db = { batch, prepare } as unknown as D1Database;
  const count = await bulkInsertOddsSnapshotRows(db, []);
  expect(count).toBe(0);
  expect(batch).not.toHaveBeenCalled();
});

it("getLatestOddsFromD1 returns null when no rows", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestOddsFromD1(db, "nar:20260528:42:01")).toBeNull();
});

it("getLatestOddsFromD1 groups rows by odds_type and applies null fallbacks", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        rank: 1,
      },
      {
        average_odds: 5.5,
        combination: "02",
        fetched_at: "2026-05-28T10:00:00+09:00",
        max_odds: 6.0,
        min_odds: 5.0,
        odds: 5.5,
        odds_type: "fukusho",
        rank: 2,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestOddsFromD1(db, "nar:20260528:42:01");
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    latest: {
      fukusho: [
        {
          averageOdds: 5.5,
          combination: "02",
          maxOdds: 6.0,
          minOdds: 5.0,
          odds: 5.5,
          rank: 2,
        },
      ],
      tansho: [
        {
          averageOdds: undefined,
          combination: "01",
          maxOdds: undefined,
          minOdds: undefined,
          odds: 2.5,
          rank: 1,
        },
      ],
    },
  });
});

it("getLatestOddsFromD1 skips rows with missing odds_type", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        rank: 1,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestOddsFromD1(db, "nar:20260528:42:01");
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    latest: {},
  });
});

it("listTanshoHistory maps rows to history points", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 2.5,
        rank: 1,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listTanshoHistory(db, "nar:20260528:42:01");
  expect(result).toStrictEqual([
    {
      fetchedAt: "2026-05-28T10:00:00+09:00",
      horseNumber: "01",
      odds: 2.5,
      popularity: 1,
    },
  ]);
});

it("getLatestOddsFromD1 falls back to undefined when odds and rank are null", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        max_odds: null,
        min_odds: null,
        odds: null,
        odds_type: "tansho",
        rank: null,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestOddsFromD1(db, "nar:20260528:42:01");
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    latest: {
      tansho: [
        {
          averageOdds: undefined,
          combination: "01",
          maxOdds: undefined,
          minOdds: undefined,
          odds: undefined,
          rank: undefined,
        },
      ],
    },
  });
});

it("listOddsHistoryByType returns empty when no rows", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await listOddsHistoryByType(db, "nar:20260528:42:01")).toStrictEqual({});
});

it("listOddsHistoryByType picks top-N latest combinations per odds_type", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        combination: "01",
        fetched_at: "2026-05-28T09:50:00+09:00",
        odds: 3.0,
        odds_type: "tansho",
        rank: null,
      },
      {
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 2.5,
        odds_type: "tansho",
        rank: 1,
      },
      {
        combination: "02",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 5.0,
        odds_type: "tansho",
        rank: 2,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listOddsHistoryByType(db, "nar:20260528:42:01");
  expect(result.tansho?.length).toBe(3);
});

it("listOddsHistoryByType skips rows with missing odds_type", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 2.5,
        rank: 1,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await listOddsHistoryByType(db, "nar:20260528:42:01")).toStrictEqual({});
});

it("listOddsHistoryByType uses odds tiebreaker when ranks are equal-nulls", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 5.0,
        odds_type: "tansho",
        rank: null,
      },
      {
        combination: "02",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 3.0,
        odds_type: "tansho",
        rank: null,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listOddsHistoryByType(db, "nar:20260528:42:01");
  expect(result.tansho?.map((point) => point.combination)).toStrictEqual(["01", "02"]);
});

it("listOddsHistoryByType falls back to MAX_SAFE_RANK when odds are null in tiebreaker", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        combination: "02",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: null,
        odds_type: "tansho",
        rank: null,
      },
      {
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: null,
        odds_type: "tansho",
        rank: null,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listOddsHistoryByType(db, "nar:20260528:42:01");
  expect(result.tansho?.length).toBe(2);
});

it("listOddsHistoryByType skips odds_type when latest row has empty fetched_at", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        combination: "01",
        fetched_at: "",
        odds: 2.5,
        odds_type: "tansho",
        rank: 1,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await listOddsHistoryByType(db, "nar:20260528:42:01")).toStrictEqual({});
});

it("listOddsHistoryByType uses combination tiebreaker when ranks and odds are equal", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        combination: "02",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 2.5,
        odds_type: "tansho",
        rank: 1,
      },
      {
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        odds: 2.5,
        odds_type: "tansho",
        rank: 1,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listOddsHistoryByType(db, "nar:20260528:42:01");
  expect(result.tansho?.map((point) => point.combination)).toStrictEqual(["02", "01"]);
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

it("listOddsSnapshotsBeforeCutoff returns the result rows", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-21T00:00:00Z",
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        rank: 1,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listOddsSnapshotsBeforeCutoff(db, {
    cutoffIso: "2026-05-21T00:00:00.000Z",
    limit: 100,
  });
  expect(result.length).toBe(1);
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

it("listArchiveCandidatesBeforeCutoff returns grouped rows", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        fetched_at: "2026-05-21T00:00:00+09:00",
        odds_type: "tansho",
        race_key: "nar:20260520:42:01",
        snapshot_json: '[{"combination":"01","odds":2.5}]',
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listArchiveCandidatesBeforeCutoff(db, {
    cutoffIso: "2026-05-21T00:00:00.000Z",
    limit: 100,
  });
  expect(result.length).toBe(1);
});
