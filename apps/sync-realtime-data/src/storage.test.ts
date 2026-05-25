// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  claimOddsFetch,
  claimPremiumPaddockNotificationSend,
  claimResultFetch,
  claimTrackConditionFetch,
  completeOddsFetch,
  completeResultFetch,
  completeTrackConditionFetch,
  countJraRaceSourcesMissingRaceDateFieldsByDate,
  countRaceSourcesByDate,
  failOddsFetch,
  failResultFetch,
  failTrackConditionFetch,
  getLatestHorseWeights,
  getLatestOddsFromD1,
  getLatestRaceEntries,
  getLatestRaceResults,
  getLatestTrackConditionForRace,
  getPremiumPaddockFetchState,
  getPremiumPaddockNotificationState,
  getPremiumRaceDataFetchState,
  getPremiumRaceLink,
  getRaceSource,
  getVenueLastRaceStartAtJst,
  buildRealtimePayload,
  getPremiumRacePayload,
  getSameDayVenueJockeyWins,
  insertJraTrackConditionSnapshot,
  listOddsHistoryByType,
  replacePremiumRaceData,
  listPremiumRaceDataFetchCandidatesByDate,
  listSchedulableRaceSourcesByDate,
  listTanshoHistory,
  insertHorseWeightSnapshot,
  insertOddsSnapshot,
  insertRaceEntrySnapshot,
  insertRaceResultSnapshot,
  listRaceSourceKeibajoCodesByDate,
  logFetch,
  markOddsFetchQueued,
  markPremiumPaddockQueued,
  markPremiumRaceDataQueued,
  markResultFetchQueued,
  markTrackConditionQueued,
  recordPremiumPaddockNotificationEvent,
  runD1Retention,
  toHorseTrends,
  toOddsTrendsByType,
  updateLastFetch,
  updateOddsLinks,
  updatePremiumPaddockFetchState,
  updatePremiumPaddockNotificationState,
  updatePremiumRaceDataFetchState,
  upsertPremiumRaceLink,
} from "./storage";

afterEach(() => {
  vi.restoreAllMocks();
});

it("claimOddsFetch returns true when changes > 0", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await claimOddsFetch(db, "key", "2026-05-12T13:00:00+09:00")).toBe(true);
});

it("claimOddsFetch returns false when no rows changed", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await claimOddsFetch(db, "key", "2026-05-12T13:00:00+09:00")).toBe(false);
});

it("claimResultFetch returns true when changes > 0", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await claimResultFetch(db, "key", "2026-05-12T13:00:00+09:00")).toBe(true);
});

it("claimResultFetch returns false when no rows changed", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await claimResultFetch(db, "key", "2026-05-12T13:00:00+09:00")).toBe(false);
});

it("claimTrackConditionFetch returns true when changes > 0", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await claimTrackConditionFetch(db, {
    date: "20260512",
    keibajoCode: "08",
    lockUntil: "2026-05-12T13:00:00+09:00",
    now: "2026-05-12T12:00:00+09:00",
  });
  expect(result).toBe(true);
});

it("claimTrackConditionFetch returns false when changes = 0", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(
    await claimTrackConditionFetch(db, {
      date: "20260512",
      keibajoCode: "08",
      lockUntil: "2026-05-12T13:00:00+09:00",
      now: "2026-05-12T12:00:00+09:00",
    }),
  ).toBe(false);
});

it("completeOddsFetch binds fetchedAt and raceKey", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await completeOddsFetch(db, "key", "2026-05-12T11:30:00+09:00");
  expect(bind).toHaveBeenCalledTimes(1);
  const args = bind.mock.calls[0]!;
  expect(args[0]).toBe("2026-05-12T11:30:00+09:00");
  expect(args[2]).toBe("key");
});

it("failOddsFetch binds raceKey", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await failOddsFetch(db, "key");
  expect(bind.mock.calls[0]![1]).toBe("key");
});

it("completeResultFetch passes completion flag and counts", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await completeResultFetch(db, "key", "2026-05-12T12:00:00+09:00", {
    expectedHorseCount: 16,
    isComplete: true,
    savedHorseCount: 16,
  });
  const args = bind.mock.calls[0]!;
  expect(args[0]).toBe("2026-05-12T12:00:00+09:00");
  expect(args[1]).toBe(1);
  expect(args[3]).toBe(16);
  expect(args[4]).toBe(16);
});

it("completeResultFetch encodes isComplete=false as 0", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await completeResultFetch(db, "key", "x", {
    expectedHorseCount: 10,
    isComplete: false,
    savedHorseCount: 5,
  });
  expect(bind.mock.calls[0]![1]).toBe(0);
});

it("failResultFetch binds raceKey", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await failResultFetch(db, "key");
  expect(bind.mock.calls[0]![1]).toBe("key");
});

it("completeTrackConditionFetch binds fetchedAt and keibajoCode", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await completeTrackConditionFetch(db, {
    date: "20260512",
    fetchedAt: "2026-05-12T11:00:00+09:00",
    keibajoCode: "08",
  });
  expect(bind).toHaveBeenCalledTimes(1);
});

it("failTrackConditionFetch binds keibajoCode", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await failTrackConditionFetch(db, { date: "20260512", keibajoCode: "08" });
  expect(bind).toHaveBeenCalledTimes(1);
});

it("updateOddsLinks serializes oddsLinks JSON and binds raceKey last", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updateOddsLinks(db, "key", { tansho: "https://x.test/tansho" });
  const args = bind.mock.calls[0]!;
  expect(args[0]).toBe('{"tansho":"https://x.test/tansho"}');
  expect(args[2]).toBe("key");
});

it("updateLastFetch builds the SQL with the requested column name", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn((..._args: unknown[]) => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updateLastFetch(db, "key", "last_odds_fetch_at", "2026-05-12T11:30:00+09:00");
  expect(prepare.mock.calls[0]![0]).toBe(
    "update realtime_race_sources set last_odds_fetch_at = ?, updated_at = ? where race_key = ?",
  );
});

it("markResultFetchQueued returns immediately when raceKeys is empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  await markResultFetchQueued(db, [], "2026-05-12T12:00:00+09:00");
  expect(batch).not.toHaveBeenCalled();
});

it("markResultFetchQueued batches per-race statements", async () => {
  const bind = vi.fn(() => ({ bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await markResultFetchQueued(db, ["k1", "k2"], "2026-05-12T12:00:00+09:00");
  expect(prepare).toHaveBeenCalledTimes(2);
});

it("markOddsFetchQueued returns immediately when raceKeys is empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  await markOddsFetchQueued(db, [], "2026-05-12T12:00:00+09:00");
  expect(batch).not.toHaveBeenCalled();
});

it("markTrackConditionQueued short-circuits when jobs array is empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  await markTrackConditionQueued(db, [], "2026-05-12T12:00:00+09:00");
  expect(batch).not.toHaveBeenCalled();
});

it("markTrackConditionQueued batches per-venue statements", async () => {
  const bind = vi.fn(() => ({ bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await markTrackConditionQueued(
    db,
    [
      { date: "20260512", keibajoCode: "08" },
      { date: "20260512", keibajoCode: "09" },
    ],
    "2026-05-12T12:00:00+09:00",
  );
  expect(prepare).toHaveBeenCalledTimes(2);
});

it("listRaceSourceKeibajoCodesByDate returns mapped keibajo codes", async () => {
  const all = vi.fn(async () => ({
    results: [{ keibajo_code: "08" }, { keibajo_code: "09" }],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await listRaceSourceKeibajoCodesByDate(db, "20260512")).toStrictEqual(["08", "09"]);
});

it("getRaceSource returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getRaceSource(db, "key")).toBeNull();
});

it("getVenueLastRaceStartAtJst returns null when row missing", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getVenueLastRaceStartAtJst(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    source: "jra",
  });
  expect(result).toBeNull();
});

it("getVenueLastRaceStartAtJst returns the start time from the row", async () => {
  const first = vi.fn(async () => ({ race_start_at_jst: "2026-05-12T13:00:00+09:00" }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getVenueLastRaceStartAtJst(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    source: "jra",
  });
  expect(result).toBe("2026-05-12T13:00:00+09:00");
});

it("countRaceSourcesByDate returns numeric count", async () => {
  const first = vi.fn(async () => ({ count: 5 }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await countRaceSourcesByDate(db, "20260512")).toBe(5);
});

it("countRaceSourcesByDate returns 0 when row missing", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await countRaceSourcesByDate(db, "20260512")).toBe(0);
});

it("countJraRaceSourcesMissingRaceDateFieldsByDate returns numeric count", async () => {
  const first = vi.fn(async () => ({ count: 3 }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await countJraRaceSourcesMissingRaceDateFieldsByDate(db, "20260512")).toBe(3);
});

it("logFetch binds raceKey, jobType, status and message in storage order", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await logFetch(db, "plan-realtime-fetches", "ok", "key", "1 job queued");
  const args = bind.mock.calls[0]!;
  expect(args[0]).toBe("key");
  expect(args[1]).toBe("plan-realtime-fetches");
  expect(args[2]).toBe("ok");
  expect(args[3]).toBe("1 job queued");
});

it("logFetch tolerates null raceKey and null message", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await logFetch(db, "x", "error", null, null);
  expect(bind).toHaveBeenCalledTimes(1);
});

it("runD1Retention returns counts from D1 prepare/bind/run results", async () => {
  const run = vi.fn(async () => ({ meta: { rows_written: 3 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await runD1Retention(db);
  expect(result).toStrictEqual({ fetchLogsDeleted: 3, oddsSnapshotsDeleted: 3 });
});

it("runD1Retention defaults counts to 0 when a delete rejects", async () => {
  const run = vi.fn(async () => {
    throw new Error("boom");
  });
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await runD1Retention(db);
  expect(result).toStrictEqual({ fetchLogsDeleted: 0, oddsSnapshotsDeleted: 0 });
});

it("upsertPremiumRaceLink binds raceKey then link fields", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertPremiumRaceLink(db, "jra:2026:0512:08:01", {
    entryUrl: "https://x.test/race",
    sourceRaceId: "202605120801",
  });
  const args = bind.mock.calls[0]!;
  expect(args[0]).toBe("jra:2026:0512:08:01");
  expect(args[1]).toBe("202605120801");
  expect(args[2]).toBe("https://x.test/race");
});

it("getPremiumRaceLink returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getPremiumRaceLink(db, "jra:2026:0512:08:01")).toBeNull();
});

it("getPremiumRaceLink maps row columns to camelCase fields", async () => {
  const first = vi.fn(async () => ({
    entry_url: "https://x.test/race",
    race_key: "jra:2026:0512:08:01",
    source_race_id: "202605120801",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const link = await getPremiumRaceLink(db, "jra:2026:0512:08:01");
  expect(link).toStrictEqual({
    entryUrl: "https://x.test/race",
    sourceRaceId: "202605120801",
  });
});

it("insertOddsSnapshot returns 0 when odds map is empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  expect(await insertOddsSnapshot(db, "key", "now", {})).toBe(0);
});

it("insertOddsSnapshot inserts one row per (type, odds) entry", async () => {
  const bind = vi.fn(() => ({ bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  const count = await insertOddsSnapshot(db, "key", "now", {
    tansho: [
      { combination: "1", odds: 1.5 },
      { combination: "2", odds: 3.5 },
    ],
  });
  expect(count).toBe(2);
});

it("insertHorseWeightSnapshot deletes then no-ops when weights is empty", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await insertHorseWeightSnapshot(db, "key", "now", []);
  expect(prepare).toHaveBeenCalledTimes(1);
  expect(batch).not.toHaveBeenCalled();
});

it("insertHorseWeightSnapshot replaces existing rows when weights present", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run, bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await insertHorseWeightSnapshot(db, "key", "now", [
    {
      changeAmount: 2,
      changeSign: "+",
      horseName: "サンプル",
      horseNumber: "1",
      weight: 500,
    },
  ]);
  expect(batch).toHaveBeenCalledTimes(1);
});

it("insertRaceEntrySnapshot returns 0 when entries empty after delete", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  expect(await insertRaceEntrySnapshot(db, "key", "now", [])).toBe(0);
});

it("insertRaceEntrySnapshot returns row count when entries present", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run, bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  const count = await insertRaceEntrySnapshot(db, "key", "now", [
    { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
    { horseName: "h2", horseNumber: "2", jockeyName: "j", status: null },
  ]);
  expect(count).toBe(2);
});

it("insertRaceResultSnapshot returns 0 when results empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  expect(await insertRaceResultSnapshot(db, "key", "now", [])).toBe(0);
});

it("insertRaceResultSnapshot returns row count when results present", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run, bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  const count = await insertRaceResultSnapshot(db, "key", "now", [
    { finishPosition: "1", horseName: "h", horseNumber: "1", time: "1:23.4" },
  ]);
  expect(count).toBe(1);
});

it("toHorseTrends groups OddsHistoryPoints by horseNumber preserving input order", () => {
  expect(
    toHorseTrends([
      { fetchedAt: "2026-05-12T12:00:00+09:00", horseNumber: "1", odds: 2, popularity: 1 },
      { fetchedAt: "2026-05-12T11:00:00+09:00", horseNumber: "1", odds: 3, popularity: 2 },
      { fetchedAt: "2026-05-12T12:00:00+09:00", horseNumber: "2", odds: 5, popularity: 3 },
    ]),
  ).toStrictEqual([
    {
      horseNumber: "1",
      points: [
        { fetchedAt: "2026-05-12T12:00:00+09:00", horseNumber: "1", odds: 2, popularity: 1 },
        { fetchedAt: "2026-05-12T11:00:00+09:00", horseNumber: "1", odds: 3, popularity: 2 },
      ],
    },
    {
      horseNumber: "2",
      points: [
        { fetchedAt: "2026-05-12T12:00:00+09:00", horseNumber: "2", odds: 5, popularity: 3 },
      ],
    },
  ]);
});

it("toOddsTrendsByType returns empty record when input is empty", () => {
  expect(toOddsTrendsByType({})).toStrictEqual({});
});

it("markPremiumRaceDataQueued returns immediately when raceKeys empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  await markPremiumRaceDataQueued(db, [], "now");
  expect(batch).not.toHaveBeenCalled();
});

it("getPremiumRaceDataFetchState returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getPremiumRaceDataFetchState(db, "key")).toBeNull();
});

it("updatePremiumRaceDataFetchState binds status fields", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumRaceDataFetchState(db, {
    fetchedAt: "now",
    message: null,
    raceKey: "key",
    retryAfter: null,
    status: "ok",
  });
  expect(bind).toHaveBeenCalledTimes(1);
});

it("markPremiumPaddockQueued returns immediately when raceKeys empty", async () => {
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  await markPremiumPaddockQueued(db, [], "now");
  expect(batch).not.toHaveBeenCalled();
});

it("getPremiumPaddockFetchState returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getPremiumPaddockFetchState(db, "key")).toBeNull();
});

it("updatePremiumPaddockFetchState binds status fields", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumPaddockFetchState(db, {
    fetchedAt: "now",
    message: null,
    raceKey: "key",
    retryAfter: null,
    status: "ok",
  });
  expect(bind).toHaveBeenCalledTimes(1);
});

it("getPremiumPaddockNotificationState returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getPremiumPaddockNotificationState(db, "key")).toBeNull();
});

it("updatePremiumPaddockNotificationState binds inputs", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumPaddockNotificationState(db, {
    notifiedAt: "now",
    payloadFetchedAt: "now",
    payloadSignature: "sig",
    raceKey: "key",
    sendAttemptAt: "now",
    status: "ok",
  });
  expect(bind).toHaveBeenCalledTimes(1);
});

it("claimPremiumPaddockNotificationSend returns true when changes > 0", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await claimPremiumPaddockNotificationSend(db, {
    lockBefore: "x",
    payloadFetchedAt: "x",
    payloadSignature: "sig",
    raceKey: "key",
    sendAttemptAt: "x",
  });
  expect(result).toBe(true);
});

it("claimPremiumPaddockNotificationSend returns false when changes = 0", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await claimPremiumPaddockNotificationSend(db, {
    lockBefore: "x",
    payloadFetchedAt: "x",
    payloadSignature: "sig",
    raceKey: "key",
    sendAttemptAt: "x",
  });
  expect(result).toBe(false);
});

it("recordPremiumPaddockNotificationEvent runs the insert", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await recordPremiumPaddockNotificationEvent(db, {
    fetchedAt: "now",
    payloadSignature: "sig",
    raceKey: "key",
    status: "ok",
  });
  expect(run).toHaveBeenCalledTimes(1);
});

it("listSchedulableRaceSourcesByDate maps rows to SchedulableRaceSource shape", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        baba_code: "22",
        deba_url: "https://x.test/race",
        discovered_at: "now",
        kaisai_kai: "02",
        kaisai_nen: "2026",
        kaisai_nichime: "06",
        kaisai_tsukihi: "0512",
        keibajo_code: "55",
        last_odds_fetch_at: null,
        last_odds_queued_at: null,
        last_result_fetch_at: null,
        last_result_queued_at: null,
        last_weight_fetch_at: null,
        odds_fetch_lock_until: null,
        odds_links_json: "{}",
        race_bango: "01",
        race_key: "nar:2026:0512:55:01",
        race_name: "サンプル",
        race_start_at_jst: "2026-05-12T13:00:00+09:00",
        result_complete_at: null,
        result_expected_horse_count: null,
        result_fetch_lock_until: null,
        result_saved_horse_count: null,
        source: "nar",
        updated_at: "now",
      },
    ],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listSchedulableRaceSourcesByDate(db, "20260512");
  expect(result.length).toBe(1);
  expect(result[0]!.raceKey).toBe("nar:2026:0512:55:01");
});

it("listPremiumRaceDataFetchCandidatesByDate returns mapped candidate rows", async () => {
  const all = vi.fn(async () => ({
    results: [{ race_key: "jra:2026:0512:08:01" }],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listPremiumRaceDataFetchCandidatesByDate(db, "20260512", "now");
  expect(result.length).toBe(1);
  expect(result[0]!.raceKey).toBe("jra:2026:0512:08:01");
});

it("listTanshoHistory returns mapped OddsHistoryPoints", async () => {
  const all = vi.fn(async () => ({
    results: [{ combination: "1", fetched_at: "now", odds: "1.5", rank: 1 }],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listTanshoHistory(db, "key");
  expect(result.length).toBe(1);
  expect(result[0]!.horseNumber).toBe("1");
});

it("listOddsHistoryByType groups history by oddsType", async () => {
  const all = vi.fn(async () => ({
    results: [
      { combination: "1", fetched_at: "now", odds: "1.5", odds_type: "tansho", rank: 1 },
      { combination: "1-2", fetched_at: "now", odds: "5.0", odds_type: "umaren", rank: 1 },
    ],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listOddsHistoryByType(db, "key");
  expect(result.tansho).toBeDefined();
  expect(result.umaren).toBeDefined();
});

it("getLatestOddsFromD1 returns null when no fetched_at exists", async () => {
  const first = vi.fn(async () => ({ fetched_at: null }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestOddsFromD1(db, "key")).toBeNull();
});

it("getLatestOddsFromD1 returns latest odds grouped by type", async () => {
  const callPlan: Array<{ first?: unknown; all?: unknown }> = [
    { first: { fetched_at: "now" } },
    {
      all: {
        results: [{ combination: "1", odds: "1.5", odds_type: "tansho", rank: 1 }],
      },
    },
  ];
  const prepare = vi.fn(() => {
    const plan = callPlan.shift();
    const first = vi.fn(async () => plan?.first);
    const all = vi.fn(async () => plan?.all);
    const bind = vi.fn(() => ({ all, first }));
    return { bind };
  });
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestOddsFromD1(db, "key");
  expect(result?.fetchedAt).toBe("now");
  expect(result?.latest.tansho?.length).toBe(1);
});

it("getLatestHorseWeights returns null when no fetched_at exists", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestHorseWeights(db, "key")).toBeNull();
});

it("getLatestHorseWeights returns latest snapshot rows", async () => {
  const callPlan: Array<{ first?: unknown; all?: unknown }> = [
    { first: { fetched_at: "now" } },
    {
      all: {
        results: [
          {
            change_amount: "+2",
            change_sign: "+",
            fetched_at: "now",
            horse_name: "h",
            horse_number: "1",
            weight: "500",
          },
        ],
      },
    },
  ];
  const prepare = vi.fn(() => {
    const plan = callPlan.shift();
    const first = vi.fn(async () => plan?.first);
    const all = vi.fn(async () => plan?.all);
    const bind = vi.fn(() => ({ all, first }));
    return { bind };
  });
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestHorseWeights(db, "key");
  expect(result?.horses.length).toBe(1);
});

it("getLatestRaceEntries returns null when no fetched_at exists", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestRaceEntries(db, "key")).toBeNull();
});

it("getLatestRaceEntries returns horses sorted by horse_number", async () => {
  const callPlan: Array<{ first?: unknown; all?: unknown }> = [
    { first: { fetched_at: "now" } },
    {
      all: {
        results: [
          {
            fetched_at: "now",
            horse_name: "h1",
            horse_number: "1",
            jockey_name: "j",
            status: null,
          },
        ],
      },
    },
  ];
  const prepare = vi.fn(() => {
    const plan = callPlan.shift();
    const first = vi.fn(async () => plan?.first);
    const all = vi.fn(async () => plan?.all);
    const bind = vi.fn(() => ({ all, first }));
    return { bind };
  });
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestRaceEntries(db, "key");
  expect(result?.horses.length).toBe(1);
});

it("getLatestRaceResults returns null when no fetched_at exists", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestRaceResults(db, "key")).toBeNull();
});

it("getLatestRaceResults returns mapped result rows when present", async () => {
  const callPlan: Array<{ first?: unknown; all?: unknown }> = [
    { first: { fetched_at: "now" } },
    {
      all: {
        results: [
          {
            fetched_at: "now",
            finish_position: "1",
            horse_name: "h",
            horse_number: "1",
            time: "1:23.4",
          },
        ],
      },
    },
  ];
  const prepare = vi.fn(() => {
    const plan = callPlan.shift();
    const first = vi.fn(async () => plan?.first);
    const all = vi.fn(async () => plan?.all);
    const bind = vi.fn(() => ({ all, first }));
    return { bind };
  });
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestRaceResults(db, "key");
  expect(result?.horses.length).toBe(1);
});

it("getLatestTrackConditionForRace returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestTrackConditionForRace(db, "key")).toBeNull();
});

it("getSameDayVenueJockeyWins returns mapped jockey win rows", async () => {
  const all = vi.fn(async () => ({
    results: [
      { jockey_name: "Yamada", latest_race_bango: "08", win_count: 2 },
      { jockey_name: "Suzuki", latest_race_bango: "05", win_count: 1 },
    ],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getSameDayVenueJockeyWins(db, {
    beforeRaceBango: "09",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
  });
  expect(result.length).toBe(2);
  expect(result[0]!.jockeyName).toBe("Yamada");
  expect(result[0]!.winCount).toBe(2);
});

it("buildRealtimePayload composes the realtime payload from helpers", async () => {
  const callPlan: Array<{ first?: unknown; all?: unknown }> = [
    { all: { results: [] } },
    { all: { results: [] } },
    { first: null },
    { first: null },
    { first: null },
  ];
  const prepare = vi.fn(() => {
    const plan = callPlan.shift();
    const first = vi.fn(async () => plan?.first ?? null);
    const all = vi.fn(async () => plan?.all ?? { results: [] });
    const bind = vi.fn(() => ({ all, first }));
    return { bind };
  });
  const db = { prepare } as unknown as D1Database;
  const payload = await buildRealtimePayload(db, "key", null, null, null);
  expect(payload.raceKey).toBe("key");
  expect(payload.odds).toBeNull();
});

it("buildRealtimePayload includes oddsHistory + latest when odds are provided", async () => {
  const callPlan: Array<{ first?: unknown; all?: unknown }> = [
    { all: { results: [{ combination: "1", fetched_at: "x", odds: "1.5", rank: 1 }] } },
    {
      all: {
        results: [{ combination: "1", fetched_at: "x", odds: "1.5", odds_type: "tansho", rank: 1 }],
      },
    },
    { first: null },
    { first: null },
    { first: null },
  ];
  const prepare = vi.fn(() => {
    const plan = callPlan.shift();
    const first = vi.fn(async () => plan?.first ?? null);
    const all = vi.fn(async () => plan?.all ?? { results: [] });
    const bind = vi.fn(() => ({ all, first }));
    return { bind };
  });
  const db = { prepare } as unknown as D1Database;
  const payload = await buildRealtimePayload(
    db,
    "key",
    null,
    { fetchedAt: "x", latest: { tansho: [] } },
    null,
  );
  expect(payload.odds?.fetchedAt).toBe("x");
});

it("replacePremiumRaceData batches deletes and inserts for every section provided", async () => {
  const bind = vi.fn(() => ({ run: vi.fn(), bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await replacePremiumRaceData(db, {
    dataTopHorses: [{ horseName: "h1", horseNumber: "1", rank: 1, reasons: ["a"] }],
    fetchedAt: "2026-05-12T11:00:00+09:00",
    link: { entryUrl: "https://x.test/race", sourceRaceId: "202605120801" },
    paddockBulletins: [
      {
        commentText: "コメント",
        evaluationText: "◎",
        frameNumber: "1",
        groupKey: "value",
        horseName: "h1",
        horseNumber: "1",
      },
    ],
    raceKey: "jra:2026:0512:08:01",
    stableComments: [
      {
        commentText: "厩舎コメント",
        evaluationGrade: 1,
        evaluationText: "◎",
        frameNumber: "1",
        horseName: "h1",
        horseNumber: "1",
      },
    ],
    trainingReviews: [
      {
        commentText: "良い動き",
        evaluationGrade: "A",
        evaluationText: "良好",
        horseName: "h1",
        horseNumber: "1",
        riderName: "rider",
        trainingDate: "2026-05-10",
      },
    ],
  });
  expect(prepare.mock.calls.length).toBeGreaterThanOrEqual(8);
  expect(batch).toHaveBeenCalled();
});

it("replacePremiumRaceData supports partial sections (only training reviews)", async () => {
  const bind = vi.fn(() => ({ run: vi.fn(), bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await replacePremiumRaceData(db, {
    fetchedAt: "2026-05-12T11:00:00+09:00",
    link: { entryUrl: "https://x.test/race", sourceRaceId: "202605120801" },
    raceKey: "jra:2026:0512:08:01",
    trainingReviews: [
      {
        commentText: "良い動き",
        evaluationGrade: "A",
        evaluationText: "良好",
        horseName: "h1",
        horseNumber: "1",
        riderName: "rider",
        trainingDate: "2026-05-10",
      },
    ],
  });
  expect(batch).toHaveBeenCalled();
});

it("getPremiumRacePayload aggregates rows from premium tables in a single Promise.all batch", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const payload = await getPremiumRacePayload(db, "jra:2026:0512:08:01");
  expect(payload.trainingReviews).toStrictEqual([]);
  expect(payload.stableComments).toStrictEqual([]);
  expect(payload.paddockBulletins).toStrictEqual([]);
  expect(payload.dataTopHorses).toStrictEqual([]);
});

it("insertJraTrackConditionSnapshot inserts snapshots and returns mapped race rows when JRA races found", async () => {
  const callPlan: Array<{ all?: unknown }> = [
    {
      all: {
        results: [
          { race_key: "jra:2026:0512:08:01", race_start_at_jst: "2026-05-12T13:00:00+09:00" },
        ],
      },
    },
  ];
  const prepare = vi.fn(() => {
    const plan = callPlan.shift();
    const all = vi.fn(async () => plan?.all ?? { results: [] });
    const bind = vi.fn(() => ({ all, bind: vi.fn() }));
    return { bind };
  });
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  const result = await insertJraTrackConditionSnapshot(db, {
    condition: {
      dirt: {
        condition: "良",
        measurementDate: "2026-05-12",
        moisture: { finalBend: "10.0", finalFurlong: "9.0", measuredAt: "10:30" },
      },
      fetchedAt: "now",
      sourceUpdatedAt: null,
      turf: {
        condition: "良",
        courseLayout: "A",
        cushionMeasuredAt: "08:30",
        cushionValue: "9.0",
        going: "良",
        height: { japaneseZoysiaGrass: "10", perennialRyegrass: "12" },
        measurementDate: "2026-05-12",
        moisture: { finalBend: "10", finalFurlong: "9", measuredAt: "10:00" },
      },
      weather: "晴",
    },
    date: "20260512",
    fetchedAt: "now",
    keibajoCode: "08",
  });
  expect(result).toStrictEqual([
    { raceKey: "jra:2026:0512:08:01", raceStartAtJst: "2026-05-12T13:00:00+09:00" },
  ]);
  expect(batch).toHaveBeenCalled();
});

it("insertJraTrackConditionSnapshot returns empty array when no JRA races found", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await insertJraTrackConditionSnapshot(db, {
    condition: {
      dirt: {
        condition: null,
        measurementDate: null,
        moisture: { finalBend: null, finalFurlong: null, measuredAt: null },
      },
      fetchedAt: "now",
      sourceUpdatedAt: null,
      turf: {
        condition: null,
        courseLayout: null,
        cushionMeasuredAt: null,
        cushionValue: null,
        going: null,
        height: { japaneseZoysiaGrass: null, perennialRyegrass: null },
        measurementDate: null,
        moisture: { finalBend: null, finalFurlong: null, measuredAt: null },
      },
      weather: null,
    },
    date: "20260512",
    fetchedAt: "now",
    keibajoCode: "08",
  });
  expect(result).toStrictEqual([]);
});
