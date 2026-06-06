// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  claimPremiumPaddockNotificationSend,
  claimResultFetch,
  claimTrackConditionFetch,
  completeResultFetch,
  completeTrackConditionFetch,
  countJraRaceSourcesMissingRaceDateFieldsByDate,
  countRaceSourcesByDate,
  failResultFetch,
  failTrackConditionFetch,
  getLatestHorseWeights,
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
  listJraVenueTrackConditionSchedulesByDate,
  listRaceKeysByDateFromHyperdrive,
  replacePremiumRaceData,
  listPremiumRaceDataFetchCandidatesByDate,
  listRaceSourcesForSeed,
  listSchedulableRaceSourcesByDate,
  insertHorseWeightSnapshot,
  insertRaceEntrySnapshot,
  insertRaceResultSnapshot,
  listRaceSourceKeibajoCodesByDate,
  logFetch,
  markPremiumPaddockQueued,
  markPremiumRaceDataQueued,
  markResultFetchQueued,
  markTrackConditionQueued,
  recordPartialResultFetch,
  recordPremiumPaddockNotificationEvent,
  runD1Retention,
  toHorseTrends,
  toOddsTrendsByType,
  updateLastFetch,
  updatePremiumPaddockFetchState,
  updatePremiumPaddockNotificationState,
  updatePremiumRaceDataFetchState,
  upsertJraRaceSource,
  upsertNarRaceSource,
  upsertPremiumRaceLink,
} from "./storage";

afterEach(() => {
  vi.restoreAllMocks();
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

it("recordPartialResultFetch binds fetchedAt retryLockUntil and counts", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await recordPartialResultFetch(
    db,
    "key",
    "2026-06-02T10:00:00+09:00",
    "2026-06-02T10:02:00+09:00",
    {
      expectedHorseCount: 12,
      savedHorseCount: 3,
    },
  );
  const args = bind.mock.calls[0]!;
  expect(args[0]).toBe("2026-06-02T10:00:00+09:00");
  expect(args[1]).toBe("2026-06-02T10:02:00+09:00");
  expect(args[2]).toBe(12);
  expect(args[3]).toBe(3);
  expect(args[5]).toBe("key");
});

it("recordPartialResultFetch invokes run once", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await recordPartialResultFetch(db, "nar:2026:0602:55:01", "x", "y", {
    expectedHorseCount: 8,
    savedHorseCount: 0,
  });
  expect(run).toHaveBeenCalledTimes(1);
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

it("runD1Retention returns fetch-logs count from D1 prepare/bind/run result", async () => {
  const run = vi.fn(async () => ({ meta: { rows_written: 3 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await runD1Retention(db);
  expect(result).toStrictEqual({ fetchLogsDeleted: 3 });
});

it("runD1Retention defaults fetch-logs count to 0 when the delete rejects", async () => {
  const run = vi.fn(async () => {
    throw new Error("boom");
  });
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await runD1Retention(db);
  expect(result).toStrictEqual({ fetchLogsDeleted: 0 });
});

it("runD1Retention defaults fetch-logs count to 0 when meta has no rows_written", async () => {
  const run = vi.fn(async () => ({ meta: {} }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await runD1Retention(db);
  expect(result).toStrictEqual({ fetchLogsDeleted: 0 });
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

it("insertHorseWeightSnapshot short-circuits without deleting when weights is empty", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await insertHorseWeightSnapshot(db, "key", "now", []);
  expect(prepare).not.toHaveBeenCalled();
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

it("listRaceSourcesForSeed returns mapped rows when results present", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        deba_url: "https://x.test/race",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "08",
        odds_links_json: "{}",
        race_bango: "01",
        race_key: "jra:2026:0529:08:01",
        race_start_at_jst: "2026-05-29T13:00:00+09:00",
        rowid: 7,
        source: "jra",
      },
    ],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listRaceSourcesForSeed(db, { batchSize: 50, sinceId: 0 });
  expect(result.length).toBe(1);
  expect(result[0]!.race_key).toBe("jra:2026:0529:08:01");
  expect(result[0]!.rowid).toBe(7);
});

it("listRaceSourcesForSeed returns empty array when no rows match", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listRaceSourcesForSeed(db, { batchSize: 50, sinceId: 100 });
  expect(result.length).toBe(0);
});

it("listRaceSourcesForSeed binds sinceId and batchSize to the prepared statement", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await listRaceSourcesForSeed(db, { batchSize: 25, sinceId: 5 });
  expect(bind).toHaveBeenCalledWith(5, 25);
});

it("listRaceKeysByDateFromHyperdrive returns mapped distinct race_keys", async () => {
  const all = vi.fn(async () => ({
    results: [{ race_key: "nar:2026:0529:30:08" }, { race_key: "jra:2026:0529:08:01" }],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const rows = await listRaceKeysByDateFromHyperdrive(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
  });
  expect(rows.length).toBe(2);
  expect(rows[0]!.race_key).toBe("nar:2026:0529:30:08");
});

it("listRaceKeysByDateFromHyperdrive returns empty array when no rows match", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const rows = await listRaceKeysByDateFromHyperdrive(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0101",
  });
  expect(rows.length).toBe(0);
});

it("listRaceKeysByDateFromHyperdrive binds kaisaiNen and kaisaiTsukihi", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await listRaceKeysByDateFromHyperdrive(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
  });
  expect(bind).toHaveBeenCalledWith("2026", "0529");
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

it("getLatestHorseWeights returns null when results are empty", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestHorseWeights(db, "key")).toBeNull();
});

it("getLatestHorseWeights returns latest snapshot rows", async () => {
  const all = vi.fn(async () => ({
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
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestHorseWeights(db, "key");
  expect(result?.horses.length).toBe(1);
});

it("getLatestRaceEntries returns null when results are empty", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestRaceEntries(db, "key")).toBeNull();
});

it("getLatestRaceEntries returns horses sorted by horse_number", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        fetched_at: "now",
        horse_name: "h1",
        horse_number: "1",
        jockey_name: "j",
        status: null,
      },
    ],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestRaceEntries(db, "key");
  expect(result?.horses.length).toBe(1);
});

it("getLatestRaceResults returns null when results are empty", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getLatestRaceResults(db, "key")).toBeNull();
});

it("getLatestRaceResults returns mapped result rows when present", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        fetched_at: "now",
        finish_position: "1",
        horse_name: "h",
        horse_number: "1",
        time: "1:23.4",
      },
    ],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
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

it("buildRealtimePayload sends a single batch RPC and composes the realtime payload", async () => {
  const bind = vi.fn(() => ({}));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => [{ results: [] }, { results: [] }, { results: [] }]);
  const db = { batch, prepare } as unknown as D1Database;
  const payload = await buildRealtimePayload(db, "key", null, null, null);
  expect(payload.raceKey).toBe("key");
  expect(payload.odds).toBeNull();
  expect(payload.raceEntries).toBeNull();
  expect(payload.horseWeights).toBeNull();
  expect(payload.raceResults).toBeNull();
  expect(prepare).toHaveBeenCalledTimes(3);
  expect(batch).toHaveBeenCalledTimes(1);
});

it("buildRealtimePayload maps populated batch results into raceEntries / horseWeights / raceResults", async () => {
  const bind = vi.fn(() => ({}));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => [
    {
      results: [
        {
          fetched_at: "2026-05-29T01:00:00+09:00",
          horse_name: "EntryHorse",
          horse_number: "1",
          jockey_name: "Yamada",
          status: "running",
        },
      ],
    },
    {
      results: [
        {
          change_amount: -2,
          change_sign: "-",
          fetched_at: "2026-05-29T01:01:00+09:00",
          horse_name: "WeightHorse",
          horse_number: "1",
          weight: 480,
        },
      ],
    },
    {
      results: [
        {
          fetched_at: "2026-05-29T01:30:00+09:00",
          finish_position: "1",
          horse_name: "ResultHorse",
          horse_number: "1",
          time: "1:23.4",
        },
      ],
    },
  ]);
  const db = { batch, prepare } as unknown as D1Database;
  const payload = await buildRealtimePayload(db, "key", null, null, null);
  expect(payload.raceEntries?.fetchedAt).toBe("2026-05-29T01:00:00+09:00");
  expect(payload.raceEntries?.horses[0]?.horseName).toBe("EntryHorse");
  expect(payload.horseWeights?.horses[0]?.weight).toBe(480);
  expect(payload.raceResults?.horses[0]?.finishPosition).toBe("1");
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

it("upsertNarRaceSource skips when babaCode is not mapped to a keibajoCode", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertNarRaceSource(
    db,
    { babaCode: "99", raceNumber: "01", url: "https://x.test" },
    {
      hasso_jikoku: "1500",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "06",
      kyosomei_hondai: "T",
      race_bango: "1",
    },
    {},
  );
  expect(prepare).not.toHaveBeenCalled();
});

it("upsertNarRaceSource skips when hasso_jikoku is missing", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertNarRaceSource(
    db,
    { babaCode: "36", raceNumber: "01", url: "https://x.test" },
    {
      hasso_jikoku: null,
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "30",
      kyosomei_hondai: "T",
      race_bango: "1",
    },
    {},
  );
  expect(prepare).not.toHaveBeenCalled();
});

it("upsertNarRaceSource binds normalized raceBango and json oddsLinks when valid", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertNarRaceSource(
    db,
    { babaCode: "36", raceNumber: "01", url: "https://x.test" },
    {
      hasso_jikoku: "1500",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "30",
      kyosomei_hondai: "テスト",
      race_bango: "1",
    },
    { tansho: "https://x.test/odds/tansho" },
  );
  expect(prepare).toHaveBeenCalledTimes(1);
  expect(run).toHaveBeenCalledTimes(1);
  const args = bind.mock.calls[0];
  expect(args?.[0]).toBe("nar:2026:0512:30:01");
  expect(args?.[9]).toBe('{"tansho":"https://x.test/odds/tansho"}');
});

it("upsertJraRaceSource skips when hasso_jikoku is missing", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertJraRaceSource(
    db,
    {
      hasso_jikoku: null,
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "T",
      race_bango: "01",
    },
    "https://jra.example/race",
  );
  expect(prepare).not.toHaveBeenCalled();
});

it("upsertJraRaceSource skips when entryUrl is null", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertJraRaceSource(
    db,
    {
      hasso_jikoku: "1500",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "T",
      race_bango: "01",
    },
    null,
  );
  expect(prepare).not.toHaveBeenCalled();
});

it("upsertJraRaceSource binds normalized raceKey and entry URL when valid", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertJraRaceSource(
    db,
    {
      hasso_jikoku: "1500",
      kaisai_kai: "01",
      kaisai_nichime: "02",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "京都記念",
      race_bango: "1",
    },
    "https://jra.example/race",
  );
  expect(prepare).toHaveBeenCalledTimes(1);
  expect(run).toHaveBeenCalledTimes(1);
  const args = bind.mock.calls[0];
  expect(args?.[0]).toBe("jra:2026:0512:08:01");
  expect(args?.[10]).toBe("https://jra.example/race");
});

it("listJraVenueTrackConditionSchedulesByDate maps rows to JraVenueTrackConditionSchedule shape", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        first_race_start_at_jst: "2026-05-12T10:00:00+09:00",
        keibajo_code: "08",
        last_fetch_at: null,
        last_queued_at: null,
        last_race_start_at_jst: "2026-05-12T15:00:00+09:00",
      },
    ],
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listJraVenueTrackConditionSchedulesByDate(db, "20260512");
  expect(result).toStrictEqual([
    {
      firstRaceStartAtJst: "2026-05-12T10:00:00+09:00",
      keibajoCode: "08",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T15:00:00+09:00",
    },
  ]);
});

it("markPremiumRaceDataQueued runs a batch when raceKeys is non-empty", async () => {
  const batch = vi.fn(async () => undefined);
  const bind = vi.fn((..._args: unknown[]) => ({}));
  const prepare = vi.fn(() => ({ bind }));
  const db = { batch, prepare } as unknown as D1Database;
  await markPremiumRaceDataQueued(db, ["jra:2026:0512:08:01"], "2026-05-12T13:00:00+09:00");
  expect(prepare).toHaveBeenCalledTimes(1);
  expect(batch).toHaveBeenCalledTimes(1);
});

it("markPremiumPaddockQueued runs a batch when raceKeys is non-empty", async () => {
  const batch = vi.fn(async () => undefined);
  const bind = vi.fn((..._args: unknown[]) => ({}));
  const prepare = vi.fn(() => ({ bind }));
  const db = { batch, prepare } as unknown as D1Database;
  await markPremiumPaddockQueued(db, ["jra:2026:0512:08:01"], "2026-05-12T13:00:00+09:00");
  expect(prepare).toHaveBeenCalledTimes(1);
  expect(batch).toHaveBeenCalledTimes(1);
});

it("getPremiumRacePayload aggregates non-empty rows from all four tables", async () => {
  const callPlan: Array<{ results: Record<string, unknown>[] }> = [
    {
      results: [
        {
          comment_text: "good",
          evaluation_grade: 9,
          evaluation_text: "A",
          fetched_at: "2026-05-12T11:00:00+09:00",
          frame_number: "1",
          horse_name: "馬",
          horse_number: "1",
          rider_name: "Y田",
          training_date: "2026-05-10",
        },
      ],
    },
    {
      results: [
        {
          comment_text: "stable",
          evaluation_grade: 8,
          evaluation_text: "B",
          fetched_at: "2026-05-12T10:00:00+09:00",
          frame_number: "2",
          horse_name: "馬2",
          horse_number: "2",
        },
      ],
    },
    {
      results: [
        {
          comment_text: "paddock",
          evaluation_text: "A+",
          fetched_at: "2026-05-12T12:00:00+09:00",
          frame_number: "3",
          group_key: "favorite",
          horse_name: "馬3",
          horse_number: "3",
        },
      ],
    },
    {
      results: [
        {
          fetched_at: "2026-05-12T08:00:00+09:00",
          horse_name: "馬4",
          horse_number: "4",
          rank: 1,
          reasons_json: '["a","b"]',
        },
      ],
    },
  ];
  let callIndex = 0;
  const all = vi.fn(async () => {
    const row = callPlan[callIndex];
    callIndex += 1;
    return row;
  });
  const bind = vi.fn((..._args: unknown[]) => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const payload = await getPremiumRacePayload(db, "jra:2026:0512:08:01");
  expect(payload.trainingReviews).toHaveLength(1);
  expect(payload.stableComments).toHaveLength(1);
  expect(payload.paddockBulletins).toHaveLength(1);
  expect(payload.dataTopHorses).toHaveLength(1);
  expect(payload.dataTopHorses[0]?.reasons).toStrictEqual(["a", "b"]);
});

it("upsertJraRaceSource binds null when kaisai_kai/kaisai_nichime are undefined", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertJraRaceSource(
    db,
    {
      hasso_jikoku: "1500",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "T",
      race_bango: "1",
    },
    "https://jra.example/race",
  );
  const args = bind.mock.calls[0];
  expect(args?.[6]).toBeNull();
  expect(args?.[7]).toBeNull();
});

it("updatePremiumPaddockNotificationState binds non-null values for all optional fields", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumPaddockNotificationState(db, {
    message: "test",
    notifiedAt: "2026-05-12T13:30:00+09:00",
    payloadFetchedAt: "2026-05-12T13:00:00+09:00",
    payloadSignature: "sig",
    raceKey: "jra:2026:0512:08:01",
    sendAttemptAt: "2026-05-12T13:25:00+09:00",
    skipReason: "test",
    status: "ok",
  });
  const args = bind.mock.calls[0];
  expect(args?.[3]).toBe("2026-05-12T13:00:00+09:00");
  expect(args?.[4]).toBe("2026-05-12T13:25:00+09:00");
  expect(args?.[5]).toBe("2026-05-12T13:30:00+09:00");
  expect(args?.[6]).toBe("test");
});

it("updatePremiumRaceDataFetchState binds non-null retryAfter and fetchedAt", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumRaceDataFetchState(db, {
    fetchedAt: "2026-05-12T13:00:00+09:00",
    message: "ok",
    raceKey: "jra:2026:0512:08:01",
    retryAfter: "2026-05-12T13:05:00+09:00",
    status: "ok",
  });
  const args = bind.mock.calls[0];
  expect(args?.[3]).toBe("2026-05-12T13:00:00+09:00");
  expect(args?.[4]).toBe("2026-05-12T13:05:00+09:00");
});

it("updatePremiumPaddockFetchState binds non-null retryAfter and fetchedAt", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumPaddockFetchState(db, {
    fetchedAt: "2026-05-12T13:00:00+09:00",
    message: "msg",
    raceKey: "jra:2026:0512:08:01",
    retryAfter: "2026-05-12T13:05:00+09:00",
    status: "ok",
  });
  const args = bind.mock.calls[0];
  expect(args?.[2]).toBe("msg");
  expect(args?.[3]).toBe("2026-05-12T13:00:00+09:00");
  expect(args?.[4]).toBe("2026-05-12T13:05:00+09:00");
});

it("recordPremiumPaddockNotificationEvent binds optional sentAt and message", async () => {
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await recordPremiumPaddockNotificationEvent(db, {
    fetchedAt: "2026-05-12T13:00:00+09:00",
    message: "msg",
    payloadSignature: "sig",
    raceKey: "jra:2026:0512:08:01",
    sentAt: "2026-05-12T13:25:00+09:00",
    skipReason: "skipped",
    status: "skipped_duplicate",
  });
  expect(prepare).toHaveBeenCalledTimes(1);
});

it("getRaceSource maps oddsLinks to {} when odds_links_json is JSON null", async () => {
  const first = vi.fn(async () => ({
    baba_code: "22",
    deba_url: "u",
    discovered_at: null,
    kaisai_kai: null,
    kaisai_nen: "2026",
    kaisai_nichime: null,
    kaisai_tsukihi: "0512",
    keibajo_code: "55",
    last_odds_fetch_at: null,
    last_weight_fetch_at: null,
    odds_links_json: "null",
    race_bango: "01",
    race_key: "nar:2026:0512:55:01",
    race_name: null,
    race_start_at_jst: "2026-05-12T18:00:00+09:00",
    source: "nar",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getRaceSource(db, "nar:2026:0512:55:01");
  expect(result?.oddsLinks).toStrictEqual({});
});

it("getRaceSource maps oddsLinks to {} when odds_links_json is malformed JSON", async () => {
  const first = vi.fn(async () => ({
    baba_code: "22",
    deba_url: "u",
    discovered_at: null,
    kaisai_kai: null,
    kaisai_nen: "2026",
    kaisai_nichime: null,
    kaisai_tsukihi: "0512",
    keibajo_code: "55",
    last_odds_fetch_at: null,
    last_weight_fetch_at: null,
    odds_links_json: "not-json",
    race_bango: "01",
    race_key: "nar:2026:0512:55:01",
    race_name: null,
    race_start_at_jst: "2026-05-12T18:00:00+09:00",
    source: "nar",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getRaceSource(db, "nar:2026:0512:55:01");
  expect(result?.oddsLinks).toStrictEqual({});
});

it("getRaceSource returns a mapped NarRaceSource when the row exists", async () => {
  const first = vi.fn(async () => ({
    baba_code: "22",
    deba_url: "https://nar.example/race",
    discovered_at: "2026-05-12T00:00:00+09:00",
    kaisai_kai: null,
    kaisai_nen: "2026",
    kaisai_nichime: null,
    kaisai_tsukihi: "0512",
    keibajo_code: "55",
    last_odds_fetch_at: null,
    last_weight_fetch_at: null,
    odds_links_json: '{"tansho":"/odds"}',
    race_bango: "01",
    race_key: "nar:2026:0512:55:01",
    race_name: "T",
    race_start_at_jst: "2026-05-12T18:00:00+09:00",
    source: "nar",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getRaceSource(db, "nar:2026:0512:55:01");
  expect(result?.raceKey).toBe("nar:2026:0512:55:01");
  expect(result?.oddsLinks).toStrictEqual({ tansho: "/odds" });
});

it("getLatestTrackConditionForRace returns mapped TrackCondition when row exists", async () => {
  const first = vi.fn(async () => ({
    dirt_condition: "重",
    dirt_measurement_date: "2026-05-12",
    dirt_moisture_final_bend: "10.0",
    dirt_moisture_final_furlong: "9.5",
    dirt_moisture_measured_at: "2026-05-12T10:00:00+09:00",
    fetched_at: "2026-05-12T11:00:00+09:00",
    source_updated_at: null,
    turf_condition: "良",
    turf_course_layout: "A",
    turf_cushion_measured_at: "2026-05-12T09:30:00+09:00",
    turf_cushion_value: "9.0",
    turf_going: "B",
    turf_height_japanese_zoysia_grass: "9.0",
    turf_height_perennial_ryegrass: "8.0",
    turf_measurement_date: "2026-05-12",
    turf_moisture_final_bend: "8.0",
    turf_moisture_final_furlong: "7.5",
    turf_moisture_measured_at: "2026-05-12T09:00:00+09:00",
    weather: "晴",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getLatestTrackConditionForRace(db, "jra:2026:0512:08:01");
  expect(result?.weather).toBe("晴");
  expect(result?.turf.condition).toBe("良");
  expect(result?.turf.courseLayout).toBe("A");
  expect(result?.turf.height.japaneseZoysiaGrass).toBe("9.0");
  expect(result?.dirt.condition).toBe("重");
  expect(result?.dirt.moisture.finalFurlong).toBe("9.5");
});

it("countJraRaceSourcesMissingRaceDateFieldsByDate falls back to 0 when row is null", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await countJraRaceSourcesMissingRaceDateFieldsByDate(db, "20260512")).toBe(0);
});

it("replacePremiumRaceData defaults trainingReviews to [] when omitted", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run, bind: vi.fn(() => ({ run })) }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  await replacePremiumRaceData(db, {
    dataTopHorses: [],
    fetchedAt: "2026-05-12T11:00:00+09:00",
    link: { entryUrl: "https://x.test/race", sourceRaceId: "202605120801" },
    paddockBulletins: [],
    raceKey: "key",
    stableComments: [],
  });
  expect(batch).toHaveBeenCalledTimes(1);
});

it("getPremiumRaceDataFetchState returns mapped row when present", async () => {
  const first = vi.fn(async () => ({
    last_fetch_at: "2026-05-12T11:00:00+09:00",
    last_queued_at: "2026-05-12T10:00:00+09:00",
    retry_after: null,
    status: "ready",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getPremiumRaceDataFetchState(db, "key")).toStrictEqual({
    lastFetchAt: "2026-05-12T11:00:00+09:00",
    lastQueuedAt: "2026-05-12T10:00:00+09:00",
    retryAfter: null,
    status: "ready",
  });
});

it("updatePremiumRaceDataFetchState defaults optional params to null when omitted", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumRaceDataFetchState(db, { raceKey: "key", status: "queued" });
  const args = bind.mock.calls[0]!;
  expect(args[1]).toBe("queued");
  expect(args[2]).toBeNull();
  expect(args[3]).toBeNull();
  expect(args[4]).toBeNull();
});

it("getPremiumPaddockFetchState returns mapped row when present", async () => {
  const first = vi.fn(async () => ({
    last_fetch_at: "2026-05-12T11:00:00+09:00",
    last_queued_at: null,
    retry_after: null,
    status: "ready",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getPremiumPaddockFetchState(db, "key")).toStrictEqual({
    lastFetchAt: "2026-05-12T11:00:00+09:00",
    lastQueuedAt: null,
    retryAfter: null,
    status: "ready",
  });
});

it("updatePremiumPaddockFetchState defaults optional params to null when omitted", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumPaddockFetchState(db, { raceKey: "key", status: "queued" });
  const args = bind.mock.calls[0]!;
  expect(args[2]).toBeNull();
  expect(args[3]).toBeNull();
  expect(args[4]).toBeNull();
});

it("getPremiumPaddockNotificationState returns mapped row when present", async () => {
  const first = vi.fn(async () => ({
    last_notified_at: "2026-05-12T11:00:00+09:00",
    last_payload_fetched_at: "2026-05-12T11:00:00+09:00",
    last_send_attempt_at: null,
    message: null,
    payload_signature: "sig",
    skip_reason: null,
    status: "ok",
  }));
  const bind = vi.fn((..._args: unknown[]) => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getPremiumPaddockNotificationState(db, "key")).toStrictEqual({
    lastNotifiedAt: "2026-05-12T11:00:00+09:00",
    lastPayloadFetchedAt: "2026-05-12T11:00:00+09:00",
    lastSendAttemptAt: null,
    message: null,
    payloadSignature: "sig",
    skipReason: null,
    status: "ok",
  });
});

it("updatePremiumPaddockNotificationState defaults optional params to null when omitted", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await updatePremiumPaddockNotificationState(db, {
    payloadSignature: "sig",
    raceKey: "key",
    status: "queued",
  });
  const args = bind.mock.calls[0]!;
  expect(args[3]).toBeNull();
  expect(args[4]).toBeNull();
  expect(args[5]).toBeNull();
  expect(args[6]).toBeNull();
  expect(args[7]).toBeNull();
});

it("getPremiumRacePayload returns [] reasons when reasons_json is not parseable", async () => {
  const trainingAll = vi.fn(async () => ({ results: [] }));
  const commentAll = vi.fn(async () => ({ results: [] }));
  const paddockAll = vi.fn(async () => ({ results: [] }));
  const dataTopAll = vi.fn(async () => ({
    results: [
      {
        fetched_at: "now",
        horse_name: "h",
        horse_number: "1",
        rank: 1,
        reasons_json: "not-json",
      },
    ],
  }));
  const allsByOrder = [trainingAll, commentAll, paddockAll, dataTopAll];
  const prepare = vi.fn(() => {
    const next = allsByOrder.shift();
    return { bind: vi.fn(() => ({ all: next })) };
  });
  const db = { prepare } as unknown as D1Database;
  const result = await getPremiumRacePayload(db, "key");
  expect(result.dataTopHorses[0]?.reasons).toStrictEqual([]);
});

it("getPremiumRacePayload returns [] reasons when reasons_json parses to a non-array", async () => {
  const trainingAll = vi.fn(async () => ({ results: [] }));
  const commentAll = vi.fn(async () => ({ results: [] }));
  const paddockAll = vi.fn(async () => ({ results: [] }));
  const dataTopAll = vi.fn(async () => ({
    results: [
      {
        fetched_at: "now",
        horse_name: "h",
        horse_number: "1",
        rank: 1,
        reasons_json: '{"a":1}',
      },
    ],
  }));
  const allsByOrder = [trainingAll, commentAll, paddockAll, dataTopAll];
  const prepare = vi.fn(() => {
    const next = allsByOrder.shift();
    return { bind: vi.fn(() => ({ all: next })) };
  });
  const db = { prepare } as unknown as D1Database;
  const result = await getPremiumRacePayload(db, "key");
  expect(result.dataTopHorses[0]?.reasons).toStrictEqual([]);
});

it("insertRaceEntrySnapshot normalizes jockey marks/whitespace through normalizeStoredJockeyName", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run, bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  const count = await insertRaceEntrySnapshot(db, "key", "now", [
    { horseName: null, horseNumber: "1", jockeyName: "△武  豊", status: null },
    { horseName: null, horseNumber: "2", jockeyName: "▲ ", status: null },
    { horseName: null, horseNumber: "3", jockeyName: null, status: null },
  ]);
  expect(count).toBe(3);
});

it("listOddsSnapshotsForExport returns rows without fetched_at filter", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 1,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260528:42:01",
        rank: 1,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const { listOddsSnapshotsForExport } = await import("./storage");
  const rows = await listOddsSnapshotsForExport(db, { batchSize: 200, sinceId: 0 });
  expect(rows.length).toBe(1);
  expect(bind).toHaveBeenCalledWith(0, 200);
});

it("listOddsSnapshotsForExport binds fetched_at when option present", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const { listOddsSnapshotsForExport } = await import("./storage");
  await listOddsSnapshotsForExport(db, {
    afterFetchedAt: "2026-05-27T00:00:00+09:00",
    batchSize: 100,
    sinceId: 5,
  });
  expect(bind).toHaveBeenCalledWith(5, "2026-05-27T00:00:00+09:00", 100);
});

it("deleteOddsSnapshotsChunk returns done=true when no rows match", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const { deleteOddsSnapshotsChunk } = await import("./storage");
  const result = await deleteOddsSnapshotsChunk(db, {
    batchSize: 500,
    sinceId: 0,
    upperBoundId: 100,
  });
  expect(result).toStrictEqual({ deleted: 0, done: true, next_since_id: 0 });
});

it("deleteOddsSnapshotsChunk deletes the selected ids and returns metadata", async () => {
  const selectAll = vi.fn(async () => ({ results: [{ id: 1 }, { id: 2 }, { id: 3 }] }));
  const selectBind = vi.fn(() => ({ all: selectAll }));
  const deleteRun = vi.fn(async () => ({ meta: { rows_written: 3 } }));
  const deleteBind = vi.fn(() => ({ run: deleteRun }));
  const prepare = vi.fn((sql: string) => {
    if (sql.includes("select id")) {
      return { bind: selectBind };
    }
    return { bind: deleteBind };
  });
  const db = { prepare } as unknown as D1Database;
  const { deleteOddsSnapshotsChunk } = await import("./storage");
  const result = await deleteOddsSnapshotsChunk(db, {
    batchSize: 500,
    sinceId: 0,
    upperBoundId: 100,
  });
  expect(result).toStrictEqual({ deleted: 3, done: true, next_since_id: 3 });
});

it("deleteOddsSnapshotsChunk reports done=false when batch saturates limit", async () => {
  const selectAll = vi.fn(async () => ({ results: [{ id: 10 }, { id: 11 }] }));
  const selectBind = vi.fn(() => ({ all: selectAll }));
  const deleteRun = vi.fn(async () => ({ meta: { rows_written: 2 } }));
  const deleteBind = vi.fn(() => ({ run: deleteRun }));
  const prepare = vi.fn((sql: string) => {
    if (sql.includes("select id")) {
      return { bind: selectBind };
    }
    return { bind: deleteBind };
  });
  const db = { prepare } as unknown as D1Database;
  const { deleteOddsSnapshotsChunk } = await import("./storage");
  const result = await deleteOddsSnapshotsChunk(db, {
    batchSize: 2,
    sinceId: 5,
    upperBoundId: 100,
  });
  expect(result).toStrictEqual({ deleted: 2, done: false, next_since_id: 11 });
});

it("deleteOddsSnapshotsChunk falls back to ids.length when rows_written is missing", async () => {
  const selectAll = vi.fn(async () => ({ results: [{ id: 7 }] }));
  const selectBind = vi.fn(() => ({ all: selectAll }));
  const deleteRun = vi.fn(async () => ({ meta: {} }));
  const deleteBind = vi.fn(() => ({ run: deleteRun }));
  const prepare = vi.fn((sql: string) => {
    if (sql.includes("select id")) {
      return { bind: selectBind };
    }
    return { bind: deleteBind };
  });
  const db = { prepare } as unknown as D1Database;
  const { deleteOddsSnapshotsChunk } = await import("./storage");
  const result = await deleteOddsSnapshotsChunk(db, {
    batchSize: 500,
    sinceId: 0,
    upperBoundId: 100,
  });
  expect(result.deleted).toBe(1);
});

it("deleteDailyRaceEntriesChunk returns deletedRowCount=0 when no rows match", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const { deleteDailyRaceEntriesChunk } = await import("./storage");
  const result = await deleteDailyRaceEntriesChunk(db, { chunkSize: 500, sinceRowid: 17 });
  expect(result).toStrictEqual({ deletedRowCount: 0, nextSinceRowid: 17 });
});

it("deleteDailyRaceEntriesChunk deletes the selected rowids and returns the max rowid", async () => {
  const selectAll = vi.fn(async () => ({ results: [{ rowid: 1 }, { rowid: 2 }, { rowid: 3 }] }));
  const selectBind = vi.fn(() => ({ all: selectAll }));
  const deleteRun = vi.fn(async () => ({ meta: { rows_written: 3 } }));
  const deleteBind = vi.fn(() => ({ run: deleteRun }));
  const prepare = vi.fn((sql: string) => {
    if (sql.includes("select rowid")) {
      return { bind: selectBind };
    }
    return { bind: deleteBind };
  });
  const db = { prepare } as unknown as D1Database;
  const { deleteDailyRaceEntriesChunk } = await import("./storage");
  const result = await deleteDailyRaceEntriesChunk(db, { chunkSize: 500, sinceRowid: 0 });
  expect(result).toStrictEqual({ deletedRowCount: 3, nextSinceRowid: 3 });
});

it("deleteDailyRaceEntriesChunk falls back to rowids.length when rows_written is missing", async () => {
  const selectAll = vi.fn(async () => ({ results: [{ rowid: 9 }] }));
  const selectBind = vi.fn(() => ({ all: selectAll }));
  const deleteRun = vi.fn(async () => ({ meta: {} }));
  const deleteBind = vi.fn(() => ({ run: deleteRun }));
  const prepare = vi.fn((sql: string) => {
    if (sql.includes("select rowid")) {
      return { bind: selectBind };
    }
    return { bind: deleteBind };
  });
  const db = { prepare } as unknown as D1Database;
  const { deleteDailyRaceEntriesChunk } = await import("./storage");
  const result = await deleteDailyRaceEntriesChunk(db, { chunkSize: 500, sinceRowid: 0 });
  expect(result.deletedRowCount).toBe(1);
});

it("deleteRaceRunningStylesChunk returns deletedRowCount=0 when no rows match", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const { deleteRaceRunningStylesChunk } = await import("./storage");
  const result = await deleteRaceRunningStylesChunk(db, { chunkSize: 500, sinceRowid: 42 });
  expect(result).toStrictEqual({ deletedRowCount: 0, nextSinceRowid: 42 });
});

it("deleteRaceRunningStylesChunk deletes the selected rowids and returns the max rowid", async () => {
  const selectAll = vi.fn(async () => ({
    results: [{ rowid: 10 }, { rowid: 11 }, { rowid: 12 }],
  }));
  const selectBind = vi.fn(() => ({ all: selectAll }));
  const deleteRun = vi.fn(async () => ({ meta: { rows_written: 3 } }));
  const deleteBind = vi.fn(() => ({ run: deleteRun }));
  const prepare = vi.fn((sql: string) => {
    if (sql.includes("select rowid")) {
      return { bind: selectBind };
    }
    return { bind: deleteBind };
  });
  const db = { prepare } as unknown as D1Database;
  const { deleteRaceRunningStylesChunk } = await import("./storage");
  const result = await deleteRaceRunningStylesChunk(db, { chunkSize: 500, sinceRowid: 0 });
  expect(result).toStrictEqual({ deletedRowCount: 3, nextSinceRowid: 12 });
});

it("deleteRaceRunningStylesChunk falls back to rowids.length when rows_written is missing", async () => {
  const selectAll = vi.fn(async () => ({ results: [{ rowid: 21 }] }));
  const selectBind = vi.fn(() => ({ all: selectAll }));
  const deleteRun = vi.fn(async () => ({ meta: {} }));
  const deleteBind = vi.fn(() => ({ run: deleteRun }));
  const prepare = vi.fn((sql: string) => {
    if (sql.includes("select rowid")) {
      return { bind: selectBind };
    }
    return { bind: deleteBind };
  });
  const db = { prepare } as unknown as D1Database;
  const { deleteRaceRunningStylesChunk } = await import("./storage");
  const result = await deleteRaceRunningStylesChunk(db, { chunkSize: 500, sinceRowid: 0 });
  expect(result.deletedRowCount).toBe(1);
});
