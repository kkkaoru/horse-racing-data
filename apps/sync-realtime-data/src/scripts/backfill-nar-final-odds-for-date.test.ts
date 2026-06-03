// Run with bun: `bun run --filter sync-realtime-data test`
import { afterEach, expect, test, vi } from "vitest";

vi.mock("../keiba-go", () => ({
  extractOddsLinks: vi.fn(() => ({})),
  fetchOdds: vi.fn(async () => ({})),
  fetchRacePage: vi.fn(async () => ""),
}));

import { extractOddsLinks, fetchOdds, fetchRacePage } from "../keiba-go";
import type { OddsData, OddsType } from "../types";
import {
  backfillRaceFinalOdds,
  buildFinalFetchedAt,
  buildOddsInsertSql,
  buildSqlFilePath,
  filterAlreadyStarted,
  listNarRaceSources,
  parseTargetDate,
  runBackfill,
  sqlNullableNumber,
  sqlString,
  type BackfillRaceSummary,
  type NarRaceSource,
} from "./backfill-nar-final-odds-for-date";

afterEach(() => {
  vi.restoreAllMocks();
  vi.mocked(extractOddsLinks).mockReset();
  vi.mocked(extractOddsLinks).mockReturnValue({});
  vi.mocked(fetchOdds).mockReset();
  vi.mocked(fetchOdds).mockResolvedValue({});
  vi.mocked(fetchRacePage).mockReset();
  vi.mocked(fetchRacePage).mockResolvedValue("");
});

test("sqlString single-quotes the value", () => {
  expect(sqlString("hello")).toBe("'hello'");
});

test("sqlString escapes embedded single quotes", () => {
  expect(sqlString("it's")).toBe("'it''s'");
});

test("sqlNullableNumber returns null literal for null", () => {
  expect(sqlNullableNumber(null)).toBe("null");
});

test("sqlNullableNumber returns null literal for undefined", () => {
  expect(sqlNullableNumber(undefined)).toBe("null");
});

test("sqlNullableNumber stringifies a finite number", () => {
  expect(sqlNullableNumber(2.5)).toBe("2.5");
});

test("sqlNullableNumber preserves zero", () => {
  expect(sqlNullableNumber(0)).toBe("0");
});

test("parseTargetDate returns the YYYYMMDD argv element", () => {
  expect(parseTargetDate(["bun", "script.ts", "20260603"])).toBe("20260603");
});

test("parseTargetDate throws when argv lacks a date", () => {
  expect(() => parseTargetDate(["bun", "script.ts"])).toThrow(
    "usage: bun src/scripts/backfill-nar-final-odds-for-date.ts YYYYMMDD",
  );
});

test("parseTargetDate throws when argv contains a malformed date", () => {
  expect(() => parseTargetDate(["bun", "script.ts", "2026-06-03"])).toThrow(
    "usage: bun src/scripts/backfill-nar-final-odds-for-date.ts YYYYMMDD",
  );
});

test("buildFinalFetchedAt adds two minutes to the race start time", () => {
  expect(buildFinalFetchedAt("2026-06-03T17:00:00+09:00")).toBe("2026-06-03T17:02:00+09:00");
});

test("buildFinalFetchedAt wraps to the next hour when needed", () => {
  expect(buildFinalFetchedAt("2026-06-03T17:59:00+09:00")).toBe("2026-06-03T18:01:00+09:00");
});

test("buildFinalFetchedAt throws on invalid date strings", () => {
  expect(() => buildFinalFetchedAt("not-a-date")).toThrow("invalid race_start_at_jst: not-a-date");
});

test("filterAlreadyStarted keeps races whose start time has passed", () => {
  expect(
    filterAlreadyStarted({
      now: new Date("2026-06-03T17:30:00+09:00"),
      races: [
        {
          deba_url: "https://x/1",
          race_key: "a",
          race_start_at_jst: "2026-06-03T17:00:00+09:00",
        },
      ],
    }),
  ).toStrictEqual([
    {
      deba_url: "https://x/1",
      race_key: "a",
      race_start_at_jst: "2026-06-03T17:00:00+09:00",
    },
  ]);
});

test("filterAlreadyStarted drops races whose start time is in the future", () => {
  expect(
    filterAlreadyStarted({
      now: new Date("2026-06-03T17:30:00+09:00"),
      races: [
        {
          deba_url: "https://x/1",
          race_key: "a",
          race_start_at_jst: "2026-06-03T20:00:00+09:00",
        },
      ],
    }),
  ).toStrictEqual([]);
});

test("filterAlreadyStarted drops races with an unparsable start time", () => {
  expect(
    filterAlreadyStarted({
      now: new Date("2026-06-03T17:30:00+09:00"),
      races: [
        {
          deba_url: "https://x/1",
          race_key: "a",
          race_start_at_jst: "garbage",
        },
      ],
    }),
  ).toStrictEqual([]);
});

test("buildOddsInsertSql produces zero statements when odds is empty", () => {
  expect(
    buildOddsInsertSql({
      fetchedAt: "2026-06-03T17:02:00+09:00",
      odds: {},
      raceKey: "nar:2026:0603:30:01",
    }),
  ).toStrictEqual([]);
});

test("buildOddsInsertSql emits one idempotent INSERT per tansho row", () => {
  const tanshoRow: OddsData = { combination: "5", odds: 2.4, rank: 1 };
  const result = buildOddsInsertSql({
    fetchedAt: "2026-06-03T17:02:00+09:00",
    odds: { tansho: [tanshoRow] },
    raceKey: "nar:2026:0603:30:01",
  });
  expect(result).toStrictEqual([
    "insert into odds_snapshots\n  (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank)\nselect 'nar:2026:0603:30:01', '2026-06-03T17:02:00+09:00', 'tansho',\n       '5', 2.4,\n       null, null,\n       null, 1\nwhere not exists (\n  select 1 from odds_snapshots\n  where race_key = 'nar:2026:0603:30:01'\n    and odds_type = 'tansho'\n    and combination = '5'\n    and fetched_at = '2026-06-03T17:02:00+09:00'\n);",
  ]);
});

test("buildOddsInsertSql emits min/max/average for fukusho rows", () => {
  const fukushoRow: OddsData = {
    averageOdds: 1.5,
    combination: "3",
    maxOdds: 1.7,
    minOdds: 1.3,
    rank: 2,
  };
  const result = buildOddsInsertSql({
    fetchedAt: "2026-06-03T17:02:00+09:00",
    odds: { fukusho: [fukushoRow] },
    raceKey: "nar:2026:0603:30:01",
  });
  expect(result).toStrictEqual([
    "insert into odds_snapshots\n  (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank)\nselect 'nar:2026:0603:30:01', '2026-06-03T17:02:00+09:00', 'fukusho',\n       '3', null,\n       1.3, 1.7,\n       1.5, 2\nwhere not exists (\n  select 1 from odds_snapshots\n  where race_key = 'nar:2026:0603:30:01'\n    and odds_type = 'fukusho'\n    and combination = '3'\n    and fetched_at = '2026-06-03T17:02:00+09:00'\n);",
  ]);
});

test("buildOddsInsertSql concatenates rows from multiple odds types", () => {
  const tanshoRow: OddsData = { combination: "5", odds: 2.4, rank: 1 };
  const umarenRow: OddsData = { combination: "1-5", odds: 12.5, rank: 1 };
  const result = buildOddsInsertSql({
    fetchedAt: "2026-06-03T17:02:00+09:00",
    odds: { tansho: [tanshoRow], umaren: [umarenRow] },
    raceKey: "nar:2026:0603:30:01",
  });
  expect(result.length).toBe(2);
});

test("buildOddsInsertSql falls back to empty arrays when an OddsType has undefined value", () => {
  const oddsWithUndefined: Partial<Record<OddsType, OddsData[]>> = {
    tansho: undefined,
  };
  expect(
    buildOddsInsertSql({
      fetchedAt: "2026-06-03T17:02:00+09:00",
      odds: oddsWithUndefined,
      raceKey: "nar:2026:0603:30:01",
    }),
  ).toStrictEqual([]);
});

test("buildSqlFilePath sanitizes a race key that contains colons", () => {
  expect(buildSqlFilePath("nar:2026:0603:30:01")).toBe(
    "/tmp/backfill-nar-final-odds-nar_2026_0603_30_01.sql",
  );
});

test("listNarRaceSources passes a SELECT against the legacy D1 to wrangler", async () => {
  const runWranglerImpl = vi.fn(
    async (_args: readonly string[]): Promise<string> =>
      JSON.stringify([
        {
          results: [
            {
              deba_url: "https://x/1",
              race_key: "nar:2026:0603:30:01",
              race_start_at_jst: "2026-06-03T17:00:00+09:00",
            },
          ],
          success: true,
        },
      ]),
  );
  const rows = await listNarRaceSources("20260603", { runWranglerImpl });
  expect(rows).toStrictEqual([
    {
      deba_url: "https://x/1",
      race_key: "nar:2026:0603:30:01",
      race_start_at_jst: "2026-06-03T17:00:00+09:00",
    },
  ]);
  expect(runWranglerImpl).toHaveBeenCalledOnce();
  const wranglerArgs = runWranglerImpl.mock.calls[0]![0];
  expect(wranglerArgs[0]).toBe("d1");
  expect(wranglerArgs[1]).toBe("execute");
  expect(wranglerArgs[2]).toBe("sync-realtime-data");
  expect(wranglerArgs[3]).toBe("--remote");
  expect(wranglerArgs[4]).toBe("--json");
  expect(wranglerArgs[5]).toBe("--command");
  expect(wranglerArgs[6]).toBe(
    "select race_key, deba_url, race_start_at_jst\nfrom realtime_race_sources\nwhere source = 'nar'\n  and kaisai_nen = '2026'\n  and kaisai_tsukihi = '0603'\norder by race_start_at_jst, keibajo_code, race_bango",
  );
});

test("listNarRaceSources throws when wrangler reports failure", async () => {
  const runWranglerImpl = vi.fn(
    async (_args: readonly string[]): Promise<string> =>
      JSON.stringify([{ results: [], success: false }]),
  );
  await expect(listNarRaceSources("20260603", { runWranglerImpl })).rejects.toThrow(
    "failed to list NAR race sources for 20260603",
  );
});

test("listNarRaceSources returns an empty array when results is missing", async () => {
  const runWranglerImpl = vi.fn(
    async (_args: readonly string[]): Promise<string> => JSON.stringify([{ success: true }]),
  );
  expect(await listNarRaceSources("20260603", { runWranglerImpl })).toStrictEqual([]);
});

test("backfillRaceFinalOdds writes the SQL file and runs wrangler against the hot D1", async () => {
  vi.mocked(fetchRacePage).mockResolvedValue("<html>entry</html>");
  vi.mocked(extractOddsLinks).mockReturnValue({
    tansho: "https://x/odds-tansho",
  });
  vi.mocked(fetchOdds).mockResolvedValue({
    tansho: [{ combination: "5", odds: 2.4, rank: 1 }],
  });
  const writeFileImpl = vi.fn(async (_path: string, _contents: string): Promise<number> => 1);
  const runWranglerImpl = vi.fn(async (_args: readonly string[]): Promise<string> => "{}");
  const summary = await backfillRaceFinalOdds({
    fetchOddsImpl: fetchOdds,
    fetchPageImpl: fetchRacePage,
    race: {
      deba_url: "https://x/deba",
      race_key: "nar:2026:0603:30:01",
      race_start_at_jst: "2026-06-03T17:00:00+09:00",
    },
    runWranglerImpl,
    writeFileImpl,
  });
  expect(summary).toStrictEqual({
    fetchedAt: "2026-06-03T17:02:00+09:00",
    oddsRows: 1,
    raceKey: "nar:2026:0603:30:01",
    typesFetched: 1,
  });
  expect(writeFileImpl).toHaveBeenCalledOnce();
  const writeArgs = writeFileImpl.mock.calls[0]!;
  expect(writeArgs[0]).toBe("/tmp/backfill-nar-final-odds-nar_2026_0603_30_01.sql");
  expect(typeof writeArgs[1]).toBe("string");
  expect(runWranglerImpl).toHaveBeenCalledOnce();
  const wranglerArgs = runWranglerImpl.mock.calls[0]![0];
  expect(wranglerArgs[0]).toBe("d1");
  expect(wranglerArgs[1]).toBe("execute");
  expect(wranglerArgs[2]).toBe("sync-realtime-data-hot");
  expect(wranglerArgs[3]).toBe("--remote");
  expect(wranglerArgs[4]).toBe("--json");
  expect(wranglerArgs[5]).toBe("--file");
  expect(wranglerArgs[6]).toBe("/tmp/backfill-nar-final-odds-nar_2026_0603_30_01.sql");
});

test("backfillRaceFinalOdds skips writes when no odds rows are produced", async () => {
  vi.mocked(fetchRacePage).mockResolvedValue("<html>entry</html>");
  vi.mocked(extractOddsLinks).mockReturnValue({});
  vi.mocked(fetchOdds).mockResolvedValue({});
  const writeFileImpl = vi.fn(async (_path: string, _contents: string): Promise<number> => 0);
  const runWranglerImpl = vi.fn(async (_args: readonly string[]): Promise<string> => "{}");
  const summary = await backfillRaceFinalOdds({
    fetchOddsImpl: fetchOdds,
    fetchPageImpl: fetchRacePage,
    race: {
      deba_url: "https://x/deba",
      race_key: "nar:2026:0603:30:01",
      race_start_at_jst: "2026-06-03T17:00:00+09:00",
    },
    runWranglerImpl,
    writeFileImpl,
  });
  expect(summary).toStrictEqual({
    fetchedAt: "2026-06-03T17:02:00+09:00",
    oddsRows: 0,
    raceKey: "nar:2026:0603:30:01",
    typesFetched: 0,
  });
  expect(writeFileImpl).not.toHaveBeenCalled();
  expect(runWranglerImpl).not.toHaveBeenCalled();
});

test("runBackfill processes only races whose start time has passed", async () => {
  const logCalls: string[] = [];
  const log = (message: string): void => {
    logCalls.push(message);
  };
  const listSources = vi.fn(
    async (_targetDate: string): Promise<NarRaceSource[]> => [
      {
        deba_url: "https://x/started",
        race_key: "nar:2026:0603:30:01",
        race_start_at_jst: "2026-06-03T17:00:00+09:00",
      },
      {
        deba_url: "https://x/upcoming",
        race_key: "nar:2026:0603:30:02",
        race_start_at_jst: "2026-06-03T20:00:00+09:00",
      },
    ],
  );
  const backfillRaceImpl = vi.fn(
    async (race: NarRaceSource): Promise<BackfillRaceSummary> => ({
      fetchedAt: "2026-06-03T17:02:00+09:00",
      oddsRows: 3,
      raceKey: race.race_key,
      typesFetched: 2,
    }),
  );
  const summary = await runBackfill({
    backfillRaceImpl,
    listSources,
    log,
    now: new Date("2026-06-03T17:30:00+09:00"),
    targetDate: "20260603",
  });
  expect(summary).toStrictEqual({ oddsRows: 3, races: 1, skipped: 1 });
  expect(backfillRaceImpl).toHaveBeenCalledOnce();
  expect(backfillRaceImpl.mock.calls[0]![0].race_key).toBe("nar:2026:0603:30:01");
  expect(logCalls.length).toBe(3);
});

test("runBackfill reports zero totals when no races are listed", async () => {
  const logCalls: string[] = [];
  const log = (message: string): void => {
    logCalls.push(message);
  };
  const listSources = vi.fn(async (_targetDate: string): Promise<NarRaceSource[]> => []);
  const backfillRaceImpl = vi.fn(
    async (_race: NarRaceSource): Promise<BackfillRaceSummary> => ({
      fetchedAt: "2026-06-03T17:02:00+09:00",
      oddsRows: 0,
      raceKey: "x",
      typesFetched: 0,
    }),
  );
  const summary = await runBackfill({
    backfillRaceImpl,
    listSources,
    log,
    now: new Date("2026-06-03T17:30:00+09:00"),
    targetDate: "20260603",
  });
  expect(summary).toStrictEqual({ oddsRows: 0, races: 0, skipped: 0 });
  expect(backfillRaceImpl).not.toHaveBeenCalled();
  expect(logCalls.length).toBe(2);
});
