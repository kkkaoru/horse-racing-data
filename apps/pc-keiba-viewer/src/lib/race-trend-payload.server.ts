// Run with bun: bunx vitest run src/lib/race-trend-payload.server.test.ts
// Server-only helper extracted from the trends API route so the
// race-detail Server Component can fetch the same aggregated race-trend
// payload via SSR. The route delegates here for the heavy fetch +
// aggregation work; the SSR call adds a thin wrapper that resolves race
// detail / runners and aggregates the raw payload with sensible defaults
// so the runners-table can merge trend-derived finishPosition values
// before first paint.
import "server-only";
import type { RaceTrendStarterRow } from "horse-racing-realtime/race-trend-daily-track-types";

import {
  buildPast14WindowForTarget,
  getLatestTanshoOddsFromHotD1,
  getRaceTrendPast14StarterRows,
  getRaceTrendRunningStylesFromD1,
  getRaceTrendTodayRunningStylesFromD1,
  getRaceTrendTodayStarterRows,
} from "../db/d1-trend-queries.server";
import { getRaceDetail, getRaceRunners } from "../db/queries";
import { getRaceTrendTodaySiblingRunnerData } from "../db/today-sibling-runner-data.server";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type { RaceSource } from "./codes";
import {
  aggregateForTargets,
  filterTodaySiblingRows,
  mergeStarterRows,
  starterRaceKey,
} from "./race-trend-aggregate";
import { buildDefaultRaceTrendCacheOptions, type RaceTrendCacheOptions } from "./race-trend-cache";
import {
  fetchRaceTrendDailyTrack,
  type RaceTrendDailyTrackFetchResult,
} from "./race-trend-daily-track-client.server";
import { DEFAULT_RACE_TREND_TARGETS, type RaceTrendTargets } from "./race-trend-query";
import type {
  RaceDetail,
  RaceTrendPayload,
  RaceTrendRawPayload,
  RaceTrendRunningStyle,
  Runner,
} from "./race-types";
import { getRaceRunningStylesWithCache } from "./running-style-cache.server";
import {
  mergeTanshoOddsEnrichment,
  mergeTodaySiblingRunnerData,
  type TanshoOddsEnrichmentEntry,
  type TodaySiblingRunnerEntry,
} from "./today-sibling-runner-merge";

export type RaceTrendSourceHeaderValue = "do-hit" | "do-miss-fallback" | "do-error-fallback";

export interface RaceTrendBuildResult {
  payload: RaceTrendRawPayload;
  sourceHeader: RaceTrendSourceHeaderValue;
}

export interface BuildRaceTrendRawPayloadForRaceInput {
  options: RaceTrendCacheOptions;
  race: RaceDetail;
  runners: Runner[];
}

export interface PickTodaySiblingRowsAndSourceInput {
  fallbackRows: RaceTrendStarterRow[];
  result: RaceTrendDailyTrackFetchResult;
}

export interface PickTodaySiblingRowsAndSourceResult {
  rows: RaceTrendStarterRow[];
  sourceHeader: RaceTrendSourceHeaderValue;
}

export interface GetRaceTrendPayloadForRaceInput {
  day: string;
  jockeySameVenue?: boolean;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  trendTargets?: RaceTrendTargets;
  year: string;
}

const DO_HIT_HEADER: RaceTrendSourceHeaderValue = "do-hit";
const DO_MISS_HEADER: RaceTrendSourceHeaderValue = "do-miss-fallback";
const DO_ERROR_HEADER: RaceTrendSourceHeaderValue = "do-error-fallback";

const DO_ERROR_RESULT: RaceTrendDailyTrackFetchResult = { rows: [], status: "error" };

const pickSiblingRowsFromDoResult = (
  result: RaceTrendDailyTrackFetchResult,
): RaceTrendStarterRow[] => result.rows.flatMap((row) => row.starterRows);

export const pickTodaySiblingRowsAndSource = ({
  fallbackRows,
  result,
}: PickTodaySiblingRowsAndSourceInput): PickTodaySiblingRowsAndSourceResult => {
  if (result.status === "hit") {
    return { rows: pickSiblingRowsFromDoResult(result), sourceHeader: DO_HIT_HEADER };
  }
  return {
    rows: fallbackRows,
    sourceHeader: result.status === "miss" ? DO_MISS_HEADER : DO_ERROR_HEADER,
  };
};

const safePast14Promise = (
  promise: Promise<RaceTrendStarterRow[]>,
): Promise<RaceTrendStarterRow[]> => promise.catch(() => []);

const safeDoResultPromise = (
  promise: Promise<RaceTrendDailyTrackFetchResult>,
): Promise<RaceTrendDailyTrackFetchResult> => promise.catch(() => DO_ERROR_RESULT);

const safeLegacyTodayPromise = (
  promise: Promise<RaceTrendStarterRow[]>,
): Promise<RaceTrendStarterRow[]> => promise.catch(() => []);

// Hyperdrive (R1) fetch wrapper: a single jvd_se / nvd_se outage must not
// black out the trend section. Fall back to an empty entry list so the
// merge step degrades to a no-op and the today-sibling rows still render
// with whatever wakuban the DO derived plus a "-" trainer column.
const safeSiblingRunnerEntriesPromise = (
  promise: Promise<TodaySiblingRunnerEntry[]>,
): Promise<TodaySiblingRunnerEntry[]> => promise.catch(() => []);

// Tansho odds live in REALTIME_HOT_DB (separate D1 binding) — the
// race-trend DO cannot join across databases so the do-hit starter rows
// arrive with tanshoOdds / tanshoPopularity null. Fetch the same map the
// legacy do-miss path uses and translate it into the merge format. A
// missing binding (preview deploy) or a query failure surfaces as an
// empty array so the trend section degrades to "-" rather than 500.
const ODDS_TENTH_MULTIPLIER = 10;

const buildTanshoEnrichmentEntries = (
  oddsMap: Map<string, Map<string, { odds: number | null; rank: number | null }>>,
): TanshoOddsEnrichmentEntry[] => {
  const entries: TanshoOddsEnrichmentEntry[] = [];
  for (const [raceKey, perHorse] of oddsMap) {
    for (const [umaban, odds] of perHorse) {
      entries.push({
        raceKey,
        tanshoOddsTenth: odds.odds === null ? null : Math.round(odds.odds * ODDS_TENTH_MULTIPLIER),
        tanshoPopularity: odds.rank,
        umaban,
      });
    }
  }
  return entries;
};

const safeTanshoEnrichmentPromise = (
  promise: Promise<TanshoOddsEnrichmentEntry[]>,
): Promise<TanshoOddsEnrichmentEntry[]> => promise.catch(() => []);

const toCurrentRunningStyles = (
  rows: ReadonlyArray<{ horseNumber: number; predictedLabel: RaceTrendRunningStyle }>,
): RaceTrendRawPayload["currentRunningStyles"] =>
  rows.map((row) => ({
    horseNumber: String(row.horseNumber),
    predictedLabel: row.predictedLabel,
  }));

const dedupeHistoricalRunningStyles = (
  direct: RaceTrendRawPayload["historicalRunningStyles"],
): RaceTrendRawPayload["historicalRunningStyles"] => {
  const merged = new Map<string, RaceTrendRawPayload["historicalRunningStyles"][number]>();
  for (const row of direct) {
    const key = `${row.raceKey}:${row.horseNumber}`;
    if (!merged.has(key)) merged.set(key, row);
  }
  return Array.from(merged.values());
};

// Reject degenerate trend payloads from the cache write path. The
// payload is only useful for client-side aggregation when both starter
// rows and the matching running-style history are populated — if
// either side came back empty it almost always means D1 was
// momentarily saturated, and pinning that for the cache TTL hides the
// recovered data the moment D1 catches up.
export const isCacheableTrendPayload = (payload: RaceTrendRawPayload): boolean =>
  payload.starterRows.length > 0 && payload.historicalRunningStyles.length > 0;

export const buildRaceTrendRawPayloadForRace = async ({
  options,
  race,
  runners,
}: BuildRaceTrendRawPayloadForRaceInput): Promise<RaceTrendBuildResult> => {
  const targetYmd = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  const past14Window = buildPast14WindowForTarget(targetYmd);
  const currentRunningStylesPromise = getRaceRunningStylesWithCache({
    kaisaiNen: race.kaisaiNen,
    kaisaiTsukihi: race.kaisaiTsukihi,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
  }).catch(() => []);
  // Each upstream is wrapped in `.catch(() => fallback)` so a single
  // rejected branch cannot black out the whole trend payload. This
  // matches the existing `currentRunningStylesPromise.catch(() => [])`
  // pattern above.
  const past14Promise = safePast14Promise(
    getRaceTrendPast14StarterRows({
      endYmd: past14Window.endYmd,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      source: options.source,
      startYmd: past14Window.startYmd,
    }),
  );
  // DO-primary path: sync-realtime-data's RaceTrendDailyTrackDO maintains
  // a per-(source, ymd, keibajoCode) daily aggregate refreshed by the
  // 5 min poller. When the DO has the answer ready we skip the legacy
  // D1-backed today-cache entirely. The legacy helper stays around as a
  // fallback for DO miss / error so a single missing DO entry can't
  // black out the trend section.
  //
  // We await DO before deciding whether to fire the legacy fetch: hitting
  // both in parallel wasted a D1 round-trip every time DO won (= the hot
  // path now that the DO is populated by the 5 min poller). On DO miss /
  // error we still fall back to legacy, paying one extra D1 read of
  // sequential latency only in that minority case.
  const env = await safeGetCloudflareEnv();
  const doResultPromise = safeDoResultPromise(
    fetchRaceTrendDailyTrack(env, {
      beforeRaceBango: race.raceBango,
      keibajoCode: race.keibajoCode,
      source: race.source,
      targetYmd,
    }),
  );
  const doResult = await doResultPromise;
  const legacyTodayRows =
    doResult.status === "hit"
      ? []
      : await safeLegacyTodayPromise(
          getRaceTrendTodayStarterRows({
            keibajoCode: race.keibajoCode,
            source: options.source,
            targetYmd,
          }),
        );
  const past14Rows = await past14Promise;
  const legacyTodaySiblingRows = filterTodaySiblingRows(legacyTodayRows, {
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
    targetYmd,
  });
  const { rows: rawTodaySiblingRows, sourceHeader } = pickTodaySiblingRowsAndSource({
    fallbackRows: legacyTodaySiblingRows,
    result: doResult,
  });
  // Defense-in-depth: re-apply the sibling filter so DO state with
  // stale-day or other-venue rows (the DO is partitioned per
  // (source, ymd, keibajoCode) but the flattened payload still carries
  // raw `RaceTrendStarterRow` records we should re-narrow before merge).
  const todaySiblingRowsFromSnapshots = filterTodaySiblingRows(rawTodaySiblingRows, {
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
    targetYmd,
  });
  // race_entry_snapshots carries neither wakuban nor chokyoshi_name, so the
  // DO and legacy snapshot paths populate at most a derived wakuban (from
  // umaban + horse_count) and always leave chokyoshiName undefined. Backfill
  // both columns from jvd_se / nvd_se in a single round trip so the trend
  // section's expanded detail panel for a sibling race shows real values
  // instead of "-".
  const siblingRunnerEntries =
    todaySiblingRowsFromSnapshots.length === 0
      ? []
      : await safeSiblingRunnerEntriesPromise(
          getRaceTrendTodaySiblingRunnerData({
            beforeRaceBango: race.raceBango,
            keibajoCode: race.keibajoCode,
            monthDay: race.kaisaiTsukihi,
            source: race.source,
            year: race.kaisaiNen,
          }),
        );
  const todaySiblingRowsWithRunner = mergeTodaySiblingRunnerData(
    todaySiblingRowsFromSnapshots,
    siblingRunnerEntries,
  );
  // Tansho enrichment: only the do-hit path needs it because the legacy
  // do-miss path already returns starter rows with tanshoOdds /
  // tanshoPopularity populated by getRaceTrendTodayStarterRows. Skipping the
  // hot-DB round-trip on do-miss avoids the duplicate query without
  // changing the rendered output.
  const tanshoEnrichmentEntries =
    sourceHeader === "do-hit" && todaySiblingRowsWithRunner.length > 0
      ? await safeTanshoEnrichmentPromise(
          getLatestTanshoOddsFromHotD1({
            env: env ?? null,
            raceKeys: Array.from(new Set(todaySiblingRowsWithRunner.map(starterRaceKey))),
          }).then((map) => buildTanshoEnrichmentEntries(map)),
        )
      : [];
  const todaySiblingRows = mergeTanshoOddsEnrichment(
    todaySiblingRowsWithRunner,
    tanshoEnrichmentEntries,
  );
  const starterRows = mergeStarterRows(past14Rows, todaySiblingRows);
  const currentRunningStyles = await currentRunningStylesPromise;
  const past14RaceKeys = Array.from(new Set(past14Rows.map(starterRaceKey)));
  const todayRaceKeys = Array.from(new Set(todaySiblingRows.map(starterRaceKey)));
  // Split running-style fetches: past-14 keys go through the KV-backed
  // helper (stable history, safe to cache cross-colo), while today's
  // sibling keys bypass KV because new inferences land throughout the
  // race day and a KV mirror would pin them out of date.
  const [past14RunningStyles, todayRunningStyles] = await Promise.all([
    getRaceTrendRunningStylesFromD1(past14RaceKeys),
    getRaceTrendTodayRunningStylesFromD1(todayRaceKeys),
  ]);
  const mergedHistoricalRunningStyles = dedupeHistoricalRunningStyles([
    ...past14RunningStyles,
    ...todayRunningStyles,
  ]);
  return {
    payload: {
      raceContext: {
        keibajoCode: race.keibajoCode,
        raceBango: race.raceBango,
        source: race.source,
      },
      runners: runners.map((runner) => ({
        frameNumber: runner.wakuban,
        horseNumber: runner.umaban,
        jockeyName: runner.kishumeiRyakusho,
        trainerName: runner.chokyoshimeiRyakusho,
      })),
      starterRows,
      currentRunningStyles: toCurrentRunningStyles(currentRunningStyles),
      historicalRunningStyles: mergedHistoricalRunningStyles,
    },
    sourceHeader,
  };
};

const EMPTY_RACE_TREND_PAYLOAD: RaceTrendPayload = {
  raceCount: 0,
  runningStyleRows: [],
};

// SSR aggregation defaults: the runners-table merge only needs the
// flattened detail rows that carry historical finishPosition values per
// horse, so we use the same DEFAULT_RACE_TREND_TARGETS the client picks
// when the user has not customised the panel. jockeySameVenue stays
// false so we still capture detail rows from venues other than the
// current one — the merge picks the latest entry per horseNumber so
// extra rows do not corrupt the map.
const DEFAULT_SSR_JOCKEY_SAME_VENUE = false;

export const getRaceTrendPayloadForRace = async (
  input: GetRaceTrendPayloadForRaceInput,
): Promise<RaceTrendPayload> => {
  const { day, keibajoCode, month, raceNumber, source, year } = input;
  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return EMPTY_RACE_TREND_PAYLOAD;
  }
  const runners = await getRaceRunners(source, year, month, day, keibajoCode, raceNumber);
  const targetYmd = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  const options = buildDefaultRaceTrendCacheOptions(source, targetYmd);
  const { payload } = await buildRaceTrendRawPayloadForRace({ options, race, runners });
  const trendTargets = input.trendTargets ?? DEFAULT_RACE_TREND_TARGETS;
  const jockeySameVenue = input.jockeySameVenue ?? DEFAULT_SSR_JOCKEY_SAME_VENUE;
  return aggregateForTargets(
    {
      starterRows: payload.starterRows,
      currentRunningStyles: payload.currentRunningStyles,
      historicalRunningStyles: payload.historicalRunningStyles,
      raceContext: payload.raceContext,
      runners: payload.runners,
    },
    trendTargets,
    jockeySameVenue,
    options.jockeyStartYmd,
    options.jockeyEndYmd,
  );
};
