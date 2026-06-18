// Run with bun. Per-race coordinator for finish-position rescore scheduling.
//
// Mirrors the running-style per-race planner (sync-realtime-data
// running-style-cron.ts planRunningStylePredictionsForDate): a short-interval
// cron inspects realtime_race_sources.race_start_at_jst (JST ISO post-time) and
// enqueues a per-race "rescore" message for each race whose post time falls
// within the [now, now + T-X] window and that has not already been enqueued.
// This is the timing layer that lets each race be re-scored with the freshest
// bataiju/odds just before post (bataiju lands ~T-30..50min, odds drift until
// post). The per-race rescore consumer (task B) is what actually consumes the
// message; until it is wired this coordinator is shadow-safe (enqueueing a
// message has no production effect — see worker.ts gating).

import { claimRescoreRace } from "./do-state";
import type { Env, PredictCategory, PredictMode, PredictQueueMessage } from "./types";

// Default lead time: enqueue a race for rescore when its post time is within
// T-X minutes from now. 25 min sits after bataiju is announced (~T-30..50) and
// before odds finalise at post, so the rescore picks up fresh weight + odds.
export const DEFAULT_RESCORE_LEAD_MINUTES = 25;
// Shadow gate: the coordinator only enqueues when COORDINATOR_ENABLED === "1".
// Until task B (rescore consumer) is wired and this flag is flipped, the cron
// fires but enqueues nothing — deploying it does not change production
// predictions.
const COORDINATOR_ENABLED_FLAG = "1";
const RESCORE_MODE: PredictMode = "rescore";
const RESCORE_DAYS_AHEAD = 0;
const MS_PER_MINUTE = 60 * 1000;
const JST_OFFSET_HOURS = 9;
const JST_OFFSET_MS = JST_OFFSET_HOURS * 60 * 60 * 1000;
const ISO_DATE_LENGTH = 10;
const DATE_SEPARATOR = "-";
const KEIBAJO_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
// realtime_race_sources stores per-source rows; finish-position rescore targets
// the same jra/nar/ban-ei categories the predict pipeline produces. JRA/NAR map
// 1:1 to source; ban-ei rows live under the nar source with keibajo_code 65/83
// (帯広). Coordinator keys off the explicit category, so the SQL filters by the
// underlying source for that category.
const CATEGORY_SOURCES: Readonly<Record<PredictCategory, ReadonlyArray<string>>> = {
  "ban-ei": ["nar"],
  jra: ["jra"],
  nar: ["nar"],
};
const ALL_CATEGORIES: ReadonlyArray<PredictCategory> = ["jra", "nar", "ban-ei"];

interface RaceSourceRow {
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
}

interface RunYmdParts {
  nen: string;
  tsukihi: string;
}

export interface CoordinatorRaceTarget {
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
}

export interface RaceCoordinatorSummary {
  category: PredictCategory;
  date: string;
  enqueued: number;
  scanned: number;
  withinWindow: number;
  alreadyClaimed: number;
}

interface PlanRescoreParams {
  category: PredictCategory;
  // "YYYY-MM-DD" JST calendar date the coordinator is scanning.
  date: string;
  env: Env;
  leadMinutes: number;
  now: Date;
  // "YYYYMMDD" form of date — used for the realtime_race_sources lookup, the DO
  // dedup key, and the container /predict runDate param.
  runYmd: string;
}

interface BuildRescoreMessageParams {
  category: PredictCategory;
  date: string;
  runYmd: string;
  target: CoordinatorRaceTarget;
}

interface ClaimAndEnqueueParams {
  category: PredictCategory;
  date: string;
  env: Env;
  runYmd: string;
  target: CoordinatorRaceTarget;
}

interface RaceCoordinatorTickParams {
  env: Env;
  now: Date;
  leadMinutes: number;
}

const pad = (value: string, width: number): string => value.padStart(width, "0");

// "YYYYMMDD" (JST) -> the (kaisai_nen, kaisai_tsukihi) pair realtime_race_sources
// stores its rows under. date is already a JST calendar date string.
const splitRunYmd = (runYmd: string): RunYmdParts => ({
  nen: runYmd.slice(0, 4),
  tsukihi: runYmd.slice(4, 8),
});

// "YYYY-MM-DD" (JST) for a UTC instant, used to scope the coordinator to today.
export const formatRunDateJst = (now: Date): string =>
  new Date(now.getTime() + JST_OFFSET_MS).toISOString().slice(0, ISO_DATE_LENGTH);

// "YYYYMMDD" (JST) form used for DO dedup keys and the queue runYmd field.
export const formatRunYmdJst = (now: Date): string =>
  formatRunDateJst(now).split(DATE_SEPARATOR).join("");

const buildPlaceholders = (count: number): string =>
  Array.from({ length: count }, () => "?").join(", ");

const listRacesForCategory = async (
  env: Env,
  category: PredictCategory,
  runYmd: string,
): Promise<RaceSourceRow[]> => {
  const { nen, tsukihi } = splitRunYmd(runYmd);
  const sources = CATEGORY_SOURCES[category];
  const sql = `select keibajo_code, race_bango, race_start_at_jst
       from realtime_race_sources
      where source in (${buildPlaceholders(sources.length)})
        and kaisai_nen = ?
        and kaisai_tsukihi = ?
      order by race_start_at_jst, keibajo_code, race_bango`;
  const result = await env.REALTIME_DB.prepare(sql)
    .bind(...sources, nen, tsukihi)
    .all<RaceSourceRow>();
  return result.results;
};

// A race is within window when its post time is in [now, now + leadMinutes].
// Past-post races are excluded (already gone), as are races further out than
// the lead window (they get picked up by a later cron tick closer to post).
export const isWithinRescoreWindow = (
  raceStartAtJst: string,
  now: Date,
  leadMinutes: number,
): boolean => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) {
    return false;
  }
  const nowMs = now.getTime();
  const windowEndMs = nowMs + leadMinutes * MS_PER_MINUTE;
  return postMs >= nowMs && postMs <= windowEndMs;
};

export const selectRacesWithinWindow = (
  rows: ReadonlyArray<RaceSourceRow>,
  now: Date,
  leadMinutes: number,
): CoordinatorRaceTarget[] =>
  rows
    .filter((row) => isWithinRescoreWindow(row.race_start_at_jst, now, leadMinutes))
    .map((row) => ({
      keibajoCode: pad(row.keibajo_code, KEIBAJO_PAD_WIDTH),
      raceBango: pad(row.race_bango, RACE_BANGO_PAD_WIDTH),
      raceStartAtJst: row.race_start_at_jst,
    }));

const buildRescoreMessage = (params: BuildRescoreMessageParams): PredictQueueMessage => ({
  category: params.category,
  daysAhead: RESCORE_DAYS_AHEAD,
  keibajoCode: params.target.keibajoCode,
  mode: RESCORE_MODE,
  raceBango: params.target.raceBango,
  runDate: params.date,
  runDateIso: params.date,
  runYmd: params.runYmd,
});

// Claim a single race in the DO (strong consistency) and, when this is the first
// claim, enqueue its per-race rescore message. Returns true when enqueued.
const claimAndEnqueueRace = async (params: ClaimAndEnqueueParams): Promise<boolean> => {
  const claim = await claimRescoreRace({
    category: params.category,
    env: params.env,
    keibajoCode: params.target.keibajoCode,
    raceBango: params.target.raceBango,
    runYmd: params.runYmd,
  });
  if (!claim.proceed) {
    return false;
  }
  await params.env.PREDICT_QUEUE.send(
    buildRescoreMessage({
      category: params.category,
      date: params.date,
      runYmd: params.runYmd,
      target: params.target,
    }),
  );
  return true;
};

export const planRescoreForCategory = async (
  params: PlanRescoreParams,
): Promise<RaceCoordinatorSummary> => {
  const rows = await listRacesForCategory(params.env, params.category, params.runYmd);
  const targets = selectRacesWithinWindow(rows, params.now, params.leadMinutes);
  const claimResults = await Promise.all(
    targets.map((target) =>
      claimAndEnqueueRace({
        category: params.category,
        date: params.date,
        env: params.env,
        runYmd: params.runYmd,
        target,
      }),
    ),
  );
  const enqueued = claimResults.filter((proceeded) => proceeded).length;
  return {
    alreadyClaimed: targets.length - enqueued,
    category: params.category,
    date: params.date,
    enqueued,
    scanned: rows.length,
    withinWindow: targets.length,
  };
};

const buildShadowSummary = (category: PredictCategory, date: string): RaceCoordinatorSummary => ({
  alreadyClaimed: 0,
  category,
  date,
  enqueued: 0,
  scanned: 0,
  withinWindow: 0,
});

export const isCoordinatorEnabled = (env: Env): boolean =>
  env.COORDINATOR_ENABLED === COORDINATOR_ENABLED_FLAG;

export const runRaceCoordinatorTick = async (
  params: RaceCoordinatorTickParams,
): Promise<RaceCoordinatorSummary[]> => {
  const date = formatRunDateJst(params.now);
  // Shadow gate: when disabled, skip the D1 read + enqueue entirely so the cron
  // is a no-op for production. Reports empty per-category summaries.
  if (!isCoordinatorEnabled(params.env)) {
    return ALL_CATEGORIES.map((category) => buildShadowSummary(category, date));
  }
  const runYmd = formatRunYmdJst(params.now);
  return Promise.all(
    ALL_CATEGORIES.map((category) =>
      planRescoreForCategory({
        category,
        date,
        env: params.env,
        leadMinutes: params.leadMinutes,
        now: params.now,
        runYmd,
      }),
    ),
  );
};
