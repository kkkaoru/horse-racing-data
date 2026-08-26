// Run with bun. Per-race coverage self-healing + pre-race readiness cron
// (doc §4.3, docs/cf-only-serving-architecture.md).
//
// Post-race path (unchanged contract): for every race whose post time is more
// than a grace window in the past, check the same Neon completion query the
// queue consumer already trusts (isFocusedFullPredictionComplete); for any
// race still incomplete, enqueue through the shared producer reservation. The
// producer sends only when no fresh focused-full lineage exists; a race with a
// fresh in-flight reservation/claim is left alone.
//
// Pre-race readiness path (PRE_RACE_READY): the post-race path cannot fix
// user-visible gaps before post by design (isPastGraceWindow). Production
// evidence (2026-07-21 NAR) showed mode=full rows generated after post or
// missing entirely when the primary RS→finish-position trigger lagged. This
// module also scans races with race_start_at_jst in
// (now, now + PRE_RACE_LEAD_MINUTES], enqueues incomplete ones as the same
// mode=full skipDedup focused-full message, prioritised by earliest post
// first, with a per-tick enqueue cap and a per-race pre-race budget that does
// not burn the post-race MAX_SELF_HEAL budget (counted via recorded_at vs
// race start -- no D1 migration).

import { isFocusedFullPredictionComplete } from "./focused-full-completion";
import { isOldDateRunYmd } from "./old-date-guard";
import { warmNeon } from "./neon-warm";
import { enqueuePredict } from "./queue-producer";
import { getRunDateJst, getRunYmdJst } from "./time";
import type { Env, PredictCategory, PredictMode } from "./types";
import {
  buildCoverageGapEventBindParams,
  buildCoverageGapEventInsertSql,
  buildCoverageGapEventRecord,
} from "./coverage-gap-events";

// Every 15 min during JST 10:00-23:59 (01-14 UTC), offset by 7 minutes from
// both the existing */10 per-race rescore coordinator and the */30 Neon
// warm cron so no two race-hours crons ever fire on the same tick (mirrors
// the offset reasoning already used for COORDINATOR_CRON_RACE_HOURS in
// cron-decision.ts). Same tick also runs the pre-race readiness scan.
export const COVERAGE_SELF_HEAL_CRON = "7,22,37,52 1-14 * * *";
const SELF_HEAL_GRACE_MINUTES = 15;
// Mirrors WEIGHT_FETCH_LEAD_MINUTES in sync-realtime-data (180): open the
// pre-race readiness window early enough that a worst-case serialised
// focused-full pipeline (~13-27 min/race, single container slot per category)
// still has room to finish before post for the earliest races in the window.
export const PRE_RACE_LEAD_MINUTES = 180;
// Keep the guard well above the maximum number of domestic races that can be
// in one 180-minute window. The old value (16) silently left later races out
// of the pre-race window until the next tick; for a 15:35 race that meant the
// prediction could start after post. Queue/DO lane claims already provide the
// real concurrency bound, so this is only a last-resort corruption guard.
export const PRE_RACE_ENQUEUE_CAP_PER_TICK = 256;
const MS_PER_MINUTE = 60 * 1000;
// Bounded lower than DLQ's MAX_DLQ_REDRIVES=1 escalation philosophy would
// suggest at first glance, but self-heal covers a broader failure class than
// the DLQ consumer (a genuine "never enqueued at all" discovery gap needs
// exactly one successful trigger) -- 2 re-triggers gives one retry beyond the
// first before treating repeated failure as a poison-pill race that needs a
// human, not more blind re-triggering. Counts only post-race enqueues (see
// countPriorEnqueues) so pre-race attempts do not exhaust this budget.
const MAX_SELF_HEAL_ENQUEUES_PER_RACE = 2;
// Separate pre-race budget: two attempts inside the lead window before
// PRE_RACE_READY escalate. Independent of MAX_SELF_HEAL so a race that was
// retried pre-race still gets post-grace heal attempts.
export const MAX_PRE_RACE_ENQUEUES_PER_RACE = 2;
// After the pre-race budget is exhausted, keep retrying incomplete races that
// are still before post -- 2026-08-09 Ban-ei 83/06-12 were left at 0 rows once
// PRE_RACE_READY_ESCALATE fired at prior=2. Interval is 2x the 15-min tick so
// a dead container is not stampeded every tick, while a 180-min lead window
// still gets several more attempts before post.
export const PRE_RACE_ESCALATED_RETRY_MINUTES = 30;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_LENGTH = 8;
const KEIBAJO_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
const PAD_CHAR = "0";
const JRA_SOURCE = "jra";
const BAN_EI_KEIBAJO_CODE = "83";
const JRA_CATEGORY: PredictCategory = "jra";
const NAR_CATEGORY: PredictCategory = "nar";
const BAN_EI_CATEGORY: PredictCategory = "ban-ei";
const FULL_MODE: PredictMode = "full";
const ISO_UTC_SECONDS_LENGTH = 19;
const LIST_RACE_SOURCES_SQL = `select source, keibajo_code, race_bango, race_start_at_jst
   from realtime_race_sources
  where kaisai_nen = ?1 and kaisai_tsukihi = ?2
  order by race_start_at_jst, keibajo_code, race_bango`;
// Phase-scoped prior-enqueue counts: D1 recorded_at is UTC "YYYY-MM-DD HH:MM:SS"
// (datetime('now')). Compare against the race post instant reformatted the same
// way so pre-race rows (recorded before post) and post-race rows (recorded at
// or after post) keep independent budgets without a schema migration.
const COUNT_PRIOR_ENQUEUES_BEFORE_POST_SQL = `select count(*) as count from finish_position_coverage_gap_events
  where run_ymd = ?1 and category = ?2 and keibajo_code = ?3 and race_bango = ?4 and enqueued = 1
    and recorded_at < ?5`;
const COUNT_PRIOR_ENQUEUES_ON_OR_AFTER_POST_SQL = `select count(*) as count from finish_position_coverage_gap_events
  where run_ymd = ?1 and category = ?2 and keibajo_code = ?3 and race_bango = ?4 and enqueued = 1
    and recorded_at >= ?5`;
const SELECT_LAST_PRE_RACE_ENQUEUE_AT_SQL = `select max(recorded_at) as last_enqueued_at from finish_position_coverage_gap_events
  where run_ymd = ?1 and category = ?2 and keibajo_code = ?3 and race_bango = ?4 and enqueued = 1
    and recorded_at < ?5`;

type HealPhase = "pre-race" | "post-race";

interface RaceSourceRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
}

interface CountRow {
  count: number;
}

interface LastEnqueueRow {
  last_enqueued_at: string | null;
}

export interface GapCandidate {
  category: PredictCategory;
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
  phase: HealPhase;
}

export interface CoverageSelfHealSummary {
  scanned: number;
  candidates: number;
  alreadyComplete: number;
  alreadyInFlight: number;
  enqueued: number;
  escalated: number;
  errors: number;
  capped: number;
  preRaceCandidates: number;
  preRaceEnqueued: number;
  postRaceCandidates: number;
}

type HealOutcome = "capped" | "complete" | "enqueued" | "error" | "escalated" | "in-flight";

interface RunCoverageSelfHealParams {
  env: Env;
  now: Date;
}

interface HealCandidateParams {
  candidate: GapCandidate;
  env: Env;
  now: Date;
  // Mutable tick-local counter for the pre-race per-tick enqueue cap. Shared
  // across sequential pre-race heals in the same runCoverageSelfHeal call.
  preRaceEnqueuedThisTick: { count: number };
  runDate: string;
  runYmd: string;
}

interface CountPriorEnqueuesParams {
  candidate: GapCandidate;
  env: Env;
  runYmd: string;
}

interface RecordCoverageGapEventParams {
  candidate: GapCandidate;
  enqueued: boolean;
  env: Env;
  escalated: boolean;
  priorEnqueueCount: number;
  runYmd: string;
}

const EMPTY_SUMMARY: CoverageSelfHealSummary = {
  alreadyComplete: 0,
  alreadyInFlight: 0,
  capped: 0,
  candidates: 0,
  enqueued: 0,
  errors: 0,
  escalated: 0,
  postRaceCandidates: 0,
  preRaceCandidates: 0,
  preRaceEnqueued: 0,
  scanned: 0,
};

// Exhaustive outcome -> summary-key dispatch (Record, not a Map, so
// TypeScript enforces every HealOutcome has an entry at compile time and
// indexing never yields undefined -- mirrors CATEGORY_RACE_FILTERS in
// race-coordinator.ts).
const OUTCOME_SUMMARY_KEYS: Readonly<Record<HealOutcome, keyof CoverageSelfHealSummary>> = {
  capped: "capped",
  complete: "alreadyComplete",
  enqueued: "enqueued",
  error: "errors",
  escalated: "escalated",
  "in-flight": "alreadyInFlight",
};

// Only the configured cron triggers a self-heal / pre-race readiness scan.
export const shouldRunCoverageSelfHealCron = (cron: string): boolean =>
  cron === COVERAGE_SELF_HEAL_CRON;

const pad = (value: string, width: number): string => value.padStart(width, PAD_CHAR);

// jra source -> jra; otherwise keibajo 83 (帯広) is ban-ei and every other
// nar-source keibajo is plain nar. Duplicated from cron-decision.ts's
// resolveRaceCategory (not imported) per the same locked-file avoidance
// reasoning as the other scheduling constants above.
const resolveRaceCategory = (source: string, keibajoCode: string): PredictCategory => {
  if (source === JRA_SOURCE) return JRA_CATEGORY;
  if (keibajoCode === BAN_EI_KEIBAJO_CODE) return BAN_EI_CATEGORY;
  return NAR_CATEGORY;
};

const listTodaysRaceSources = async (db: D1Database, runYmd: string): Promise<RaceSourceRow[]> => {
  const nen = runYmd.slice(0, RUN_YMD_YEAR_END);
  const tsukihi = runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH);
  const result = await db.prepare(LIST_RACE_SOURCES_SQL).bind(nen, tsukihi).all<RaceSourceRow>();
  return result.results;
};

// D1 datetime('now') stores UTC as "YYYY-MM-DD HH:MM:SS". Match that shape so
// lexicographic compare against recorded_at is a true time compare.
const raceStartAtJstToD1Utc = (raceStartAtJst: string): string | null => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) return null;
  return new Date(postMs).toISOString().slice(0, ISO_UTC_SECONDS_LENGTH).replace("T", " ");
};

// A race is a post-race self-heal candidate once its post time is more than
// SELF_HEAL_GRACE_MINUTES in the past -- a genuinely on-schedule pipeline
// (even a worst-case ~27.5 min JRA full build) should be long done by then if
// it started anywhere near post, and a race still inside the grace window is
// left to the normal dispatch path (and/or the pre-race readiness path if it
// was incomplete going into post).
export const isPastGraceWindow = (raceStartAtJst: string, now: Date): boolean => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) return false;
  return postMs <= now.getTime() - SELF_HEAL_GRACE_MINUTES * MS_PER_MINUTE;
};

// Upcoming incomplete races in (now, now + PRE_RACE_LEAD_MINUTES]: exclusive of
// now (already-posted races are either in grace or post-heal), inclusive of the
// lead boundary so a race exactly PRE_RACE_LEAD_MINUTES out is scanned.
export const isWithinPreRaceLeadWindow = (raceStartAtJst: string, now: Date): boolean => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) return false;
  const deltaMs = postMs - now.getTime();
  return deltaMs > 0 && deltaMs <= PRE_RACE_LEAD_MINUTES * MS_PER_MINUTE;
};

// D1 datetime('now') is UTC "YYYY-MM-DD HH:MM:SS" with no timezone suffix.
export const isEscalatedRetryDue = (
  lastEnqueuedAtUtc: string | null | undefined,
  now: Date,
  intervalMinutes: number,
): boolean => {
  if (lastEnqueuedAtUtc === undefined || lastEnqueuedAtUtc === null || lastEnqueuedAtUtc === "") {
    return true;
  }
  const lastMs = Date.parse(`${lastEnqueuedAtUtc.replace(" ", "T")}Z`);
  if (Number.isNaN(lastMs)) return true;
  return now.getTime() - lastMs >= intervalMinutes * MS_PER_MINUTE;
};

const rowToCandidate = (row: RaceSourceRow, phase: HealPhase): GapCandidate => {
  const keibajoCode = pad(row.keibajo_code, KEIBAJO_PAD_WIDTH);
  return {
    category: resolveRaceCategory(row.source, keibajoCode),
    keibajoCode,
    phase,
    raceBango: pad(row.race_bango, RACE_BANGO_PAD_WIDTH),
    raceStartAtJst: row.race_start_at_jst,
  };
};

// Earliest post first so the races closest to (or already past) post time get
// heal / readiness attempts before later ones when a tick is capacity-bound.
const sortByRaceStartAscending = (candidates: GapCandidate[]): GapCandidate[] =>
  [...candidates].sort((a, b) => {
    // From 17:00 JST onward, prioritize the late-card races as a group, then
    // preserve strict post-time order inside that group. This prevents the
    // earlier broad window from consuming the enqueue budget before the
    // evening races have received their pre-weight prediction.
    const aHour = Number.parseInt(a.raceStartAtJst.slice(11, 13), 10);
    const bHour = Number.parseInt(b.raceStartAtJst.slice(11, 13), 10);
    const aLate = Number.isFinite(aHour) && aHour >= 17;
    const bLate = Number.isFinite(bHour) && bHour >= 17;
    if (aLate !== bLate) return aLate ? -1 : 1;
    const aMs = Date.parse(a.raceStartAtJst);
    const bMs = Date.parse(b.raceStartAtJst);
    if (aMs !== bMs) return aMs - bMs;
    const keibajoCmp = a.keibajoCode.localeCompare(b.keibajoCode);
    if (keibajoCmp !== 0) return keibajoCmp;
    return a.raceBango.localeCompare(b.raceBango);
  });

export const buildPostRaceGapCandidates = (
  rows: readonly RaceSourceRow[],
  now: Date,
): GapCandidate[] =>
  sortByRaceStartAscending(
    rows
      .filter((row) => isPastGraceWindow(row.race_start_at_jst, now))
      .map((row) => rowToCandidate(row, "post-race")),
  );

export const buildPreRaceGapCandidates = (
  rows: readonly RaceSourceRow[],
  now: Date,
): GapCandidate[] =>
  sortByRaceStartAscending(
    rows
      .filter((row) => isWithinPreRaceLeadWindow(row.race_start_at_jst, now))
      .map((row) => rowToCandidate(row, "pre-race")),
  );

const countPriorEnqueues = async (params: CountPriorEnqueuesParams): Promise<number> => {
  const { candidate, env, runYmd } = params;
  const postUtc = raceStartAtJstToD1Utc(candidate.raceStartAtJst);
  if (postUtc === null) return 0;
  const sql =
    candidate.phase === "pre-race"
      ? COUNT_PRIOR_ENQUEUES_BEFORE_POST_SQL
      : COUNT_PRIOR_ENQUEUES_ON_OR_AFTER_POST_SQL;
  const row = await env.FINISH_POSITION_CRON_DB.prepare(sql)
    .bind(runYmd, candidate.category, candidate.keibajoCode, candidate.raceBango, postUtc)
    .first<CountRow>();
  return row?.count ?? 0;
};

const maxEnqueuesForPhase = (phase: HealPhase): number =>
  phase === "pre-race" ? MAX_PRE_RACE_ENQUEUES_PER_RACE : MAX_SELF_HEAL_ENQUEUES_PER_RACE;

const recordCoverageGapEvent = async (params: RecordCoverageGapEventParams): Promise<void> => {
  const { candidate, enqueued, env, escalated, priorEnqueueCount, runYmd } = params;
  const record = buildCoverageGapEventRecord({
    category: candidate.category,
    enqueued,
    escalated,
    keibajoCode: candidate.keibajoCode,
    priorEnqueueCount,
    raceBango: candidate.raceBango,
    raceStartAtJst: candidate.raceStartAtJst,
    runYmd,
  });
  await env.FINISH_POSITION_CRON_DB.prepare(buildCoverageGapEventInsertSql())
    .bind(...buildCoverageGapEventBindParams(record))
    .run();
};

const describeCandidate = (candidate: GapCandidate, runYmd: string): string =>
  `category=${candidate.category} runYmd=${runYmd} keibajo=${candidate.keibajoCode} race=${candidate.raceBango}`;

const escalateCandidate = async (
  params: HealCandidateParams,
  priorEnqueueCount: number,
): Promise<HealOutcome> => {
  const { candidate, env, runYmd } = params;
  await recordCoverageGapEvent({
    candidate,
    enqueued: false,
    env,
    escalated: true,
    priorEnqueueCount,
    runYmd,
  });
  if (candidate.phase === "pre-race") {
    console.error(
      `PRE_RACE_READY_ESCALATE pre_race=1 ${describeCandidate(candidate, runYmd)} priorEnqueueCount=${priorEnqueueCount}`,
    );
  } else {
    console.error(
      `SELF_HEAL_ESCALATE ${describeCandidate(candidate, runYmd)} priorEnqueueCount=${priorEnqueueCount}`,
    );
  }
  return "escalated";
};

const enqueueGapFill = async (
  params: HealCandidateParams,
  priorEnqueueCount: number,
  options?: { escalated?: boolean },
): Promise<HealOutcome> => {
  const { candidate, env, runDate, runYmd } = params;
  const escalated = options?.escalated === true;
  // Wake Neon immediately before the pre-weight focused-full message is
  // handed to Queue. This is best-effort and never blocks a valid enqueue;
  // Worker warm cannot load the Python model or Container day-base itself.
  if (env.NEON_DATABASE_URL !== undefined) {
    await warmNeon(env.NEON_DATABASE_URL);
  }
  const enqueuedCategories = await enqueuePredict({
    category: candidate.category,
    daysAhead: Number(env.PREDICT_DAYS_AHEAD),
    deliveryTrackingId: crypto.randomUUID(),
    env,
    keibajoCode: candidate.keibajoCode,
    mode: FULL_MODE,
    raceBango: candidate.raceBango,
    raceStartAtJst: candidate.raceStartAtJst,
    runDate,
    runYmd,
    skipDedup: true,
  });
  if (enqueuedCategories.length === 0) return "in-flight";
  await recordCoverageGapEvent({
    candidate,
    enqueued: true,
    env,
    escalated,
    priorEnqueueCount,
    runYmd,
  });
  if (candidate.phase === "pre-race") {
    params.preRaceEnqueuedThisTick.count += 1;
    if (escalated) {
      console.warn(
        `[coverage-self-heal] PRE_RACE_READY_ESCALATED_RETRY pre_race=1 ${describeCandidate(candidate, runYmd)} priorEnqueueCount=${priorEnqueueCount}`,
      );
    } else {
      console.warn(
        `[coverage-self-heal] PRE_RACE_READY enqueued gap-fill pre_race=1 ${describeCandidate(candidate, runYmd)} priorEnqueueCount=${priorEnqueueCount}`,
      );
    }
  } else {
    console.warn(
      `[coverage-self-heal] enqueued gap-fill ${describeCandidate(candidate, runYmd)} priorEnqueueCount=${priorEnqueueCount}`,
    );
  }
  return "enqueued";
};

const readLastPreRaceEnqueueAt = async (params: HealCandidateParams): Promise<string | null> => {
  const { candidate, env, runYmd } = params;
  const postUtc = raceStartAtJstToD1Utc(candidate.raceStartAtJst);
  if (postUtc === null) return null;
  const row = await env.FINISH_POSITION_CRON_DB.prepare(SELECT_LAST_PRE_RACE_ENQUEUE_AT_SQL)
    .bind(runYmd, candidate.category, candidate.keibajoCode, candidate.raceBango, postUtc)
    .first<LastEnqueueRow>();
  return row?.last_enqueued_at ?? null;
};

// Never throws: a per-race failure here (Neon, D1, DO, or Queue) must not
// block the rest of this tick's candidates, mirroring day-base-prewarm.ts's
// per-category isolation.
const healCandidate = async (params: HealCandidateParams): Promise<HealOutcome> => {
  const { candidate, env, now, preRaceEnqueuedThisTick, runYmd } = params;
  try {
    const complete = await isFocusedFullPredictionComplete({
      category: candidate.category,
      env,
      keibajoCode: candidate.keibajoCode,
      raceBango: candidate.raceBango,
      runYmd,
    });
    if (complete) return "complete";
    const priorEnqueueCount = await countPriorEnqueues({ candidate, env, runYmd });
    const overBudget = priorEnqueueCount >= maxEnqueuesForPhase(candidate.phase);
    if (overBudget && candidate.phase !== "pre-race") {
      return await escalateCandidate(params, priorEnqueueCount);
    }
    if (overBudget && candidate.phase === "pre-race") {
      const lastEnqueuedAt = await readLastPreRaceEnqueueAt(params);
      if (!isEscalatedRetryDue(lastEnqueuedAt, now, PRE_RACE_ESCALATED_RETRY_MINUTES)) {
        return await escalateCandidate(params, priorEnqueueCount);
      }
    }
    if (
      candidate.phase === "pre-race" &&
      preRaceEnqueuedThisTick.count >= PRE_RACE_ENQUEUE_CAP_PER_TICK
    ) {
      console.warn(
        `[coverage-self-heal] PRE_RACE_READY capped pre_race=1 ${describeCandidate(candidate, runYmd)} tickEnqueued=${preRaceEnqueuedThisTick.count}`,
      );
      return "capped";
    }
    return await enqueueGapFill(params, priorEnqueueCount, { escalated: overBudget });
  } catch (err) {
    const prefix =
      candidate.phase === "pre-race"
        ? `[coverage-self-heal] PRE_RACE_READY failed to heal pre_race=1`
        : `[coverage-self-heal] failed to heal`;
    console.error(`${prefix} ${describeCandidate(candidate, runYmd)}:`, String(err));
    return "error";
  }
};

const summarizeOutcomes = (
  scanned: number,
  preRaceCandidates: number,
  postRaceCandidates: number,
  outcomes: readonly HealOutcome[],
  preRaceEnqueued: number,
): CoverageSelfHealSummary =>
  outcomes.reduce(
    (acc, outcome) => {
      const key = OUTCOME_SUMMARY_KEYS[outcome];
      return { ...acc, [key]: acc[key] + 1 };
    },
    {
      ...EMPTY_SUMMARY,
      candidates: preRaceCandidates + postRaceCandidates,
      postRaceCandidates,
      preRaceCandidates,
      preRaceEnqueued,
      scanned,
    },
  );

const logTickSummary = (runYmd: string, summary: CoverageSelfHealSummary): void => {
  console.log(
    `[coverage-self-heal] tick runYmd=${runYmd} scanned=${summary.scanned} candidates=${summary.candidates} preRaceCandidates=${summary.preRaceCandidates} postRaceCandidates=${summary.postRaceCandidates} enqueued=${summary.enqueued} preRaceEnqueued=${summary.preRaceEnqueued} escalated=${summary.escalated} alreadyInFlight=${summary.alreadyInFlight} alreadyComplete=${summary.alreadyComplete} capped=${summary.capped} errors=${summary.errors}`,
  );
};

// Heal post-race candidates in parallel (existing behaviour -- typically few
// past-grace incomplete races). Heal pre-race candidates sequentially in
// race_start ascending order so the per-tick enqueue cap is deterministic and
// earliest posts are preferred.
const healAllCandidates = async (params: {
  env: Env;
  now: Date;
  postRaceCandidates: readonly GapCandidate[];
  preRaceCandidates: readonly GapCandidate[];
  runDate: string;
  runYmd: string;
}): Promise<{ outcomes: HealOutcome[]; preRaceEnqueued: number }> => {
  const { env, now, postRaceCandidates, preRaceCandidates, runDate, runYmd } = params;
  const preRaceEnqueuedThisTick = { count: 0 };
  const postOutcomes = await Promise.all(
    postRaceCandidates.map((candidate) =>
      healCandidate({ candidate, env, now, preRaceEnqueuedThisTick, runDate, runYmd }),
    ),
  );
  const preOutcomes: HealOutcome[] = [];
  for (const candidate of preRaceCandidates) {
    preOutcomes.push(
      await healCandidate({ candidate, env, now, preRaceEnqueuedThisTick, runDate, runYmd }),
    );
  }
  return {
    outcomes: [...postOutcomes, ...preOutcomes],
    preRaceEnqueued: preRaceEnqueuedThisTick.count,
  };
};

// Entry point: scan today's races for pre-race readiness gaps and post-race
// coverage gaps, re-triggering the ones that need it. Scoped to a single JST
// calendar day (the day the cron fires on).
export const runCoverageSelfHeal = async (
  params: RunCoverageSelfHealParams,
): Promise<CoverageSelfHealSummary> => {
  const { env, now } = params;
  const runYmd = getRunYmdJst(now);
  if (isOldDateRunYmd(runYmd, now)) {
    console.warn(`[coverage-self-heal] runYmd=${runYmd} unexpectedly old -- skipping scan`);
    return { ...EMPTY_SUMMARY };
  }
  const runDate = getRunDateJst(now);
  const rows = await listTodaysRaceSources(env.REALTIME_DB, runYmd);
  const postRaceCandidates = buildPostRaceGapCandidates(rows, now);
  const preRaceCandidates = buildPreRaceGapCandidates(rows, now);
  if (postRaceCandidates.length === 0 && preRaceCandidates.length === 0) {
    return {
      ...EMPTY_SUMMARY,
      scanned: rows.length,
    };
  }
  const { outcomes, preRaceEnqueued } = await healAllCandidates({
    env,
    now,
    postRaceCandidates,
    preRaceCandidates,
    runDate,
    runYmd,
  });
  const summary = summarizeOutcomes(
    rows.length,
    preRaceCandidates.length,
    postRaceCandidates.length,
    outcomes,
    preRaceEnqueued,
  );
  logTickSummary(runYmd, summary);
  return summary;
};
