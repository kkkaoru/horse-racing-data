// Run with bun. Per-race coverage self-healing cron (doc §4.3,
// docs/cf-only-serving-architecture.md). Replaces race-prediction-guard.sh's
// day-wide COUNT check -- which could not distinguish "36 races done, 0
// pending" from "34 races done, 2 genuinely still in flight" and periodically
// escalated against races that were not actually stuck -- with a per-race
// scan (feedback_production_per_race_granularity): for every race whose post
// time is more than a grace window in the past, check the same Neon
// completion query the queue consumer already trusts
// (isFocusedFullPredictionComplete); for any race still incomplete, check the
// focused-full DO claim before touching anything, and only re-enqueue a fresh
// skipDedup focused-full message for a race with no claim (a
// coordinator/discovery gap) or a stale/terminal-error claim (already
// exhausted the normal retry/DLQ recovery paths). A race with a fresh
// in-flight claim is left alone -- the coordinator's own redelivery loop or
// the DLQ consumer (dlq-consumer.ts) will handle it if it truly dies.
//
// Not wired into worker.ts/wrangler.jsonc yet -- see the hookup diff in this
// change's commit message / handoff report.

import { claimFocusedFullRace } from "./do-state";
import { isFocusedFullPredictionComplete } from "./focused-full-completion";
import { isOldDateRunYmd } from "./old-date-guard";
import { enqueuePredict } from "./queue-producer";
import { getRunDateJst, getRunYmdJst } from "./time";
import type { Env, PredictCategory, PredictMode } from "./types";
import {
  buildCoverageGapEventBindParams,
  buildCoverageGapEventInsertSql,
  buildCoverageGapEventRecord,
} from "./coverage-gap-events";

// Every 15 min during JST 10:00-20:59 (01-11 UTC), offset by 7 minutes from
// both the existing */10 per-race rescore coordinator and the */30 Neon
// warm cron so no two race-hours crons ever fire on the same tick (mirrors
// the offset reasoning already used for COORDINATOR_CRON_RACE_HOURS in
// cron-decision.ts).
export const COVERAGE_SELF_HEAL_CRON = "7,22,37,52 1-11 * * *";
const SELF_HEAL_GRACE_MINUTES = 15;
const MS_PER_MINUTE = 60 * 1000;
// Bounded lower than DLQ's MAX_DLQ_REDRIVES=1 escalation philosophy would
// suggest at first glance, but self-heal covers a broader failure class than
// the DLQ consumer (a genuine "never enqueued at all" discovery gap needs
// exactly one successful trigger) -- 2 re-triggers gives one retry beyond the
// first before treating repeated failure as a poison-pill race that needs a
// human, not more blind re-triggering.
const MAX_SELF_HEAL_ENQUEUES_PER_RACE = 2;
// Mirrors FOCUSED_FULL_IN_FLIGHT_STALE_MS in queue-consumer.ts (duplicated,
// not imported, matching the same duplication precedent day-base-prewarm.ts
// already established for PREDICT_CONTAINER_NAME_PREFIX -- avoids coupling
// this module to a file locked for concurrent edits by another agent this
// session). Keep in sync if that value ever changes.
const SELF_HEAL_FOCUSED_FULL_STALE_MS = 15 * 60 * 1000;
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
const LIST_RACE_SOURCES_SQL = `select source, keibajo_code, race_bango, race_start_at_jst
   from realtime_race_sources
  where kaisai_nen = ?1 and kaisai_tsukihi = ?2
  order by race_start_at_jst, keibajo_code, race_bango`;
const COUNT_PRIOR_ENQUEUES_SQL = `select count(*) as count from finish_position_coverage_gap_events
  where run_ymd = ?1 and category = ?2 and keibajo_code = ?3 and race_bango = ?4 and enqueued = 1`;

interface RaceSourceRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
}

interface CountRow {
  count: number;
}

export interface GapCandidate {
  category: PredictCategory;
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
}

export interface CoverageSelfHealSummary {
  scanned: number;
  candidates: number;
  alreadyComplete: number;
  alreadyInFlight: number;
  enqueued: number;
  escalated: number;
  errors: number;
}

type HealOutcome = "complete" | "enqueued" | "error" | "escalated" | "in-flight";

interface RunCoverageSelfHealParams {
  env: Env;
  now: Date;
}

interface HealCandidateParams {
  candidate: GapCandidate;
  env: Env;
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
  candidates: 0,
  enqueued: 0,
  errors: 0,
  escalated: 0,
  scanned: 0,
};

// Exhaustive outcome -> summary-key dispatch (Record, not a Map, so
// TypeScript enforces every HealOutcome has an entry at compile time and
// indexing never yields undefined -- mirrors CATEGORY_RACE_FILTERS in
// race-coordinator.ts).
const OUTCOME_SUMMARY_KEYS: Readonly<Record<HealOutcome, keyof CoverageSelfHealSummary>> = {
  complete: "alreadyComplete",
  enqueued: "enqueued",
  error: "errors",
  escalated: "escalated",
  "in-flight": "alreadyInFlight",
};

// Only the configured cron triggers a self-heal scan.
export const shouldRunCoverageSelfHealCron = (cron: string): boolean =>
  cron === COVERAGE_SELF_HEAL_CRON;

const pad = (value: string, width: number): string => value.padStart(width, PAD_CHAR);

// jra source -> jra; otherwise keibajo 83 (帯広) is ban-ei and every other
// nar-source keibajo is plain nar. Duplicated from cron-decision.ts's
// resolveRaceCategory (not imported) per the same locked-file avoidance
// reasoning as SELF_HEAL_FOCUSED_FULL_STALE_MS above.
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

// A race is a self-heal candidate once its post time is more than
// SELF_HEAL_GRACE_MINUTES in the past -- a genuinely on-schedule pipeline
// (even a worst-case ~27.5 min JRA full build) should be long done by then if
// it started anywhere near post, and a race whose post time has not yet
// arrived or is still inside the grace window is left to the normal
// pre-race dispatch path.
const isPastGraceWindow = (raceStartAtJst: string, now: Date): boolean => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) return false;
  return postMs <= now.getTime() - SELF_HEAL_GRACE_MINUTES * MS_PER_MINUTE;
};

const buildGapCandidates = (rows: readonly RaceSourceRow[], now: Date): GapCandidate[] =>
  rows
    .filter((row) => isPastGraceWindow(row.race_start_at_jst, now))
    .map((row) => {
      const keibajoCode = pad(row.keibajo_code, KEIBAJO_PAD_WIDTH);
      return {
        category: resolveRaceCategory(row.source, keibajoCode),
        keibajoCode,
        raceBango: pad(row.race_bango, RACE_BANGO_PAD_WIDTH),
        raceStartAtJst: row.race_start_at_jst,
      };
    });

const countPriorSelfHealEnqueues = async (params: CountPriorEnqueuesParams): Promise<number> => {
  const { candidate, env, runYmd } = params;
  const row = await env.FINISH_POSITION_CRON_DB.prepare(COUNT_PRIOR_ENQUEUES_SQL)
    .bind(runYmd, candidate.category, candidate.keibajoCode, candidate.raceBango)
    .first<CountRow>();
  return row?.count ?? 0;
};

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
  console.error(
    `SELF_HEAL_ESCALATE ${describeCandidate(candidate, runYmd)} priorEnqueueCount=${priorEnqueueCount}`,
  );
  return "escalated";
};

const enqueueGapFill = async (
  params: HealCandidateParams,
  priorEnqueueCount: number,
): Promise<HealOutcome> => {
  const { candidate, env, runDate, runYmd } = params;
  await enqueuePredict({
    category: candidate.category,
    daysAhead: Number(env.PREDICT_DAYS_AHEAD),
    env,
    keibajoCode: candidate.keibajoCode,
    mode: FULL_MODE,
    raceBango: candidate.raceBango,
    runDate,
    runYmd,
    skipDedup: true,
  });
  await recordCoverageGapEvent({
    candidate,
    enqueued: true,
    env,
    escalated: false,
    priorEnqueueCount,
    runYmd,
  });
  console.warn(
    `[coverage-self-heal] enqueued gap-fill ${describeCandidate(candidate, runYmd)} priorEnqueueCount=${priorEnqueueCount}`,
  );
  return "enqueued";
};

// Never throws: a per-race failure here (Neon, D1, DO, or Queue) must not
// block the rest of this tick's candidates, mirroring day-base-prewarm.ts's
// per-category isolation.
const healCandidate = async (params: HealCandidateParams): Promise<HealOutcome> => {
  const { candidate, env, runYmd } = params;
  try {
    const complete = await isFocusedFullPredictionComplete({
      category: candidate.category,
      env,
      keibajoCode: candidate.keibajoCode,
      raceBango: candidate.raceBango,
      runYmd,
    });
    if (complete) return "complete";
    const priorEnqueueCount = await countPriorSelfHealEnqueues({ candidate, env, runYmd });
    if (priorEnqueueCount >= MAX_SELF_HEAL_ENQUEUES_PER_RACE) {
      return await escalateCandidate(params, priorEnqueueCount);
    }
    const claim = await claimFocusedFullRace({
      category: candidate.category,
      env,
      keibajoCode: candidate.keibajoCode,
      raceBango: candidate.raceBango,
      runYmd,
      staleAfterMs: SELF_HEAL_FOCUSED_FULL_STALE_MS,
    });
    if (!claim.proceed) return "in-flight";
    return await enqueueGapFill(params, priorEnqueueCount);
  } catch (err) {
    console.error(
      `[coverage-self-heal] failed to heal ${describeCandidate(candidate, runYmd)}:`,
      String(err),
    );
    return "error";
  }
};

const summarizeOutcomes = (
  scanned: number,
  candidateCount: number,
  outcomes: readonly HealOutcome[],
): CoverageSelfHealSummary =>
  outcomes.reduce(
    (acc, outcome) => {
      const key = OUTCOME_SUMMARY_KEYS[outcome];
      return { ...acc, [key]: acc[key] + 1 };
    },
    { ...EMPTY_SUMMARY, candidates: candidateCount, scanned },
  );

const logTickSummary = (runYmd: string, summary: CoverageSelfHealSummary): void => {
  console.log(
    `[coverage-self-heal] tick runYmd=${runYmd} scanned=${summary.scanned} candidates=${summary.candidates} enqueued=${summary.enqueued} escalated=${summary.escalated} alreadyInFlight=${summary.alreadyInFlight} alreadyComplete=${summary.alreadyComplete} errors=${summary.errors}`,
  );
};

// Entry point: scan today's races for coverage gaps and re-trigger the
// ones that need it. Scoped to a single JST calendar day (the day the cron
// fires on) -- a future-dated race can never satisfy the past-grace-window
// filter yet, so it is naturally picked up once its own day becomes "today"
// and this cron scans it again; no multi-day lookahead scan is needed.
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
  const candidates = buildGapCandidates(rows, now);
  if (candidates.length === 0) {
    return { ...EMPTY_SUMMARY, scanned: rows.length };
  }
  const outcomes = await Promise.all(
    candidates.map((candidate) => healCandidate({ candidate, env, runDate, runYmd })),
  );
  const summary = summarizeOutcomes(rows.length, candidates.length, outcomes);
  logTickSummary(runYmd, summary);
  return summary;
};
