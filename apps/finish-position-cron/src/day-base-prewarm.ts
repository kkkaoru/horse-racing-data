// Run with bun. Cron dispatch that calls the Python container's
// GET /prewarm-day-base endpoint once per category per day, so the day-stable
// feature layers are cached before any race needs the day-base parquet. Every
// per-category call is caught and logged, never thrown: this is a best-effort
// cache warm, not a hard dependency -- the container's per-race lazy fallback
// (build the day-base synchronously when the cache is missing) is the actual
// safety net.
//
// The WHOLE dispatch (runDayBasePrewarm), not just each per-category call,
// must uphold this "never throws" contract -- 2026-07-12 incident: the
// enumerateTodaysRaces D1 query at the top of runDayBasePrewarm was
// unguarded, so when it failed the exception propagated all the way up
// through worker.ts's handleScheduled uncaught, and because that failure
// happened BEFORE this module's own first console.log call, the run left
// ZERO "[day-base-prewarm]" log lines -- indistinguishable from "cron never
// fired" from the log tail alone. Fixed by (a) logging an unconditional
// "start" line before the query, and (b) wrapping the query in its own
// try/catch that logs and returns rather than rethrows.

import { enumerateTodaysRaces, type RaceEntry } from "./cron-decision";
import { PREDICT_DO_NAME_PREFIX } from "./predict-do-shard";
import type { Env, PredictCategory } from "./types";

// Deliberately stays category-scoped (never resolvePredictDoName) even when
// RACE_SHARDED_DO is on: this prewarm is a day-stable, category-wide cache
// warm, not a race-scoped request, so there is no (keibajoCode, raceBango)
// to shard by. When sharding is enabled each shard still gets its day-base
// cache lazily on that shard's first race (the container's per-race
// synchronous fallback, see the module docstring above) -- slower on that
// first hit per shard, never incorrect.
const PREWARM_HOST = "http://do";
const PREWARM_PATH = "/prewarm-day-base";
const PREWARM_RESULT_TYPE = "result";
const PREWARM_SUCCESS_STATUS = "success";
const PREWARM_EMPTY_STATUS = "empty";
const NONE_LABEL = "-";

interface PrewarmNdjsonLine {
  type: string;
}

interface PrewarmResultLine extends PrewarmNdjsonLine {
  type: "result";
  category?: string;
  error?: string;
  parquetKey?: string;
  runDate?: string;
  status?: string;
}

interface PrewarmCategoryParams {
  category: PredictCategory;
  daysAhead: number;
  env: Env;
  runYmd: string;
}

interface RunDayBasePrewarmParams {
  daysAhead: number;
  env: Env;
  runYmd: string;
}

const isPrewarmResultLine = (line: PrewarmNdjsonLine): line is PrewarmResultLine =>
  line.type === PREWARM_RESULT_TYPE;

const buildPrewarmUrl = (params: Omit<PrewarmCategoryParams, "env">): string => {
  const searchParams = new URLSearchParams({
    category: params.category,
    daysAhead: String(params.daysAhead),
    runDate: params.runYmd,
  });
  return `${PREWARM_HOST}${PREWARM_PATH}?${searchParams.toString()}`;
};

const parsePrewarmResultLine = (text: string): PrewarmResultLine | null => {
  const nonEmptyLines = text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const lastLine = nonEmptyLines.at(-1);
  if (lastLine === undefined) return null;
  const parsed = JSON.parse(lastLine) as PrewarmNdjsonLine;
  return isPrewarmResultLine(parsed) ? parsed : null;
};

const describePrewarmResult = (result: PrewarmResultLine): string =>
  `status=${result.status ?? NONE_LABEL} parquetKey=${result.parquetKey ?? NONE_LABEL} error=${
    result.error ?? NONE_LABEL
  }`;

const logPrewarmResult = (
  category: PredictCategory,
  runYmd: string,
  result: PrewarmResultLine,
): void => {
  const summary = `category=${category} runYmd=${runYmd} ${describePrewarmResult(result)}`;
  if (result.status === PREWARM_SUCCESS_STATUS) {
    console.log(`[day-base-prewarm] success ${summary}`);
    return;
  }
  if (result.status === PREWARM_EMPTY_STATUS) {
    console.log(`[day-base-prewarm] empty ${summary}`);
    return;
  }
  console.warn(`[day-base-prewarm] failed ${summary}`);
};

const handlePrewarmResponse = async (
  response: Response,
  category: PredictCategory,
  runYmd: string,
): Promise<void> => {
  if (!response.ok || !response.body) {
    console.warn(
      `[day-base-prewarm] non-ok response category=${category} runYmd=${runYmd} status=${response.status}`,
    );
    return;
  }
  const text = await response.text();
  const result = parsePrewarmResultLine(text);
  if (result === null) {
    console.warn(`[day-base-prewarm] unparseable result category=${category} runYmd=${runYmd}`);
    return;
  }
  logPrewarmResult(category, runYmd, result);
};

// Fire-and-log dispatch for one category: never throws. The container's
// per-race lazy day-base build is the safety net when this cache warm fails
// or times out, so every failure path here is caught and logged instead of
// propagated.
export const prewarmCategory = async (params: PrewarmCategoryParams): Promise<void> => {
  const { category, daysAhead, env, runYmd } = params;
  const doName = `${PREDICT_DO_NAME_PREFIX}${category}`;
  const url = buildPrewarmUrl({ category, daysAhead, runYmd });
  try {
    const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(doName);
    const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
    const response = await stub.fetch(new Request(url));
    await handlePrewarmResponse(response, category, runYmd);
  } catch (err) {
    console.error(
      `[day-base-prewarm] failed category=${category} runYmd=${runYmd}: ${String(err)}`,
    );
  }
};

const distinctCategories = (races: readonly RaceEntry[]): PredictCategory[] => [
  ...new Set(races.map((race) => race.category)),
];

export const runDayBasePrewarm = async (params: RunDayBasePrewarmParams): Promise<void> => {
  const { daysAhead, env, runYmd } = params;
  // Unconditional first line: if enumerateTodaysRaces below throws (a real
  // 2026-07-12 incident -- the D1 query itself failed and the exception
  // propagated uncaught through handleScheduled), this is the only evidence
  // the cron fired at all. Logged BEFORE the query, not after, so it cannot
  // be skipped by the same failure it exists to catch.
  console.log(`[day-base-prewarm] start runYmd=${runYmd}`);
  let races: readonly RaceEntry[];
  try {
    races = await enumerateTodaysRaces(env.REALTIME_DB, runYmd);
  } catch (err) {
    // Matches this module's own "never throws" contract (see file docstring)
    // -- previously only prewarmCategory honored it; enumerateTodaysRaces
    // itself was unguarded, so its failure propagated all the way up through
    // handleScheduled uncaught instead of degrading to the per-race lazy
    // fallback this cache warm is only ever a best-effort optimization for.
    console.error(`[day-base-prewarm] enumerate failed runYmd=${runYmd}: ${String(err)}`);
    return;
  }
  const categories = distinctCategories(races);
  if (categories.length === 0) {
    console.log(`[day-base-prewarm] no races scheduled runYmd=${runYmd} -- skipping dispatch`);
    return;
  }
  console.log(`[day-base-prewarm] dispatching runYmd=${runYmd} categories=${categories.join(",")}`);
  await Promise.all(
    categories.map((category) => prewarmCategory({ category, daysAhead, env, runYmd })),
  );
};
