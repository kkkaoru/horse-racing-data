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

import { CONTAINER_DAY_BASE_SLOT_STALE_MS, type ContainerSlotKind } from "./container-slot-cap";
import { enumerateTodaysRaces, type RaceEntry } from "./cron-decision";
import {
  DAY_BASE_PICKUP_FIRST_ATTEMPT,
  buildDayBaseWorkKey,
  enqueueDayBasePickup,
} from "./day-base-pickup";
import { headDayBaseObject, pickUpPrewarmDayBase } from "./day-base-prewarm-pickup";
import { enqueueContainerStop } from "./container-control";
import { claimContainerSlot, releaseContainerSlot } from "./do-state";
import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import type { DaybaseWatermark } from "./ndjson-stream";
import { PREDICT_DO_NAME_PREFIX } from "./predict-do-shard";
import type { DayBasePrewarmMessage, Env, PredictCategory } from "./types";

type PrewarmResultType = "result";
type PrewarmSuccessStatus = "success";
type PrewarmAcceptedStatus = "accepted";
type PrewarmEmptyStatus = "empty";
export type PrewarmCategoryOutcome = "failed" | "landed" | "pickup-scheduled";

interface PrewarmNdjsonLine {
  type: string;
}

interface PrewarmResultLine extends PrewarmNdjsonLine {
  type: PrewarmResultType;
  category?: string;
  daybaseWatermark?: DaybaseWatermark;
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
  generatePredictionsAfterHit?: boolean;
}

interface RunDayBasePrewarmParams {
  daysAhead: number;
  env: Env;
  runYmd: string;
}

interface EnqueueDayBasePrewarmParams extends PrewarmCategoryParams {
  requestedAt?: Date;
}

interface LogPrewarmResultParams {
  category: PredictCategory;
  result: PrewarmResultLine;
  runYmd: string;
}

interface HandlePrewarmResponseParams {
  category: PredictCategory;
  response: Response;
  runYmd: string;
}

interface ReleaseDayBaseSlotParams {
  category: PredictCategory;
  doName: string;
  env: Env;
  runYmd: string;
}

// Deliberately stays category-scoped (never resolvePredictDoName) even when
// RACE_SHARDED_DO is on: this prewarm is a day-stable, category-wide cache
// warm, not a race-scoped request, so there is no (keibajoCode, raceBango)
// to shard by. When sharding is enabled each shard still gets its day-base
// cache lazily on that shard's first race (the container's per-race
// synchronous fallback, see the module docstring above) -- slower on that
// first hit per shard, never incorrect.
const PREWARM_HOST: string = "http://do";
const PREWARM_PATH: string = "/prewarm-day-base";
const PREWARM_RESULT_TYPE: PrewarmResultType = "result";
const PREWARM_SUCCESS_STATUS: PrewarmSuccessStatus = "success";
const PREWARM_ACCEPTED_STATUS: PrewarmAcceptedStatus = "accepted";
const PREWARM_EMPTY_STATUS: PrewarmEmptyStatus = "empty";
const NONE_LABEL: string = "-";
const DAY_BASE_SLOT_KIND: ContainerSlotKind = "day-base";
export const DAY_BASE_PREWARM_TYPE = "day-base-prewarm";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const isDayBasePrewarmMessage = (value: unknown): value is DayBasePrewarmMessage =>
  isRecord(value) &&
  value.type === DAY_BASE_PREWARM_TYPE &&
  (value.category === "jra" || value.category === "nar" || value.category === "ban-ei") &&
  typeof value.runYmd === "string" &&
  /^\d{8}$/u.test(value.runYmd) &&
  typeof value.daysAhead === "number" &&
  Number.isFinite(value.daysAhead) &&
  typeof value.requestedAt === "string" &&
  (value.generatePredictionsAfterHit === undefined ||
    typeof value.generatePredictionsAfterHit === "boolean");

export const isDayBasePrewarmQueueMessage = (
  message: Message<unknown>,
): message is Message<DayBasePrewarmMessage> => isDayBasePrewarmMessage(message.body);

export const enqueueDayBasePrewarm = async (params: EnqueueDayBasePrewarmParams): Promise<void> => {
  await params.env.PREDICT_QUEUE.send({
    category: params.category,
    daysAhead: params.daysAhead,
    ...(params.generatePredictionsAfterHit === true ? { generatePredictionsAfterHit: true } : {}),
    requestedAt: (params.requestedAt ?? new Date()).toISOString(),
    runYmd: params.runYmd,
    type: DAY_BASE_PREWARM_TYPE,
  });
};

const isPrewarmNdjsonLine = (value: unknown): value is PrewarmNdjsonLine =>
  isRecord(value) && typeof value.type === "string";

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
  const parsed: unknown = JSON.parse(lastLine);
  if (!isPrewarmNdjsonLine(parsed)) return null;
  return isPrewarmResultLine(parsed) ? parsed : null;
};

const describePrewarmResult = (result: PrewarmResultLine): string =>
  `status=${result.status ?? NONE_LABEL} parquetKey=${result.parquetKey ?? NONE_LABEL} watermark=${
    result.daybaseWatermark ? "present" : "absent"
  } error=${result.error ?? NONE_LABEL}`;

const hasUploadableParquet = (result: PrewarmResultLine): boolean => {
  const parquetKey = result.parquetKey?.trim() ?? "";
  return parquetKey.length > 0;
};

const logPrewarmResult = (params: LogPrewarmResultParams): void => {
  const { category, result, runYmd } = params;
  const summary = `category=${category} runYmd=${runYmd} ${describePrewarmResult(result)}`;
  if (result.status === PREWARM_SUCCESS_STATUS && hasUploadableParquet(result)) {
    console.log(`[day-base-prewarm] success ${summary}`);
    return;
  }
  if (result.status === PREWARM_ACCEPTED_STATUS && hasUploadableParquet(result)) {
    console.log(`[day-base-prewarm] started ${summary}`);
    return;
  }
  if (result.status === PREWARM_EMPTY_STATUS) {
    console.log(`[day-base-prewarm] empty ${summary}`);
    return;
  }
  console.warn(`[day-base-prewarm] failed ${summary}`);
};

const handlePrewarmResponse = async (
  params: HandlePrewarmResponseParams,
): Promise<PrewarmResultLine | null> => {
  const { category, response, runYmd } = params;
  if (!response.ok || !response.body) {
    console.warn(
      `[day-base-prewarm] non-ok response category=${category} runYmd=${runYmd} status=${response.status}`,
    );
    return null;
  }
  const text = await response.text();
  const result = parsePrewarmResultLine(text);
  if (result === null) {
    console.warn(`[day-base-prewarm] unparseable result category=${category} runYmd=${runYmd}`);
    return null;
  }
  logPrewarmResult({ category, result, runYmd });
  return result;
};

const landDayBaseFromPickup = async (
  params: Omit<PrewarmCategoryParams, "daysAhead">,
): Promise<boolean> => {
  const picked = await pickUpPrewarmDayBase(params);
  if (!picked) return false;
  return (await headDayBaseObject(params)) !== null;
};

const releaseDayBaseSlotBestEffort = async (params: ReleaseDayBaseSlotParams): Promise<void> => {
  const workKey = buildDayBaseWorkKey(params.category, params.runYmd);
  try {
    if (await enqueueContainerStop(params.env, params.doName, workKey)) return;
    await releaseContainerSlot({
      doName: params.doName,
      env: params.env,
      kind: DAY_BASE_SLOT_KIND,
      workKey,
    });
  } catch (releaseErr) {
    console.error(
      `[day-base-prewarm] failed to release container slot category=${params.category} doName=${params.doName}: ${String(releaseErr)}`,
    );
  }
};

// Fire-and-log dispatch for one category: never throws. The container's
// per-race lazy day-base build is the safety net when this cache warm fails
// or times out, so every failure path here is caught and logged instead of
// propagated.
export const prewarmCategoryWithOutcome = async (
  params: PrewarmCategoryParams,
): Promise<PrewarmCategoryOutcome> => {
  const { category, daysAhead, env, runYmd } = params;
  // Do not trust R2 presence or an old in-process pickup here. The
  // container's prewarm endpoint must first compare the object's metadata
  // with the live Catalog + running-style watermark.
  const doName = `${PREDICT_DO_NAME_PREFIX}${category}`;
  const url = buildPrewarmUrl({ category, daysAhead, runYmd });
  const claim = await claimContainerSlot({
    category,
    doName,
    env,
    kind: DAY_BASE_SLOT_KIND,
    staleAfterMs: CONTAINER_DAY_BASE_SLOT_STALE_MS,
    workKey: buildDayBaseWorkKey(category, runYmd),
  });
  if (!claim.proceed) {
    console.warn(
      `[day-base-prewarm] container slot ${claim.state ?? "capped"} doName=${doName} kind=${DAY_BASE_SLOT_KIND} category=${category} runYmd=${runYmd} -- skipping start`,
    );
    if (params.generatePredictionsAfterHit === true) {
      await enqueueDayBasePickup({
        attempt: DAY_BASE_PICKUP_FIRST_ATTEMPT,
        category,
        env,
        generatePredictionsAfterHit: true,
        runYmd,
      });
    }
    return params.generatePredictionsAfterHit === true ? "pickup-scheduled" : "failed";
  }
  let releaseSlot = true;
  try {
    const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(doName);
    const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
    const response = await stub.fetch(new Request(url));
    const result = await handlePrewarmResponse({ category, response, runYmd });
    // Only the container can declare an existing R2 object fresh because its
    // prewarm fast path compares the live Catalog + running-style watermark.
    if (result?.status === PREWARM_SUCCESS_STATUS && hasUploadableParquet(result)) {
      if (params.generatePredictionsAfterHit === true) {
        await fanOutPredictionsAfterDayBaseHit({ category, env, runYmd });
      }
      return "landed";
    }
    if (await landDayBaseFromPickup({ category, env, runYmd })) {
      if (params.generatePredictionsAfterHit === true) {
        await fanOutPredictionsAfterDayBaseHit({ category, env, runYmd });
      }
      return "landed";
    }
    if (result?.status !== PREWARM_ACCEPTED_STATUS) return "failed";
    await enqueueDayBasePickup({
      attempt: DAY_BASE_PICKUP_FIRST_ATTEMPT,
      category,
      env,
      ...(params.generatePredictionsAfterHit === true ? { generatePredictionsAfterHit: true } : {}),
      runYmd,
    });
    // The detached DAY_CHAIN still owns this capacity. Its delayed pickup
    // releases the lease after the fresh object lands (or retries exhaust),
    // preventing accepted work from immediately freeing a competing slot.
    releaseSlot = false;
    console.warn(
      `[day-base-prewarm] pickup-scheduled category=${category} runYmd=${runYmd} status=missing-object parquetKey=feat-daybase/catalog-v1/${category}/${runYmd}/features.parquet watermark=absent error=day-base object missing after prewarm`,
    );
    return "pickup-scheduled";
  } catch (err) {
    console.error(
      `[day-base-prewarm] failed category=${category} runYmd=${runYmd}: ${String(err)}`,
    );
    return "failed";
  } finally {
    if (releaseSlot) {
      await releaseDayBaseSlotBestEffort({ category, doName, env, runYmd });
    }
  }
};

export const prewarmCategory = async (params: PrewarmCategoryParams): Promise<boolean> =>
  (await prewarmCategoryWithOutcome(params)) === "landed";

const distinctCategories = (races: readonly RaceEntry[]): PredictCategory[] => [
  ...new Set(races.map((race) => race.category)),
];

const enumerateTodaysRacesOrNull = async (
  db: D1Database,
  runYmd: string,
): Promise<readonly RaceEntry[] | null> => {
  try {
    return await enumerateTodaysRaces(db, runYmd);
  } catch (err) {
    // Matches this module's own "never throws" contract (see file docstring)
    // -- previously only prewarmCategory honored it; enumerateTodaysRaces
    // itself was unguarded, so its failure propagated all the way up through
    // handleScheduled uncaught instead of degrading to the per-race lazy
    // fallback this cache warm is only ever a best-effort optimization for.
    console.error(`[day-base-prewarm] enumerate failed runYmd=${runYmd}: ${String(err)}`);
    return null;
  }
};

export const runDayBasePrewarm = async (params: RunDayBasePrewarmParams): Promise<boolean> => {
  const { daysAhead, env, runYmd } = params;
  // Unconditional first line: if enumerateTodaysRaces below throws (a real
  // 2026-07-12 incident -- the D1 query itself failed and the exception
  // propagated uncaught through handleScheduled), this is the only evidence
  // the cron fired at all. Logged BEFORE the query, not after, so it cannot
  // be skipped by the same failure it exists to catch.
  console.log(`[day-base-prewarm] start runYmd=${runYmd}`);
  const races = await enumerateTodaysRacesOrNull(env.REALTIME_DB, runYmd);
  if (races === null) return false;
  const categories = distinctCategories(races);
  if (categories.length === 0) {
    console.log(`[day-base-prewarm] no races scheduled runYmd=${runYmd} -- skipping dispatch`);
    return false;
  }
  console.log(`[day-base-prewarm] dispatching runYmd=${runYmd} categories=${categories.join(",")}`);
  const queued = await Promise.allSettled(
    categories.map((category) => enqueueDayBasePrewarm({ category, daysAhead, env, runYmd })),
  );
  queued.forEach((result, index) => {
    if (result.status === "rejected") {
      console.error(
        `[day-base-prewarm] enqueue failed category=${categories[index] ?? "-"} runYmd=${runYmd}: ${String(result.reason)}`,
      );
    }
  });
  return queued.every((result) => result.status === "fulfilled");
};
