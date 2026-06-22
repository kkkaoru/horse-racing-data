// Run with bun. Pure orchestration helper for admin "predict for day" workflow.
// Enumerates today's race_keys from Hyperdrive and enqueues
// predict-running-style + predict-finish-position jobs for each via the
// existing REALTIME_FEATURES_JOBS queue producer. Deliberately enqueues
// (does NOT directly invoke handlers) so the queue's max_concurrency=4
// throttles correctly and the result aligns with the existing cron path.

import { listTodayRaceKeysWithKvCache } from "./scheduled-race-list";
import type { Env, Job, RaceJobKey } from "./types";

export type PredictForDaySource = "jra" | "nar" | "all";

export interface RunPredictionsForDayParams {
  source: PredictForDaySource;
  targetYmd: string;
  skipCompleted?: boolean;
}

export interface RunPredictionsForDaySkip {
  raceKey: string;
  reason: string;
}

export interface RunPredictionsForDayResult {
  enqueuedRunningStyle: string[];
  enqueuedFinishPosition: string[];
  skippedReasons: RunPredictionsForDaySkip[];
}

interface EnqueueInferenceJobsArgs {
  env: Env;
  raceJobKey: RaceJobKey;
  predictedAt: string;
}

interface EnqueueInferenceJobsResult {
  enqueuedRunningStyle: boolean;
  enqueuedFinishPosition: boolean;
  skippedReasons: RunPredictionsForDaySkip[];
}

interface InferenceStateRaceKeyRow {
  race_key: string;
}

interface AccumulatedResult {
  enqueuedRunningStyle: string[];
  enqueuedFinishPosition: string[];
  skippedReasons: RunPredictionsForDaySkip[];
}

interface SequentialEnqueueArgs {
  env: Env;
  raceJobKeys: RaceJobKey[];
  predictedAt: string;
}

const YMD_TOTAL_LENGTH = 8;
const YYYY_START_INDEX = 0;
const YYYY_LENGTH = 4;
const MMDD_START_INDEX = 4;
const MMDD_LENGTH = 4;
const RACE_KEY_YEAR_SUBSTR_START = 5;
const RACE_KEY_YEAR_SUBSTR_LENGTH = 4;
const RACE_KEY_MMDD_SUBSTR_START = 10;
const RACE_KEY_MMDD_SUBSTR_LENGTH = 4;
const ALREADY_COMPLETED_REASON = "already-completed";

const SELECT_COMPLETED_RUNNING_STYLE_SQL =
  "select race_key from running_style_inference_state " +
  "where status = 'completed' and substr(race_key, ?, ?) = ? and substr(race_key, ?, ?) = ?";

const SELECT_COMPLETED_FINISH_POSITION_SQL =
  "select race_key from finish_position_inference_state " +
  "where status = 'completed' and substr(race_key, ?, ?) = ? and substr(race_key, ?, ?) = ?";

const SOURCE_FILTERS = {
  all: (): boolean => true,
  jra: (rowSource: "jra" | "nar"): boolean => rowSource === "jra",
  nar: (rowSource: "jra" | "nar"): boolean => rowSource === "nar",
} satisfies Record<PredictForDaySource, (rowSource: "jra" | "nar") => boolean>;

const toRunningStyleMessage = (
  raceJobKey: RaceJobKey,
  predictedAt: string,
): Extract<Job, { type: "predict-running-style" }> => ({
  kaisaiNen: raceJobKey.kaisaiNen,
  kaisaiTsukihi: raceJobKey.kaisaiTsukihi,
  keibajoCode: raceJobKey.keibajoCode,
  predictedAt,
  raceBango: raceJobKey.raceBango,
  raceKey: raceJobKey.raceKey,
  source: raceJobKey.source,
  type: "predict-running-style",
});

const toFinishPositionMessage = (
  raceJobKey: RaceJobKey,
  predictedAt: string,
): Extract<Job, { type: "predict-finish-position" }> => ({
  kaisaiNen: raceJobKey.kaisaiNen,
  kaisaiTsukihi: raceJobKey.kaisaiTsukihi,
  keibajoCode: raceJobKey.keibajoCode,
  predictedAt,
  raceBango: raceJobKey.raceBango,
  raceKey: raceJobKey.raceKey,
  source: raceJobKey.source,
  type: "predict-finish-position",
});

const toSendErrorReason = (jobType: string, error: unknown): string => {
  const detail = error instanceof Error ? error.message : String(error);
  return `${jobType}: ${detail}`;
};

const trySendRunningStyle = async (
  args: EnqueueInferenceJobsArgs,
): Promise<{ enqueued: boolean; skipped: RunPredictionsForDaySkip | null }> => {
  try {
    await args.env.REALTIME_FEATURES_JOBS.send(
      toRunningStyleMessage(args.raceJobKey, args.predictedAt),
    );
    return { enqueued: true, skipped: null };
  } catch (error) {
    return {
      enqueued: false,
      skipped: {
        raceKey: args.raceJobKey.raceKey,
        reason: toSendErrorReason("predict-running-style", error),
      },
    };
  }
};

const trySendFinishPosition = async (
  args: EnqueueInferenceJobsArgs,
): Promise<{ enqueued: boolean; skipped: RunPredictionsForDaySkip | null }> => {
  try {
    await args.env.REALTIME_FEATURES_JOBS.send(
      toFinishPositionMessage(args.raceJobKey, args.predictedAt),
    );
    return { enqueued: true, skipped: null };
  } catch (error) {
    return {
      enqueued: false,
      skipped: {
        raceKey: args.raceJobKey.raceKey,
        reason: toSendErrorReason("predict-finish-position", error),
      },
    };
  }
};

const enqueueInferenceJobsForRace = async (
  args: EnqueueInferenceJobsArgs,
): Promise<EnqueueInferenceJobsResult> => {
  const runningStyleOutcome = await trySendRunningStyle(args);
  const finishPositionOutcome = await trySendFinishPosition(args);
  const skippedReasons = [runningStyleOutcome.skipped, finishPositionOutcome.skipped].filter(
    (entry): entry is RunPredictionsForDaySkip => entry !== null,
  );
  return {
    enqueuedFinishPosition: finishPositionOutcome.enqueued,
    enqueuedRunningStyle: runningStyleOutcome.enqueued,
    skippedReasons,
  };
};

const initialResult = (): AccumulatedResult => ({
  enqueuedFinishPosition: [],
  enqueuedRunningStyle: [],
  skippedReasons: [],
});

const mergeOutcome = (
  acc: AccumulatedResult,
  raceJobKey: RaceJobKey,
  outcome: EnqueueInferenceJobsResult,
): AccumulatedResult => ({
  enqueuedFinishPosition: outcome.enqueuedFinishPosition
    ? [...acc.enqueuedFinishPosition, raceJobKey.raceKey]
    : acc.enqueuedFinishPosition,
  enqueuedRunningStyle: outcome.enqueuedRunningStyle
    ? [...acc.enqueuedRunningStyle, raceJobKey.raceKey]
    : acc.enqueuedRunningStyle,
  skippedReasons: [...acc.skippedReasons, ...outcome.skippedReasons],
});

const stepSequentialEnqueue = async (
  args: SequentialEnqueueArgs,
  index: number,
  acc: AccumulatedResult,
): Promise<AccumulatedResult> => {
  if (index >= args.raceJobKeys.length) {
    return acc;
  }
  const raceJobKey = args.raceJobKeys[index]!;
  const outcome = await enqueueInferenceJobsForRace({
    env: args.env,
    predictedAt: args.predictedAt,
    raceJobKey,
  });
  const nextAcc = mergeOutcome(acc, raceJobKey, outcome);
  return stepSequentialEnqueue(args, index + 1, nextAcc);
};

const runSequentialEnqueue = (args: SequentialEnqueueArgs): Promise<AccumulatedResult> =>
  stepSequentialEnqueue(args, 0, initialResult());

const toRaceKeySet = (rows: InferenceStateRaceKeyRow[] | undefined): Set<string> =>
  new Set((rows ?? []).map((row) => row.race_key));

const intersectRaceKeySets = (left: Set<string>, right: Set<string>): Set<string> => {
  const result = new Set<string>();
  left.forEach((key) => {
    if (right.has(key)) {
      result.add(key);
    }
  });
  return result;
};

const buildCompletedRaceKeySetForYmd = async (
  db: D1Database,
  targetYmd: string,
): Promise<Set<string>> => {
  if (targetYmd.length !== YMD_TOTAL_LENGTH) {
    return new Set<string>();
  }
  const year = targetYmd.slice(YYYY_START_INDEX, YYYY_START_INDEX + YYYY_LENGTH);
  const monthDay = targetYmd.slice(MMDD_START_INDEX, MMDD_START_INDEX + MMDD_LENGTH);
  const runningStyleResult = await db
    .prepare(SELECT_COMPLETED_RUNNING_STYLE_SQL)
    .bind(
      RACE_KEY_YEAR_SUBSTR_START,
      RACE_KEY_YEAR_SUBSTR_LENGTH,
      year,
      RACE_KEY_MMDD_SUBSTR_START,
      RACE_KEY_MMDD_SUBSTR_LENGTH,
      monthDay,
    )
    .all<InferenceStateRaceKeyRow>();
  const finishPositionResult = await db
    .prepare(SELECT_COMPLETED_FINISH_POSITION_SQL)
    .bind(
      RACE_KEY_YEAR_SUBSTR_START,
      RACE_KEY_YEAR_SUBSTR_LENGTH,
      year,
      RACE_KEY_MMDD_SUBSTR_START,
      RACE_KEY_MMDD_SUBSTR_LENGTH,
      monthDay,
    )
    .all<InferenceStateRaceKeyRow>();
  const runningStyleSet = toRaceKeySet(runningStyleResult.results);
  const finishPositionSet = toRaceKeySet(finishPositionResult.results);
  return intersectRaceKeySets(runningStyleSet, finishPositionSet);
};

const toRaceJobKey = (entry: {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceKey: string;
  source: "jra" | "nar";
}): RaceJobKey => ({
  kaisaiNen: entry.kaisaiNen,
  kaisaiTsukihi: entry.kaisaiTsukihi,
  keibajoCode: entry.keibajoCode,
  raceBango: entry.raceBango,
  raceKey: entry.raceKey,
  source: entry.source,
});

const toAlreadyCompletedSkip = (raceJobKey: RaceJobKey): RunPredictionsForDaySkip => ({
  raceKey: raceJobKey.raceKey,
  reason: ALREADY_COMPLETED_REASON,
});

export const runPredictionsForDay = async (
  env: Env,
  params: RunPredictionsForDayParams,
): Promise<RunPredictionsForDayResult> => {
  const todayRaceKeys = await listTodayRaceKeysWithKvCache({
    context: {},
    env,
    yyyymmdd: params.targetYmd,
  });
  const filter = SOURCE_FILTERS[params.source];
  const sourceFiltered = todayRaceKeys.filter((entry) => filter(entry.source));
  const allRaceJobKeys: RaceJobKey[] = sourceFiltered.map(toRaceJobKey);
  const completedRaceKeys =
    params.skipCompleted === true
      ? await buildCompletedRaceKeySetForYmd(env.REALTIME_FEATURES_DB, params.targetYmd)
      : new Set<string>();
  const eligibleRaceJobKeys = allRaceJobKeys.filter(
    (raceJobKey) => !completedRaceKeys.has(raceJobKey.raceKey),
  );
  const alreadyCompletedSkips = allRaceJobKeys
    .filter((raceJobKey) => completedRaceKeys.has(raceJobKey.raceKey))
    .map(toAlreadyCompletedSkip);
  const predictedAt = new Date().toISOString();
  const accumulated = await runSequentialEnqueue({
    env,
    predictedAt,
    raceJobKeys: eligibleRaceJobKeys,
  });
  return {
    enqueuedFinishPosition: accumulated.enqueuedFinishPosition,
    enqueuedRunningStyle: accumulated.enqueuedRunningStyle,
    skippedReasons: [...alreadyCompletedSkips, ...accumulated.skippedReasons],
  };
};
