// Run with bun. Queue consumer: processes one predict message per batch invocation.
// For each message: dedup via DO coordinator (strong consistency), call the Container
// DO stub's fetch, track state.

import { claimRun, completeRun } from "./do-state";
import { parseNdjsonStream } from "./ndjson-stream";
import {
  warmPredictionCacheForCategory,
  warmPredictionCacheForRace,
} from "./prediction-cache-warm";
import { rescoreJraRace } from "./scoring/rescore-consumer";
import type { Env, PredictQueueMessage } from "./types";

const RUN_YMD_YEAR_START = 0;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_START = 4;
const RUN_YMD_MONTH_END = 6;
const RUN_YMD_DAY_START = 6;
const RUN_YMD_DAY_END = 8;
const EMPTY_RACE_TARGET = "";

const PREDICT_DO_NAME_PREFIX = "predict-";
const PREDICT_PATH = "/predict";
const PREDICT_HOST = "http://do";
const RESCORE_MODE = "rescore";
const JRA_CATEGORY = "jra";
const NAR_CATEGORY = "nar";
const BAN_EI_CATEGORY = "ban-ei";
// Categories whose per-race rescore is served by the container DO held /predict
// (Python ensemble re-score). JRA stays Worker-native and is excluded here.
const CONTAINER_PER_RACE_CATEGORIES = new Set<string>([NAR_CATEGORY, BAN_EI_CATEGORY]);

interface PredictUrlParams {
  category: string;
  daysAhead: number;
  mode: string;
  // runYmd is the YYYYMMDD date string required by the container /predict endpoint.
  runYmd: string;
}

// PredictQueueMessage with the per-race target fields proven present. isPerRaceRescore
// narrows to this so the container path never needs an unreachable undefined guard.
interface PerRaceRescoreMessage extends PredictQueueMessage {
  keibajoCode: string;
  raceBango: string;
}

interface PerRaceRescoreUrlParams {
  category: string;
  daysAhead: number;
  // keibajoCode / raceBango are 2-digit zero-padded strings from the per-race coordinator.
  keibajoCode: string;
  raceBango: string;
  // runYmd is the YYYYMMDD date string required by the container /predict endpoint.
  runYmd: string;
}

const buildPredictUrl = (params: PredictUrlParams): string => {
  const searchParams = new URLSearchParams({
    category: params.category,
    daysAhead: String(params.daysAhead),
    mode: params.mode,
    runDate: params.runYmd,
  });
  return `${PREDICT_HOST}${PREDICT_PATH}?${searchParams.toString()}`;
};

const buildPerRaceRescoreUrl = (params: PerRaceRescoreUrlParams): string => {
  const searchParams = new URLSearchParams({
    category: params.category,
    daysAhead: String(params.daysAhead),
    mode: RESCORE_MODE,
    keibajoCode: params.keibajoCode,
    raceBango: params.raceBango,
    runDate: params.runYmd,
  });
  return `${PREDICT_HOST}${PREDICT_PATH}?${searchParams.toString()}`;
};

// A per-race rescore is targeted at one race (mode="rescore" with both a
// keibajo_code and a race_bango set by the per-race coordinator). Per-category
// rescores (no keibajo_code) stay on the container path. Narrows the message so the
// keibajoCode / raceBango are known-present downstream (no unreachable guard needed).
const isPerRaceRescore = (
  message: Message<PredictQueueMessage>,
): message is Message<PerRaceRescoreMessage> =>
  message.body.mode === RESCORE_MODE &&
  message.body.keibajoCode !== undefined &&
  message.body.raceBango !== undefined;

// Container per-race rescore (NAR / Ban-ei): held /predict on the per-category DO
// runs the Python ensemble re-score for one race. No per-category run state is
// touched (completeRun is not called) so concurrent full/per-category runs are
// unaffected. racesPredicted > 0 and racesPredicted === 0 (cache_miss) both ack —
// the container already produced an NDJSON result, so retry would be futile.
// Fetch / stream / DO errors retry via the queue's DLQ machinery.
const processContainerPerRaceRescore = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { category, daysAhead, keibajoCode, raceBango, runYmd } = message.body;
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(
    `${PREDICT_DO_NAME_PREFIX}${category}`,
  );
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  try {
    const response = await stub.fetch(
      new Request(buildPerRaceRescoreUrl({ category, daysAhead, keibajoCode, raceBango, runYmd })),
    );
    if (!response.body) throw new Error("Empty response from predict DO");
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Container DO returned ${response.status}: ${text}`);
    }
    const result = await parseNdjsonStream(response.body);
    console.log(
      `Rescore NAR(container) runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} races=${result.racesPredicted}`,
    );
    message.ack();
  } catch (err) {
    console.error(
      `Container per-race rescore failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}:`,
      String(err),
    );
    message.retry();
  }
};

// Worker-native JRA per-race rescore: no container, no 21y Neon scan. cache_miss /
// race_not_found are acked (retry is futile); fetch / score / UPSERT errors retry.
const processJraPerRaceRescore = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { runYmd, keibajoCode, raceBango } = message.body;
  try {
    const result = await rescoreJraRace({ env, fetchImpl: fetch, message: message.body });
    console.log(
      `Rescore JRA runYmd=${runYmd} status=${result.status} predictions=${result.predictionCount} etop2=${result.etop2Fired}`,
    );
    message.ack();
    void warmPredictionCacheForRace({
      day: runYmd.slice(RUN_YMD_DAY_START, RUN_YMD_DAY_END),
      keibajoCode: keibajoCode ?? EMPTY_RACE_TARGET,
      month: runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_MONTH_END),
      raceNumber: raceBango ?? EMPTY_RACE_TARGET,
      year: runYmd.slice(RUN_YMD_YEAR_START, RUN_YMD_YEAR_END),
    });
  } catch (err) {
    console.error(`Rescore JRA failed runYmd=${runYmd}:`, String(err));
    message.retry();
  }
};

// Per-race rescore dispatch: JRA stays Worker-native, NAR / Ban-ei route to the
// container held /predict, unknown categories are skipped + acked.
const processPerRaceRescore = (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { category, runYmd } = message.body;
  if (category === JRA_CATEGORY) return processJraPerRaceRescore(message, env);
  if (CONTAINER_PER_RACE_CATEGORIES.has(category))
    return processContainerPerRaceRescore(message, env);
  console.warn(`Skipping per-race rescore for unsupported category=${category} runYmd=${runYmd}`);
  message.ack();
  return Promise.resolve();
};

const processMessage = async (message: Message<PredictQueueMessage>, env: Env): Promise<void> => {
  if (isPerRaceRescore(message)) return processPerRaceRescore(message, env);
  const { category, runYmd, daysAhead, mode } = message.body;
  if (!message.body.skipDedup) {
    const claimed = await claimRun({ category, env, runYmd });
    if (!claimed.proceed) {
      message.ack();
      return;
    }
  }
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(
    `${PREDICT_DO_NAME_PREFIX}${category}`,
  );
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  try {
    const response = await stub.fetch(
      new Request(buildPredictUrl({ category, daysAhead, mode, runYmd })),
    );
    if (!response.body) throw new Error("Empty response from predict DO");
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Container DO returned ${response.status}: ${text}`);
    }
    const result = await parseNdjsonStream(response.body);
    await completeRun({
      category,
      env,
      racesPredicted: result.racesPredicted,
      runYmd,
      status: "success",
    });
    message.ack();
    if (message.body.skipDedup) {
      void warmPredictionCacheForCategory({
        category,
        env,
        runDate: message.body.runDateIso ?? message.body.runDate,
        runYmd,
      });
    }
  } catch (err) {
    console.error(`Predict failed for category=${category} runYmd=${runYmd}:`, String(err));
    await completeRun({
      category,
      env,
      racesPredicted: 0,
      runYmd,
      status: "error",
    });
    message.retry();
  }
};

export const handleQueue = async (
  batch: MessageBatch<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  await Promise.all(batch.messages.map((msg) => processMessage(msg, env)));
};
