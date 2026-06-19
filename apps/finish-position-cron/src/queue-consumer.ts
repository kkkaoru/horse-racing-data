// Run with bun. Queue consumer: processes one predict message per batch invocation.
// For each message: dedup via DO coordinator (strong consistency), call the Container
// DO stub's fetch, track state.

import { claimRun, completeRun } from "./do-state";
import { parseNdjsonStream } from "./ndjson-stream";
import { rescoreJraRace } from "./scoring/rescore-consumer";
import type { Env, PredictQueueMessage } from "./types";

const PREDICT_DO_NAME_PREFIX = "predict-";
const PREDICT_PATH = "/predict";
const PREDICT_HOST = "http://do";
const RESCORE_MODE = "rescore";
const JRA_CATEGORY = "jra";

interface PredictUrlParams {
  category: string;
  daysAhead: number;
  mode: string;
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

// A per-race rescore is targeted at one race (mode="rescore" with both a
// keibajo_code and a race_bango set by the per-race coordinator). Per-category
// rescores (no keibajo_code) stay on the container path.
const isPerRaceRescore = (body: PredictQueueMessage): boolean =>
  body.mode === RESCORE_MODE && body.keibajoCode !== undefined && body.raceBango !== undefined;

// Worker-native JRA per-race rescore: no container, no 21y Neon scan. Only JRA
// is wired (NAR / Ban-ei per-race rescore is a follow-up), so other categories
// are skipped + acked. cache_miss / race_not_found are acked (retry is futile);
// fetch / score / UPSERT errors retry.
const processPerRaceRescore = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  const { category, runYmd } = message.body;
  if (category !== JRA_CATEGORY) {
    console.warn(`Skipping per-race rescore for unsupported category=${category} runYmd=${runYmd}`);
    message.ack();
    return;
  }
  try {
    const result = await rescoreJraRace({ env, fetchImpl: fetch, message: message.body });
    console.log(
      `Rescore JRA runYmd=${runYmd} status=${result.status} predictions=${result.predictionCount} etop2=${result.etop2Fired}`,
    );
    message.ack();
  } catch (err) {
    console.error(`Rescore JRA failed runYmd=${runYmd}:`, String(err));
    message.retry();
  }
};

const processMessage = async (message: Message<PredictQueueMessage>, env: Env): Promise<void> => {
  if (isPerRaceRescore(message.body)) return processPerRaceRescore(message, env);
  const { category, runYmd, daysAhead, mode } = message.body;
  const claimed = await claimRun({ category, env, runYmd });
  if (!claimed.proceed) {
    message.ack();
    return;
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
    const result = await parseNdjsonStream(response.body);
    await completeRun({
      category,
      env,
      racesPredicted: result.racesPredicted,
      runYmd,
      status: "success",
    });
    message.ack();
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
