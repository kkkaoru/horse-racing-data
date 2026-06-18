// Run with bun. Queue consumer: processes one predict message per batch invocation.
// For each message: dedup via DO coordinator (strong consistency), call the Container
// DO stub's fetch, track state.

import { claimRun, completeRun } from "./do-state";
import { parseNdjsonStream } from "./ndjson-stream";
import type { Env, PredictQueueMessage } from "./types";

const PREDICT_DO_NAME_PREFIX = "predict-";
const PREDICT_PATH = "/predict";
const PREDICT_HOST = "http://do";

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

const processMessage = async (message: Message<PredictQueueMessage>, env: Env): Promise<void> => {
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
