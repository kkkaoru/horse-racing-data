// Run with bun. KV-backed run-state for deduplication and progress tracking.
// Key format: predict:{runYmd}:{category}, TTL: 24 hours.

import type { Env, PredictRunState } from "./types";

const KV_TTL_SECONDS = 86400;
const KV_KEY_PREFIX = "predict";

interface KvStateKey {
  runYmd: string;
  category: string;
}

interface WriteStateParams {
  env: Env;
  runYmd: string;
  category: string;
  state: PredictRunState;
}

interface ReadStateParams {
  env: Env;
  runYmd: string;
  category: string;
}

export const buildKvKey = (key: KvStateKey): string =>
  `${KV_KEY_PREFIX}:${key.runYmd}:${key.category}`;

export const writeRunState = async (params: WriteStateParams): Promise<void> => {
  await params.env.PREDICT_STATE.put(
    buildKvKey({ runYmd: params.runYmd, category: params.category }),
    JSON.stringify(params.state),
    { expirationTtl: KV_TTL_SECONDS },
  );
};

export const readRunState = async (params: ReadStateParams): Promise<PredictRunState | null> => {
  const value = await params.env.PREDICT_STATE.get(
    buildKvKey({ runYmd: params.runYmd, category: params.category }),
  );
  if (!value) return null;
  return JSON.parse(value) as PredictRunState;
};

export const isAlreadyRunning = async (params: ReadStateParams): Promise<boolean> => {
  const state = await readRunState(params);
  return state?.status === "started";
};
