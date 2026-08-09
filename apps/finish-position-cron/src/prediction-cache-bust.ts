// Run with bun. Fires pc-keiba-viewer's prediction Cache-API bust after a
// producer overwrite of pred:fp / pred:rs KV. Same auth header and retry
// policy as viewer-race-cache-bust in sync-realtime-data: 5xx/network retry
// once, 4xx terminal, 8s timeout.

import { PREDICTION_CACHE_BUST_INTERNAL_PATH } from "./prediction-kv-keys";
import type { Env, PredictCategory } from "./types";

const AUTH_HEADER = "x-pc-keiba-internal-token";
const DEFAULT_VIEWER_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const FETCH_TIMEOUT_MS = 8_000;
const MAX_ATTEMPTS = 2;
const SERVER_ERROR_STATUS_MIN = 500;
const RETRY_DELAY_MS = 200;

export interface PredictionCacheBustBody {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  source: "jra" | "nar";
  year: string;
}

interface OkOutcome {
  attempts: number;
  status: "ok";
}

interface ErrorOutcome {
  attempts: number;
  message: string;
  status: "error";
}

interface SkippedOutcome {
  message: string;
  status: "skipped";
}

export type PredictionCacheBustOutcome = ErrorOutcome | OkOutcome | SkippedOutcome;

interface AttemptOkOutcome {
  retryable: false;
  status: "ok";
}

interface AttemptErrorOutcome {
  message: string;
  retryable: boolean;
  status: "error";
}

type AttemptOutcome = AttemptErrorOutcome | AttemptOkOutcome;

interface BustLoopArgs {
  body: PredictionCacheBustBody;
  token: string;
  url: string;
}

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const resolveViewerOrigin = (env: Env): string => {
  const configured = env.PC_KEIBA_VIEWER_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_VIEWER_ORIGIN;
};

const buildRequestInit = (token: string, body: PredictionCacheBustBody): RequestInit => ({
  body: JSON.stringify(body),
  headers: {
    [AUTH_HEADER]: token,
    "content-type": "application/json",
  },
  method: "POST",
  signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
});

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const performAttempt = async (url: string, init: RequestInit): Promise<AttemptOutcome> => {
  try {
    const response = await fetch(url, init);
    if (response.ok) {
      return { retryable: false, status: "ok" };
    }
    return {
      message: `HTTP ${response.status}`,
      retryable: response.status >= SERVER_ERROR_STATUS_MIN,
      status: "error",
    };
  } catch (error) {
    return { message: formatError(error), retryable: true, status: "error" };
  }
};

const reduceAttempts =
  (args: BustLoopArgs) =>
  async (chain: Promise<AttemptOutcome[]>, index: number): Promise<AttemptOutcome[]> => {
    const previous = await chain;
    const last = previous.at(-1);
    if (last?.status === "ok") {
      return previous;
    }
    if (last && !last.retryable) {
      return previous;
    }
    if (index > 0) {
      await sleep(RETRY_DELAY_MS);
    }
    const outcome = await performAttempt(args.url, buildRequestInit(args.token, args.body));
    return [...previous, outcome];
  };

const runWithRetry = async (args: BustLoopArgs): Promise<AttemptOutcome[]> =>
  Array.from({ length: MAX_ATTEMPTS }, (_, index) => index).reduce<Promise<AttemptOutcome[]>>(
    reduceAttempts(args),
    Promise.resolve([]),
  );

export const predictionCacheSourceForCategory = (category: PredictCategory): "jra" | "nar" =>
  category === "jra" ? "jra" : "nar";

export const triggerPredictionCacheBust = async (
  env: Env,
  body: PredictionCacheBustBody,
): Promise<PredictionCacheBustOutcome> => {
  const token = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN?.trim();
  if (!token) {
    return { message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured", status: "skipped" };
  }
  const url = `${resolveViewerOrigin(env)}${PREDICTION_CACHE_BUST_INTERNAL_PATH}`;
  const attempts = await runWithRetry({ body, token, url });
  const last = attempts[attempts.length - 1]!;
  if (last.status === "ok") {
    return { attempts: attempts.length, status: "ok" };
  }
  return { attempts: attempts.length, message: last.message, status: "error" };
};
