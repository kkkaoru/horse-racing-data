// Run with bun. Fires the pc-keiba-viewer day-level trend cache bust endpoint
// when a race result row lands. The viewer drops the merged race-trend cache
// plus the d1-daily / d1-snapshot caches for the source x YMD, so the next
// request from any open trend page rebuilds the payload immediately.
//
// 2026-05-31 hardening: a single retry on 5xx / network error plus a fetch
// timeout. Without this a transiently overloaded viewer Worker silently
// swallows the bust signal: 11R results land in D1 yet the merged race-trend
// payload (used by the 12R detail "race trend" panel) keeps serving the stale
// "1R-10R" snapshot until natural TTL expiry.

import { formatError } from "./format-error";
import type { Env } from "./types";

const VIEWER_INTERNAL_BUST_PATH = "/api/internal/trend-cache-bust";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const DEFAULT_VIEWER_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const FETCH_TIMEOUT_MS = 8_000;
const MAX_ATTEMPTS = 2;
const SERVER_ERROR_STATUS_MIN = 500;
const RETRY_DELAY_MS = 200;

export type TrendBustSource = "jra" | "nar";

export interface TrendBustRequest {
  source: TrendBustSource;
  targetYmd: string;
}

export interface TrendBustOutcome {
  attempts?: number;
  message?: string;
  status: "error" | "ok" | "skipped";
}

interface TrendBustAttemptOutcome {
  message?: string;
  retryable: boolean;
  status: "error" | "ok";
}

interface BustLoopArgs {
  body: TrendBustRequest;
  token: string;
  url: string;
}

const isYyyymmdd = (value: string): boolean => /^\d{8}$/u.test(value);

const resolveViewerOrigin = (env: Env): string => {
  const configured = env.RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_VIEWER_ORIGIN;
};

const buildBustRequestInit = (token: string, body: TrendBustRequest): RequestInit => ({
  body: JSON.stringify(body),
  headers: {
    [AUTH_HEADER]: token,
    "content-type": "application/json",
  },
  method: "POST",
  signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
});

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const performBustAttempt = async (
  url: string,
  init: RequestInit,
): Promise<TrendBustAttemptOutcome> => {
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

const reduceBustAttempts =
  (args: BustLoopArgs) =>
  async (
    chain: Promise<TrendBustAttemptOutcome[]>,
    index: number,
  ): Promise<TrendBustAttemptOutcome[]> => {
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
    const outcome = await performBustAttempt(args.url, buildBustRequestInit(args.token, args.body));
    return [...previous, outcome];
  };

const runBustWithRetry = async (args: BustLoopArgs): Promise<TrendBustAttemptOutcome[]> =>
  Array.from({ length: MAX_ATTEMPTS }, (_, index) => index).reduce<
    Promise<TrendBustAttemptOutcome[]>
  >(reduceBustAttempts(args), Promise.resolve([]));

export const requestTrendCacheBust = async (
  env: Env,
  body: TrendBustRequest,
): Promise<TrendBustOutcome> => {
  if (!isYyyymmdd(body.targetYmd)) {
    return { message: `invalid targetYmd: ${body.targetYmd}`, status: "error" };
  }
  const token = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN?.trim();
  if (!token) {
    return { message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured", status: "skipped" };
  }
  const url = `${resolveViewerOrigin(env)}${VIEWER_INTERNAL_BUST_PATH}`;
  const attempts = await runBustWithRetry({ body, token, url });
  const last = attempts[attempts.length - 1]!;
  return last.status === "ok"
    ? { attempts: attempts.length, status: "ok" }
    : { attempts: attempts.length, message: last.message, status: "error" };
};

export interface RaceFinishContext {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  source: TrendBustSource;
}

export const buildTrendBustFromRaceContext = (context: RaceFinishContext): TrendBustRequest => ({
  source: context.source,
  targetYmd: `${context.kaisaiNen}${context.kaisaiTsukihi}`,
});
