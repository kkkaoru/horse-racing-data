// Run with bun. Fires the pc-keiba-viewer per-race cache-bust endpoint when
// a race result row lands. The viewer drops both the main and stale-tier
// KV entries for every detail-section variant of that race plus bumps a
// generation counter so the Cache API tier cannot keep serving the old
// payload while the rebuild runs.
//
// Companion to `viewer-trend-cache-bust.ts` (the day-level signal). The
// race-level signal is what actually invalidated the section-cache after
// the 2026-06-28 outage: a stale section payload that survived 30-day KV
// TTL was hiding the rebuilt D1 row because the day-level bust only
// cleared the trend-cache namespace.
//
// Same retry semantics as the trend bust: 5xx / network errors get one
// retry, 4xx is terminal, network timeout is 8s.

import { formatError } from "./format-error";
import type { Env } from "./types";

const VIEWER_INTERNAL_BUST_PATH = "/api/internal/race-cache-bust";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const DEFAULT_VIEWER_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const FETCH_TIMEOUT_MS = 8_000;
const MAX_ATTEMPTS = 2;
const SERVER_ERROR_STATUS_MIN = 500;
const RETRY_DELAY_MS = 200;

export type RaceCacheBustSource = "jra" | "nar";

export interface RaceCacheBustBody {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  source: RaceCacheBustSource;
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

export type RaceCacheBustOutcome = ErrorOutcome | OkOutcome | SkippedOutcome;

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
  body: RaceCacheBustBody;
  token: string;
  url: string;
}

const RACE_KEY_PATTERN = /^(jra|nar):(\d{4})(\d{4}):(\d{2}):(\d{2})$/u;

export const parseRaceKey = (raceKey: string): RaceCacheBustBody | null => {
  const match = RACE_KEY_PATTERN.exec(raceKey);
  if (!match) return null;
  // Regex enforces `(jra|nar)` + non-optional fixed-width capture groups
  // so each `match[N]!` is statically guaranteed to be defined.
  const sourceMatch = match[1]!;
  const source: RaceCacheBustSource = sourceMatch === "jra" ? "jra" : "nar";
  return {
    keibajoCode: match[4]!,
    mmdd: match[3]!,
    raceBango: match[5]!,
    source,
    year: match[2]!,
  };
};

const resolveViewerOrigin = (env: Env): string => {
  const configured = env.RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_VIEWER_ORIGIN;
};

const buildRequestInit = (token: string, body: RaceCacheBustBody): RequestInit => ({
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

export const triggerRaceCacheBust = async (
  env: Env,
  body: RaceCacheBustBody,
): Promise<RaceCacheBustOutcome> => {
  const token = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN?.trim();
  if (!token) {
    return { message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured", status: "skipped" };
  }
  const url = `${resolveViewerOrigin(env)}${VIEWER_INTERNAL_BUST_PATH}`;
  const attempts = await runWithRetry({ body, token, url });
  const last = attempts[attempts.length - 1]!;
  if (last.status === "ok") {
    return { attempts: attempts.length, status: "ok" };
  }
  return { attempts: attempts.length, message: last.message, status: "error" };
};
