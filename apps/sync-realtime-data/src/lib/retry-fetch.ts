// run with: bun run test
// Bounded exponential backoff fetch helper for transient HTTP failures.
// Retries 408 / 425 / 429 / 503 with `Retry-After` honored; other non-OK statuses
// and network errors propagate immediately on the failing attempt.

export interface RetryFetchOptions {
  init?: RequestInit;
  maxAttempts?: number;
  baseDelayMs?: number;
  capDelayMs?: number;
  retryableStatuses?: ReadonlySet<number>;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

interface ResolvedRetryConfig {
  init: RequestInit | undefined;
  maxAttempts: number;
  baseDelayMs: number;
  capDelayMs: number;
  retryableStatuses: ReadonlySet<number>;
  sleep: (ms: number) => Promise<void>;
  now: () => number;
}

interface BackoffDelayArgs {
  attempt: number;
  baseDelayMs: number;
  capDelayMs: number;
}

interface RetryDelayArgs {
  retryAfterHeader: string | null;
  attempt: number;
  baseDelayMs: number;
  capDelayMs: number;
  now: number;
}

export const DEFAULT_RETRYABLE_STATUSES: ReadonlySet<number> = new Set([408, 425, 429, 503]);
export const DEFAULT_MAX_ATTEMPTS = 3;
export const DEFAULT_BASE_DELAY_MS = 1000;
export const DEFAULT_CAP_DELAY_MS = 8000;
const MS_PER_SECOND = 1000;
const POWER_OF_TWO_BASE = 2;
const MIN_DELAY_MS = 0;
const NUMERIC_HEADER_PATTERN = /^-?\d+(?:\.\d+)?$/u;

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const defaultNow = (): number => Date.now();

const resolveConfig = (options: RetryFetchOptions | undefined): ResolvedRetryConfig => ({
  init: options?.init,
  maxAttempts: options?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
  baseDelayMs: options?.baseDelayMs ?? DEFAULT_BASE_DELAY_MS,
  capDelayMs: options?.capDelayMs ?? DEFAULT_CAP_DELAY_MS,
  retryableStatuses: options?.retryableStatuses ?? DEFAULT_RETRYABLE_STATUSES,
  sleep: options?.sleep ?? defaultSleep,
  now: options?.now ?? defaultNow,
});

export const computeBackoffDelay = (args: BackoffDelayArgs): number => {
  const raw = args.baseDelayMs * POWER_OF_TWO_BASE ** (args.attempt - 1);
  return Math.min(raw, args.capDelayMs);
};

export const parseRetryAfter = (rawHeader: string | null, nowMs: number): number | null => {
  if (rawHeader === null) {
    return null;
  }
  const trimmed = rawHeader.trim();
  if (trimmed.length === 0) {
    return null;
  }
  if (NUMERIC_HEADER_PATTERN.test(trimmed)) {
    const asSeconds = Number(trimmed);
    return asSeconds >= 0 ? Math.round(asSeconds * MS_PER_SECOND) : null;
  }
  const asDate = Date.parse(trimmed);
  if (!Number.isFinite(asDate)) {
    return null;
  }
  return Math.max(MIN_DELAY_MS, asDate - nowMs);
};

const resolveRetryDelay = (args: RetryDelayArgs): number => {
  const fromHeader = parseRetryAfter(args.retryAfterHeader, args.now);
  if (fromHeader !== null) {
    return Math.min(fromHeader, args.capDelayMs);
  }
  return computeBackoffDelay({
    attempt: args.attempt,
    baseDelayMs: args.baseDelayMs,
    capDelayMs: args.capDelayMs,
  });
};

const isRetryableStatus = (status: number, retryableStatuses: ReadonlySet<number>): boolean =>
  retryableStatuses.has(status);

const tryAttempt = async (url: string, config: ResolvedRetryConfig): Promise<Response> =>
  fetch(url, config.init);

const shouldRetry = (response: Response, attempt: number, config: ResolvedRetryConfig): boolean =>
  attempt < config.maxAttempts && isRetryableStatus(response.status, config.retryableStatuses);

const runAttempt = async (
  url: string,
  attempt: number,
  config: ResolvedRetryConfig,
): Promise<Response> => {
  const response = await tryAttempt(url, config);
  if (!shouldRetry(response, attempt, config)) {
    return response;
  }
  const delay = resolveRetryDelay({
    retryAfterHeader: response.headers.get("Retry-After"),
    attempt,
    baseDelayMs: config.baseDelayMs,
    capDelayMs: config.capDelayMs,
    now: config.now(),
  });
  await config.sleep(delay);
  return runAttempt(url, attempt + 1, config);
};

export const fetchWithRetry = async (
  url: string,
  options?: RetryFetchOptions,
): Promise<Response> => {
  const config = resolveConfig(options);
  return runAttempt(url, 1, config);
};
