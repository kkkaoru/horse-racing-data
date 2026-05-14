export interface RetryOptions {
  attempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  shouldRetry?: (error: unknown, attempt: number) => boolean;
  sleep?: (ms: number) => Promise<void>;
}

interface ResolvedRetryOptions {
  attempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  shouldRetry: (error: unknown, attempt: number) => boolean;
  sleep: (ms: number) => Promise<void>;
}

const DEFAULT_ATTEMPTS = 3;
const DEFAULT_BASE_DELAY_MS = 200;
const DEFAULT_MAX_DELAY_MS = 2000;
const EXPONENTIAL_BASE = 2;

const defaultShouldRetry = (): boolean => true;

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const resolveOptions = (options: RetryOptions | undefined): ResolvedRetryOptions => ({
  attempts: options?.attempts ?? DEFAULT_ATTEMPTS,
  baseDelayMs: options?.baseDelayMs ?? DEFAULT_BASE_DELAY_MS,
  maxDelayMs: options?.maxDelayMs ?? DEFAULT_MAX_DELAY_MS,
  shouldRetry: options?.shouldRetry ?? defaultShouldRetry,
  sleep: options?.sleep ?? defaultSleep,
});

const getDelayMs = (attempt: number, baseDelayMs: number, maxDelayMs: number): number =>
  Math.min(baseDelayMs * EXPONENTIAL_BASE ** attempt, maxDelayMs);

const runAttempt = async <T>(
  load: () => Promise<T>,
  options: ResolvedRetryOptions,
  attempt: number,
): Promise<T> => {
  try {
    return await load();
  } catch (error) {
    const isLastAttempt = attempt >= options.attempts - 1;
    if (isLastAttempt || !options.shouldRetry(error, attempt)) {
      throw error;
    }
    await options.sleep(getDelayMs(attempt, options.baseDelayMs, options.maxDelayMs));
    return runAttempt(load, options, attempt + 1);
  }
};

export const retry = <T>(load: () => Promise<T>, options?: RetryOptions): Promise<T> =>
  runAttempt(load, resolveOptions(options), 0);
