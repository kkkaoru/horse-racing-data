import { retry, type RetryOptions } from "./retry";

export interface FetchWithRetryOptions extends RetryOptions {
  retryStatuses?: ReadonlySet<number>;
}

const DEFAULT_RETRY_STATUSES: ReadonlySet<number> = new Set([408, 425, 429, 500, 502, 503, 504]);

const isAbortError = (error: unknown): boolean =>
  error instanceof DOMException && error.name === "AbortError";

const buildRetryStatusError = (response: Response): Error =>
  new Error(`fetch retryable status: ${response.status} ${response.statusText}`.trim());

const performFetch = async (
  input: RequestInfo | URL,
  init: RequestInit | undefined,
  retryStatuses: ReadonlySet<number>,
): Promise<Response> => {
  const response = await fetch(input, init);
  if (retryStatuses.has(response.status)) {
    throw buildRetryStatusError(response);
  }
  return response;
};

export const fetchWithRetry = (
  input: RequestInfo | URL,
  init?: RequestInit,
  options?: FetchWithRetryOptions,
): Promise<Response> => {
  const retryStatuses = options?.retryStatuses ?? DEFAULT_RETRY_STATUSES;
  return retry(() => performFetch(input, init, retryStatuses), {
    ...options,
    shouldRetry: (error, attempt) => {
      if (isAbortError(error)) {
        return false;
      }
      return options?.shouldRetry ? options.shouldRetry(error, attempt) : true;
    },
  });
};
