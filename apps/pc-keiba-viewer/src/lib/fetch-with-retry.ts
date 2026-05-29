import { retry, type RetryOptions } from "./retry";

// Allow Cloudflare Workers fetch extensions (cf.cacheTtl, cf.cacheEverything, etc.)
// without forcing every caller to depend on @cloudflare/workers-types directly.
interface CloudflareFetchExtension {
  cf?: { cacheTtl?: number; cacheEverything?: boolean } & Record<string, unknown>;
}
export type FetchInit = RequestInit & CloudflareFetchExtension;

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
  init: FetchInit | undefined,
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
  init?: FetchInit,
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
