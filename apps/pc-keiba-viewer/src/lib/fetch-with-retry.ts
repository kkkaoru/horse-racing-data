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

const resolveSameOriginGetUrl = (
  input: RequestInfo | URL,
  init: FetchInit | undefined,
): string | null => {
  if (typeof window === "undefined" || typeof XMLHttpRequest === "undefined") return null;
  const inputMethod = input instanceof Request ? input.method : "GET";
  const method = (init?.method ?? inputMethod).toUpperCase();
  if (method !== "GET" || init?.body !== undefined) return null;
  const inputUrl = input instanceof Request ? input.url : input.toString();
  const url = new URL(inputUrl, window.location.href);
  return url.origin === window.location.origin ? url.href : null;
};

const fetchSameOriginGetViaXhr = (url: string, init: FetchInit | undefined): Promise<Response> =>
  new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const signal = init?.signal;
    const rejectAborted = (): void => reject(new DOMException("aborted", "AbortError"));
    if (signal?.aborted) {
      rejectAborted();
      return;
    }
    const removeAbortListener = (): void => signal?.removeEventListener("abort", abortRequest);
    const abortRequest = (): void => xhr.abort();
    xhr.addEventListener("load", () => {
      removeAbortListener();
      const headers = new Headers();
      const contentType = xhr.getResponseHeader("content-type");
      if (contentType !== null) headers.set("content-type", contentType);
      resolve(
        new Response(xhr.responseText, {
          headers,
          status: xhr.status,
          statusText: xhr.statusText,
        }),
      );
    });
    xhr.addEventListener("error", () => {
      removeAbortListener();
      reject(new TypeError("XMLHttpRequest failed"));
    });
    xhr.addEventListener("abort", () => {
      removeAbortListener();
      rejectAborted();
    });
    signal?.addEventListener("abort", abortRequest, { once: true });
    xhr.open("GET", url);
    xhr.send();
  });

const buildRetryStatusError = (response: Response): Error =>
  new Error(`fetch retryable status: ${response.status} ${response.statusText}`.trim());

const performFetch = async (
  input: RequestInfo | URL,
  init: FetchInit | undefined,
  retryStatuses: ReadonlySet<number>,
): Promise<Response> => {
  let response: Response;
  try {
    response = await fetch(input, init);
  } catch (error) {
    const xhrUrl = error instanceof TypeError ? resolveSameOriginGetUrl(input, init) : null;
    if (xhrUrl === null) throw error;
    response = await fetchSameOriginGetViaXhr(xhrUrl, init);
  }
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
