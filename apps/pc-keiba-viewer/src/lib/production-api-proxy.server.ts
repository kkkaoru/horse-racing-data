import "server-only";
import {
  getProductionAccessHeaders,
  getProductionApiOrigin,
  useProductionApiProxy,
} from "./production-access.server";

export { useProductionApiProxy };

// Per-attempt timeout. Observed real-world response times for the upstream
// trends / realtime endpoints range from ~0.3s (cache hit) to ~8s (Hyperdrive
// cold start + populated 14-day window). A 10s cap absorbs that variance
// without making the total retry budget pathologically long.
const ATTEMPT_TIMEOUT_MS = 10000;
// Max attempts including the first try. Tries each request twice on
// transient network/timeout errors so the user-facing page is far more
// likely to render with real values rather than the empty fallback.
const MAX_ATTEMPTS = 2;
// Backoff between attempts so a momentary upstream blip has time to
// settle without blowing the SSR budget.
const RETRY_BACKOFF_MS = 200;
const isTransientStatus = (status: number): boolean => status === 502 || status === 503;
const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

export const buildProductionApiUrl = (path: string): string => {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${getProductionApiOrigin()}${normalizedPath}`;
};

const fetchOnce = async (
  url: string,
  baseInit: RequestInit | undefined,
  headers: Headers,
): Promise<Response> => {
  const init: RequestInit = {
    ...baseInit,
    cache: "no-store",
    headers,
    signal: baseInit?.signal ?? AbortSignal.timeout(ATTEMPT_TIMEOUT_MS),
  };
  return fetch(url, init);
};

interface AttemptContext {
  url: string;
  init: RequestInit | undefined;
  headers: Headers;
}

// DOMException (thrown by AbortSignal.timeout) has a getter-only `.message`.
// Next.js dev error reporting writes through `.message` when surfacing
// rejections, which crashes the request with
// `TypeError: Cannot set property message of [DOMException] which has only a
// getter`. Rewrap so the slot is writable while keeping the original as cause.
const toWritableError = (error: unknown): Error => {
  if (error instanceof Error) {
    const wrapped = new Error(error.message, { cause: error });
    wrapped.name = error.name;
    return wrapped;
  }
  return new Error(String(error));
};

const attemptFetch = async (context: AttemptContext, attempt: number): Promise<Response> => {
  const response = await fetchOnce(context.url, context.init, context.headers).catch(
    async (error: unknown) => {
      if (attempt >= MAX_ATTEMPTS) {
        throw toWritableError(error);
      }
      await sleep(RETRY_BACKOFF_MS);
      return attemptFetch(context, attempt + 1);
    },
  );
  if (!isTransientStatus(response.status) || attempt >= MAX_ATTEMPTS) {
    return response;
  }
  await sleep(RETRY_BACKOFF_MS);
  return attemptFetch(context, attempt + 1);
};

export const fetchProductionApi = async (path: string, init?: RequestInit): Promise<Response> => {
  const accessHeaders = getProductionAccessHeaders();
  if (!accessHeaders) {
    throw new Error("Production Access credentials are unavailable.");
  }
  const url = buildProductionApiUrl(path);
  const headers = new Headers(init?.headers);
  for (const [key, value] of Object.entries(accessHeaders)) {
    headers.set(key, value);
  }
  // Caller-supplied AbortSignal means "I own cancellation; don't retry."
  if (init?.signal) {
    return fetchOnce(url, init, headers);
  }
  return attemptFetch({ headers, init, url }, 1);
};
