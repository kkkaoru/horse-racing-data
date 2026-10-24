// Runs with bun (Vitest) / Cloudflare Workers. Short-lived KV cache of heavy R2 SQL results.
import { createHash } from "node:crypto";
import type { Fetcher, KvStore } from "./types";

// A heavy endpoint runs several R2 SQL queries and is cached only when all of
// them succeed, so one stalled query used to discard the others and the next
// sweep recomputed every one. Successful answers are kept briefly so the
// next 15-minute sweep's retry reuses them, while current-day inputs (odds,
// scratches, runner lists) are never reused for longer than this.
export const R2_SQL_RESULT_TTL_SECONDS: number = 20 * 60;
const KEY_PREFIX = "r2sql-result:v1:";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

// Empty answers are cached too: every sweep used to pay for the same empty
// scan again. Trade-off: a race whose runners are not ingested yet stays
// empty for up to the TTL (the heatmap layer still never caches an empty
// heatmap, so it is rebuilt from these answers once they expire).
const isCacheableAnswer = (text: string): boolean => {
  try {
    const value: unknown = JSON.parse(text);
    return (
      isRecord(value) &&
      value.success === true &&
      isRecord(value.result) &&
      Array.isArray(value.result.rows)
    );
  } catch {
    return false;
  }
};

const resultKey = (body: string): string =>
  `${KEY_PREFIX}${createHash("sha256").update(body).digest("hex")}`;

const fetchAndStore = async (
  fetchImpl: Fetcher,
  kv: KvStore,
  key: string,
  input: Parameters<Fetcher>[0],
  init: RequestInit,
): Promise<Response> => {
  const response = await fetchImpl(input, init);
  if (!response.ok) return response;
  const text = await response.text();
  if (isCacheableAnswer(text))
    await kv.put(key, text, { expirationTtl: R2_SQL_RESULT_TTL_SECONDS }).catch(() => undefined);
  return new Response(text, { headers: response.headers, status: response.status });
};

// Wraps the R2 SQL fetcher; the request body (warehouse + SQL) is the key.
// Without a KV binding the fetcher is returned unchanged. With waitUntil, a
// scan the client stopped waiting for still finishes and is stored (within
// the runtime's grace period), so the next sweep does not pay for it again.
export const cachedR2SqlFetcher =
  (
    fetchImpl: Fetcher,
    kv: KvStore | undefined,
    waitUntil?: (promise: Promise<unknown>) => void,
  ): Fetcher =>
  async (input, init) => {
    const body = init?.body;
    if (kv === undefined || typeof body !== "string") return fetchImpl(input, init);
    const key = resultKey(body);
    const cached = await kv.get(key).catch(() => null);
    if (cached !== null && isCacheableAnswer(cached))
      return new Response(cached, { headers: { "Content-Type": "application/json" } });
    const stored = fetchAndStore(fetchImpl, kv, key, input, init!);
    waitUntil?.(stored.catch(() => undefined));
    return stored;
  };
