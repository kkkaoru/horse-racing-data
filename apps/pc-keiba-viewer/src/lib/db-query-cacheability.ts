// Run with bun. Decides whether a DB query result should be written to the
// shared query cache (Cloudflare Cache API + KV). An "empty" result
// (null / undefined / empty array) is treated as "data not available yet":
// when the upstream Neon mirror sync is delayed a query can transiently
// return no rows, and caching that would serve a stale "no data" state for
// up to the cache TTL. Skipping the write keeps the no-data window short
// while still letting populated results be cached aggressively.
export const isEmptyQueryResult = (value: unknown): boolean =>
  value === null || value === undefined || (Array.isArray(value) && value.length === 0);
