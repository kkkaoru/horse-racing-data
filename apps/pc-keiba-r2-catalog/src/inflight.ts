// Run with bun. Per-isolate single-flight de-duplication for expensive R2 SQL
// reads.
//
// A running-style feature build is a 44-CTE, ~60KB query that rescans a decade
// of raw Iceberg data (see running-style-sql.ts). When the same build is
// requested concurrently -- a retry landing on top of a still-running attempt,
// or several callers asking for the same venue-level slice -- issuing it twice
// doubles the R2 SQL load for an identical answer. This collapses those into a
// single upstream execution whose result (or rejection) every caller shares.
//
// Scope is deliberately one isolate: Workers gives no cross-isolate shared
// memory, so this dedupes retry storms and overlapping callers landing on the
// same isolate, NOT a cold fan-out of distinct races across many isolates.

const inflight = new Map<string, Promise<string>>();

export const inflightSize = (): number => inflight.size;

export const coalesce = (key: string, build: () => Promise<string>): Promise<string> => {
  const existing = inflight.get(key);
  if (existing !== undefined) return existing;
  // The map entry is cleared as soon as the upstream call settles, so the next
  // request re-executes rather than replaying a stale or failed result.
  const created = build().finally(() => {
    inflight.delete(key);
  });
  inflight.set(key, created);
  return created;
};
