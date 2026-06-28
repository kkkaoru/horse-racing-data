// Run with bun. Stale-tier envelope helpers for the race-detail-section
// cache. Stale entries are wrapped in `{ payload, writtenAt }` so the read
// path can enforce a max-stale cap (4h) AND a JST-midnight boundary
// without depending on KV's `expirationTtl` (which the put path keeps at 30
// days so the SWR background-refresh path still has a body to serve).
//
// 2026-06-28: introduced after the fetch-results queue stalled for ~7h and
// the stale tier kept serving a body 6+ hours past the most recent D1
// write, hiding the recovery.

export interface StaleDetailSectionEnvelope {
  payload: string;
  writtenAt: number;
}

const HOURS_PER_DAY = 24;
const MINUTES_PER_HOUR = 60;
const SECONDS_PER_MINUTE = 60;
const MILLISECONDS_PER_SECOND = 1000;

const JST_OFFSET_HOURS = 9;
const JST_OFFSET_MS =
  JST_OFFSET_HOURS * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND;

// Max-stale cap: a stale entry whose writtenAt is older than this is
// treated as a cache miss even though the KV TTL has not expired yet.
const STALE_DETAIL_SECTION_MAX_AGE_HOURS = 4;
export const STALE_DETAIL_SECTION_MAX_AGE_MS =
  STALE_DETAIL_SECTION_MAX_AGE_HOURS *
  MINUTES_PER_HOUR *
  SECONDS_PER_MINUTE *
  MILLISECONDS_PER_SECOND;

const MS_PER_DAY = HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND;

// Most recent JST-midnight (Asia/Tokyo 00:00) less-than-or-equal nowMs,
// expressed as a UTC epoch. Any cache entry written strictly before this
// instant carries yesterday's data (the upstream cron writes that drive
// these caches roll over at JST midnight).
export const getJstMidnightMsForToday = (nowMs: number): number => {
  const jstNow = nowMs + JST_OFFSET_MS;
  const jstMidnight = Math.floor(jstNow / MS_PER_DAY) * MS_PER_DAY;
  return jstMidnight - JST_OFFSET_MS;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const isStaleDetailSectionEnvelope = (
  value: unknown,
): value is StaleDetailSectionEnvelope => {
  if (!isRecord(value)) return false;
  return (
    typeof value.payload === "string" &&
    typeof value.writtenAt === "number" &&
    Number.isFinite(value.writtenAt)
  );
};

// Legacy stale entries (written before the envelope migration) were raw
// payload strings. Those are treated as "expired" so the caller forces a
// recompute — same effect as the legacy TTL having lapsed.
export const parseStaleDetailSectionEnvelope = (raw: string): StaleDetailSectionEnvelope | null => {
  try {
    const parsed: unknown = JSON.parse(raw);
    return isStaleDetailSectionEnvelope(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

export const serializeStaleDetailSectionEnvelope = (payload: string, writtenAt: number): string =>
  JSON.stringify({ payload, writtenAt } satisfies StaleDetailSectionEnvelope);
