// Run with: bun run --filter sync-realtime-data-hot test
// KV-backed cache for the NAR "last race start at JST" lookup. The underlying
// D1 query (`select max(race_start_at_jst) from odds_fetch_state where
// source='nar' and kaisai_nen=? and kaisai_tsukihi=? and keibajo_code=?`) is
// invoked once per NAR fetch-odds job inside `resolveOddsSlotAt`. Each NAR
// venue's last-race-start is fixed by the morning Neon mirror (~08:30 JST),
// so re-querying D1 per fetch (~30 polls per race) is pure waste.
//
// The cached value can be `null` (no row for that venue/day yet — happens
// before the populate cron has seeded the day). We encode `null` distinctly
// from "key absent" using a sentinel string so a freshly-populated venue is
// re-queried on the next call instead of being pinned at `null` for the full
// TTL window.

import { getNarVenueLastRaceStartAtJst } from "./storage";
import type { Env } from "./types";

const KV_KEY_PREFIX = "nar:venue-last-start";
const KV_TTL_SECONDS = 21_600;
const KV_NULL_SENTINEL = "__null__";

interface VenueLastStartKey {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
}

const buildKvKey = ({ kaisaiNen, kaisaiTsukihi, keibajoCode }: VenueLastStartKey): string =>
  `${KV_KEY_PREFIX}:${kaisaiNen}${kaisaiTsukihi}:${keibajoCode}`;

const decodeCached = (raw: string | null): string | null | undefined => {
  if (raw === null) {
    return undefined;
  }
  return raw === KV_NULL_SENTINEL ? null : raw;
};

const encodeForKv = (value: string | null): string => value ?? KV_NULL_SENTINEL;

const readCached = async (env: Env, key: VenueLastStartKey): Promise<string | null | undefined> => {
  try {
    return decodeCached(await env.ODDS_HOT_KV.get(buildKvKey(key)));
  } catch {
    // KV read failure must never break the fetch path — return undefined to
    // signal a cache miss so the caller falls through to D1.
    return undefined;
  }
};

const writeCached = async (
  env: Env,
  key: VenueLastStartKey,
  value: string | null,
): Promise<void> => {
  try {
    await env.ODDS_HOT_KV.put(buildKvKey(key), encodeForKv(value), {
      expirationTtl: KV_TTL_SECONDS,
    });
  } catch {
    // KV write failure must never break the fetch path — the next call will
    // re-query D1 and retry the write.
  }
};

export const getCachedNarVenueLastRaceStartAtJst = async (
  env: Env,
  key: VenueLastStartKey,
): Promise<string | null> => {
  const cached = await readCached(env, key);
  if (cached !== undefined) {
    return cached;
  }
  const fresh = await getNarVenueLastRaceStartAtJst(
    env.REALTIME_HOT_DB,
    key.kaisaiNen,
    key.kaisaiTsukihi,
    key.keibajoCode,
  );
  await writeCached(env, key, fresh);
  return fresh;
};
