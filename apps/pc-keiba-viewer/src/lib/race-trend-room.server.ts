// Run with bun (vitest).
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type { RaceSource } from "./codes";
import { fetchProductionApi, useProductionApiProxy } from "./production-api-proxy.server";
import { RACE_TREND_CACHE_REFRESH_PARAM } from "./race-trend-cache";

export interface RaceTrendRoomParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

export interface RaceTrendRoomEvent {
  cacheKey: string;
}

export interface NotifyRaceTrendRoomIfChangedArgs {
  body: string;
  event: RaceTrendRoomEvent;
  params: RaceTrendRoomParams;
}

const LAST_HASH_KV_PREFIX = "race-trend-last-hash:";
const LAST_HASH_TTL_SECONDS = 300;
const HEX_RADIX = 16;
const HEX_PAD = 2;
const HEX_PAD_CHAR = "0";

export const isRaceTrendRoomParams = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RaceTrendRoomParams): boolean =>
  (source === "jra" || source === "nar") &&
  /^\d{4}$/u.test(year) &&
  /^\d{2}$/u.test(month) &&
  /^\d{2}$/u.test(day) &&
  /^[0-9A-Z]{2}$/u.test(keibajoCode) &&
  /^\d{2}$/u.test(raceNumber);

const getRaceTrendRoomKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RaceTrendRoomParams): string => `${source}:${year}${month}${day}:${keibajoCode}:${raceNumber}`;

const getTrendsApiPath = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RaceTrendRoomParams): string => {
  const params = new URLSearchParams({
    source,
    [RACE_TREND_CACHE_REFRESH_PARAM]: "1",
  });
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends?${params.toString()}`;
};

const getRaceTrendRoom = async (
  params: RaceTrendRoomParams,
): Promise<PcKeibaDurableObjectStub | null> => {
  if (useProductionApiProxy()) {
    return null;
  }
  const env = await safeGetCloudflareEnv();
  if (!env?.RACE_TREND_ROOM) {
    return null;
  }
  const raceKey = getRaceTrendRoomKey(params);
  return env.RACE_TREND_ROOM.get(env.RACE_TREND_ROOM.idFromName(raceKey));
};

const getRoomRequest = (
  operation: "state" | "update" | "ws",
  raceKey: string,
  init?: RequestInit,
): Request =>
  new Request(`https://race-trend-room/${operation === "ws" ? "ws" : ""}?raceKey=${raceKey}`, init);

export const connectRaceTrendRoom = async (
  params: RaceTrendRoomParams,
  request: Request,
): Promise<Response | null> => {
  if (useProductionApiProxy()) {
    return null;
  }
  const room = await getRaceTrendRoom(params);
  if (!room) {
    return null;
  }
  return room.fetch(
    getRoomRequest("ws", getRaceTrendRoomKey(params), {
      headers: request.headers,
      method: request.method,
    }),
  );
};

export const notifyRaceTrendRoom = async (
  params: RaceTrendRoomParams,
  event: RaceTrendRoomEvent,
): Promise<boolean> => {
  if (useProductionApiProxy()) {
    void event;
    const response = await fetchProductionApi(getTrendsApiPath(params));
    return response.ok;
  }
  const room = await getRaceTrendRoom(params);
  if (!room) {
    return false;
  }
  const response = await room.fetch(
    getRoomRequest("update", getRaceTrendRoomKey(params), {
      body: JSON.stringify(event),
      headers: { "content-type": "application/json" },
      method: "POST",
    }),
  );
  return response.ok;
};

// SHA-256 hex digest of the trend response body. Used to short-circuit
// notifyRaceTrendRoom when the rebuilt payload matches the last broadcast
// payload, which is the root cause of the race-trend notify-storm
// (browser refreshCache=1 -> rebuild -> notify -> refreshCache=1 ...).
export const computeRaceTrendBodyHash = async (body: string): Promise<string> => {
  const buffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(body));
  const bytes = Array.from(new Uint8Array(buffer));
  return bytes.map((byte) => byte.toString(HEX_RADIX).padStart(HEX_PAD, HEX_PAD_CHAR)).join("");
};

const getLastHashKvKey = (cacheKey: string): string => `${LAST_HASH_KV_PREFIX}${cacheKey}`;

const readLastHash = async (kv: PcKeibaKvNamespace, cacheKey: string): Promise<string | null> => {
  try {
    return await kv.get(getLastHashKvKey(cacheKey));
  } catch {
    return null;
  }
};

const writeLastHash = async (
  kv: PcKeibaKvNamespace,
  cacheKey: string,
  hash: string,
): Promise<void> => {
  try {
    await kv.put(getLastHashKvKey(cacheKey), hash, { expirationTtl: LAST_HASH_TTL_SECONDS });
  } catch {
    // KV write failure is non-fatal: we still notify so subscribers re-render.
  }
};

// Notify the DO room only when the rebuilt body hash differs from the last
// broadcast hash recorded in DETAIL_SECTION_CACHE_KV. When the KV binding is
// unavailable (local dev without bindings, production-api proxy) we degrade
// to the unconditional notifyRaceTrendRoom path so behaviour stays correct.
export const notifyRaceTrendRoomIfChanged = async ({
  body,
  event,
  params,
}: NotifyRaceTrendRoomIfChangedArgs): Promise<boolean> => {
  const env = await safeGetCloudflareEnv();
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) {
    return notifyRaceTrendRoom(params, event);
  }
  const hash = await computeRaceTrendBodyHash(body);
  const lastHash = await readLastHash(kv, event.cacheKey);
  if (lastHash === hash) {
    return false;
  }
  await writeLastHash(kv, event.cacheKey, hash);
  return notifyRaceTrendRoom(params, event);
};
