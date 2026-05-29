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
  event: { cacheKey: string },
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
