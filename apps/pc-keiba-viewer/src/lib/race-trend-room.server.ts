import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "./codes";
import { RACE_TREND_CACHE_REFRESH_PARAM } from "./race-trend-cache";
import {
  fetchProductionApi,
  useProductionApiProxy,
} from "./production-api-proxy.server";

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

const getCloudflareEnv = (): CloudflareEnv | null => {
  try {
    return getCloudflareContext().env;
  } catch {
    return null;
  }
};

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

const getRaceTrendRoom = (params: RaceTrendRoomParams): PcKeibaDurableObjectStub | null => {
  if (useProductionApiProxy()) {
    return null;
  }
  const env = getCloudflareEnv();
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

export const connectRaceTrendRoom = (
  params: RaceTrendRoomParams,
  request: Request,
): Promise<Response> | null => {
  if (useProductionApiProxy()) {
    return null;
  }
  const room = getRaceTrendRoom(params);
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
  const room = getRaceTrendRoom(params);
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
