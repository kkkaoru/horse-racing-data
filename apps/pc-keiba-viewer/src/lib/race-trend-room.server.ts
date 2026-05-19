import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "./codes";

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

const useLocalPlatformBindings = (): boolean =>
  process.env.NODE_ENV !== "development" ||
  process.env.PC_KEIBA_RACE_TREND_LOCAL_PLATFORM_BINDINGS === "1";

const getRaceTrendRoomKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RaceTrendRoomParams): string => `${source}:${year}${month}${day}:${keibajoCode}:${raceNumber}`;

const getRaceTrendRoom = (params: RaceTrendRoomParams): PcKeibaDurableObjectStub | null => {
  if (!useLocalPlatformBindings()) {
    return null;
  }
  const env = getCloudflareEnv();
  const raceKey = getRaceTrendRoomKey(params);
  return env?.RACE_TREND_ROOM?.get(env.RACE_TREND_ROOM.idFromName(raceKey)) ?? null;
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
