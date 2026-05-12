import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import {
  applyPaddockAction,
  createPaddockState,
  getRacePaddockKey,
  isPaddockAction,
  isPaddockState,
  type PaddockAction,
  type PaddockState,
} from "./paddock";

export interface PaddockRaceParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}

const memoryStates = new Map<string, PaddockState>();
const DEFAULT_REMOTE_PADDOCK_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";

export const isPaddockRaceParams = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  year,
}: PaddockRaceParams): boolean =>
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

const useLocalPlatformPaddockBindings = (): boolean =>
  process.env.NODE_ENV !== "development" ||
  process.env.PC_KEIBA_PADDOCK_LOCAL_PLATFORM_BINDINGS === "1";

const getPaddockRoom = (raceKey: string): PcKeibaDurableObjectStub | null => {
  if (!useLocalPlatformPaddockBindings()) {
    return null;
  }
  const env = getCloudflareEnv();
  if (!env?.PADDOCK_ROOM) {
    return null;
  }
  return env.PADDOCK_ROOM.get(env.PADDOCK_ROOM.idFromName(raceKey));
};

const useRemotePaddockBindings = (): boolean =>
  process.env.NODE_ENV === "development" &&
  process.env.PC_KEIBA_PADDOCK_REMOTE_BINDINGS !== "0" &&
  process.env.PC_KEIBA_PADDOCK_REMOTE_SERVER_PROXY === "1";

const getRemotePaddockOrigin = (): string =>
  process.env.PC_KEIBA_PADDOCK_REMOTE_ORIGIN ?? DEFAULT_REMOTE_PADDOCK_ORIGIN;

const getPaddockApiPath = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  year,
}: PaddockRaceParams): string =>
  `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/paddock`;

const getRemotePaddockApiUrl = (params: PaddockRaceParams, suffix = ""): string =>
  `${getRemotePaddockOrigin()}${getPaddockApiPath(params)}${suffix}`;

const fetchRemotePaddockState = async (params: PaddockRaceParams): Promise<PaddockState> => {
  const response = await fetch(getRemotePaddockApiUrl(params), { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`remote paddock state ${response.status}`);
  }
  const payload: unknown = await response.json();
  if (!isPaddockState(payload)) {
    throw new Error("invalid remote paddock state");
  }
  return payload;
};

const updateRemotePaddockState = async (
  params: PaddockRaceParams,
  action: PaddockAction,
): Promise<PaddockState> => {
  const response = await fetch(getRemotePaddockApiUrl(params), {
    body: JSON.stringify(action),
    cache: "no-store",
    headers: { "content-type": "application/json" },
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(`remote paddock update ${response.status}`);
  }
  const payload: unknown = await response.json();
  if (!isPaddockState(payload)) {
    throw new Error("invalid remote paddock state");
  }
  return payload;
};

export const getPaddockLiveUrl = (params: PaddockRaceParams): string | null => {
  if (!useRemotePaddockBindings()) {
    return null;
  }
  const liveUrl = new URL(getRemotePaddockApiUrl(params, "/live"));
  liveUrl.protocol = liveUrl.protocol === "http:" ? "ws:" : "wss:";
  return liveUrl.toString();
};

export const isPaddockRealtimeAvailable = (): boolean =>
  useRemotePaddockBindings() ||
  (useLocalPlatformPaddockBindings() && Boolean(getCloudflareEnv()?.PADDOCK_ROOM));

const getMemoryState = (raceKey: string): PaddockState => {
  const existing = memoryStates.get(raceKey);
  if (existing) {
    return existing;
  }
  const created = createPaddockState(raceKey);
  memoryStates.set(raceKey, created);
  return created;
};

const getRoomRequest = (
  operation: "state" | "update" | "ws",
  raceKey: string,
  init?: RequestInit,
): Request =>
  new Request(`https://paddock-room/${operation === "ws" ? "ws" : ""}?raceKey=${raceKey}`, init);

export const getPaddockState = async (params: PaddockRaceParams): Promise<PaddockState> => {
  if (useRemotePaddockBindings()) {
    return fetchRemotePaddockState(params);
  }

  const raceKey = getRacePaddockKey(params);
  const room = getPaddockRoom(raceKey);
  if (!room) {
    return getMemoryState(raceKey);
  }
  const response = await room.fetch(getRoomRequest("state", raceKey));
  if (!response.ok) {
    throw new Error(`paddock state ${response.status}`);
  }
  const payload: unknown = await response.json();
  if (!isPaddockState(payload)) {
    throw new Error("invalid paddock state");
  }
  return payload;
};

export const updatePaddockState = async (
  params: PaddockRaceParams,
  action: PaddockAction,
): Promise<PaddockState> => {
  if (useRemotePaddockBindings()) {
    return updateRemotePaddockState(params, action);
  }

  const raceKey = getRacePaddockKey(params);
  const room = getPaddockRoom(raceKey);
  if (!room) {
    const nextState = applyPaddockAction(getMemoryState(raceKey), action);
    memoryStates.set(raceKey, nextState);
    return nextState;
  }
  const response = await room.fetch(
    getRoomRequest("update", raceKey, {
      body: JSON.stringify(action),
      headers: { "content-type": "application/json" },
      method: "POST",
    }),
  );
  if (!response.ok) {
    throw new Error(`paddock update ${response.status}`);
  }
  const payload: unknown = await response.json();
  if (!isPaddockState(payload)) {
    throw new Error("invalid paddock state");
  }
  return payload;
};

export const connectPaddockRoom = (
  params: PaddockRaceParams,
  request: Request,
): Promise<Response> | null => {
  const raceKey = getRacePaddockKey(params);
  const room = getPaddockRoom(raceKey);
  if (!room) {
    return null;
  }
  return room.fetch(
    getRoomRequest("ws", raceKey, {
      headers: request.headers,
      method: request.method,
    }),
  );
};

export { isPaddockAction };
