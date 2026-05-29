import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import {
  applyPaddockAction,
  createPaddockState,
  getRacePaddockKey,
  isPaddockAction,
  isPaddockState,
  type PaddockAction,
  type PaddockState,
} from "./paddock";
import { getProductionLiveRelayOrigin } from "./production-access.server";
import { fetchProductionApi, useProductionApiProxy } from "./production-api-proxy.server";

export interface PaddockRaceParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}

const memoryStates = new Map<string, PaddockState>();

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

const getPaddockApiPath = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  year,
}: PaddockRaceParams): string =>
  `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/paddock`;

const getPaddockRoom = async (raceKey: string): Promise<PcKeibaDurableObjectStub | null> => {
  if (useProductionApiProxy()) {
    return null;
  }
  const env = await safeGetCloudflareEnv();
  if (!env?.PADDOCK_ROOM) {
    return null;
  }
  return env.PADDOCK_ROOM.get(env.PADDOCK_ROOM.idFromName(raceKey));
};

export const getPaddockLiveUrl = async (params: PaddockRaceParams): Promise<string | null> => {
  if (useProductionApiProxy()) {
    const relayOrigin = getProductionLiveRelayOrigin();
    if (!relayOrigin) {
      return null;
    }
    return `${relayOrigin}${getPaddockApiPath(params)}/live`;
  }
  const env = await safeGetCloudflareEnv();
  if (!env?.PADDOCK_ROOM) {
    return null;
  }
  return `${getPaddockApiPath(params)}/live`;
};

export const isPaddockRealtimeAvailable = async (): Promise<boolean> =>
  useProductionApiProxy() || Boolean((await safeGetCloudflareEnv())?.PADDOCK_ROOM);

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
  if (useProductionApiProxy()) {
    const response = await fetchProductionApi(getPaddockApiPath(params));
    if (!response.ok) {
      throw new Error(`remote paddock state ${response.status}`);
    }
    const payload: unknown = await response.json();
    if (!isPaddockState(payload)) {
      throw new Error("invalid remote paddock state");
    }
    return payload;
  }

  const raceKey = getRacePaddockKey(params);
  const room = await getPaddockRoom(raceKey);
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
  if (useProductionApiProxy()) {
    const response = await fetchProductionApi(getPaddockApiPath(params), {
      body: JSON.stringify(action),
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
  }

  const raceKey = getRacePaddockKey(params);
  const room = await getPaddockRoom(raceKey);
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

export const connectPaddockRoom = async (
  params: PaddockRaceParams,
  request: Request,
): Promise<Response | null> => {
  if (useProductionApiProxy()) {
    return null;
  }
  const raceKey = getRacePaddockKey(params);
  const room = await getPaddockRoom(raceKey);
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
