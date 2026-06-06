// Run with bun. Shared server-only loader that builds the realtime payload
// for a single race. Used by both the `/api/.../realtime` route handler and
// the SSR seed inside `race-detail-page.tsx` so the viewer can paint odds /
// horse weights immediately on the first server response instead of waiting
// for the client-side SSE poll. Subrequest-friendly: it talks to the
// `REALTIME_HOT` and `REALTIME_DATA` service bindings (and the legacy
// `REALTIME_DB` D1) directly via the Cloudflare env — no recursive HTTP hop
// back into this Worker. A whole-pipeline `Promise.race` timeout guards SSR
// latency.
import "server-only";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";

import type { RaceSource } from "./codes";
import { fetchHorseWeightsFromD1 } from "./horse-weight-d1-fallback.server";
import {
  buildRealtimePayloadFromHot,
  HOT_WORKER_ORIGIN,
  type HotOddsPayload,
  isHotOddsPayload,
} from "./hot-odds-payload.server";

export interface RealtimePayloadRequest {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

export interface HorseWeightEntry {
  changeAmount: number | null;
  changeSign: string | null;
  horseName: string | null;
  horseNumber: string;
  weight: number | null;
}

export interface HorseWeightSnapshot {
  fetchedAt: string;
  horses: HorseWeightEntry[];
}

interface FetchHorseWeightsLatestParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  realtimeData: { fetch: typeof fetch };
  source: RaceSource;
  year: string;
}

interface ResolveHorseWeightsParams {
  db: PcKeibaD1Database | undefined;
  fromDO: HorseWeightSnapshot | null;
  raceKey: string;
}

export interface BuildRealtimePayloadForRequestParams {
  env: CloudflareEnv | null | undefined;
  request: RealtimePayloadRequest;
}

export interface LoadInitialRealtimePayloadServerParams extends BuildRealtimePayloadForRequestParams {
  timeoutMs?: number;
}

const REALTIME_DATA_ORIGIN = "https://realtime";
const HORSE_WEIGHTS_LATEST_OK_STATUS = 200;
const DEFAULT_SSR_TIMEOUT_MS = 3_000;

const padRaceNumber = (raceNumber: string): string => raceNumber.padStart(2, "0");

const isHorseWeightSnapshot = (value: unknown): value is HorseWeightSnapshot => {
  if (typeof value !== "object" || value === null) return false;
  const fetchedAt: unknown = Reflect.get(value, "fetchedAt");
  const horses: unknown = Reflect.get(value, "horses");
  return typeof fetchedAt === "string" && Array.isArray(horses);
};

export const buildRaceKey = (request: RealtimePayloadRequest): string =>
  `${request.source}:${request.year}:${request.month}${request.day}:${request.keibajoCode}:${padRaceNumber(request.raceNumber)}`;

const buildHorseWeightsLatestUrl = (
  params: Omit<FetchHorseWeightsLatestParams, "realtimeData">,
): string =>
  `${REALTIME_DATA_ORIGIN}/api/${params.source}/races/${params.year}/${params.month}/${params.day}/${params.keibajoCode}/${padRaceNumber(params.raceNumber)}/horse-weights-latest`;

export const fetchHorseWeightsLatest = async (
  params: FetchHorseWeightsLatestParams,
): Promise<HorseWeightSnapshot | null> => {
  try {
    const response = await params.realtimeData.fetch(buildHorseWeightsLatestUrl(params));
    if (response.status !== HORSE_WEIGHTS_LATEST_OK_STATUS) {
      return null;
    }
    const json: unknown = await response.json();
    return isHorseWeightSnapshot(json) ? json : null;
  } catch {
    return null;
  }
};

export const fetchOddsFromHot = async (
  hot: CloudflareEnv["REALTIME_HOT"],
  raceKey: string,
): Promise<HotOddsPayload | null> => {
  if (!hot) {
    return null;
  }
  try {
    const response = await hot.fetch(`${HOT_WORKER_ORIGIN}/api/odds/${raceKey}`);
    if (!response.ok) {
      return null;
    }
    const json: unknown = await response.json();
    return isHotOddsPayload(json) ? json : null;
  } catch {
    return null;
  }
};

export const resolveHorseWeights = async (
  params: ResolveHorseWeightsParams,
): Promise<HorseWeightSnapshot | null> => {
  if (params.fromDO !== null) return params.fromDO;
  if (params.db === undefined) return null;
  try {
    return await fetchHorseWeightsFromD1({ db: params.db, raceKey: params.raceKey });
  } catch {
    return null;
  }
};

const fetchHorseWeightsForRequest = (
  realtimeData: { fetch: typeof fetch } | undefined,
  request: RealtimePayloadRequest,
): Promise<HorseWeightSnapshot | null> =>
  realtimeData
    ? fetchHorseWeightsLatest({
        day: request.day,
        keibajoCode: request.keibajoCode,
        month: request.month,
        raceNumber: request.raceNumber,
        realtimeData,
        source: request.source,
        year: request.year,
      })
    : Promise.resolve(null);

export const buildRealtimePayloadForRequest = async (
  params: BuildRealtimePayloadForRequestParams,
): Promise<RealtimeRacePayload> => {
  const raceKey = buildRaceKey(params.request);
  const env = params.env ?? null;
  const [odds, fromDO] = await Promise.all([
    fetchOddsFromHot(env?.REALTIME_HOT, raceKey),
    fetchHorseWeightsForRequest(env?.REALTIME_DATA, params.request),
  ]);
  const horseWeights = await resolveHorseWeights({ db: env?.REALTIME_DB, fromDO, raceKey });
  const payload = buildRealtimePayloadFromHot(raceKey, odds);
  return horseWeights === null ? payload : { ...payload, horseWeights };
};

const sleepThenNull = (timeoutMs: number): Promise<null> =>
  new Promise((resolve) => {
    setTimeout(() => resolve(null), timeoutMs);
  });

export const loadInitialRealtimePayloadServer = async (
  params: LoadInitialRealtimePayloadServerParams,
): Promise<RealtimeRacePayload | null> => {
  const timeoutMs = params.timeoutMs ?? DEFAULT_SSR_TIMEOUT_MS;
  try {
    return await Promise.race([
      buildRealtimePayloadForRequest({ env: params.env, request: params.request }),
      sleepThenNull(timeoutMs),
    ]);
  } catch {
    return null;
  }
};
