// Run with bun. Read live weights only while producing a warmed presentation.
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type { LiveHorseWeight } from "./horse-weight-class";
import {
  buildRaceKey,
  fetchHorseWeightsLatest,
  resolveHorseWeights,
  type RealtimePayloadRequest,
} from "./realtime-payload.server";

export const fetchHeatmapLiveWeights = async (
  request: RealtimePayloadRequest,
): Promise<LiveHorseWeight[]> => {
  const env = await safeGetCloudflareEnv();
  const fromDO = env?.REALTIME_DATA
    ? await fetchHorseWeightsLatest({ ...request, realtimeData: env.REALTIME_DATA })
    : null;
  // Reuse the realtime route's DO -> D1 fallback and unavailable-data policy.
  const snapshot = await resolveHorseWeights({
    db: env?.REALTIME_DB,
    fromDO,
    raceKey: buildRaceKey(request),
  });
  return snapshot?.horses.map(({ horseNumber, weight }) => ({ horseNumber, weight })) ?? [];
};
