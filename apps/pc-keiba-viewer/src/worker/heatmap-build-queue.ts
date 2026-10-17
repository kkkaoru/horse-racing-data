// Run with bun (vitest) / Cloudflare Workers runtime.
// Consumer of owned win-rate heatmap builds. The consumer's max_concurrency
// (wrangler.jsonc) is the global bound on concurrent heatmap computations.
import type { HeatmapBuildMessage } from "../lib/heatmap-execution";
import {
  isHeatmapWarmWorkflowParams,
  warmHeatmapRaceWithin,
  type HeatmapWarmFetch,
} from "../lib/win-rate-heatmap-warm-workflow";

export const HEATMAP_BUILD_QUEUE_NAME = "pc-keiba-heatmap-build";
// A bounded build can wait behind R2 SQL stall retries; aborting it at the
// old 120s wasted the queries it still held. Stay under the 15 min consumer
// wall-clock limit.
const HEATMAP_BUILD_TIMEOUT_MS = 10 * 60_000;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isHeatmapBuildMessage = (value: unknown): value is HeatmapBuildMessage =>
  isRecord(value) &&
  isHeatmapWarmWorkflowParams(value.params) &&
  value.params.ownerToken !== undefined;

const buildOne = async (fetchSelf: HeatmapWarmFetch, body: unknown): Promise<string> => {
  if (!isHeatmapBuildMessage(body)) return "invalid";
  return warmHeatmapRaceWithin({
    fetchSelf,
    params: body.params,
    raceNumber: body.params.raceNumbers[0]!,
    timeoutMs: HEATMAP_BUILD_TIMEOUT_MS,
  }).catch((error: unknown) => `failed: ${error instanceof Error ? error.message : String(error)}`);
};

// Every message is acknowledged after one attempt. A redelivery would carry an
// owner token that the first attempt already claimed, so it could never
// produce; the owner's backoff and the 15-minute sweep are the retry path.
export const handleHeatmapBuildQueue = async (
  batch: PcKeibaMessageBatch,
  fetchSelf: HeatmapWarmFetch,
): Promise<void> => {
  await Promise.all(
    batch.messages.map(async (message) => {
      const startedAt = Date.now();
      const status = await buildOne(fetchSelf, message.body);
      console.log(
        JSON.stringify({
          elapsedMs: Date.now() - startedAt,
          event: "heatmap_build",
          race: isHeatmapBuildMessage(message.body)
            ? `${message.body.params.year}${message.body.params.month}${message.body.params.day}:${message.body.params.keibajoCode}:${message.body.params.raceNumbers[0]!}`
            : null,
          status,
        }),
      );
      message.ack();
    }),
  );
};
