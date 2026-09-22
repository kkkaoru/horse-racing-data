// Run with bun (vitest) / Cloudflare Workers runtime.
// Cloudflare Workflow that warms win-rate heatmap caches for one venue-day.
import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";

import {
  runHeatmapWarmWorkflow,
  type HeatmapWarmFetch,
  type HeatmapWarmRaceResult,
} from "../lib/win-rate-heatmap-warm-workflow";

interface SelfReferenceBinding {
  fetch(request: Request): Promise<Response>;
}

interface HeatmapWarmWorkflowEnv {
  WORKER_SELF_REFERENCE?: SelfReferenceBinding;
}

export const createSelfFetch =
  (env: HeatmapWarmWorkflowEnv): HeatmapWarmFetch =>
  (request) =>
    env.WORKER_SELF_REFERENCE === undefined
      ? Promise.reject(new Error("WORKER_SELF_REFERENCE binding is unavailable"))
      : env.WORKER_SELF_REFERENCE.fetch(request);

export class HeatmapWarmWorkflow extends WorkflowEntrypoint<HeatmapWarmWorkflowEnv> {
  run(event: WorkflowEvent<unknown>, step: WorkflowStep): Promise<HeatmapWarmRaceResult[]> {
    return runHeatmapWarmWorkflow({
      fetchSelf: createSelfFetch(this.env),
      params: event.payload,
      step,
    });
  }
}
