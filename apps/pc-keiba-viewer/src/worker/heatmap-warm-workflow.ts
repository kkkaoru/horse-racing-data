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
  PC_KEIBA_INTERNAL_TOKEN?: string;
  WORKER_SELF_REFERENCE?: SelfReferenceBinding;
}

export const createSelfFetch =
  (env: HeatmapWarmWorkflowEnv): HeatmapWarmFetch =>
  (request) =>
    env.WORKER_SELF_REFERENCE === undefined
      ? Promise.reject(new Error("WORKER_SELF_REFERENCE binding is unavailable"))
      : env.WORKER_SELF_REFERENCE.fetch(withInternalToken(request, env.PC_KEIBA_INTERNAL_TOKEN));

const withInternalToken = (request: Request, token: string | undefined): Request => {
  if (!token) throw new Error("PC_KEIBA_INTERNAL_TOKEN is unavailable");
  const headers = new Headers(request.headers);
  headers.set("x-pc-keiba-internal-token", token);
  return new Request(request, { headers });
};

export class HeatmapWarmWorkflow extends WorkflowEntrypoint<HeatmapWarmWorkflowEnv> {
  run(event: WorkflowEvent<unknown>, step: WorkflowStep): Promise<HeatmapWarmRaceResult[]> {
    return runHeatmapWarmWorkflow({
      fetchSelf: createSelfFetch(this.env),
      params: event.payload,
      step,
    });
  }
}
