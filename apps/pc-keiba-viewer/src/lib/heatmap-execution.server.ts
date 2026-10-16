// Run with bun (vitest) / Cloudflare Workers runtime.
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import { getCurrentGateMappingRevision } from "./gate-mapping-revision.server";
import {
  HEATMAP_ALGORITHM_REVISION,
  heatmapExecutionKey,
  type HeatmapExecutionRequest,
  type HeatmapExecutionOwner,
} from "./heatmap-execution";

export const getHeatmapExecutionOwner = async (
  request: HeatmapExecutionRequest,
): Promise<HeatmapExecutionOwner> => {
  const namespace = (await safeGetCloudflareEnv())?.HEATMAP_EXECUTION_OWNER;
  if (namespace === undefined) throw new Error("HEATMAP_EXECUTION_OWNER binding is unavailable");
  const key = heatmapExecutionKey(request);
  const owner = namespace.getByName(key);
  if (!/^[A-Z][0-9A-Z]$/u.test(request.keibajoCode)) return owner;
  return {
    ensure: async (input) => {
      // Only the private Catalog RPC supplies provenance; never accept caller revisions.
      const revision = await getCurrentGateMappingRevision(key).catch(() => undefined);
      return owner.ensure({
        ...input,
        dependencyRevision:
          revision === undefined ? undefined : HEATMAP_ALGORITHM_REVISION + ":" + revision,
      });
    },
    claim: (token) => owner.claim(token),
    settle: (token, success) => owner.settle(token, success),
  };
};

export const runOwnedHeatmap = async (input: {
  owner: HeatmapExecutionOwner;
  token: string;
  compute: () => Promise<Response>;
}): Promise<Response> => {
  if (!(await input.owner.claim(input.token))) return heatmapPendingResponse("running_or_unknown");
  const response = await input.compute().catch(async (error: unknown) => {
    await input.owner.settle(input.token, false);
    throw error;
  });
  if (!(await input.owner.settle(input.token, response.ok)))
    throw new Error("Heatmap completion ownership lost");
  return response;
};

export const heatmapPendingResponse = (status: string): Response => {
  // Only known retryable states may put the client into preparation polling.
  if (!["queued", "running", "backoff"].includes(status))
    return Response.json(
      {
        error: status === "blocked" ? "heatmap_execution_blocked" : "heatmap_execution_unknown",
        preparation: status,
      },
      {
        status: 503,
        headers: {
          "Cache-Control": "private, max-age=0, no-store",
          "X-Win-Rate-Heatmap-Cache": "ERROR",
        },
      },
    );
  return Response.json(
    { error: "heatmap_not_warmed", preparation: status },
    {
      status: 503,
      headers: {
        "Cache-Control": "private, max-age=0, no-store",
        "Retry-After": "30",
        "X-Win-Rate-Heatmap-Cache": "PENDING",
      },
    },
  );
};
