// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import { createSelfFetch, HeatmapWarmWorkflow } from "./heatmap-warm-workflow";

const executionContext: PcKeibaExecutionContext = { waitUntil: () => undefined };

it("rejects self fetches when the self reference binding is missing", async () => {
  await expect(createSelfFetch({})(new Request("https://pc-keiba-viewer.local/"))).rejects.toThrow(
    "WORKER_SELF_REFERENCE binding is unavailable",
  );
});

it("warms each race through the self reference binding", async () => {
  const selfFetch = vi.fn<(request: Request) => Promise<Response>>(
    async () => new Response("{}", { headers: { "X-Win-Rate-Heatmap-Cache": "HIT" } }),
  );
  const workflow = new HeatmapWarmWorkflow(executionContext, {
    WORKER_SELF_REFERENCE: { fetch: selfFetch },
  });
  const results = await workflow.run(
    {
      instanceId: "heatmap-20260923-30-16mqj",
      payload: {
        day: "23",
        keibajoCode: "30",
        month: "09",
        raceNumbers: ["01"],
        source: "nar",
        year: "2026",
      },
      timestamp: new Date(0),
    },
    { do: (_name, _config, callback) => callback() },
  );
  expect(results).toStrictEqual([{ raceNumber: "01", status: "hit" }]);
  expect(selfFetch.mock.calls[0]?.[0].url).toBe(
    "https://pc-keiba-viewer.local/api/races/2026/09/23/30/01/sections/win-rate-heatmap",
  );
});
