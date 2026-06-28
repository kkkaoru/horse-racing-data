// Run with: bun run --filter pipeline-health-monitor test
import { expect, it, vi } from "vitest";

import { fetchQueueHealth } from "./queue-health-client";
import type { Env } from "./types";

const buildEnv = (fetchMock: typeof fetch): Env =>
  ({
    REALTIME: { fetch: fetchMock },
    REALTIME_ADMIN_TOKEN: "test-token",
  }) as unknown as Env;

it("fetchQueueHealth returns the parsed JSON body on a 200 response", async () => {
  const fetchMock = vi.fn(
    async () =>
      new Response(
        JSON.stringify({
          lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
          lastSuccessfulFetchWeightsAt: "2026-06-28T05:55:00Z",
          racesQueuedNotFetchedToday: 3,
          racesStuckOverThirtyMin: 1,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
  );
  const env = buildEnv(fetchMock as unknown as typeof fetch);
  const metrics = await fetchQueueHealth(env);
  expect(metrics).toStrictEqual({
    lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
    lastSuccessfulFetchWeightsAt: "2026-06-28T05:55:00Z",
    racesQueuedNotFetchedToday: 3,
    racesStuckOverThirtyMin: 1,
  });
});

it("fetchQueueHealth sends a Bearer Authorization header with REALTIME_ADMIN_TOKEN", async () => {
  const capturedRequests: Request[] = [];
  const fetchMock = vi.fn(async (request: Request) => {
    capturedRequests.push(request);
    return new Response(
      JSON.stringify({
        lastSuccessfulFetchResultsAt: null,
        lastSuccessfulFetchWeightsAt: null,
        racesQueuedNotFetchedToday: 0,
        racesStuckOverThirtyMin: 0,
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  });
  const env = buildEnv(fetchMock as unknown as typeof fetch);
  await fetchQueueHealth(env);
  const calledRequest = capturedRequests[0];
  expect(calledRequest?.headers.get("authorization")).toBe("Bearer test-token");
  expect(calledRequest?.url).toBe(
    "https://sync-realtime-data.kkk4oru.com/api/internal/queue-health",
  );
});

it("fetchQueueHealth throws when the response is not ok", async () => {
  const fetchMock = vi.fn(async () => new Response("nope", { status: 503 }));
  const env = buildEnv(fetchMock as unknown as typeof fetch);
  await expect(fetchQueueHealth(env)).rejects.toThrow(
    "queue-health request failed with status 503",
  );
});
