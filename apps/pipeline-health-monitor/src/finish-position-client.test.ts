import { expect, it, vi } from "vitest";

import { fetchDeliveryCanaries, fetchPredictionReadiness } from "./finish-position-client";
import type { Env } from "./types";

const makeEnv = (response: Response): { env: Env; fetchMock: ReturnType<typeof vi.fn> } => {
  const fetchMock = vi.fn(async () => response);
  return {
    env: {
      FINISH_POSITION_CRON: { fetch: fetchMock },
      FINISH_POSITION_CRON_TOKEN: "secret",
    } as unknown as Env,
    fetchMock,
  };
};

const validRace = (deadline: string) => ({
  complete: false,
  deadline,
  expectedCount: 10,
  keibajoCode: "30",
  minutesToPost: 30,
  missingCount: 2,
  newestPredictionAt: "2026-08-15T00:01:00Z",
  oldestPredictionAt: null,
  predictionCount: 8,
  raceBango: "11",
  raceKey: "nar:30:11",
  raceStartAtJst: "2026-08-15T10:00:00+09:00",
  source: "nar",
});

it("fetches authenticated prediction readiness through the service binding", async () => {
  const { env, fetchMock } = makeEnv(
    Response.json({ checkedAt: "now", races: [], runYmd: "20260815" }),
  );
  await expect(fetchPredictionReadiness(env)).resolves.toMatchObject({ runYmd: "20260815" });
  const request = fetchMock.mock.calls[0]?.[0] as Request;
  expect(request.url).toBe(
    "https://finish-position-cron.internal/api/internal/prediction-readiness",
  );
  expect(request.headers.get("authorization")).toBe("Bearer secret");
});

it("accepts fully validated readiness races and delivery canaries", async () => {
  const readiness = makeEnv(
    Response.json({
      checkedAt: "now",
      races: [validRace("T-120"), validRace("T-60"), validRace("T-30"), validRace("post")],
      runYmd: "20260815",
    }),
  );
  await expect(fetchPredictionReadiness(readiness.env)).resolves.toMatchObject({
    runYmd: "20260815",
  });
  const canary = makeEnv(
    Response.json({
      canaries: [
        {
          consumedAt: "2026-08-15T00:01:00Z",
          deliveryLagMs: 60_000,
          enqueuedAt: "2026-08-15T00:00:00Z",
          id: "canary",
        },
      ],
      checkedAt: "now",
    }),
  );
  await expect(fetchDeliveryCanaries(canary.env)).resolves.toMatchObject({ checkedAt: "now" });
});

it("fetches delivery canaries and rejects non-success responses", async () => {
  const success = makeEnv(Response.json({ canaries: [], checkedAt: "now" }));
  await expect(fetchDeliveryCanaries(success.env)).resolves.toMatchObject({ canaries: [] });
  const failure = makeEnv(new Response("failed", { status: 503 }));
  await expect(fetchDeliveryCanaries(failure.env)).rejects.toThrow("status=503");
});

it("rejects a health catch-all response instead of treating it as readiness", async () => {
  const { env } = makeEnv(
    Response.json({ cron: "0 18 * * *", name: "finish-position-cron", ok: true }),
  );
  await expect(fetchPredictionReadiness(env)).rejects.toThrow(
    "prediction-readiness endpoint returned an unexpected response shape; prediction-readiness may not be deployed",
  );
});

it("rejects a health catch-all response instead of treating it as healthy canary data", async () => {
  const { env } = makeEnv(
    Response.json({ cron: "0 18 * * *", name: "finish-position-cron", ok: true }),
  );
  await expect(fetchDeliveryCanaries(env)).rejects.toThrow(
    "delivery-canaries endpoint returned an unexpected response shape; delivery-canaries may not be deployed",
  );
});

it("rejects malformed nested readiness races", async () => {
  const { env } = makeEnv(
    Response.json({
      checkedAt: "now",
      races: [{ complete: false, raceKey: "nar:30:11" }],
      runYmd: "20260815",
    }),
  );
  await expect(fetchPredictionReadiness(env)).rejects.toThrow(
    "prediction-readiness endpoint returned an unexpected response shape; prediction-readiness may not be deployed",
  );
});

it("rejects malformed nested delivery canaries", async () => {
  const { env } = makeEnv(
    Response.json({ canaries: [{ enqueuedAt: "now", id: "id" }], checkedAt: "now" }),
  );
  await expect(fetchDeliveryCanaries(env)).rejects.toThrow(
    "delivery-canaries endpoint returned an unexpected response shape; delivery-canaries may not be deployed",
  );
});
