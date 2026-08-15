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
