// Run with: bun run --filter mlflow-ui-proxy test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("./auth", () => ({
  isAuthorized: vi.fn(),
  unauthorizedResponse: vi.fn(() => new Response("Unauthorized", { status: 401 })),
}));

vi.mock("./proxy", () => ({
  proxyRequest: vi.fn(async () => new Response("proxied", { status: 200 })),
}));

import { isAuthorized, unauthorizedResponse } from "./auth";
import { proxyRequest } from "./proxy";
import worker from "./worker";
import type { Env } from "./types";

const TEST_ENV: Env = {
  MLFLOW_ORIGIN: "https://mlflow-origin.test",
  MLFLOW_UI_USERNAME: "operator",
  MLFLOW_UI_PASSWORD: "s3cret",
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("worker.fetch returns unauthorizedResponse's result when isAuthorized resolves false", async () => {
  vi.mocked(isAuthorized).mockResolvedValue(false);
  const request = new Request("https://worker.test/");
  const response = await worker.fetch(request, TEST_ENV);
  expect(response.status).toBe(401);
  expect(vi.mocked(unauthorizedResponse)).toHaveBeenCalledTimes(1);
  expect(vi.mocked(proxyRequest)).not.toHaveBeenCalled();
});

it("worker.fetch delegates to proxyRequest with the same request and env when authorized", async () => {
  vi.mocked(isAuthorized).mockResolvedValue(true);
  const request = new Request("https://worker.test/api/2.0/mlflow/experiments/list");
  const response = await worker.fetch(request, TEST_ENV);
  expect(response.status).toBe(200);
  await expect(response.text()).resolves.toBe("proxied");
  expect(vi.mocked(proxyRequest)).toHaveBeenCalledWith(request, TEST_ENV);
});
