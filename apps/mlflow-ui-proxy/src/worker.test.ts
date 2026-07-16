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
import worker, { buildSyncWindow, handleScheduled } from "./worker";
import type { Env, MlflowContainerStub, MlflowSyncResult } from "./types";

const SUCCESS_RESULT: MlflowSyncResult = {
  exitCode: 0,
  stderr: "",
  stdout: "fp runs created: 3",
};

const buildEnv = (
  container: MlflowContainerStub,
  getByName: (name: string) => MlflowContainerStub = vi.fn(() => container),
): Env => ({
  MLFLOW_CONTAINER: {
    getByName,
  },
  MLFLOW_UI_USERNAME: "operator",
  MLFLOW_UI_PASSWORD: "s3cret",
  HORSE_RACING_MLFLOW_BACKEND_URI: "postgresql://mlflow.test/mlflow",
  NEON_PRIMARY_URL: "postgresql://racing.test/racing",
  R2_ACCESS_KEY_ID: "access-key",
  R2_ACCOUNT_ID: "account-id",
  R2_SECRET_ACCESS_KEY: "secret-key",
  HORSE_RACING_MLFLOW_R2_BUCKET: "mlflow-artifacts",
  HORSE_RACING_MLFLOW_R2_PREFIX: "mlflow",
});

const buildContainer = (
  syncProductionPreview: MlflowContainerStub["syncProductionPreview"] = vi.fn(
    async () => SUCCESS_RESULT,
  ),
): MlflowContainerStub => ({
  fetch: vi.fn(async () => new Response("container", { status: 200 })),
  syncProductionPreview,
});

afterEach(() => {
  vi.clearAllMocks();
  vi.restoreAllMocks();
});

it("worker.fetch returns unauthorizedResponse's result when isAuthorized resolves false", async () => {
  vi.mocked(isAuthorized).mockResolvedValue(false);
  const env = buildEnv(buildContainer());
  const response = await worker.fetch(new Request("https://worker.test/"), env);
  expect(response.status).toBe(401);
  expect(vi.mocked(unauthorizedResponse)).toHaveBeenCalledTimes(1);
  expect(vi.mocked(proxyRequest)).not.toHaveBeenCalled();
});

it("worker.fetch delegates to proxyRequest with the same request and env when authorized", async () => {
  vi.mocked(isAuthorized).mockResolvedValue(true);
  const env = buildEnv(buildContainer());
  const request = new Request("https://worker.test/api/2.0/mlflow/experiments/list");
  const response = await worker.fetch(request, env);
  expect(response.status).toBe(200);
  await expect(response.text()).resolves.toBe("proxied");
  expect(vi.mocked(proxyRequest)).toHaveBeenCalledWith(request, env);
});

it("worker.fetch rejects public access to internal paths before authentication", async () => {
  const response = await worker.fetch(
    new Request("https://worker.test/__internal/sync"),
    buildEnv(buildContainer()),
  );
  expect(response.status).toBe(404);
  expect(vi.mocked(isAuthorized)).not.toHaveBeenCalled();
  expect(vi.mocked(proxyRequest)).not.toHaveBeenCalled();
});

it("buildSyncWindow uses JST calendar dates across the UTC day boundary", () => {
  const timestampMs = Date.parse("2026-07-16T15:05:00.000Z");
  expect(buildSyncWindow(timestampMs)).toStrictEqual({
    dateFrom: "20260717",
    dateTo: "20260719",
  });
});

it("handleScheduled invokes the primary container for today through two days ahead", async () => {
  const syncProductionPreview = vi.fn(async () => SUCCESS_RESULT);
  const container = buildContainer(syncProductionPreview);
  const getByName = vi.fn(() => container);
  const env = buildEnv(container, getByName);
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const result = await handleScheduled(
    { scheduledTime: Date.parse("2026-07-17T03:00:00.000Z") },
    env,
  );
  expect(getByName).toHaveBeenCalledWith("primary");
  expect(syncProductionPreview).toHaveBeenCalledWith("20260717", "20260719");
  expect(result).toStrictEqual(SUCCESS_RESULT);
  expect(consoleLog).toHaveBeenCalledWith(
    "[mlflow-sync] range=20260717..20260719 fp runs created: 3",
  );
});

it("handleScheduled throws when the container sync exits unsuccessfully", async () => {
  const failedResult: MlflowSyncResult = {
    exitCode: 1,
    stderr: "neon unavailable",
    stdout: "",
  };
  const env = buildEnv(buildContainer(vi.fn(async () => failedResult)));
  await expect(
    handleScheduled({ scheduledTime: Date.parse("2026-07-17T03:00:00.000Z") }, env),
  ).rejects.toThrow("MLflow production preview sync failed: neon unavailable");
});
