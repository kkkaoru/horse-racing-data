// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, expect, it, vi } from "vitest";

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<(options: { async: boolean }) => unknown>(),
}));

vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import {
  safeGetCloudflareEnv,
  safeGetCloudflareExecutionContext,
  safeGetCloudflareRuntime,
} from "./cloudflare-context.server";

afterEach(() => {
  getCloudflareContextMock.mockReset();
  vi.unstubAllEnvs();
});

it("safeGetCloudflareRuntime uses the synchronous global context in production", async () => {
  vi.stubEnv("NODE_ENV", "production");
  const ctx = { waitUntil: vi.fn<(promise: Promise<unknown>) => void>() };
  const env = { DETAIL_SECTION_CACHE_KV: { mark: "kv" } };
  getCloudflareContextMock.mockReturnValue({ ctx, env });
  const runtime = await safeGetCloudflareRuntime();
  expect(runtime).toStrictEqual({ ctx, env });
  expect(getCloudflareContextMock).toHaveBeenCalledWith({ async: false });
});

it("safeGetCloudflareRuntime does not start a Wrangler context when production global lookup fails", async () => {
  vi.stubEnv("NODE_ENV", "production");
  getCloudflareContextMock.mockImplementation(() => {
    throw new Error("missing production context");
  });
  const runtime = await safeGetCloudflareRuntime();
  expect(runtime).toStrictEqual({ ctx: null, env: null });
  expect(getCloudflareContextMock).toHaveBeenCalledTimes(1);
  expect(getCloudflareContextMock).toHaveBeenCalledWith({ async: false });
});

it("safeGetCloudflareRuntime falls back to the asynchronous Wrangler context in development", async () => {
  vi.stubEnv("NODE_ENV", "development");
  const ctx = { waitUntil: vi.fn<(promise: Promise<unknown>) => void>() };
  const env = { DETAIL_SECTION_CACHE_KV: { mark: "kv" } };
  getCloudflareContextMock
    .mockImplementationOnce(() => {
      throw new Error("missing development global context");
    })
    .mockResolvedValueOnce({ ctx, env });
  const runtime = await safeGetCloudflareRuntime();
  expect(runtime).toStrictEqual({ ctx, env });
  expect(getCloudflareContextMock).toHaveBeenNthCalledWith(1, { async: false });
  expect(getCloudflareContextMock).toHaveBeenNthCalledWith(2, { async: true });
});

it("safeGetCloudflareRuntime returns ctx and env when wrangler resolves successfully", async () => {
  const ctx = { waitUntil: vi.fn<(promise: Promise<unknown>) => void>() };
  const env = { DETAIL_SECTION_CACHE_KV: { mark: "kv" } };
  getCloudflareContextMock.mockResolvedValue({ ctx, env });
  const runtime = await safeGetCloudflareRuntime();
  expect(runtime).toStrictEqual({ ctx, env });
});

it("safeGetCloudflareRuntime degrades to nulls when wrangler throws an APIError", async () => {
  getCloudflareContextMock.mockRejectedValue(
    new Error(
      "A request to the Cloudflare API (/accounts/test/workers/subdomain/edge-preview) failed.",
    ),
  );
  const runtime = await safeGetCloudflareRuntime();
  expect(runtime).toStrictEqual({ ctx: null, env: null });
});

it("safeGetCloudflareRuntime normalises missing ctx and env to null", async () => {
  getCloudflareContextMock.mockResolvedValue({ ctx: undefined, env: undefined });
  const runtime = await safeGetCloudflareRuntime();
  expect(runtime).toStrictEqual({ ctx: null, env: null });
});

it("safeGetCloudflareEnv returns env on the success path", async () => {
  const env = { PADDOCK_STATE_KV: { mark: "kv" } };
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env });
  const result = await safeGetCloudflareEnv();
  expect(result).toBe(env);
});

it("safeGetCloudflareEnv returns null when wrangler throws", async () => {
  getCloudflareContextMock.mockRejectedValue(new Error("nope"));
  const result = await safeGetCloudflareEnv();
  expect(result).toBe(null);
});

it("safeGetCloudflareExecutionContext returns ctx on the success path", async () => {
  const ctx = { waitUntil: vi.fn<(promise: Promise<unknown>) => void>() };
  getCloudflareContextMock.mockResolvedValue({ ctx, env: null });
  const result = await safeGetCloudflareExecutionContext();
  expect(result).toBe(ctx);
});

it("safeGetCloudflareExecutionContext returns null when wrangler throws", async () => {
  getCloudflareContextMock.mockRejectedValue(new Error("nope"));
  const result = await safeGetCloudflareExecutionContext();
  expect(result).toBe(null);
});
