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
  expect(runtime).toStrictEqual({
    ctx,
    env: { ...env, R2_CATALOG: undefined, R2_RACE_DETAIL: undefined },
  });
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

it("development catalog bindings pass URLs across the Request realm boundary", async () => {
  vi.stubEnv("NODE_ENV", "development");
  const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(new Response("ok"));
  getCloudflareContextMock.mockReturnValue({
    env: { R2_CATALOG: { fetch }, R2_RACE_DETAIL: { fetch } },
  });
  const env = await safeGetCloudflareEnv();
  const request = new Request("https://catalog.internal/v1/race-course", { redirect: "manual" });
  await env?.R2_CATALOG?.fetch(request);
  expect(fetch).toHaveBeenCalledWith("https://catalog.internal/v1/race-course", {
    method: "GET",
    headers: request.headers,
    body: null,
    redirect: "manual",
    signal: request.signal,
  });
  await env?.R2_RACE_DETAIL?.fetch("https://catalog.internal/v1/race-detail");
  expect(fetch).toHaveBeenLastCalledWith("https://catalog.internal/v1/race-detail", undefined);
});

it("development catalog bindings preserve POST bodies and explicit init overrides", async () => {
  vi.stubEnv("NODE_ENV", "development");
  const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(new Response("ok"));
  getCloudflareContextMock.mockReturnValue({ env: { R2_CATALOG: { fetch } } });
  const env = await safeGetCloudflareEnv();
  await env?.R2_CATALOG?.fetch(
    new Request("https://catalog.internal/read", { method: "POST", body: "query" }),
    { redirect: "error" },
  );
  expect(fetch.mock.calls[0]?.[0]).toBe("https://catalog.internal/read");
  expect(fetch.mock.calls[0]?.[1]).toMatchObject({
    method: "POST",
    redirect: "error",
    body: new TextEncoder().encode("query").buffer,
  });
});

it("production catalog bindings remain unchanged", async () => {
  vi.stubEnv("NODE_ENV", "production");
  const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(new Response("ok"));
  getCloudflareContextMock.mockReturnValue({ env: { R2_CATALOG: { fetch } } });
  const env = await safeGetCloudflareEnv();
  const request = new Request("https://catalog.internal/read");
  await env?.R2_CATALOG?.fetch(request);
  expect(fetch).toHaveBeenCalledWith(request);
});

it("safeGetCloudflareExecutionContext returns null when wrangler throws", async () => {
  getCloudflareContextMock.mockRejectedValue(new Error("nope"));
  const result = await safeGetCloudflareExecutionContext();
  expect(result).toBe(null);
});

it("development forwards private RPC with its receiver while converting POST Requests", async () => {
  vi.stubEnv("NODE_ENV", "development");
  const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(new Response("ok"));
  const binding: NonNullable<CloudflareEnv["R2_CATALOG"]> = {
    fetch,
    async getGateMappingRevision(input) {
      expect(this === binding).toBe(true);
      expect(input).toStrictEqual({ date: "20261004", keibajoCode: "A8" });
      return "gate-revision";
    },
  };
  getCloudflareContextMock.mockReturnValue({ env: { R2_CATALOG: binding } });
  const env = await safeGetCloudflareEnv();
  await env?.R2_CATALOG?.fetch(
    new Request("https://catalog.internal/read", { method: "POST", body: "query" }),
    { redirect: "error" },
  );
  expect(fetch.mock.calls[0]?.[0]).toBe("https://catalog.internal/read");
  expect(fetch.mock.calls[0]?.[1]?.redirect).toBe("error");
  expect(fetch.mock.calls[0]?.[1]).toMatchObject({
    body: new TextEncoder().encode("query").buffer,
  });
  expect(
    await env?.R2_CATALOG?.getGateMappingRevision?.({ date: "20261004", keibajoCode: "A8" }),
  ).toBe("gate-revision");
});

it("development leaves private RPC absent on fetch-only and race-detail bindings", async () => {
  vi.stubEnv("NODE_ENV", "development");
  const fetch = vi.fn<typeof globalThis.fetch>();
  getCloudflareContextMock.mockReturnValue({
    env: { R2_CATALOG: { fetch }, R2_RACE_DETAIL: { fetch } },
  });
  const env = await safeGetCloudflareEnv();
  expect(Object.hasOwn(env?.R2_CATALOG ?? {}, "getGateMappingRevision")).toBe(false);
  expect(Object.hasOwn(env?.R2_RACE_DETAIL ?? {}, "getGateMappingRevision")).toBe(false);
});

it("development propagates private RPC rejection without fetching", async () => {
  vi.stubEnv("NODE_ENV", "development");
  const fetch = vi.fn<typeof globalThis.fetch>();
  const binding: NonNullable<CloudflareEnv["R2_CATALOG"]> = {
    fetch,
    getGateMappingRevision: vi
      .fn<NonNullable<NonNullable<CloudflareEnv["R2_CATALOG"]>["getGateMappingRevision"]>>()
      .mockRejectedValue(new Error("revision unavailable")),
  };
  getCloudflareContextMock.mockReturnValue({ env: { R2_CATALOG: binding } });
  const env = await safeGetCloudflareEnv();
  await expect(
    env?.R2_CATALOG?.getGateMappingRevision?.({ date: "20261004", keibajoCode: "A8" }),
  ).rejects.toThrow("revision unavailable");
  expect(fetch).not.toHaveBeenCalled();
});

it("production preserves the original private RPC binding", async () => {
  vi.stubEnv("NODE_ENV", "production");
  const binding: NonNullable<CloudflareEnv["R2_CATALOG"]> = {
    fetch: vi.fn<typeof globalThis.fetch>(),
    getGateMappingRevision: vi
      .fn<NonNullable<NonNullable<CloudflareEnv["R2_CATALOG"]>["getGateMappingRevision"]>>()
      .mockResolvedValue("production-revision"),
  };
  getCloudflareContextMock.mockReturnValue({ env: { R2_CATALOG: binding } });
  const env = await safeGetCloudflareEnv();
  expect(env?.R2_CATALOG === binding).toBe(true);
  expect(
    await env?.R2_CATALOG?.getGateMappingRevision?.({ date: "20261004", keibajoCode: "A8" }),
  ).toBe("production-revision");
});
