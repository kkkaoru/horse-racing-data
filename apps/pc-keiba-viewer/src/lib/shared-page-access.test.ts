// @vitest-environment node
import { expect, it, vi } from "vitest";

import { buildSharedPageKey, resolveSharedPageRequest } from "./shared-page-access";

const { verifyAccessJwtMock } = vi.hoisted(() => ({
  verifyAccessJwtMock: vi.fn<(token: string, config: unknown) => Promise<boolean>>(),
}));

vi.mock("./access-jwt", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./access-jwt")>()),
  verifyAccessJwt: verifyAccessJwtMock,
}));

const NOW_MS = Date.parse("2026-10-25T12:00:00+09:00");
const ACTIVE_ENTRY = JSON.stringify({ expiresAt: "2026-10-25T23:59:59+09:00" });
const EXPIRED_ENTRY = JSON.stringify({ expiresAt: "2026-10-25T11:00:00+09:00" });

const edgeRequest = (path: string, init?: RequestInit): Request =>
  new Request(`https://pc-keiba-viewer.kkk4oru.com${path}`, {
    ...init,
    headers: { "cf-connecting-ip": "203.0.113.1" },
  });

const kvWith = (value: string | null) => ({
  get: vi.fn<() => Promise<string | null>>(async () => value),
});

const readSharedHeader = async (result: Request | Response): Promise<string | null> =>
  result instanceof Request ? result.headers.get("x-pc-keiba-shared-until") : null;

it("builds the KV key from the race page path", () => {
  expect(buildSharedPageKey("/races/2026/10/25/08/11")).toBe(
    "shared-page:v1:/races/2026/10/25/08/11",
  );
});

it("marks the shared race page with its expiry while the share is active", async () => {
  const kv = kvWith(ACTIVE_ENTRY);
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv,
    nowMs: NOW_MS,
    request: edgeRequest("/races/2026/10/25/08/11?tab=odds"),
  });
  expect(await readSharedHeader(result)).toBe("2026-10-25T23:59:59+09:00");
  expect(kv.get).toHaveBeenCalledWith("shared-page:v1:/races/2026/10/25/08/11");
});

it("allows GET requests to the shared race API resources", async () => {
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request: edgeRequest("/api/races/2026/10/25/08/11/sections/training"),
  });
  expect(await readSharedHeader(result)).toBe("2026-10-25T23:59:59+09:00");
});

it("allows HEAD requests to the shared race page", async () => {
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request: edgeRequest("/races/2026/10/25/08/11", { method: "HEAD" }),
  });
  expect(await readSharedHeader(result)).toBe("2026-10-25T23:59:59+09:00");
});

it("rejects writes to the shared race API", async () => {
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request: edgeRequest("/api/races/2026/10/25/08/11/paddock", { method: "POST" }),
  });
  expect(result instanceof Response ? result.status : null).toBe(403);
});

it("rejects sibling pages of the shared race such as the paddock page", async () => {
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request: edgeRequest("/races/2026/10/25/08/11/paddock"),
  });
  expect(result instanceof Response ? result.status : null).toBe(403);
});

it("rejects the bare race API path", async () => {
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request: edgeRequest("/api/races/2026/10/25/08/11"),
  });
  expect(result instanceof Response ? result.status : null).toBe(403);
});

it("rejects the shared race page after the share expired", async () => {
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(EXPIRED_ENTRY),
    nowMs: NOW_MS,
    request: edgeRequest("/races/2026/10/25/08/11"),
  });
  expect(result instanceof Response ? result.status : null).toBe(403);
});

it("passes a race without a share entry through unchanged", async () => {
  const request = edgeRequest("/races/2026/10/25/08/10");
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(null),
    nowMs: NOW_MS,
    request,
  });
  expect(result === request).toBe(true);
});

it("passes a malformed share entry through unchanged", async () => {
  const request = edgeRequest("/races/2026/10/25/08/11");
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith(JSON.stringify({ until: "2026-10-25" })),
    nowMs: NOW_MS,
    request,
  });
  expect(result === request).toBe(true);
});

it("passes a non-object share entry through unchanged", async () => {
  const request = edgeRequest("/races/2026/10/25/08/11");
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: kvWith("null"),
    nowMs: NOW_MS,
    request,
  });
  expect(result === request).toBe(true);
});

it("passes non-race paths through without reading KV", async () => {
  const kv = kvWith(ACTIVE_ENTRY);
  const request = edgeRequest("/_next/static/chunks/app.js");
  const result = await resolveSharedPageRequest({ access: undefined, kv, nowMs: NOW_MS, request });
  expect(result === request).toBe(true);
  expect(kv.get).not.toHaveBeenCalled();
});

it("passes Access-authenticated requests through without reading KV", async () => {
  const kv = kvWith(EXPIRED_ENTRY);
  const request = new Request("https://pc-keiba-viewer.kkk4oru.com/races/2026/10/25/08/11", {
    headers: { "cf-access-jwt-assertion": "jwt", "cf-connecting-ip": "203.0.113.1" },
  });
  const result = await resolveSharedPageRequest({ access: undefined, kv, nowMs: NOW_MS, request });
  expect(result === request).toBe(true);
  expect(kv.get).not.toHaveBeenCalled();
});

it("passes service-binding requests without cf-connecting-ip through", async () => {
  const kv = kvWith(EXPIRED_ENTRY);
  const request = new Request("https://pc-keiba-viewer.kkk4oru.com/api/races/2026/10/25/08/11/x");
  const result = await resolveSharedPageRequest({ access: undefined, kv, nowMs: NOW_MS, request });
  expect(result === request).toBe(true);
  expect(kv.get).not.toHaveBeenCalled();
});

it("passes requests for other hosts through", async () => {
  const kv = kvWith(EXPIRED_ENTRY);
  const request = new Request("http://localhost:8787/races/2026/10/25/08/11", {
    headers: { "cf-connecting-ip": "127.0.0.1" },
  });
  const result = await resolveSharedPageRequest({ access: undefined, kv, nowMs: NOW_MS, request });
  expect(result === request).toBe(true);
  expect(kv.get).not.toHaveBeenCalled();
});

it("passes requests through when the KV binding is missing", async () => {
  const request = edgeRequest("/races/2026/10/25/08/11");
  const result = await resolveSharedPageRequest({
    access: undefined,
    kv: undefined,
    nowMs: NOW_MS,
    request,
  });
  expect(result === request).toBe(true);
});

const ACCESS = {
  aud: "aud",
  fetchImpl: vi.fn<typeof fetch>(),
  teamDomain: "team.cloudflareaccess.com",
};

const signedInRequest = (path: string, init?: RequestInit): Request =>
  new Request(`https://pc-keiba-viewer.kkk4oru.com${path}`, {
    ...init,
    headers: { cookie: "CF_Authorization=signed.jwt.token", "cf-connecting-ip": "203.0.113.1" },
  });

it("lets a viewer signed in to Access use an expired share's sibling pages and writes", async () => {
  verifyAccessJwtMock.mockResolvedValue(true);
  const request = signedInRequest("/api/races/2026/10/25/08/11/paddock", { method: "POST" });
  const result = await resolveSharedPageRequest({
    access: ACCESS,
    kv: kvWith(EXPIRED_ENTRY),
    nowMs: NOW_MS,
    request,
  });
  expect(result === request).toBe(true);
  expect(verifyAccessJwtMock).toHaveBeenLastCalledWith("signed.jwt.token", {
    aud: "aud",
    fetchImpl: ACCESS.fetchImpl,
    nowMs: NOW_MS,
    teamDomain: "team.cloudflareaccess.com",
  });
});

it("shows the shared race page unlimited to a viewer signed in to Access", async () => {
  verifyAccessJwtMock.mockResolvedValue(true);
  const request = signedInRequest("/races/2026/10/25/08/11");
  const result = await resolveSharedPageRequest({
    access: ACCESS,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request,
  });
  expect(result === request).toBe(true);
});

it("keeps the share limits when the Access cookie does not verify", async () => {
  verifyAccessJwtMock.mockResolvedValue(false);
  const result = await resolveSharedPageRequest({
    access: ACCESS,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request: signedInRequest("/races/2026/10/25/08/11/paddock"),
  });
  expect(result instanceof Response ? result.status : null).toBe(403);
});

it("keeps the share limits without an Access cookie", async () => {
  verifyAccessJwtMock.mockClear();
  const result = await resolveSharedPageRequest({
    access: ACCESS,
    kv: kvWith(ACTIVE_ENTRY),
    nowMs: NOW_MS,
    request: edgeRequest("/races/2026/10/25/08/11"),
  });
  expect(await readSharedHeader(result)).toBe("2026-10-25T23:59:59+09:00");
  expect(verifyAccessJwtMock).not.toHaveBeenCalled();
});
