// Run with bun (vitest).
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock, useProductionApiProxyMock, fetchProductionApiMock } = vi.hoisted(
  () => ({
    fetchProductionApiMock: vi.fn<(...args: never[]) => unknown>(),
    getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
    useProductionApiProxyMock: vi.fn<() => boolean>(),
  }),
);

vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

vi.mock("./production-api-proxy.server", () => ({
  fetchProductionApi: fetchProductionApiMock,
  useProductionApiProxy: useProductionApiProxyMock,
}));

import {
  computeRaceTrendBodyHash,
  isRaceTrendRoomParams,
  notifyRaceTrendRoom,
  notifyRaceTrendRoomIfChanged,
} from "./race-trend-room.server";

type AnyMockFn = (...args: never[]) => unknown;

interface KvStub {
  get: ReturnType<typeof vi.fn<AnyMockFn>>;
  put: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface DoStub {
  fetch: ReturnType<typeof vi.fn<AnyMockFn>>;
}

const buildKvStub = (): KvStub => ({
  get: vi.fn<AnyMockFn>().mockResolvedValue(null),
  put: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
});

const buildDoStub = (response: Response): DoStub => ({
  fetch: vi.fn<AnyMockFn>().mockResolvedValue(response),
});

const buildRoomNamespace = (room: DoStub) => ({
  get: () => room,
  idFromName: (_name: string) => ({ toString: () => _name }),
});

beforeEach(() => {
  getCloudflareContextMock.mockReset();
  useProductionApiProxyMock.mockReset();
  fetchProductionApiMock.mockReset();
  useProductionApiProxyMock.mockReturnValue(false);
});

afterEach(() => {
  vi.clearAllMocks();
});

it("isRaceTrendRoomParams accepts a valid JRA params object", () => {
  expect(
    isRaceTrendRoomParams({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    }),
  ).toBe(true);
});

it("isRaceTrendRoomParams accepts a valid NAR params object", () => {
  expect(
    isRaceTrendRoomParams({
      day: "01",
      keibajoCode: "42",
      month: "06",
      raceNumber: "11",
      source: "nar",
      year: "2026",
    }),
  ).toBe(true);
});

it("isRaceTrendRoomParams rejects malformed numeric segments", () => {
  expect(
    isRaceTrendRoomParams({
      day: "2",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    }),
  ).toBe(false);
});

it("computeRaceTrendBodyHash returns the SHA-256 hex digest of an empty body", async () => {
  expect(await computeRaceTrendBodyHash("")).toBe(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  );
});

it("computeRaceTrendBodyHash returns deterministic SHA-256 hex for a fixed body", async () => {
  expect(await computeRaceTrendBodyHash("abc")).toBe(
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  );
});

it("notifyRaceTrendRoom proxies to production trends API when useProductionApiProxy is true", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  fetchProductionApiMock.mockResolvedValue(new Response("ok", { status: 200 }));
  const ok = await notifyRaceTrendRoom(
    { day: "29", keibajoCode: "05", month: "05", raceNumber: "07", source: "jra", year: "2026" },
    { cacheKey: "race-trend-key" },
  );
  expect(ok).toBe(true);
  expect(fetchProductionApiMock).toHaveBeenCalledTimes(1);
});

it("notifyRaceTrendRoom returns false when RACE_TREND_ROOM binding is missing", async () => {
  getCloudflareContextMock.mockResolvedValue({ env: {}, ctx: null });
  const ok = await notifyRaceTrendRoom(
    { day: "29", keibajoCode: "05", month: "05", raceNumber: "07", source: "jra", year: "2026" },
    { cacheKey: "race-trend-key" },
  );
  expect(ok).toBe(false);
});

it("notifyRaceTrendRoom hits the DO room when RACE_TREND_ROOM binding is present", async () => {
  const room = buildDoStub(new Response("ok", { status: 200 }));
  getCloudflareContextMock.mockResolvedValue({
    env: { RACE_TREND_ROOM: buildRoomNamespace(room) },
    ctx: null,
  });
  const ok = await notifyRaceTrendRoom(
    { day: "29", keibajoCode: "05", month: "05", raceNumber: "07", source: "jra", year: "2026" },
    { cacheKey: "race-trend-key" },
  );
  expect(ok).toBe(true);
  expect(room.fetch).toHaveBeenCalledTimes(1);
});

it("notifyRaceTrendRoomIfChanged falls back to notifyRaceTrendRoom when KV binding is missing", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  fetchProductionApiMock.mockResolvedValue(new Response("ok", { status: 200 }));
  getCloudflareContextMock.mockResolvedValue({ env: {}, ctx: null });
  const ok = await notifyRaceTrendRoomIfChanged({
    body: "abc",
    event: { cacheKey: "race-trend-key" },
    params: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    },
  });
  expect(ok).toBe(true);
  expect(fetchProductionApiMock).toHaveBeenCalledTimes(1);
});

it("notifyRaceTrendRoomIfChanged skips notify and returns false when body hash matches last hash", async () => {
  const knownHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
  const kv = buildKvStub();
  kv.get.mockResolvedValue(knownHash);
  const room = buildDoStub(new Response("ok", { status: 200 }));
  getCloudflareContextMock.mockResolvedValue({
    env: { DETAIL_SECTION_CACHE_KV: kv, RACE_TREND_ROOM: buildRoomNamespace(room) },
    ctx: null,
  });
  const ok = await notifyRaceTrendRoomIfChanged({
    body: "abc",
    event: { cacheKey: "race-trend-key" },
    params: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    },
  });
  expect(ok).toBe(false);
  expect(room.fetch).not.toHaveBeenCalled();
  expect(kv.put).not.toHaveBeenCalled();
});

it("notifyRaceTrendRoomIfChanged writes new hash and notifies when body changes", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue("stale-hash");
  const room = buildDoStub(new Response("ok", { status: 200 }));
  getCloudflareContextMock.mockResolvedValue({
    env: { DETAIL_SECTION_CACHE_KV: kv, RACE_TREND_ROOM: buildRoomNamespace(room) },
    ctx: null,
  });
  const ok = await notifyRaceTrendRoomIfChanged({
    body: "abc",
    event: { cacheKey: "race-trend-key" },
    params: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    },
  });
  expect(ok).toBe(true);
  expect(room.fetch).toHaveBeenCalledTimes(1);
  expect(kv.put).toHaveBeenCalledTimes(1);
  expect(kv.put.mock.calls[0]?.[0]).toBe("race-trend-last-hash:race-trend-key");
  expect(kv.put.mock.calls[0]?.[1]).toBe(
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  );
});

it("notifyRaceTrendRoomIfChanged treats KV get failure as miss and still notifies", async () => {
  const kv = buildKvStub();
  kv.get.mockRejectedValue(new Error("kv boom"));
  const room = buildDoStub(new Response("ok", { status: 200 }));
  getCloudflareContextMock.mockResolvedValue({
    env: { DETAIL_SECTION_CACHE_KV: kv, RACE_TREND_ROOM: buildRoomNamespace(room) },
    ctx: null,
  });
  const ok = await notifyRaceTrendRoomIfChanged({
    body: "abc",
    event: { cacheKey: "race-trend-key" },
    params: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    },
  });
  expect(ok).toBe(true);
  expect(room.fetch).toHaveBeenCalledTimes(1);
});

it("notifyRaceTrendRoomIfChanged still notifies when KV put rejects mid-flow", async () => {
  const kv = buildKvStub();
  kv.put.mockRejectedValue(new Error("kv put boom"));
  const room = buildDoStub(new Response("ok", { status: 200 }));
  getCloudflareContextMock.mockResolvedValue({
    env: { DETAIL_SECTION_CACHE_KV: kv, RACE_TREND_ROOM: buildRoomNamespace(room) },
    ctx: null,
  });
  const ok = await notifyRaceTrendRoomIfChanged({
    body: "abc",
    event: { cacheKey: "race-trend-key" },
    params: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    },
  });
  expect(ok).toBe(true);
  expect(room.fetch).toHaveBeenCalledTimes(1);
});
