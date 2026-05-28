// Run with bun.
import { expect, it, vi } from "vitest";

import {
  readLatestOddsFromKv,
  writeLatestOddsToKv,
  type LatestOddsMirrorPayload,
} from "./latest-odds-kv-mirror";
import type { Env } from "../types";

interface KvMockHandle {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

const buildEnv = (overrides: Partial<Env> = {}): Env => {
  const kv = {
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  } as unknown as KVNamespace;
  return { ODDS_HOT_KV: kv, ...overrides } as Env;
};

const samplePayload = (): LatestOddsMirrorPayload => ({
  fetchedAt: "2026-05-28T10:00:00+09:00",
  latest: { tansho: [{ combination: "01", odds: 2.5 }] },
});

it("readLatestOddsFromKv returns null on miss", async () => {
  const env = buildEnv();
  const result = await readLatestOddsFromKv(env, "nar:20260528:42:01", {
    allowStale: false,
    now: new Date("2026-05-28T10:00:30+09:00"),
  });
  expect(result).toBeNull();
});

it("readLatestOddsFromKv returns payload when fresh", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(samplePayload()));
  const result = await readLatestOddsFromKv(env, "nar:20260528:42:01", {
    allowStale: false,
    now: new Date("2026-05-28T10:00:30+09:00"),
  });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
});

it("readLatestOddsFromKv returns null when stale and allowStale=false", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(samplePayload()));
  const result = await readLatestOddsFromKv(env, "nar:20260528:42:01", {
    allowStale: false,
    now: new Date("2026-05-28T10:03:00+09:00"),
  });
  expect(result).toBeNull();
});

it("readLatestOddsFromKv returns payload when stale but allowStale=true", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(samplePayload()));
  const result = await readLatestOddsFromKv(env, "nar:20260528:42:01", {
    allowStale: true,
    now: new Date("2026-05-28T11:00:00+09:00"),
  });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
});

it("readLatestOddsFromKv honors env stale seconds override", async () => {
  const env = buildEnv({ ODDS_STALE_MIRROR_SECONDS: "300" });
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(samplePayload()));
  const result = await readLatestOddsFromKv(env, "nar:20260528:42:01", {
    allowStale: false,
    now: new Date("2026-05-28T10:03:00+09:00"),
  });
  expect(result).not.toBeNull();
});

it("readLatestOddsFromKv falls back to default stale seconds on invalid env", async () => {
  const env = buildEnv({ ODDS_STALE_MIRROR_SECONDS: "bad" });
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(samplePayload()));
  const result = await readLatestOddsFromKv(env, "nar:20260528:42:01", {
    allowStale: false,
    now: new Date("2026-05-28T10:03:00+09:00"),
  });
  expect(result).toBeNull();
});

it("readLatestOddsFromKv falls back to default when env stale value is zero", async () => {
  const env = buildEnv({ ODDS_STALE_MIRROR_SECONDS: "0" });
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(samplePayload()));
  const result = await readLatestOddsFromKv(env, "nar:20260528:42:01", {
    allowStale: false,
    now: new Date("2026-05-28T10:00:30+09:00"),
  });
  expect(result).not.toBeNull();
});

it("writeLatestOddsToKv uses default TTL", async () => {
  const env = buildEnv();
  await writeLatestOddsToKv(env, "nar:20260528:42:01", samplePayload());
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:latest:nar:20260528:42:01",
    JSON.stringify(samplePayload()),
    { expirationTtl: 600 },
  );
});

it("writeLatestOddsToKv honors env override TTL", async () => {
  const env = buildEnv({ ODDS_LATEST_KV_TTL_SECONDS: "1200" });
  await writeLatestOddsToKv(env, "nar:20260528:42:01", samplePayload());
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:latest:nar:20260528:42:01",
    JSON.stringify(samplePayload()),
    { expirationTtl: 1200 },
  );
});

it("writeLatestOddsToKv falls back to default when env value is invalid", async () => {
  const env = buildEnv({ ODDS_LATEST_KV_TTL_SECONDS: "junk" });
  await writeLatestOddsToKv(env, "nar:20260528:42:01", samplePayload());
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:latest:nar:20260528:42:01",
    JSON.stringify(samplePayload()),
    { expirationTtl: 600 },
  );
});

it("writeLatestOddsToKv falls back to default when env value is zero", async () => {
  const env = buildEnv({ ODDS_LATEST_KV_TTL_SECONDS: "0" });
  await writeLatestOddsToKv(env, "nar:20260528:42:01", samplePayload());
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:latest:nar:20260528:42:01",
    JSON.stringify(samplePayload()),
    { expirationTtl: 600 },
  );
});
