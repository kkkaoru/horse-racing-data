// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { readPast14Cursor, writePast14Cursor } from "./past14-cursor-kv";
import type { Env } from "../types";

interface MockKv {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

const buildEnv = (): { env: Env; kv: MockKv } => {
  const kv: MockKv = { get: vi.fn(), put: vi.fn() };
  return {
    env: { FEATURES_KV: kv as unknown as KVNamespace } as unknown as Env,
    kv,
  };
};

it("readPast14Cursor returns 0 when KV has no value", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await expect(readPast14Cursor(env)).resolves.toBe(0);
});

it("readPast14Cursor returns the parsed integer when KV has a valid numeric string", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce("17");
  await expect(readPast14Cursor(env)).resolves.toBe(17);
});

it("readPast14Cursor returns 0 when the stored value is non-numeric garbage", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce("not-a-number");
  await expect(readPast14Cursor(env)).resolves.toBe(0);
});

it("readPast14Cursor returns 0 when the stored value is negative", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce("-5");
  await expect(readPast14Cursor(env)).resolves.toBe(0);
});

it("readPast14Cursor returns 0 when env.FEATURES_KV.get throws", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockRejectedValueOnce(new Error("kv down"));
  await expect(readPast14Cursor(env)).resolves.toBe(0);
});

it("writePast14Cursor calls put with String(cursor) on the correct key", async () => {
  const { env, kv } = buildEnv();
  await writePast14Cursor(env, 11);
  expect(kv.put).toHaveBeenCalledWith("features:past14-cursor:v1", "11");
});

it("writePast14Cursor does not throw when env.FEATURES_KV.put rejects", async () => {
  const { env, kv } = buildEnv();
  kv.put.mockRejectedValueOnce(new Error("kv put failed"));
  await expect(writePast14Cursor(env, 3)).resolves.toBeUndefined();
});
