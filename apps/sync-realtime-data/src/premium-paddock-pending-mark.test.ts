// run with: bun run test
import { expect, it, vi } from "vitest";
import {
  clearPremiumPaddockPendingMark,
  readPremiumPaddockPendingMark,
  writePremiumPaddockPendingMark,
} from "./premium-paddock-pending-mark";
import type { Env } from "./types";

interface KvSpies {
  delete: (key: string) => Promise<void>;
  get: (key: string) => Promise<string | null>;
  put: (key: string, value: string, options: { expirationTtl: number }) => Promise<void>;
}

const buildEnvWithKv = (overrides: Partial<KvSpies> = {}): Env =>
  ({
    DETAIL_SECTION_CACHE_KV: {
      delete: overrides.delete ?? (async () => {}),
      get: overrides.get ?? (async () => null),
      put: overrides.put ?? (async () => {}),
    },
  }) as unknown as Env;

const buildEnvWithoutKv = (): Env => ({}) as unknown as Env;

it("readPremiumPaddockPendingMark returns false when KV binding is missing", async () => {
  expect(await readPremiumPaddockPendingMark(buildEnvWithoutKv(), "jra:1")).toBe(false);
});

it("readPremiumPaddockPendingMark returns false on KV miss", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>(async () => null);
  expect(await readPremiumPaddockPendingMark(buildEnvWithKv({ get }), "jra:1")).toBe(false);
  expect(get).toHaveBeenCalledTimes(1);
});

it("readPremiumPaddockPendingMark returns true on KV hit", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>(async () => "1");
  expect(await readPremiumPaddockPendingMark(buildEnvWithKv({ get }), "jra:1")).toBe(true);
});

it("writePremiumPaddockPendingMark noops when KV binding is missing", async () => {
  await writePremiumPaddockPendingMark(buildEnvWithoutKv(), "jra:1");
});

it("writePremiumPaddockPendingMark calls KV put with 60s ttl", async () => {
  const put = vi.fn<
    (key: string, value: string, options: { expirationTtl: number }) => Promise<void>
  >(async () => {});
  await writePremiumPaddockPendingMark(buildEnvWithKv({ put }), "jra:1");
  expect(put).toHaveBeenCalledTimes(1);
  expect(put.mock.calls[0]?.[0]).toBe("paddock:pending-mark:jra:1");
  expect(put.mock.calls[0]?.[1]).toBe("1");
  expect(put.mock.calls[0]?.[2]).toStrictEqual({ expirationTtl: 60 });
});

it("clearPremiumPaddockPendingMark noops when KV binding is missing", async () => {
  await clearPremiumPaddockPendingMark(buildEnvWithoutKv(), "jra:1");
});

it("clearPremiumPaddockPendingMark calls KV delete with prefixed key", async () => {
  const del = vi.fn<(key: string) => Promise<void>>(async () => {});
  await clearPremiumPaddockPendingMark(buildEnvWithKv({ delete: del }), "jra:1");
  expect(del).toHaveBeenCalledTimes(1);
  expect(del.mock.calls[0]?.[0]).toBe("paddock:pending-mark:jra:1");
});
