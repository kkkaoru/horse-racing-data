// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { createKvOauthStore, createMemoryOauthStore } from "./mcp-oauth-store";

it("memory store round-trips and deletes values", async () => {
  const store = createMemoryOauthStore();
  await store.put("k", "v", 60);
  expect(await store.get("k")).toBe("v");
  expect(await store.get("missing")).toBe(null);
  await store.delete("k");
  expect(await store.get("k")).toBe(null);
});

it("KV store delegates get, put, and delete", async () => {
  const values = new Map<string, string>();
  const recordedTtl: number[] = [];
  const store = createKvOauthStore({
    delete: async (key: string) => {
      values.delete(key);
    },
    get: async (key: string) => values.get(key) ?? null,
    put: async (key: string, value: string, options?: { expirationTtl?: number }) => {
      if (options?.expirationTtl !== undefined) {
        recordedTtl.push(options.expirationTtl);
      }
      values.set(key, value);
    },
  });
  await store.put("oauth", "code", 600);
  expect(await store.get("oauth")).toBe("code");
  expect(recordedTtl).toStrictEqual([600]);
  await store.delete("oauth");
  expect(await store.get("oauth")).toBe(null);
});
