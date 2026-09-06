import { describe, expect, test, vi } from "vitest";
import {
  getCachedProviderCursor,
  purgeAllProviderCursorCaches,
  purgeProviderCursorCache,
} from "./cache";

class MemoryKv {
  readonly values = new Map<string, string>();
  readonly writes: { key: string; ttl: number }[] = [];

  async delete(key: string): Promise<void> {
    this.values.delete(key);
  }

  async get(key: string, _type: "json"): Promise<unknown> {
    const value = this.values.get(key);
    return value === undefined ? null : JSON.parse(value);
  }

  async put(key: string, value: string, options: { expirationTtl: number }): Promise<void> {
    this.values.set(key, value);
    this.writes.push({ key, ttl: options.expirationTtl });
  }
}

class MemoryCache {
  readonly values = new Map<string, Response>();

  async delete(request: Request): Promise<boolean> {
    return this.values.delete(request.url);
  }

  async match(request: Request): Promise<Response | undefined> {
    return this.values.get(request.url)?.clone();
  }

  async put(request: Request, response: Response): Promise<void> {
    this.values.set(request.url, response.clone());
  }
}

const key = (provider: "jv" | "nv"): string => `acquisition-cursor:v1:${provider}`;
const url = (provider: "jv" | "nv"): string =>
  `https://daily-keiba-sync-cache.invalid/acquisition-cursor/v1/${provider}`;

describe("provider cursor cache", () => {
  test("uses Cache API before KV and D1", async () => {
    const kv = new MemoryKv();
    const cache = new MemoryCache();
    cache.values.set(url("jv"), Response.json({ cursor: "20260903200000", version: 1 }));
    const load = vi.fn().mockResolvedValue("20260901200000");

    expect(await getCachedProviderCursor(kv, "jv", load, cache)).toBe("20260903200000");
    expect(load).not.toHaveBeenCalled();
    expect(kv.writes).toEqual([]);
  });

  test("uses KV as L2 and refreshes the short-lived Cache API entry", async () => {
    const kv = new MemoryKv();
    const cache = new MemoryCache();
    kv.values.set(key("nv"), JSON.stringify({ cursor: "20260904010000", version: 1 }));
    const load = vi.fn().mockResolvedValue(null);

    expect(await getCachedProviderCursor(kv, "nv", load, cache)).toBe("20260904010000");
    expect(load).not.toHaveBeenCalled();
    expect((await cache.match(new Request(url("nv"))))?.headers.get("Cache-Control")).toBe(
      "max-age=30",
    );
  });

  test("loads D1 on a complete miss and bounds the KV lifetime", async () => {
    const kv = new MemoryKv();
    const cache = new MemoryCache();
    const load = vi.fn().mockResolvedValue(null);

    expect(await getCachedProviderCursor(kv, "jv", load, cache)).toBeNull();
    expect(load).toHaveBeenCalledTimes(1);
    expect(kv.writes).toEqual([{ key: key("jv"), ttl: 300 }]);
    expect(await getCachedProviderCursor(kv, "jv", load, cache)).toBeNull();
    expect(load).toHaveBeenCalledTimes(1);
  });

  test("rejects malformed cache values and falls back safely", async () => {
    for (const invalid of [
      [],
      { cursor: "bad", version: 1 },
      { cursor: "20260904010000", version: 2 },
      "invalid",
    ]) {
      const kv = new MemoryKv();
      const cache = new MemoryCache();
      kv.values.set(key("nv"), JSON.stringify(invalid));
      const load = vi.fn().mockResolvedValue("20260904020000");
      expect(await getCachedProviderCursor(kv, "nv", load, cache)).toBe("20260904020000");
    }

    const kv = new MemoryKv();
    const cache = new MemoryCache();
    cache.values.set(url("nv"), new Response("not-json"));
    expect(await getCachedProviderCursor(kv, "nv", async () => "20260904030000", cache)).toBe(
      "20260904030000",
    );
  });

  test("purges one provider or both providers without prefix listing", async () => {
    const kv = new MemoryKv();
    const cache = new MemoryCache();
    const providers: readonly ("jv" | "nv")[] = ["jv", "nv"];
    for (const provider of providers) {
      kv.values.set(key(provider), JSON.stringify({ cursor: null, version: 1 }));
      cache.values.set(url(provider), Response.json({ cursor: null, version: 1 }));
    }

    await purgeProviderCursorCache(kv, "jv", cache);
    expect(kv.values.has(key("jv"))).toBe(false);
    expect(kv.values.has(key("nv"))).toBe(true);

    await purgeAllProviderCursorCaches(kv, cache);
    expect(kv.values.size).toBe(0);
    expect(cache.values.size).toBe(0);
  });
});
