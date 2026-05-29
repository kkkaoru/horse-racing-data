// Run with bun.
import { expect, it, vi } from "vitest";

import {
  createCachedHostLookup,
  createResolvingFetch,
} from "./run-delete-race-running-styles-chunk";

it("createCachedHostLookup returns the IPv4 address from the lookup", async () => {
  const lookup = vi.fn(async () => [
    { address: "::1", family: 6 },
    { address: "10.0.0.1", family: 4 },
  ]);
  const cached = createCachedHostLookup(lookup);
  expect(await cached("example.com")).toBe("10.0.0.1");
});

it("createCachedHostLookup caches the first IPv4 lookup result", async () => {
  const lookup = vi.fn(async () => [{ address: "10.0.0.2", family: 4 }]);
  const cached = createCachedHostLookup(lookup);
  await cached("example.com");
  await cached("example.com");
  expect(lookup).toHaveBeenCalledTimes(1);
});

it("createCachedHostLookup throws when no IPv4 record is returned", async () => {
  const lookup = vi.fn(async () => [{ address: "::1", family: 6 }]);
  const cached = createCachedHostLookup(lookup);
  await expect(cached("example.com")).rejects.toThrow("no IPv4 record for example.com");
});

it("createResolvingFetch rewrites the URL to the resolved IPv4", async () => {
  const baseFetch = vi.fn(async () => new Response("ok"));
  const lookup = vi.fn(async () => "10.0.0.5");
  const resolving = createResolvingFetch(baseFetch as unknown as typeof fetch, lookup);
  await resolving("https://old.example.com/api/internal/ping");
  const call = baseFetch.mock.calls[0];
  expect(String(call?.[0])).toBe("https://10.0.0.5/api/internal/ping");
});

it("createResolvingFetch sets the host header to the original hostname", async () => {
  const baseFetch = vi.fn(async () => new Response("ok"));
  const lookup = vi.fn(async () => "10.0.0.5");
  const resolving = createResolvingFetch(baseFetch as unknown as typeof fetch, lookup);
  await resolving("https://old.example.com/api/internal/ping");
  const init = baseFetch.mock.calls[0]?.[1] as RequestInit | undefined;
  const headers = new Headers(init?.headers);
  expect(headers.get("host")).toBe("old.example.com");
});

it("createResolvingFetch sets tls.serverName for SNI", async () => {
  const baseFetch = vi.fn(async () => new Response("ok"));
  const lookup = vi.fn(async () => "10.0.0.5");
  const resolving = createResolvingFetch(baseFetch as unknown as typeof fetch, lookup);
  await resolving("https://old.example.com/api/internal/ping");
  const init = baseFetch.mock.calls[0]?.[1] as { tls?: { serverName: string } } | undefined;
  expect(init?.tls?.serverName).toBe("old.example.com");
});

it("createResolvingFetch accepts a URL instance input", async () => {
  const baseFetch = vi.fn(async () => new Response("ok"));
  const lookup = vi.fn(async () => "10.0.0.6");
  const resolving = createResolvingFetch(baseFetch as unknown as typeof fetch, lookup);
  await resolving(new URL("https://old.example.com/x"));
  expect(String(baseFetch.mock.calls[0]?.[0])).toBe("https://10.0.0.6/x");
});

it("createResolvingFetch accepts a Request input", async () => {
  const baseFetch = vi.fn(async () => new Response("ok"));
  const lookup = vi.fn(async () => "10.0.0.7");
  const resolving = createResolvingFetch(baseFetch as unknown as typeof fetch, lookup);
  await resolving(new Request("https://old.example.com/y"));
  expect(String(baseFetch.mock.calls[0]?.[0])).toBe("https://10.0.0.7/y");
});
