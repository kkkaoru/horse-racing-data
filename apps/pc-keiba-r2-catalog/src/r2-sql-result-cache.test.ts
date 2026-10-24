// Runs with bun via Vitest. Tests for the short-lived R2 SQL result cache.
import { expect, it, vi } from "vitest";
import {
  R2_SQL_RESULT_TTL_SECONDS,
  cachedR2SqlFetcher,
  isSettledRaceDay,
} from "./r2-sql-result-cache";
import type { Fetcher, KvStore } from "./types";

const makeKv = () => {
  const entries = new Map<string, string>();
  const put = vi.fn<KvStore["put"]>(async (key, value) => {
    entries.set(key, value);
  });
  const kv: KvStore = {
    delete: vi.fn<KvStore["delete"]>(async () => undefined),
    get: vi.fn<KvStore["get"]>(async (key) => entries.get(key) ?? null),
    put,
  };
  return { entries, kv, put };
};

const answer = (rows: unknown[]): string => JSON.stringify({ result: { rows }, success: true });
const init = (query: string): RequestInit => ({ body: JSON.stringify({ query }), method: "POST" });

it("serves a repeated query from KV without calling R2 SQL again", async () => {
  const { entries, kv, put } = makeKv();
  const fetchImpl = vi.fn<Fetcher>(async () => new Response(answer([{ n: 1 }])));
  const cached = cachedR2SqlFetcher(fetchImpl, kv);
  const first = await cached("https://r2sql.test/", init("SELECT 1"));
  expect(await first.json()).toStrictEqual({ result: { rows: [{ n: 1 }] }, success: true });
  const second = await cached("https://r2sql.test/", init("SELECT 1"));
  expect(first.headers.get("x-r2-sql-result-cache")).toBeNull();
  expect(second.headers.get("x-r2-sql-result-cache")).toBe("hit");
  expect(await second.json()).toStrictEqual({ result: { rows: [{ n: 1 }] }, success: true });
  expect(fetchImpl).toHaveBeenCalledOnce();
  expect(put).toHaveBeenCalledWith(
    expect.stringMatching(/^r2sql-result:v1:[0-9a-f]{64}$/u),
    answer([{ n: 1 }]),
    {
      expirationTtl: 1200,
    },
  );
  expect(R2_SQL_RESULT_TTL_SECONDS).toBe(1200);
  expect(entries.size).toBe(1);
});

it("keys answers by the full request body", async () => {
  const { kv } = makeKv();
  const fetchImpl = vi.fn<Fetcher>(async () => new Response(answer([{ n: 1 }])));
  const cached = cachedR2SqlFetcher(fetchImpl, kv);
  await cached("https://r2sql.test/", init("SELECT 1"));
  await cached("https://r2sql.test/", init("SELECT 2"));
  expect(fetchImpl).toHaveBeenCalledTimes(2);
});

it("caches an empty answer like any other successful answer", async () => {
  const { kv, put } = makeKv();
  const fetchImpl = vi.fn<Fetcher>(async () => new Response(answer([])));
  const cached = cachedR2SqlFetcher(fetchImpl, kv);
  await cached("https://r2sql.test/", init("SELECT 0"));
  const second = await cached("https://r2sql.test/", init("SELECT 0"));
  expect(await second.json()).toStrictEqual({ result: { rows: [] }, success: true });
  expect(fetchImpl).toHaveBeenCalledOnce();
  expect(put).toHaveBeenCalledOnce();
});

it("never caches unsuccessful, malformed or failed answers", async () => {
  const { kv, put } = makeKv();
  const bodies = [
    new Response(JSON.stringify({ errors: [{ code: 1 }], success: false })),
    new Response("not json"),
    new Response(JSON.stringify({ result: [], success: true })),
    new Response("unavailable", { status: 503 }),
  ];
  const fetchImpl = vi.fn<Fetcher>(async () => bodies.shift()!);
  const cached = cachedR2SqlFetcher(fetchImpl, kv);
  const statuses = [
    (await cached("https://r2sql.test/", init("a"))).status,
    (await cached("https://r2sql.test/", init("b"))).status,
    (await cached("https://r2sql.test/", init("c"))).status,
    (await cached("https://r2sql.test/", init("d"))).status,
  ];
  expect(statuses).toStrictEqual([200, 200, 200, 503]);
  expect(put).not.toHaveBeenCalled();
});

it("ignores a corrupt cached entry and stays usable when KV fails", async () => {
  const { entries, kv } = makeKv();
  const fetchImpl = vi.fn<Fetcher>(async () => new Response(answer([{ n: 2 }])));
  const cached = cachedR2SqlFetcher(fetchImpl, kv);
  await cached("https://r2sql.test/", init("SELECT 2"));
  const [key] = [...entries.keys()];
  entries.set(key!, "{}");
  await cached("https://r2sql.test/", init("SELECT 2"));
  expect(fetchImpl).toHaveBeenCalledTimes(2);
  vi.mocked(kv.get).mockRejectedValue(new Error("kv down"));
  vi.mocked(kv.put).mockRejectedValue(new Error("kv down"));
  const response = await cached("https://r2sql.test/", init("SELECT 2"));
  expect(await response.json()).toStrictEqual({ result: { rows: [{ n: 2 }] }, success: true });
});

it("keeps the fetch and store alive through waitUntil", async () => {
  const { kv, put } = makeKv();
  const waitUntil = vi.fn<(promise: Promise<unknown>) => void>();
  const failing = cachedR2SqlFetcher(
    vi.fn<Fetcher>(async () => {
      throw new Error("offline");
    }),
    kv,
    waitUntil,
  );
  await expect(failing("https://r2sql.test/", init("SELECT 5"))).rejects.toThrow("offline");
  const cached = cachedR2SqlFetcher(
    vi.fn<Fetcher>(async () => new Response(answer([{ n: 5 }]))),
    kv,
    waitUntil,
  );
  const response = await cached("https://r2sql.test/", init("SELECT 5"));
  expect(await response.json()).toStrictEqual({ result: { rows: [{ n: 5 }] }, success: true });
  expect(waitUntil).toHaveBeenCalledTimes(2);
  await Promise.all(waitUntil.mock.calls.map(([promise]) => promise));
  expect(put).toHaveBeenCalledOnce();
});

it("passes requests through without KV or without a string body", async () => {
  const fetchImpl = vi.fn<Fetcher>(async () => new Response(answer([{ n: 3 }])));
  await cachedR2SqlFetcher(fetchImpl, undefined)("https://r2sql.test/", init("SELECT 3"));
  await cachedR2SqlFetcher(fetchImpl, makeKv().kv)("https://r2sql.test/");
  expect(fetchImpl).toHaveBeenCalledTimes(2);
});

it("treats only race days two or more JST days old as settled", () => {
  const justAfterJstMidnight = Date.parse("2026-10-23T15:30:00Z");
  const justBeforeJstMidnight = Date.parse("2026-10-23T14:30:00Z");
  expect(isSettledRaceDay("20261022", justAfterJstMidnight)).toBe(true);
  expect(isSettledRaceDay("20261023", justAfterJstMidnight)).toBe(false);
  expect(isSettledRaceDay("20261024", justAfterJstMidnight)).toBe(false);
  expect(isSettledRaceDay("20261022", justBeforeJstMidnight)).toBe(false);
  expect(isSettledRaceDay("20261021", justBeforeJstMidnight)).toBe(true);
});
