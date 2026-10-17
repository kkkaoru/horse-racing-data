// Runs with bun via Vitest. Tests for the account-wide heavy R2 SQL gate.
import { afterEach, expect, test, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import {
  R2SqlGate,
  R2SqlGateState,
  gatedR2SqlFetcher,
  type R2SqlGateNamespace,
  type R2SqlGateStub,
} from "./r2-sql-gate";
import type { Fetcher } from "./types";

afterEach(() => {
  vi.useRealTimers();
});

const sequentialTokens = (): (() => string) => {
  const issued = { count: 0 };
  return () => {
    issued.count += 1;
    return `token-${String(issued.count)}`;
  };
};

test("grants two slots immediately and queues the third", async () => {
  const state = new R2SqlGateState(() => 0, sequentialTokens());
  expect(await state.acquire()).toBe("token-1");
  expect(await state.acquire()).toBe("token-2");
  const third = state.acquire();
  expect(state.activeCount()).toBe(2);
  state.release("token-1");
  expect(await third).toBe("token-3");
  expect(state.activeCount()).toBe(2);
});

test("admits waiters in arrival order", async () => {
  const state = new R2SqlGateState(() => 0, sequentialTokens());
  await state.acquire();
  await state.acquire();
  const order: string[] = [];
  const first = state.acquire().then((token) => order.push(`first:${String(token)}`));
  const second = state.acquire().then((token) => order.push(`second:${String(token)}`));
  state.release("token-1");
  state.release("token-2");
  await Promise.all([first, second]);
  expect(order).toStrictEqual(["first:token-3", "second:token-4"]);
});

test("lets a waiter proceed without a slot after the wait limit", async () => {
  vi.useFakeTimers();
  const state = new R2SqlGateState(() => 0, sequentialTokens());
  await state.acquire();
  await state.acquire();
  const waiting = state.acquire();
  vi.advanceTimersByTime(60_000);
  expect(await waiting).toBeNull();
  state.release("token-1");
  expect(state.activeCount()).toBe(1);
});

test("reclaims a slot whose holder never released it", async () => {
  const clock = { now: 0 };
  const state = new R2SqlGateState(() => clock.now, sequentialTokens());
  await state.acquire();
  await state.acquire();
  clock.now = 90_000;
  expect(await state.acquire()).toBe("token-3");
  expect(state.activeCount()).toBe(1);
});

test("ignores the release of an unknown or already expired token", async () => {
  const state = new R2SqlGateState(() => 0, sequentialTokens());
  await state.acquire();
  state.release("token-9");
  expect(state.activeCount()).toBe(1);
});

test("uses random tokens and the wall clock by default", async () => {
  const state = new R2SqlGateState();
  const token = await state.acquire();
  expect(typeof token).toBe("string");
  expect(state.activeCount()).toBe(1);
});

test("the Durable Object delegates acquire and release to its in-memory state", async () => {
  const gate = new R2SqlGate(mockDeep<DurableObjectState>(), mockDeep<Cloudflare.Env>());
  const token = await gate.acquire();
  expect(typeof token).toBe("string");
  gate.release(String(token));
  expect(await gate.acquire()).not.toBeNull();
});

const stubNamespace = (stub: R2SqlGateStub): R2SqlGateNamespace => ({
  getByName: vi.fn<(name: string) => R2SqlGateStub>(() => stub),
});

test("passes requests straight through without a gate binding", async () => {
  const fetchImpl = vi.fn<Fetcher>(async () => new Response("ok"));
  const response = await gatedR2SqlFetcher(fetchImpl, undefined)("https://r2sql.test/");
  expect(await response.text()).toBe("ok");
  expect(fetchImpl).toHaveBeenCalledOnce();
});

test("holds a gate slot for the duration of the R2 SQL request", async () => {
  const events: string[] = [];
  const stub: R2SqlGateStub = {
    acquire: vi.fn<() => Promise<string | null>>(async () => {
      events.push("acquire");
      return "slot";
    }),
    release: vi.fn<(token: string) => Promise<void>>(async (token) => {
      events.push(`release:${token}`);
    }),
  };
  const namespace = stubNamespace(stub);
  const fetchImpl = vi.fn<Fetcher>(async () => {
    events.push("fetch");
    return new Response("rows");
  });
  await gatedR2SqlFetcher(fetchImpl, namespace)("https://r2sql.test/", { method: "POST" });
  expect(events).toStrictEqual(["acquire", "fetch", "release:slot"]);
  expect(namespace.getByName).toHaveBeenCalledWith("heavy-r2-sql");
  expect(fetchImpl).toHaveBeenCalledWith("https://r2sql.test/", { method: "POST" });
});

test("releases the slot when the R2 SQL request fails", async () => {
  const release = vi.fn<(token: string) => Promise<void>>(async () => undefined);
  const namespace = stubNamespace({ acquire: async () => "slot", release });
  const fetchImpl = vi.fn<Fetcher>(async () => {
    throw new Error("timed out");
  });
  await expect(gatedR2SqlFetcher(fetchImpl, namespace)("https://r2sql.test/")).rejects.toThrow(
    "timed out",
  );
  expect(release).toHaveBeenCalledWith("slot");
});

test("runs ungated when the gate cannot be reached or grants no slot", async () => {
  const release = vi.fn<(token: string) => Promise<void>>(async () => undefined);
  const unreachable = stubNamespace({
    acquire: async () => {
      throw new Error("gate unavailable");
    },
    release,
  });
  const exhausted = stubNamespace({ acquire: async () => null, release });
  const fetchImpl = vi.fn<Fetcher>(async () => new Response("rows"));
  await gatedR2SqlFetcher(fetchImpl, unreachable)("https://r2sql.test/");
  await gatedR2SqlFetcher(fetchImpl, exhausted)("https://r2sql.test/");
  expect(fetchImpl).toHaveBeenCalledTimes(2);
  expect(release).not.toHaveBeenCalled();
});

test("does not fail the request when releasing the slot fails", async () => {
  const namespace = stubNamespace({
    acquire: async () => "slot",
    release: async () => {
      throw new Error("gate restarted");
    },
  });
  const fetchImpl = vi.fn<Fetcher>(async () => new Response("rows"));
  const response = await gatedR2SqlFetcher(fetchImpl, namespace)("https://r2sql.test/");
  expect(await response.text()).toBe("rows");
});
