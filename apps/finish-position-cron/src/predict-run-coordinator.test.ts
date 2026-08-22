// Run with bun. Tests for PredictRunCoordinator Durable Object.

import { beforeEach, expect, test, vi } from "vitest";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import type { Env } from "./types";

interface StoredRecord {
  status: string;
  timestamp: number;
  doName?: string;
  racesPredicted?: number;
  completedAt?: number;
  priorityMs?: number;
  reservationId?: string;
  executionId?: string;
}

const storageMap = new Map<string, unknown>();

const storageMock = {
  delete: vi.fn(async (key: string) => storageMap.delete(key)),
  get: vi.fn(async (key: string) => storageMap.get(key)),
  put: vi.fn(async (key: string, value: unknown) => {
    storageMap.set(key, value);
  }),
};

const blockConcurrencyWhileMock = vi.fn(async (fn: () => Promise<unknown>) => fn());

const stateMock = {
  blockConcurrencyWhile: blockConcurrencyWhileMock,
  storage: storageMock,
};

const makeCoordinator = (): PredictRunCoordinator =>
  new PredictRunCoordinator(stateMock as unknown as DurableObjectState, {} as unknown as Env);

beforeEach(() => {
  vi.useRealTimers();
  storageMap.clear();
  storageMock.get.mockClear();
  storageMock.put.mockClear();
  storageMock.delete.mockClear();
  blockConcurrencyWhileMock.mockClear();
  blockConcurrencyWhileMock.mockImplementation(async (fn: () => Promise<unknown>) => fn());
});

test("claim returns proceed:true for a new runYmd/category", async () => {
  const coordinator = makeCoordinator();
  const result = await coordinator.claim("20260603", "jra");
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMock.put).toHaveBeenCalledTimes(1);
});

test("claim returns proceed:true when status is started (retry allowed after crash)", async () => {
  storageMap.set("run:20260603:jra", { status: "started", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claim("20260603", "jra");
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMock.put).toHaveBeenCalledTimes(1);
});

test("claim returns proceed:false when status is success", async () => {
  storageMap.set("run:20260603:jra", { status: "success", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claim("20260603", "jra");
  expect(result).toStrictEqual({ proceed: false, state: "success" });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("claim returns proceed:true when status is error (retry allowed)", async () => {
  storageMap.set("run:20260603:jra", { status: "error", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claim("20260603", "jra");
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMock.put).toHaveBeenCalledTimes(1);
});

test("complete writes the record with given status and racesPredicted", async () => {
  const coordinator = makeCoordinator();
  await coordinator.complete({
    category: "jra",
    racesPredicted: 12,
    runYmd: "20260603",
    status: "success",
  });
  expect(storageMock.put).toHaveBeenCalledTimes(1);
  const [key, value] = storageMock.put.mock.calls[0] as [string, StoredRecord];
  expect(key).toBe("run:20260603:jra");
  expect(value.status).toBe("success");
  expect(value.racesPredicted).toBe(12);
});

test("getState returns the stored record", async () => {
  storageMap.set("run:20260603:jra", { status: "success", timestamp: 2000, racesPredicted: 7 });
  const coordinator = makeCoordinator();
  const state = await coordinator.getState("20260603", "jra");
  expect(state).toStrictEqual({ status: "success", timestamp: 2000, racesPredicted: 7 });
});

test("getState returns undefined for unknown key", async () => {
  const coordinator = makeCoordinator();
  const state = await coordinator.getState("20260603", "nar");
  expect(state).toBeUndefined();
});

test("claim uses blockConcurrencyWhile for serialisation", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claim("20260603", "jra");
  expect(blockConcurrencyWhileMock).toHaveBeenCalledTimes(1);
});

test("fetch POST /claim returns proceed:true for new run", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim", {
    body: JSON.stringify({ runYmd: "20260603", category: "jra" }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean };
  expect(body.proceed).toBe(true);
});

test("fetch POST /claim returns proceed:true when already started (retry after crash)", async () => {
  storageMap.set("run:20260603:jra", { status: "started", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim", {
    body: JSON.stringify({ runYmd: "20260603", category: "jra" }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean };
  expect(body.proceed).toBe(true);
});

test("fetch POST /complete writes state and returns ok", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/complete", {
    body: JSON.stringify({
      runYmd: "20260603",
      category: "jra",
      status: "success",
      racesPredicted: 5,
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
});

test("fetch GET /state returns the stored state", async () => {
  storageMap.set("run:20260603:nar", { status: "success", timestamp: 3000, racesPredicted: 4 });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/state?runYmd=20260603&category=nar");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { state: StoredRecord };
  expect(body.state.status).toBe("success");
  expect(body.state.racesPredicted).toBe(4);
});

test("fetch GET /state returns null when key not found", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/state?runYmd=20260603&category=ban-ei");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { state: null };
  expect(body.state).toBe(null);
});

test("fetch GET /claim returns 405 method not allowed", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(405);
});

test("fetch POST /state returns 405 method not allowed", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/state", {
    body: "{}",
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(405);
});

test("fetch GET /unknown returns 404", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/unknown");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(404);
});

test("fetch GET /state without query params uses empty string keys and returns null", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/state");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { state: null };
  expect(body.state).toBe(null);
});

test("claimRace returns proceed:true for a new per-race key", async () => {
  const coordinator = makeCoordinator();
  const result = await coordinator.claimRace({
    category: "jra",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMock.put).toHaveBeenCalledTimes(1);
});

test("claimRace stores the per-race key under the rescore namespace", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claimRace({
    category: "jra",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  const [key] = storageMock.put.mock.calls[0] as [string, StoredRecord];
  expect(key).toBe("rescore:20260619:jra:05:11");
});

test("a completed pre-weight full prediction does not consume the weight rescore claim", async () => {
  storageMap.set("focused-full:20260619:jra:05:11", {
    status: "success",
    timestamp: 1000,
  });
  const coordinator = makeCoordinator();

  const result = await coordinator.claimRace({
    category: "jra",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });

  expect(result).toStrictEqual({ proceed: true });
  expect(storageMap.get("rescore:20260619:jra:05:11")).toMatchObject({ status: "enqueued" });
  expect(storageMap.get("focused-full:20260619:jra:05:11")).toMatchObject({ status: "success" });
});

test("claimRace returns proceed:false when the per-race key already exists", async () => {
  storageMap.set("rescore:20260619:jra:05:11", { status: "enqueued", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimRace({
    category: "jra",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(result).toStrictEqual({ proceed: false, state: "enqueued" });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("claimRace uses blockConcurrencyWhile for serialisation", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claimRace({
    category: "nar",
    keibajoCode: "30",
    raceBango: "02",
    runYmd: "20260619",
  });
  expect(blockConcurrencyWhileMock).toHaveBeenCalledTimes(1);
});

test("fetch POST /claim-race returns proceed:true for a new race", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-race", {
    body: JSON.stringify({
      category: "jra",
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260619",
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean };
  expect(body.proceed).toBe(true);
});

test("fetch POST /claim-race returns proceed:false when already claimed", async () => {
  storageMap.set("rescore:20260619:jra:05:11", { status: "enqueued", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-race", {
    body: JSON.stringify({
      category: "jra",
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260619",
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean; state: string };
  expect(body.proceed).toBe(false);
  expect(body.state).toBe("enqueued");
});

test("fetch GET /claim-race returns 405 method not allowed", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-race");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(405);
});

test("claimFocusedFullRace stores a focused-full started key for a new race", async () => {
  const coordinator = makeCoordinator();
  const result = await coordinator.claimFocusedFullRace({
    category: "jra",
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(result).toStrictEqual({ proceed: true });
  const [key, value] = storageMock.put.mock.calls[0] as [string, StoredRecord];
  expect(key).toBe("focused-full:20260621:jra:02:01");
  expect(value.status).toBe("started");
});

test("claimFocusedFullRace resumes a fresh same-race redelivery without extending its deadline", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(10_000);
  storageMap.set("focused-full:20260621:jra:02:01", { status: "started", timestamp: 9_000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimFocusedFullRace({
    category: "jra",
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(result).toStrictEqual({ proceed: true, state: "resumed" });
  expect(storageMock.put).not.toHaveBeenCalled();
  vi.useRealTimers();
});

test("claimFocusedFullRace returns proceed:true and refreshes stale started key", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(3_000_000);
  storageMap.set("focused-full:20260621:jra:02:01", { status: "started", timestamp: 1_000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimFocusedFullRace({
    category: "jra",
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(result).toStrictEqual({ proceed: true, state: "stale" });
  expect(storageMock.put).toHaveBeenCalledTimes(2);
  vi.useRealTimers();
});

test("claimFocusedFullRace returns proceed:false for a successful focused key", async () => {
  storageMap.set("focused-full:20260621:jra:02:01", { status: "success", timestamp: 1_000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimFocusedFullRace({
    category: "jra",
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(result).toStrictEqual({ proceed: false, state: "success" });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("claimFocusedFullRace with force:true bypasses a successful focused key and resets it to started", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(5_000);
  storageMap.set("focused-full:20260621:jra:02:01", { status: "success", timestamp: 1_000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimFocusedFullRace({
    category: "jra",
    force: true,
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(result).toStrictEqual({ proceed: true });
  const [key, value] = storageMock.put.mock.calls[0] as [string, StoredRecord];
  expect(key).toBe("focused-full:20260621:jra:02:01");
  expect(value).toStrictEqual({
    doName: "legacy-jra",
    priorityMs: 5_000,
    status: "started",
    timestamp: 5_000,
  });
  vi.useRealTimers();
});

test("claimFocusedFullRace does not leak the stale success state after a force reset", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(5_000);
  storageMap.set("focused-full:20260621:jra:02:01", { status: "success", timestamp: 1_000 });
  const coordinator = makeCoordinator();
  const forced = await coordinator.claimFocusedFullRace({
    category: "jra",
    force: true,
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(forced).toStrictEqual({ proceed: true });
  vi.setSystemTime(6_000);
  const followUp = await coordinator.claimFocusedFullRace({
    category: "jra",
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(followUp).toStrictEqual({ proceed: true, state: "resumed" });
  vi.useRealTimers();
});

test("focused-full lane deduplicates waiters and promotes them in FIFO order", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(10_000);
  const coordinator = makeCoordinator();
  const first = await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "10",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  const second = await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  const third = await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "12",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  expect(first).toStrictEqual({ proceed: true });
  expect(second).toStrictEqual({ proceed: false, state: "queued" });
  expect(third).toStrictEqual({ proceed: false, state: "queued" });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toStrictEqual({
    activeRaceKey: "focused-full:20260813:nar:30:10",
    startedAt: 10_000,
    waiters: ["focused-full:20260813:nar:30:11", "focused-full:20260813:nar:30:12"],
  });

  vi.setSystemTime(20_000);
  await coordinator.completeFocusedFullRace({
    category: "nar",
    keibajoCode: "30",
    raceBango: "10",
    runYmd: "20260813",
    status: "success",
  });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toStrictEqual({
    activeRaceKey: "focused-full:20260813:nar:30:11",
    startedAt: 20_000,
    waiters: ["focused-full:20260813:nar:30:12"],
  });
  const promoted = await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  expect(promoted).toStrictEqual({ proceed: true, state: "promoted" });

  vi.setSystemTime(21_000);
  storageMock.put.mockClear();
  const activePoll = await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  expect(activePoll).toStrictEqual({ proceed: true, state: "resumed" });
  expect(storageMock.put).not.toHaveBeenCalled();

  await coordinator.completeFocusedFullRace({
    category: "nar",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    status: "error",
  });
  await coordinator.completeFocusedFullRace({
    category: "nar",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    status: "error",
  });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toStrictEqual({
    activeRaceKey: "focused-full:20260813:nar:30:12",
    startedAt: 21_000,
    waiters: [],
  });
  vi.useRealTimers();
});

test("focused-full lane orders waiters by scheduled post time instead of Queue delivery order", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claimFocusedFullRace({
    category: "jra",
    doName: "predict-jra-0",
    keibajoCode: "07",
    raceBango: "10",
    raceStartAtJst: "2026-08-22T17:20:00+09:00",
    runYmd: "20260822",
    staleAfterMs: 900_000,
  });
  await coordinator.claimFocusedFullRace({
    category: "jra",
    doName: "predict-jra-0",
    keibajoCode: "07",
    raceBango: "12",
    raceStartAtJst: "2026-08-22T18:20:00+09:00",
    runYmd: "20260822",
    staleAfterMs: 900_000,
  });
  await coordinator.claimFocusedFullRace({
    category: "jra",
    doName: "predict-jra-0",
    keibajoCode: "07",
    raceBango: "11",
    raceStartAtJst: "2026-08-22T17:50:00+09:00",
    runYmd: "20260822",
    staleAfterMs: 900_000,
  });
  expect(storageMap.get("focused-full-lane:predict-jra-0")).toStrictEqual({
    activeRaceKey: "focused-full:20260822:jra:07:10",
    startedAt: expect.any(Number),
    waiters: ["focused-full:20260822:jra:07:11", "focused-full:20260822:jra:07:12"],
  });
});

test("focused-full priority falls back to claim time for an invalid post time", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(12_345);
  const coordinator = makeCoordinator();
  await coordinator.claimFocusedFullRace({
    category: "jra",
    doName: "predict-jra-0",
    keibajoCode: "07",
    raceBango: "11",
    raceStartAtJst: "invalid",
    runYmd: "20260822",
    staleAfterMs: 900_000,
  });
  expect(storageMap.get("focused-full:20260822:jra:07:11")).toMatchObject({
    priorityMs: 12_345,
  });
  vi.useRealTimers();
});

test("focused-full waiter ordering tolerates an orphaned legacy waiter record", async () => {
  storageMap.set("focused-full-lane:predict-jra-0", {
    activeRaceKey: "focused-full:20260822:jra:07:10",
    startedAt: Date.now(),
    waiters: ["focused-full:20260822:jra:07:legacy"],
  });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimFocusedFullRace({
    category: "jra",
    doName: "predict-jra-0",
    keibajoCode: "07",
    raceBango: "11",
    raceStartAtJst: "2026-08-22T17:50:00+09:00",
    runYmd: "20260822",
    staleAfterMs: 900_000,
  });
  expect(result).toStrictEqual({ proceed: false, state: "queued" });
  expect(storageMap.get("focused-full-lane:predict-jra-0")).toMatchObject({
    waiters: ["focused-full:20260822:jra:07:11", "focused-full:20260822:jra:07:legacy"],
  });
});

test("a stale active focused-full owner yields to its oldest waiter exactly once", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000);
  const coordinator = makeCoordinator();
  await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "10",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  vi.setSystemTime(902_000);
  const stale = await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "10",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  expect(stale).toStrictEqual({ proceed: false, state: "queued" });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toStrictEqual({
    activeRaceKey: "focused-full:20260813:nar:30:11",
    startedAt: 902_000,
    waiters: ["focused-full:20260813:nar:30:10"],
  });
  vi.useRealTimers();
});

test("a different caller retires a stale active lane and starts the highest-priority current race", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(902_000);
  storageMap.set("focused-full-lane:predict-nar-1", {
    activeRaceKey: "focused-full:20260813:nar:30:10",
    startedAt: 1_000,
    waiters: ["focused-full:20260813:nar:30:12"],
  });
  storageMap.set("focused-full:20260813:nar:30:10", {
    doName: "predict-nar-1",
    priorityMs: 100,
    status: "started",
    timestamp: 1_000,
  });
  storageMap.set("focused-full:20260813:nar:30:11", {
    doName: "predict-nar-1",
    priorityMs: 200,
    status: "queued",
    timestamp: 2_000,
  });
  storageMap.set("focused-full:20260813:nar:30:12", {
    doName: "predict-nar-1",
    priorityMs: 300,
    status: "queued",
    timestamp: 3_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimFocusedFullRace({
      category: "nar",
      doName: "predict-nar-1",
      keibajoCode: "30",
      raceBango: "11",
      runYmd: "20260813",
      staleAfterMs: 900_000,
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "stale" });
  expect(storageMap.get("focused-full:20260813:nar:30:10")).toStrictEqual({
    doName: "predict-nar-1",
    priorityMs: 100,
    status: "error",
    timestamp: 902_000,
  });
  expect(storageMap.get("focused-full:20260813:nar:30:11")).toStrictEqual({
    doName: "predict-nar-1",
    priorityMs: 200,
    status: "started",
    timestamp: 902_000,
  });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toStrictEqual({
    activeRaceKey: "focused-full:20260813:nar:30:11",
    startedAt: 902_000,
    waiters: ["focused-full:20260813:nar:30:12"],
  });
  vi.useRealTimers();
});

test("a different caller retires a stale active lane and readies an earlier waiter", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(902_000);
  storageMap.set("focused-full-lane:predict-jra-0", {
    activeRaceKey: "focused-full:20260822:jra:07:10",
    startedAt: 1_000,
    waiters: ["focused-full:20260822:jra:07:11"],
  });
  storageMap.set("focused-full:20260822:jra:07:10", {
    doName: "predict-jra-0",
    priorityMs: 100,
    status: "started",
    timestamp: 1_000,
  });
  storageMap.set("focused-full:20260822:jra:07:11", {
    doName: "predict-jra-0",
    priorityMs: 200,
    status: "queued",
    timestamp: 2_000,
  });
  storageMap.set("focused-full:20260822:jra:07:12", {
    doName: "predict-jra-0",
    priorityMs: 300,
    status: "queued",
    timestamp: 3_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-0",
      keibajoCode: "07",
      raceBango: "12",
      runYmd: "20260822",
      staleAfterMs: 900_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "queued" });
  expect(storageMap.get("focused-full:20260822:jra:07:10")).toMatchObject({
    status: "error",
    timestamp: 902_000,
  });
  expect(storageMap.get("focused-full:20260822:jra:07:11")).toStrictEqual({
    doName: "predict-jra-0",
    priorityMs: 200,
    status: "ready",
    timestamp: 902_000,
  });
  expect(storageMap.get("focused-full-lane:predict-jra-0")).toStrictEqual({
    activeRaceKey: "focused-full:20260822:jra:07:11",
    startedAt: 902_000,
    waiters: ["focused-full:20260822:jra:07:12"],
  });
  vi.useRealTimers();
});

test("completing a queued focused-full race removes only that waiter", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "10",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  await coordinator.completeFocusedFullRace({
    category: "nar",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    status: "error",
  });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toMatchObject({
    activeRaceKey: "focused-full:20260813:nar:30:10",
    waiters: [],
  });
});

test("completing the only focused-full owner deletes its empty lane", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claimFocusedFullRace({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    staleAfterMs: 900_000,
  });
  await coordinator.completeFocusedFullRace({
    category: "nar",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    status: "success",
  });
  expect(storageMock.delete).toHaveBeenCalledWith("focused-full-lane:predict-nar-1");
  expect(storageMap.has("focused-full-lane:predict-nar-1")).toBe(false);
});

test("completing focused-full tolerates a legacy record whose lane is missing", async () => {
  storageMap.set("focused-full:20260813:nar:30:11", {
    doName: "predict-nar-1",
    status: "started",
    timestamp: 1,
  });
  const coordinator = makeCoordinator();
  await coordinator.completeFocusedFullRace({
    category: "nar",
    keibajoCode: "30",
    raceBango: "11",
    runYmd: "20260813",
    status: "error",
  });
  expect(storageMap.get("focused-full:20260813:nar:30:11")).toMatchObject({
    status: "error",
  });
});

test("completeFocusedFullRace writes terminal focused-full state", async () => {
  const coordinator = makeCoordinator();
  await coordinator.completeFocusedFullRace({
    category: "jra",
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    status: "success",
  });
  const [key, value] = storageMock.put.mock.calls[0] as [string, StoredRecord];
  expect(key).toBe("focused-full:20260621:jra:02:01");
  expect(value.status).toBe("success");
  expect(value.completedAt).toBeTypeOf("number");
});

test("fetch POST /claim-focused-full-race returns claim result", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-focused-full-race", {
    body: JSON.stringify({
      category: "jra",
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260621",
      staleAfterMs: 2100000,
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean };
  expect(body.proceed).toBe(true);
});

test("claimContainerSlot stores the first unique DO lease", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  const coordinator = makeCoordinator();
  const result = await coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
  });
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMap.get("container-slots")).toStrictEqual({
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
    ],
  });
  vi.useRealTimers();
});

test("claimContainerSlot returns busy for a second rescore on the same DO", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
  });
  expect(result).toStrictEqual({ proceed: false, state: "busy" });
  vi.useRealTimers();
});

test("claimContainerSlot returns capped when three rescore DOs are already live", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
      {
        category: "ban-ei",
        doName: "predict-ban-ei",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra-2",
    kind: "rescore",
  });
  expect(result).toStrictEqual({ proceed: false, state: "capped" });
  vi.useRealTimers();
});

test("claimContainerSlot lets Ban-ei focused-full proceed when the general pool is full", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra-0",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
      {
        category: "nar",
        doName: "predict-nar-0",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
      {
        category: "nar",
        doName: "predict-nar-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
      {
        category: "ban-ei",
        doName: "predict-ban-ei",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
      {
        category: "jra",
        doName: "predict-jra-extra",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimContainerSlot({
    category: "ban-ei",
    doName: "predict-ban-ei-0",
    kind: "focused-full",
  });
  expect(result).toStrictEqual({ proceed: true });
  vi.useRealTimers();
});

test("claimContainerSlot honors a custom staleAfterMs and drops an older lease", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(5_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        staleAfterMs: 2_000,
        timestamp: 1_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    staleAfterMs: 2_000,
  });
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMap.get("container-slots")).toStrictEqual({
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        staleAfterMs: 2_000,
        timestamp: 5_000,
      },
    ],
  });
  vi.useRealTimers();
});

test("touchContainerSlot honors a custom staleAfterMs", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(8_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar-0",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        staleAfterMs: 2_000,
        timestamp: 1_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  await coordinator.touchContainerSlot({ doName: "predict-nar-0", staleAfterMs: 2_000 });
  expect(storageMap.get("container-slots")).toStrictEqual({ leases: [] });
  vi.useRealTimers();
});

test("claimContainerSlot uses blockConcurrencyWhile for serialisation", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claimContainerSlot({
    category: "nar",
    doName: "predict-nar",
    kind: "rescore",
  });
  expect(blockConcurrencyWhileMock).toHaveBeenCalledTimes(1);
});

test("releaseContainerSlot removes the named lease", async () => {
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  await coordinator.releaseContainerSlot({ doName: "predict-jra", kind: "rescore" });
  expect(storageMap.get("container-slots")).toStrictEqual({ leases: [] });
});

test("touchContainerSlot refreshes the named lease heartbeat", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(50_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  await coordinator.touchContainerSlot({ doName: "predict-jra-1" });
  expect(storageMap.get("container-slots")).toStrictEqual({
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 50_000,
      },
    ],
  });
  vi.useRealTimers();
});

test("fetch POST /claim-container-slot returns proceed:true for a new DO", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-container-slot", {
    body: JSON.stringify({
      category: "jra",
      doName: "predict-jra",
      kind: "rescore",
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean };
  expect(body.proceed).toBe(true);
});

test("fetch POST /check-container-slot-stop protects a newer slot owner", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
        workKey: "new-work",
      },
    ],
  });
  const coordinator = makeCoordinator();
  const staleResponse = await coordinator.fetch(
    new Request("http://do/check-container-slot-stop", {
      body: JSON.stringify({
        doName: "predict-jra-1",
        requestedAt: "1970-01-01T00:00:21.000Z",
        workKey: "old-work",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  await expect(staleResponse.json()).resolves.toStrictEqual({ allowed: false });
  const ownerResponse = await coordinator.fetch(
    new Request("http://do/check-container-slot-stop", {
      body: JSON.stringify({
        doName: "predict-jra-1",
        requestedAt: "1970-01-01T00:00:21.000Z",
        workKey: "new-work",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  await expect(ownerResponse.json()).resolves.toStrictEqual({ allowed: true });
  vi.useRealTimers();
});

test("an atomic stop fence blocks claims until the matching stop clears", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 19_000,
        workKey: "work-1",
      },
    ],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.checkContainerSlotStop({
      doName: "predict-jra-1",
      requestedAt: "1970-01-01T00:00:20.000Z",
      workKey: "work-1",
    }),
  ).resolves.toBe(true);
  await expect(
    coordinator.claimContainerSlot({
      category: "jra",
      doName: "predict-jra-1",
      kind: "focused-full",
      workKey: "work-2",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "stopping" });

  await coordinator.clearContainerSlot({ doName: "predict-jra-1", workKey: "work-1" });
  expect(storageMap.has("container-stop-fences")).toBe(false);
  await expect(
    coordinator.claimContainerSlot({
      category: "jra",
      doName: "predict-jra-1",
      kind: "focused-full",
      workKey: "work-2",
    }),
  ).resolves.toStrictEqual({ proceed: true });
  vi.useRealTimers();
});

test("a delayed stop cannot fence a newer generation with the same work key", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(30_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 30_000,
        workKey: "work-1",
      },
    ],
  });

  await expect(
    makeCoordinator().checkContainerSlotStop({
      doName: "predict-jra-1",
      requestedAt: "1970-01-01T00:00:20.000Z",
      workKey: "work-1",
    }),
  ).resolves.toBe(false);
  expect(storageMap.has("container-stop-fences")).toBe(false);
  vi.useRealTimers();
});

test("stop fencing fails closed for unowned, malformed, and conflicting requests", async () => {
  const coordinator = makeCoordinator();
  await expect(
    coordinator.checkContainerSlotStop({
      doName: "predict-jra-1",
      requestedAt: "2026-08-22T00:00:00.000Z",
    }),
  ).resolves.toBe(false);
  await expect(
    coordinator.checkContainerSlotStop({
      doName: "predict-jra-1",
      requestedAt: "not-a-date",
      workKey: "work-1",
    }),
  ).resolves.toBe(false);
  storageMap.set("container-stop-fences", {
    "predict-jra-1": { requestedAtMs: 1, workKey: "work-2" },
  });
  await expect(
    coordinator.checkContainerSlotStop({
      doName: "predict-jra-1",
      requestedAt: "2026-08-22T00:00:00.000Z",
      workKey: "work-1",
    }),
  ).resolves.toBe(false);
});

test("a nonmatching clear preserves another stop fence", async () => {
  storageMap.set("container-stop-fences", {
    "predict-jra-1": { requestedAtMs: 1, workKey: "work-2" },
  });

  await makeCoordinator().clearContainerSlot({
    doName: "predict-jra-1",
    workKey: "work-1",
  });

  expect(storageMap.get("container-stop-fences")).toStrictEqual({
    "predict-jra-1": { requestedAtMs: 1, workKey: "work-2" },
  });
});

test("an explicit administrative stop can fence and clear without a work key", async () => {
  const coordinator = makeCoordinator();
  await expect(
    coordinator.checkContainerSlotStop({
      doName: "predict-jra-0",
      force: true,
      requestedAt: "2026-08-22T00:00:00.000Z",
    }),
  ).resolves.toBe(true);
  expect(storageMap.get("container-stop-fences")).toEqual({
    "predict-jra-0": { requestedAtMs: expect.any(Number) },
  });

  await coordinator.clearContainerSlot({ doName: "predict-jra-0" });
  expect(storageMap.has("container-stop-fences")).toBe(false);
});

test("fetch POST /claim-container-slot returns capped when the rescore pool is full", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
      {
        category: "ban-ei",
        doName: "predict-ban-ei",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-container-slot", {
    body: JSON.stringify({
      category: "jra",
      doName: "predict-jra-2",
      kind: "rescore",
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean; state: string };
  expect(body.proceed).toBe(false);
  expect(body.state).toBe("capped");
  vi.useRealTimers();
});

test("fetch POST /release-container-slot writes the remaining leases and returns ok", async () => {
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "rescore",
        rescoreHolders: 1,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/release-container-slot", {
    body: JSON.stringify({ doName: "predict-jra", kind: "rescore" }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
  expect(storageMap.get("container-slots")).toStrictEqual({ leases: [] });
});

test("fetch POST /clear-container-slot drops the named lease regardless of holders", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar-0",
        holders: 3,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
    ],
  });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/clear-container-slot", {
    body: JSON.stringify({ doName: "predict-nar-0" }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
  expect(storageMap.get("container-slots")).toStrictEqual({
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: 20_000,
      },
    ],
  });
  vi.useRealTimers();
});

test("fetch GET /clear-container-slot returns 405 method not allowed", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/clear-container-slot");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(405);
});

test("fetch POST /touch-container-slot returns ok", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/touch-container-slot", {
    body: JSON.stringify({ doName: "predict-jra-1" }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
});

test("fetch GET /claim-container-slot returns 405 method not allowed", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-container-slot");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(405);
});

test("fetch GET /release-container-slot returns 405 method not allowed", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/release-container-slot");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(405);
});

test("fetch GET /touch-container-slot returns 405 method not allowed", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/touch-container-slot");
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(405);
});

test("fetch POST /complete-focused-full-race writes state and returns ok", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/complete-focused-full-race", {
    body: JSON.stringify({
      category: "jra",
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260621",
      status: "success",
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
});

test("reserveFocusedFullRaceEnqueue creates one strongly consistent enqueued reservation", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000);
  const coordinator = makeCoordinator();
  const result = await coordinator.reserveFocusedFullRaceEnqueue({
    category: "jra",
    doName: "predict-jra-1",
    keibajoCode: "05",
    raceBango: "03",
    raceStartAtJst: "2026-08-23T10:50:00+09:00",
    reservationId: "reservation-1",
    runYmd: "20260823",
    staleAfterMs: 1_860_000,
  });
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMap.get("focused-full:20260823:jra:05:03")).toStrictEqual({
    doName: "predict-jra-1",
    priorityMs: 1787449800000,
    reservationId: "reservation-1",
    status: "enqueued",
    timestamp: 1000,
  });
  vi.useRealTimers();
});

test("reserveFocusedFullRaceEnqueue rejects a concurrent semantic duplicate", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(2_000);
  storageMap.set("focused-full:20260823:jra:05:03", {
    reservationId: "reservation-1",
    status: "enqueued",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.reserveFocusedFullRaceEnqueue({
      category: "jra",
      doName: "predict-jra-1",
      keibajoCode: "05",
      raceBango: "03",
      reservationId: "reservation-2",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "enqueued" });
  expect(storageMock.put).not.toHaveBeenCalled();
  vi.useRealTimers();
});

test("reserveFocusedFullRaceEnqueue reacquires an errored send reservation", async () => {
  storageMap.set("focused-full:20260823:nar:44:03", {
    status: "error",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.reserveFocusedFullRaceEnqueue({
      category: "nar",
      doName: "predict-nar-1",
      keibajoCode: "44",
      raceBango: "03",
      reservationId: "reservation-2",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("focused-full:20260823:nar:44:03")).toMatchObject({
    reservationId: "reservation-2",
    status: "enqueued",
  });
});

test("claimFocusedFullRace promotes an enqueue reservation to started", async () => {
  storageMap.set("focused-full:20260823:jra:05:03", {
    doName: "predict-jra-1",
    reservationId: "reservation-1",
    status: "enqueued",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-1",
      keibajoCode: "05",
      raceBango: "03",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("focused-full:20260823:jra:05:03")).toMatchObject({
    doName: "predict-jra-1",
    status: "started",
  });
});

test("failFocusedFullRaceEnqueue marks only its matching unconsumed reservation error", async () => {
  storageMap.set("focused-full:20260823:nar:44:03", {
    doName: "predict-nar-1",
    priorityMs: 2_000,
    reservationId: "reservation-2",
    status: "enqueued",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.failFocusedFullRaceEnqueue({
    category: "nar",
    keibajoCode: "44",
    raceBango: "03",
    reservationId: "reservation-2",
    runYmd: "20260823",
  });
  expect(storageMap.get("focused-full:20260823:nar:44:03")).toMatchObject({
    doName: "predict-nar-1",
    priorityMs: 2_000,
    status: "error",
  });
});

test("failFocusedFullRaceEnqueue does not clobber a consumed started reservation", async () => {
  storageMap.set("focused-full:20260823:nar:44:03", {
    reservationId: "reservation-2",
    status: "started",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.failFocusedFullRaceEnqueue({
    category: "nar",
    keibajoCode: "44",
    raceBango: "03",
    reservationId: "reservation-2",
    runYmd: "20260823",
  });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("failFocusedFullRaceEnqueue does not clobber a newer enqueue reservation", async () => {
  storageMap.set("focused-full:20260823:nar:44:03", {
    reservationId: "reservation-new",
    status: "enqueued",
    timestamp: 2_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.failFocusedFullRaceEnqueue({
    category: "nar",
    keibajoCode: "44",
    raceBango: "03",
    reservationId: "reservation-old",
    runYmd: "20260823",
  });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("fetch exposes enqueue reservation and failure transitions", async () => {
  const coordinator = makeCoordinator();
  const reserveResponse = await coordinator.fetch(
    new Request("http://do/reserve-focused-full-race-enqueue", {
      body: JSON.stringify({
        category: "jra",
        doName: "predict-jra-1",
        keibajoCode: "05",
        raceBango: "03",
        reservationId: "reservation-1",
        runYmd: "20260823",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(reserveResponse.status).toBe(200);
  const failResponse = await coordinator.fetch(
    new Request("http://do/fail-focused-full-race-enqueue", {
      body: JSON.stringify({
        category: "jra",
        keibajoCode: "05",
        raceBango: "03",
        reservationId: "reservation-1",
        runYmd: "20260823",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(failResponse.status).toBe(200);
});

test("reserveFocusedFullRaceEnqueue replaces a stale unconsumed reservation", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(2_000_000);
  storageMap.set("focused-full:20260823:jra:05:03", {
    reservationId: "reservation-old",
    status: "enqueued",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.reserveFocusedFullRaceEnqueue({
      category: "jra",
      doName: "predict-jra-1",
      keibajoCode: "05",
      raceBango: "03",
      reservationId: "reservation-new",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("focused-full:20260823:jra:05:03")).toMatchObject({
    reservationId: "reservation-new",
    status: "enqueued",
    timestamp: 2_000_000,
  });
  vi.useRealTimers();
});

test("claimRescoreExecution promotes an enqueued producer claim to started", async () => {
  storageMap.set("rescore:20260823:jra:05:11", { status: "enqueued", timestamp: 1_000 });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimRescoreExecution({
      category: "jra",
      executionId: "queue-message-1",
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("rescore:20260823:jra:05:11")).toMatchObject({
    executionId: "queue-message-1",
    status: "started",
  });
});

test("claimRescoreExecution starts absent and error records", async () => {
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimRescoreExecution({
      category: "nar",
      executionId: "queue-message-1",
      keibajoCode: "44",
      raceBango: "01",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  storageMap.set("rescore:20260823:nar:44:02", { status: "error", timestamp: 1_000 });
  await expect(
    coordinator.claimRescoreExecution({
      category: "nar",
      executionId: "queue-message-2",
      keibajoCode: "44",
      raceBango: "02",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
});

test("claimRescoreExecution rejects fresh started and successful execution", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(2_000);
  storageMap.set("rescore:20260823:jra:05:11", {
    executionId: "queue-message-1",
    status: "started",
    timestamp: 1_000,
  });
  storageMap.set("rescore:20260823:jra:05:12", {
    executionId: "queue-message-2",
    status: "success",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimRescoreExecution({
      category: "jra",
      executionId: "queue-message-redelivery",
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "started" });
  await expect(
    coordinator.claimRescoreExecution({
      category: "jra",
      executionId: "queue-message-new",
      keibajoCode: "05",
      raceBango: "12",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "success" });
  vi.useRealTimers();
});

test("claimRescoreExecution reclaims stale started with a new execution owner", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(2_000_000);
  storageMap.set("rescore:20260823:jra:05:11", {
    executionId: "queue-message-old",
    status: "started",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimRescoreExecution({
      category: "jra",
      executionId: "queue-message-new",
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "stale" });
  expect(storageMap.get("rescore:20260823:jra:05:11")).toMatchObject({
    executionId: "queue-message-new",
    status: "started",
  });
  vi.useRealTimers();
});

test("completeRescoreRace ignores completion from a stale execution owner", async () => {
  storageMap.set("rescore:20260823:jra:05:11", {
    executionId: "queue-message-new",
    status: "started",
    timestamp: 2_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.completeRescoreRace({
    category: "jra",
    executionId: "queue-message-old",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260823",
    status: "error",
  });
  expect(storageMap.get("rescore:20260823:jra:05:11")).toStrictEqual({
    executionId: "queue-message-new",
    status: "started",
    timestamp: 2_000,
  });
});

test("completeRescoreRace records matching success and never downgrades it to error", async () => {
  storageMap.set("rescore:20260823:jra:05:11", {
    executionId: "queue-message-1",
    status: "started",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.completeRescoreRace({
    category: "jra",
    executionId: "queue-message-1",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260823",
    status: "success",
  });
  await coordinator.completeRescoreRace({
    category: "jra",
    executionId: "queue-message-1",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260823",
    status: "error",
  });
  expect(storageMap.get("rescore:20260823:jra:05:11")).toMatchObject({
    executionId: "queue-message-1",
    status: "success",
  });
});

test("fetch exposes rescore execution claim and completion", async () => {
  const coordinator = makeCoordinator();
  const claimResponse = await coordinator.fetch(
    new Request("http://do/claim-rescore-execution", {
      body: JSON.stringify({
        category: "jra",
        executionId: "queue-message-1",
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260823",
        staleAfterMs: 1_860_000,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(claimResponse.status).toBe(200);
  const completeResponse = await coordinator.fetch(
    new Request("http://do/complete-rescore-race", {
      body: JSON.stringify({
        category: "jra",
        executionId: "queue-message-1",
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260823",
        status: "success",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(completeResponse.status).toBe(200);
});
