// Run with bun. Tests for PredictRunCoordinator Durable Object.

import { beforeEach, expect, test, vi } from "vitest";
import { MAX_FOCUSED_FULL_REPAIRS, PredictRunCoordinator } from "./predict-run-coordinator";
import type { Env, FocusedFullWatchTickMessage } from "./types";

interface StoredRecord {
  status: string;
  timestamp: number;
  doName?: string;
  racesPredicted?: number;
  completedAt?: number;
  priorityMs?: number;
  reservationId?: string;
  executionId?: string;
  claimId?: string;
}

const storageMap = new Map<string, unknown>();

const storageMock = {
  delete: vi.fn(async (key: string) => storageMap.delete(key)),
  get: vi.fn(async (key: string) => storageMap.get(key)),
  list: vi.fn(
    async ({ prefix }: { prefix?: string } = {}) =>
      new Map([...storageMap].filter(([key]) => prefix === undefined || key.startsWith(prefix))),
  ),
  put: vi.fn(async (key: string, value: unknown) => {
    storageMap.set(key, value);
  }),
  setAlarm: vi.fn(async (_scheduledTime: number | Date) => undefined),
};

const blockConcurrencyWhileMock = vi.fn(async (fn: () => Promise<unknown>) => fn());

const stateMock = {
  blockConcurrencyWhile: blockConcurrencyWhileMock,
  storage: storageMock,
};

const makeCoordinator = (env: Partial<Env> = {}): PredictRunCoordinator =>
  new PredictRunCoordinator(stateMock as unknown as DurableObjectState, env as Env);

beforeEach(() => {
  vi.useRealTimers();
  storageMap.clear();
  storageMock.get.mockClear();
  storageMock.list.mockClear();
  storageMock.put.mockClear();
  storageMock.delete.mockClear();
  storageMock.setAlarm.mockClear();
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
    claimId: "claim-1",
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
    claimId: "claim-2",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  const [key] = storageMock.put.mock.calls[0] as [string, StoredRecord];
  expect(key).toBe("rescore:20260619:jra:05:11");
});

test("claimRace deduplicates only the same horse-weight generation", async () => {
  const coordinator = makeCoordinator();
  const first = await coordinator.claimRace({
    category: "nar",
    claimId: "claim-generation-1",
    keibajoCode: "35",
    raceBango: "01",
    runYmd: "20260824",
    weightSnapshotCount: 8,
    weightSnapshotFetchedAt: "2026-08-24T12:00:00+09:00",
    weightSnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  });
  const duplicate = await coordinator.claimRace({
    category: "nar",
    claimId: "claim-generation-2",
    keibajoCode: "35",
    raceBango: "01",
    runYmd: "20260824",
    weightSnapshotCount: 8,
    weightSnapshotFetchedAt: "2026-08-24T12:00:00+09:00",
    weightSnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  });
  const newer = await coordinator.claimRace({
    category: "nar",
    claimId: "claim-generation-3",
    keibajoCode: "35",
    raceBango: "01",
    runYmd: "20260824",
    weightSnapshotCount: 8,
    weightSnapshotFetchedAt: "2026-08-24T12:01:00+09:00",
    weightSnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  });
  expect(first).toStrictEqual({ proceed: true });
  expect(duplicate).toStrictEqual({ proceed: false, state: "enqueued" });
  expect(newer).toStrictEqual({ proceed: true });
});

test("a completed pre-weight full prediction does not consume the weight rescore claim", async () => {
  storageMap.set("focused-full:20260619:jra:05:11", {
    status: "success",
    timestamp: 1000,
  });
  const coordinator = makeCoordinator();

  const result = await coordinator.claimRace({
    category: "jra",
    claimId: "claim-3",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });

  expect(result).toStrictEqual({ proceed: true });
  expect(storageMap.get("rescore:20260619:jra:05:11")).toMatchObject({ status: "enqueued" });
  expect(storageMap.get("focused-full:20260619:jra:05:11")).toMatchObject({ status: "success" });
});

test("claimRace returns proceed:false when the per-race key already exists", async () => {
  storageMap.set("rescore:20260619:jra:05:11", {
    status: "enqueued",
    timestamp: Date.now(),
  });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimRace({
    category: "jra",
    claimId: "claim-4",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(result).toStrictEqual({ proceed: false, state: "enqueued" });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("claimRace immediately reclaims a failed rescore generation", async () => {
  storageMap.set("rescore:20260619:jra:05:11", {
    completedAt: 1000,
    executionId: "queue-message-failed",
    status: "error",
    timestamp: 1000,
  });
  const coordinator = makeCoordinator();
  const result = await coordinator.claimRace({
    category: "jra",
    claimId: "claim-repair",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMap.get("rescore:20260619:jra:05:11")).toMatchObject({
    claimId: "claim-repair",
    status: "enqueued",
  });
});

test("claimRace keeps fresh active work deduplicated and reclaims abandoned claims", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(2_000_000);
  storageMap.set("rescore:20260619:nar:35:01", {
    claimId: "claim-enqueued-fresh",
    status: "enqueued",
    timestamp: 1_999_000,
  });
  storageMap.set("rescore:20260619:nar:35:02", {
    executionId: "queue-message-fresh",
    status: "started",
    timestamp: 1_999_000,
  });
  storageMap.set("rescore:20260619:nar:35:03", {
    claimId: "claim-enqueued-abandoned",
    status: "enqueued",
    timestamp: 1_699_999,
  });
  storageMap.set("rescore:20260619:nar:35:04", {
    executionId: "queue-message-abandoned",
    status: "started",
    timestamp: 139_999,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.claimRace({
      category: "nar",
      claimId: "claim-enqueued-duplicate",
      keibajoCode: "35",
      raceBango: "01",
      runYmd: "20260619",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "enqueued" });
  await expect(
    coordinator.claimRace({
      category: "nar",
      claimId: "claim-started-duplicate",
      keibajoCode: "35",
      raceBango: "02",
      runYmd: "20260619",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "started" });
  await expect(
    coordinator.claimRace({
      category: "nar",
      claimId: "claim-enqueued-repair",
      keibajoCode: "35",
      raceBango: "03",
      runYmd: "20260619",
    }),
  ).resolves.toStrictEqual({ proceed: true });
  await expect(
    coordinator.claimRace({
      category: "nar",
      claimId: "claim-started-repair",
      keibajoCode: "35",
      raceBango: "04",
      runYmd: "20260619",
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("rescore:20260619:nar:35:03")).toMatchObject({
    claimId: "claim-enqueued-repair",
    status: "enqueued",
  });
  expect(storageMap.get("rescore:20260619:nar:35:04")).toMatchObject({
    claimId: "claim-started-repair",
    status: "enqueued",
  });
  vi.useRealTimers();
});

test("claimRace uses blockConcurrencyWhile for serialisation", async () => {
  const coordinator = makeCoordinator();
  await coordinator.claimRace({
    category: "nar",
    claimId: "claim-5",
    keibajoCode: "30",
    raceBango: "02",
    runYmd: "20260619",
  });
  expect(blockConcurrencyWhileMock).toHaveBeenCalledTimes(1);
});

test("releaseRaceClaim deletes only its exact enqueued claim", async () => {
  storageMap.set("rescore:20260619:jra:05:11", {
    claimId: "claim-current",
    status: "enqueued",
    timestamp: 1000,
  });
  const coordinator = makeCoordinator();
  await coordinator.releaseRaceClaim({
    category: "jra",
    claimId: "claim-current",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(storageMap.has("rescore:20260619:jra:05:11")).toBe(false);
  expect(storageMock.delete).toHaveBeenCalledWith("rescore:20260619:jra:05:11");
});

test("an old release cannot delete a newer rescore claim", async () => {
  storageMap.set("rescore:20260619:jra:05:11", {
    claimId: "claim-new",
    status: "enqueued",
    timestamp: 2000,
  });
  const coordinator = makeCoordinator();
  await coordinator.releaseRaceClaim({
    category: "jra",
    claimId: "claim-old",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(storageMap.get("rescore:20260619:jra:05:11")).toStrictEqual({
    claimId: "claim-new",
    status: "enqueued",
    timestamp: 2000,
  });
  expect(storageMock.delete).not.toHaveBeenCalled();
});

test("fetch POST /claim-race returns proceed:true for a new race", async () => {
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-race", {
    body: JSON.stringify({
      category: "jra",
      claimId: "claim-fetch-1",
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
  storageMap.set("rescore:20260619:jra:05:11", {
    status: "enqueued",
    timestamp: Date.now(),
  });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim-race", {
    body: JSON.stringify({
      category: "jra",
      claimId: "claim-fetch-2",
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

test("fetch POST /release-race-claim releases the matching owner", async () => {
  storageMap.set("rescore:20260619:jra:05:11", {
    claimId: "claim-fetch-release",
    status: "enqueued",
    timestamp: 1000,
  });
  const coordinator = makeCoordinator();
  const response = await coordinator.fetch(
    new Request("http://do/release-race-claim", {
      body: JSON.stringify({
        category: "jra",
        claimId: "claim-fetch-release",
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260619",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(storageMap.has("rescore:20260619:jra:05:11")).toBe(false);
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

test("day-base generation fence preempts a later date and rejects its redelivery", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 20_000,
        workKey: "day-base:20260827:nar",
      },
    ],
  });
  storageMap.set("day-base-generations", {
    nar: { runYmd: "20260827", updatedAt: 10_000 },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({
    preemptedWorkKey: "day-base:20260827:nar",
    proceed: false,
    state: "preempting",
  });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "pickup",
      runYmd: "20260827",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "superseded" });
  expect(storageMap.get("day-base-generations")).toStrictEqual({
    nar: { runYmd: "20260825", updatedAt: 20_000 },
  });
  expect(storageMap.get("container-slots")).toStrictEqual({
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 20_000,
        workKey: "day-base:20260827:nar",
      },
    ],
  });
  vi.useRealTimers();
});

test("day-base generation fence activates the earliest requested date without a lease", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(25_000);
  storageMap.set("day-base-generations", {
    nar: { runYmd: "20260827", updatedAt: 10_000 },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "pickup",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      force: true,
      phase: "start",
      runYmd: "20260826",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });
  expect(storageMap.get("day-base-generations")).toStrictEqual({
    nar: { runYmd: "20260826", updatedAt: 25_000 },
  });
  vi.useRealTimers();
});

test("an abandoned day-base reservation expires when it has no live lease", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(200_000);
  storageMap.set("day-base-generations", {
    nar: { runYmd: "20260825", updatedAt: 10_000 },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "start",
      runYmd: "20260827",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });
  expect(storageMap.get("day-base-generations")).toStrictEqual({
    nar: { runYmd: "20260827", updatedAt: 200_000 },
  });
  vi.useRealTimers();
});

test("day-base generation fence keeps duplicate starts busy and clears with its owner", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(30_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 30_000,
        workKey: "day-base-stale:20260825:nar",
      },
    ],
  });
  storageMap.set("day-base-generations", {
    nar: { runYmd: "20260825", updatedAt: 20_000 },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "busy" });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "pickup",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });

  await coordinator.clearContainerSlot({
    acceptableWorkKeys: ["day-base:20260825:nar", "day-base-stale:20260825:nar"],
    doName: "predict-nar",
    workKey: "day-base:20260825:nar",
  });

  expect(storageMap.has("day-base-generations")).toBe(false);
  expect(storageMap.get("container-slots")).toStrictEqual({ leases: [] });
  vi.useRealTimers();
});

test("forced same-date day-base recovery stops the owner before a replacement build", async () => {
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: Date.now(),
        workKey: "day-base:20260825:jra",
      },
    ],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "jra",
      force: true,
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({
    preemptedWorkKey: "day-base:20260825:jra",
    proceed: false,
    state: "preempting",
  });
});

test("generation-scoped day-base ownership fences delayed force, pickup, and legacy stop", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(40_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 40_000,
        workKey: "day-base:20260825:jra:generation-a",
      },
    ],
  });
  storageMap.set("day-base-generations", {
    jra: { generationId: "generation-a", runYmd: "20260825", updatedAt: 30_000 },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "jra",
      force: true,
      generationId: "generation-b",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({
    preemptedWorkKey: "day-base:20260825:jra:generation-a",
    proceed: false,
    state: "preempting",
  });
  expect(storageMap.get("day-base-generations")).toStrictEqual({
    jra: {
      generationId: "generation-b",
      retiredGenerationIds: ["generation-a"],
      runYmd: "20260825",
      updatedAt: 40_000,
    },
  });

  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 40_000,
        workKey: "day-base:20260825:jra:generation-b",
      },
    ],
  });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "jra",
      force: true,
      generationId: "generation-a",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "superseded" });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "jra",
      generationId: "generation-a",
      phase: "pickup",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "superseded" });
  await expect(
    coordinator.checkContainerSlotStop({
      doName: "predict-jra",
      requestedAt: new Date().toISOString(),
      workKey: "day-base:20260825:jra",
    }),
  ).resolves.toBe(false);

  await coordinator.releaseContainerSlot({
    doName: "predict-jra",
    kind: "day-base",
    workKey: "day-base:20260825:jra:generation-b",
  });
  expect(storageMap.get("day-base-generations")).toStrictEqual({
    jra: {
      completed: true,
      generationId: "generation-b",
      retiredGenerationIds: ["generation-a"],
      runYmd: "20260825",
      updatedAt: 40_000,
    },
  });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "jra",
      generationId: "generation-b",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "superseded" });
  vi.useRealTimers();
});

test("generation-scoped day-base claims are idempotent across start and pickup deliveries", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(50_000);
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      generationId: "generation-a",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 50_000,
        workKey: "day-base:20260825:nar:generation-a",
      },
    ],
  });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      generationId: "generation-a",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "busy" });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      generationId: "generation-a",
      phase: "pickup",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      generationId: "generation-b",
      phase: "pickup",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "superseded" });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      phase: "start",
      runYmd: "20260825",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "superseded" });
  vi.useRealTimers();
});

test("a stale token reservation can advance while retired generations stay fenced", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(250_000);
  storageMap.set("day-base-generations", {
    nar: {
      generationId: "generation-a",
      retiredGenerationIds: ["generation-old"],
      runYmd: "20260825",
      updatedAt: 10_000,
    },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      generationId: "generation-b",
      phase: "start",
      runYmd: "20260826",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "active" });
  expect(storageMap.get("day-base-generations")).toStrictEqual({
    nar: {
      generationId: "generation-b",
      retiredGenerationIds: ["generation-old", "generation-a"],
      runYmd: "20260826",
      updatedAt: 250_000,
    },
  });
  await expect(
    coordinator.claimDayBaseGeneration({
      category: "nar",
      generationId: "generation-old",
      phase: "start",
      runYmd: "20260824",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "superseded" });
  vi.useRealTimers();
});

test("release clears only the completed day-base generation", async () => {
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: Date.now(),
        workKey: "day-base:20260825:jra",
      },
    ],
  });
  storageMap.set("day-base-generations", {
    jra: { runYmd: "20260825", updatedAt: 10_000 },
    nar: { runYmd: "20260825", updatedAt: 10_000 },
  });
  const coordinator = makeCoordinator();

  await coordinator.releaseContainerSlot({
    doName: "predict-jra",
    kind: "day-base",
    workKey: "day-base:20260825:jra",
  });

  expect(storageMap.get("day-base-generations")).toStrictEqual({
    nar: { runYmd: "20260825", updatedAt: 10_000 },
  });
  expect(storageMap.get("container-slots")).toStrictEqual({ leases: [] });
});

test("a nonmatching clear preserves the live day-base generation fence", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 20_000,
        workKey: "day-base-stale:20260825:nar",
      },
    ],
  });
  storageMap.set("day-base-generations", {
    nar: { runYmd: "20260825", updatedAt: 10_000 },
  });
  const coordinator = makeCoordinator();

  await coordinator.clearContainerSlot({
    doName: "predict-nar",
    workKey: "day-base:20260825:nar",
  });

  expect(storageMap.get("day-base-generations")).toStrictEqual({
    nar: { runYmd: "20260825", updatedAt: 10_000 },
  });
  expect(storageMap.get("container-slots")).toStrictEqual({
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 20_000,
        workKey: "day-base-stale:20260825:nar",
      },
    ],
  });
  vi.useRealTimers();
});

test("claim-day-base-generation endpoint serializes the coordinator result", async () => {
  const coordinator = makeCoordinator();
  const response = await coordinator.fetch(
    new Request("http://do/claim-day-base-generation", {
      body: JSON.stringify({ category: "ban-ei", phase: "start", runYmd: "20260825" }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );

  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({ proceed: true, state: "active" });
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

test("claimContainerSlot serializes a day-base stale-owner transfer", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(30_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 20_000,
        workKey: "day-base:20260824:nar",
      },
    ],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimContainerSlot({
      category: "nar",
      doName: "predict-nar",
      kind: "day-base",
      replaceWorkKey: "day-base:20260824:nar",
      workKey: "day-base-stale:20260824:nar",
    }),
  ).resolves.toStrictEqual({ proceed: true });
  await expect(
    coordinator.claimContainerSlot({
      category: "nar",
      doName: "predict-nar",
      kind: "day-base",
      replaceWorkKey: "day-base:20260824:nar",
      workKey: "day-base-stale:20260824:nar",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "busy" });
  expect(storageMap.get("container-slots")).toStrictEqual({
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 30_000,
        workKey: "day-base-stale:20260824:nar",
      },
    ],
  });
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
  await expect(staleResponse.json()).resolves.toStrictEqual({ allowed: false, state: "blocked" });
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
  await expect(ownerResponse.json()).resolves.toStrictEqual({ allowed: true, state: "claimed" });
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

test("rejects a concurrent same-owner stop but permits recovery after its fence lease expires", async () => {
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
  const stop = {
    doName: "predict-jra-1",
    requestedAt: "1970-01-01T00:00:20.000Z",
    workKey: "work-1",
  };

  await expect(coordinator.checkContainerSlotStop(stop)).resolves.toBe(true);
  await expect(coordinator.checkContainerSlotStop(stop)).resolves.toBe(false);
  vi.advanceTimersByTime(30_001);
  await expect(coordinator.checkContainerSlotStop(stop)).resolves.toBe(true);
  vi.useRealTimers();
});

test("persists the destroyed stop stage until slot clear and resumes without a new destroy claim", async () => {
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
  const stopBody = {
    doName: "predict-jra-1",
    requestedAt: "1970-01-01T00:00:20.000Z",
    workKey: "work-1",
  };
  await expect(coordinator.checkContainerSlotStop(stopBody)).resolves.toBe(true);

  const marked = await coordinator.fetch(
    new Request("http://do/mark-container-slot-stopped", {
      body: JSON.stringify({ doName: "predict-jra-1", workKey: "work-1" }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(marked.status).toBe(200);
  const replay = await coordinator.fetch(
    new Request("http://do/check-container-slot-stop", {
      body: JSON.stringify(stopBody),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  await expect(replay.json()).resolves.toStrictEqual({ allowed: false, state: "destroyed" });

  await coordinator.clearContainerSlot({ doName: "predict-jra-1", workKey: "work-1" });
  expect(storageMap.has("container-stop-fences")).toBe(false);
  vi.useRealTimers();
});

test("rejects a destroyed-stage marker without the matching stop fence owner", async () => {
  const coordinator = makeCoordinator();
  await expect(
    coordinator.markContainerSlotStopped({ doName: "predict-jra-1", workKey: "missing-work" }),
  ).rejects.toThrow("Container stop fence ownership lost doName=predict-jra-1");

  storageMap.set("container-stop-fences", {
    "predict-jra-1": { claimedAtMs: 1, requestedAtMs: 1, workKey: "actual-work" },
  });
  await expect(
    coordinator.markContainerSlotStopped({ doName: "predict-jra-1", workKey: "other-work" }),
  ).rejects.toThrow("Container stop fence ownership lost doName=predict-jra-1");
});

test("reports a stale matching stop fence as resumed while refreshing its claim", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(50_001);
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
  storageMap.set("container-stop-fences", {
    "predict-jra-1": { claimedAtMs: 20_000, requestedAtMs: 20_000, workKey: "work-1" },
  });
  const response = await makeCoordinator().fetch(
    new Request("http://do/check-container-slot-stop", {
      body: JSON.stringify({
        doName: "predict-jra-1",
        requestedAt: "1970-01-01T00:00:51.000Z",
        workKey: "work-1",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  await expect(response.json()).resolves.toStrictEqual({ allowed: true, state: "resumed" });
  vi.useRealTimers();
});

test("alarm durably redelivers a registered focused-full watch outbox entry then clears it", async () => {
  const send = vi.fn(async () => undefined);
  const coordinator = makeCoordinator({
    FOCUSED_FULL_COMPLETION_QUEUE: { send } as unknown as Env["FOCUSED_FULL_COMPLETION_QUEUE"],
  });
  const message: FocusedFullWatchTickMessage = {
    body: {
      category: "jra",
      daysAhead: 0,
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-08-24",
      runDateIso: "2026-08-24",
      runYmd: "20260824",
      skipDedup: true,
    },
    deadlineAtMs: 100_000,
    doName: "predict-jra",
    role: "legacy",
    type: "focused-full-watch-tick",
    watchId: "watch-1",
    workKey: "focused-full:20260824:jra:05:11",
  };
  await coordinator.registerFocusedFullWatchOutbox({
    delaySeconds: 30,
    message,
    outboxId: "tick:watch-1:1",
  });
  expect(storageMock.setAlarm).toHaveBeenCalledTimes(1);

  await coordinator.alarm();

  expect(send).toHaveBeenCalledWith(message, { delaySeconds: 30 });
  expect(storageMap.has("focused-full-watch-outbox")).toBe(false);
  expect(storageMap.has("focused-full-active-watches")).toBe(true);
});

test("alarm preserves the watch outbox when Queue delivery fails", async () => {
  const send = vi.fn(async () => {
    throw new Error("queue unavailable");
  });
  const coordinator = makeCoordinator({
    FOCUSED_FULL_COMPLETION_QUEUE: { send } as unknown as Env["FOCUSED_FULL_COMPLETION_QUEUE"],
  });
  storageMap.set("focused-full-watch-outbox", {
    "completion:watch-1": {
      message: {
        body: {
          category: "jra",
          daysAhead: 0,
          keibajoCode: "05",
          mode: "full",
          raceBango: "11",
          runDate: "2026-08-24",
          runDateIso: "2026-08-24",
          runYmd: "20260824",
          skipDedup: true,
        },
        doName: "predict-jra",
        outcome: "success",
        role: "legacy",
        type: "focused-full-completion",
        watchId: "watch-1",
        workKey: "focused-full:20260824:jra:05:11",
      },
    },
  });

  await expect(coordinator.alarm()).rejects.toThrow("queue unavailable");
  expect(storageMap.has("focused-full-watch-outbox")).toBe(true);
});

test("alarm redelivers an active watch after its producer outbox was cleared", async () => {
  const send = vi.fn(async () => undefined);
  const coordinator = makeCoordinator({
    FOCUSED_FULL_COMPLETION_QUEUE: { send } as unknown as Env["FOCUSED_FULL_COMPLETION_QUEUE"],
  });
  const message = {
    body: {
      category: "jra",
      daysAhead: 0,
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-08-24",
      runDateIso: "2026-08-24",
      runYmd: "20260824",
      skipDedup: true,
    },
    doName: "predict-jra",
    outcome: "success",
    role: "legacy",
    type: "focused-full-completion",
    watchId: "active-watch",
    workKey: "focused-full:20260824:jra:05:11",
  } as const;
  await coordinator.registerFocusedFullWatchOutbox({ message, outboxId: "active-outbox" });
  await coordinator.clearFocusedFullWatchOutbox({ outboxId: "active-outbox" });

  await coordinator.alarm();

  expect(send).toHaveBeenCalledWith(message);
  expect(storageMock.setAlarm).toHaveBeenCalledTimes(2);
});

test("outbox supports immediate messages, keeps siblings on clear, and rejects a missing binding", async () => {
  const send = vi.fn(async () => undefined);
  const coordinator = makeCoordinator({
    FOCUSED_FULL_COMPLETION_QUEUE: { send } as unknown as Env["FOCUSED_FULL_COMPLETION_QUEUE"],
  });
  const message = {
    body: {
      category: "jra",
      daysAhead: 0,
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-08-24",
      runDateIso: "2026-08-24",
      runYmd: "20260824",
      skipDedup: true,
    },
    doName: "predict-jra",
    outcome: "success",
    role: "legacy",
    type: "focused-full-completion",
    watchId: "watch-immediate",
    workKey: "focused-full:20260824:jra:05:11",
  } as const;
  await coordinator.registerFocusedFullWatchOutbox({ message, outboxId: "immediate" });
  storageMap.set("focused-full-watch-outbox", {
    immediate: { message },
    sibling: { message: { ...message, watchId: "watch-sibling" } },
  });
  await coordinator.clearFocusedFullWatchOutbox({ outboxId: "sibling" });
  expect(storageMap.get("focused-full-watch-outbox")).toStrictEqual({ immediate: { message } });
  await coordinator.alarm();
  expect(send).toHaveBeenCalledWith(message);

  storageMap.set("focused-full-watch-outbox", { immediate: { message } });
  await expect(makeCoordinator().alarm()).rejects.toThrow(
    "FOCUSED_FULL_COMPLETION_QUEUE binding is missing",
  );
  storageMap.delete("focused-full-watch-outbox");
  storageMap.delete("focused-full-active-watches");
  await expect(makeCoordinator().alarm()).resolves.toBeUndefined();
});

test("terminal stopped marker rejects a missing or mismatched terminal claim", async () => {
  const coordinator = makeCoordinator();
  await expect(
    coordinator.markFocusedFullTerminalWatchStopped({ claimId: "claim-1", watchId: "missing" }),
  ).rejects.toThrow("Focused-full terminal watch ownership lost watchId=missing");
  storageMap.set("focused-full-terminal-watch:mismatch", {
    claimId: "claim-other",
    status: "processing",
    timestamp: 1,
  });
  await expect(
    coordinator.markFocusedFullTerminalWatchStopped({ claimId: "claim-1", watchId: "mismatch" }),
  ).rejects.toThrow("Focused-full terminal watch ownership lost watchId=mismatch");
});

test("terminal watch reclaim reports the durable stopped stage", async () => {
  storageMap.set("focused-full-terminal-watch:watch-stopped", {
    claimId: "claim-1",
    containerStoppedAt: 10,
    status: "processing",
    timestamp: 10,
  });
  await expect(
    makeCoordinator().claimFocusedFullTerminalWatch({
      claimId: "claim-1",
      staleAfterMs: 960_000,
      watchId: "watch-stopped",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "stopped" });
});

test("one stop atomically accepts a stale day-base owner and rejects its lease-less redelivery", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 19_000,
        workKey: "day-base-stale:20260824:nar",
      },
    ],
  });
  const coordinator = makeCoordinator();
  const stop = {
    acceptableWorkKeys: ["day-base:20260824:nar", "day-base-stale:20260824:nar"],
    doName: "predict-nar",
    requestedAt: "1970-01-01T00:00:20.000Z",
    workKey: "day-base:20260824:nar",
  };

  await expect(coordinator.checkContainerSlotStop(stop)).resolves.toBe(true);
  await coordinator.clearContainerSlot({
    acceptableWorkKeys: stop.acceptableWorkKeys,
    doName: stop.doName,
    workKey: stop.workKey,
  });
  await expect(coordinator.checkContainerSlotStop(stop)).resolves.toBe(false);
  await expect(coordinator.checkContainerSlotStop({ ...stop, allowUnowned: true })).resolves.toBe(
    true,
  );
  await coordinator.clearContainerSlot({
    acceptableWorkKeys: stop.acceptableWorkKeys,
    doName: stop.doName,
    workKey: stop.workKey,
  });
  vi.useRealTimers();
});

test("one stop atomically accepts the canonical day-base owner", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 19_000,
        workKey: "day-base:20260824:nar",
      },
    ],
  });

  await expect(
    makeCoordinator().checkContainerSlotStop({
      acceptableWorkKeys: ["day-base:20260824:nar", "day-base-stale:20260824:nar"],
      doName: "predict-nar",
      requestedAt: "1970-01-01T00:00:20.000Z",
      workKey: "day-base:20260824:nar",
    }),
  ).resolves.toBe(true);
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

test("stop fencing permits an idle unowned stop and rejects malformed or conflicting requests", async () => {
  const coordinator = makeCoordinator();
  await expect(
    coordinator.checkContainerSlotStop({
      doName: "predict-jra-1",
      requestedAt: "2026-08-22T00:00:00.000Z",
    }),
  ).resolves.toBe(true);
  await coordinator.clearContainerSlot({ doName: "predict-jra-1" });
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

test("an unscoped administrative stop cannot fence an active lease by default", async () => {
  storageMap.set("container-slots", {
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        timestamp: Date.now(),
        workKey: "focused-full:20260823:jra:07:12",
      },
    ],
  });

  await expect(
    makeCoordinator().checkContainerSlotStop({
      doName: "predict-jra-1",
      requestedAt: new Date().toISOString(),
    }),
  ).resolves.toBe(false);
  expect(storageMap.has("container-stop-fences")).toBe(false);
});

test("day-base cleanup cannot stop a DO while a focused-full watch owns it", async () => {
  storageMap.set("container-slots", {
    leases: [
      {
        category: "nar",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: Date.now(),
        workKey: "day-base:20260826:nar",
      },
    ],
  });
  storageMap.set("focused-full-active-watches", {
    "watch-1": {
      message: {
        doName: "predict-nar",
        workKey: "focused-full:20260826:nar:30:02",
        watchId: "watch-1",
      },
    },
    "watch-2": {
      message: {
        doName: "predict-nar",
        workKey: "focused-full:20260826:nar:30:01",
        watchId: "watch-2",
      },
    },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.checkContainerSlotStop({
      acceptableWorkKeys: ["day-base:20260826:nar"],
      doName: "predict-nar",
      requestedAt: new Date().toISOString(),
      workKey: "day-base:20260826:nar",
    }),
  ).resolves.toBe(false);
  expect(storageMap.has("container-stop-fences")).toBe(false);

  await expect(
    coordinator.checkContainerSlotStop({
      acceptableWorkKeys: ["focused-full:20260826:nar:30:02"],
      doName: "predict-nar",
      requestedAt: new Date().toISOString(),
      workKey: "focused-full:20260826:nar:30:02",
    }),
  ).resolves.toBe(false);
  storageMap.set("focused-full-active-watches", {
    "watch-1": {
      message: {
        doName: "predict-nar",
        workKey: "focused-full:20260826:nar:30:02",
        watchId: "watch-1",
      },
    },
  });

  await expect(
    coordinator.claimContainerSlot({
      category: "nar",
      doName: "predict-nar",
      kind: "focused-full",
      staleAfterMs: 1_860_000,
      workKey: "focused-full:20260826:nar:30:02",
    }),
  ).resolves.toStrictEqual({ proceed: true });

  await expect(
    coordinator.checkContainerSlotStop({
      acceptableWorkKeys: ["focused-full:20260826:nar:30:02"],
      doName: "predict-nar",
      requestedAt: new Date().toISOString(),
      workKey: "focused-full:20260826:nar:30:02",
    }),
  ).resolves.toBe(true);
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
    "predict-jra-0": {
      claimedAtMs: expect.any(Number),
      requestedAtMs: expect.any(Number),
    },
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

test("reserveFocusedFullRaceEnqueue counts stale reservations as bounded repairs", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_861_001);
  storageMap.set("focused-full:20260823:nar:44:03", {
    focusedFullRepairCount: MAX_FOCUSED_FULL_REPAIRS,
    status: "enqueued",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.reserveFocusedFullRaceEnqueue({
      category: "nar",
      doName: "predict-nar-1",
      keibajoCode: "44",
      raceBango: "03",
      reservationId: "reservation-too-many",
      runYmd: "20260823",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "repair-budget-exhausted" });
  expect(storageMock.put).not.toHaveBeenCalled();
  vi.useRealTimers();
});

test("reserveFocusedFullRaceRepair atomically keeps the missing race active ahead of its waiter", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("focused-full:20260824:ban-ei:83:05", {
    doName: "predict-ban-ei-2",
    priorityMs: 10_000,
    status: "started",
    timestamp: 1_000,
  });
  storageMap.set("focused-full:20260824:ban-ei:83:08", {
    doName: "predict-ban-ei-2",
    priorityMs: 30_000,
    status: "queued",
    timestamp: 2_000,
  });
  storageMap.set("focused-full-lane:predict-ban-ei-2", {
    activeRaceKey: "focused-full:20260824:ban-ei:83:05",
    startedAt: 1_000,
    waiters: ["focused-full:20260824:ban-ei:83:08"],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.reserveFocusedFullRaceRepair({
      category: "ban-ei",
      doName: "predict-ban-ei-2",
      keibajoCode: "83",
      raceBango: "05",
      reservationId: "repair-1",
      runYmd: "20260824",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("focused-full:20260824:ban-ei:83:05")).toStrictEqual({
    doName: "predict-ban-ei-2",
    focusedFullRepairCount: 1,
    priorityMs: 10_000,
    reservationId: "repair-1",
    status: "enqueued",
    timestamp: 20_000,
  });
  expect(storageMap.get("focused-full:20260824:ban-ei:83:08")).toStrictEqual({
    doName: "predict-ban-ei-2",
    priorityMs: 30_000,
    status: "queued",
    timestamp: 2_000,
  });
  expect(storageMap.get("focused-full-lane:predict-ban-ei-2")).toStrictEqual({
    activeRaceKey: "focused-full:20260824:ban-ei:83:05",
    startedAt: 20_000,
    waiters: ["focused-full:20260824:ban-ei:83:08"],
  });

  await expect(
    coordinator.claimFocusedFullRace({
      category: "ban-ei",
      doName: "predict-ban-ei-2",
      force: true,
      keibajoCode: "83",
      raceBango: "05",
      runYmd: "20260824",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "repair-reserved" });
  expect(storageMap.get("focused-full-lane:predict-ban-ei-2")).toStrictEqual({
    activeRaceKey: "focused-full:20260824:ban-ei:83:05",
    startedAt: 20_000,
    waiters: ["focused-full:20260824:ban-ei:83:08"],
  });
  vi.useRealTimers();
});

test("reserveFocusedFullRaceRepair rejects a duplicate and a different active lane owner", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  storageMap.set("focused-full:20260824:ban-ei:83:05", {
    doName: "predict-ban-ei-2",
    reservationId: "repair-1",
    status: "enqueued",
    timestamp: 19_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.reserveFocusedFullRaceRepair({
      category: "ban-ei",
      doName: "predict-ban-ei-2",
      keibajoCode: "83",
      raceBango: "05",
      reservationId: "repair-2",
      runYmd: "20260824",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "enqueued" });
  expect(storageMock.put).not.toHaveBeenCalled();

  storageMap.set("focused-full:20260824:ban-ei:83:05", {
    doName: "predict-ban-ei-2",
    status: "error",
    timestamp: 1_000,
  });
  storageMap.set("focused-full-lane:predict-ban-ei-2", {
    activeRaceKey: "focused-full:20260824:ban-ei:83:08",
    startedAt: 2_000,
    waiters: [],
  });
  await expect(
    coordinator.reserveFocusedFullRaceRepair({
      category: "ban-ei",
      doName: "predict-ban-ei-2",
      keibajoCode: "83",
      raceBango: "05",
      reservationId: "repair-3",
      runYmd: "20260824",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "lane-conflict" });
  expect(storageMock.put).not.toHaveBeenCalled();
  vi.useRealTimers();
});

test("reserveFocusedFullRaceRepair replaces a terminal record when its required cache is missing", async () => {
  storageMap.set("focused-full:20260824:nar:35:01", {
    completedAt: 1_000,
    doName: "predict-nar-1",
    priorityMs: 500,
    status: "success",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.reserveFocusedFullRaceRepair({
      category: "nar",
      doName: "predict-nar-1",
      keibajoCode: "35",
      raceBango: "01",
      reservationId: "repair-1",
      runYmd: "20260824",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("focused-full:20260824:nar:35:01")).toMatchObject({
    reservationId: "repair-1",
    status: "enqueued",
  });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toMatchObject({
    activeRaceKey: "focused-full:20260824:nar:35:01",
  });
});

test("reserveFocusedFullRaceRepair refuses a third repair generation", async () => {
  storageMap.set("focused-full:20260824:jra:05:03", {
    doName: "predict-jra-1",
    focusedFullRepairCount: MAX_FOCUSED_FULL_REPAIRS,
    status: "error",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await expect(
    coordinator.reserveFocusedFullRaceRepair({
      category: "jra",
      doName: "predict-jra-1",
      keibajoCode: "05",
      raceBango: "03",
      reservationId: "repair-too-many",
      runYmd: "20260824",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "repair-budget-exhausted" });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("completeFocusedFullRace does not downgrade a durable success", async () => {
  storageMap.set("focused-full:20260824:jra:05:03", {
    doName: "predict-jra-1",
    focusedFullRepairCount: 1,
    status: "success",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.completeFocusedFullRace({
    category: "jra",
    keibajoCode: "05",
    raceBango: "03",
    runYmd: "20260824",
    status: "error",
  });
  expect(storageMock.put).not.toHaveBeenCalled();
  expect(storageMap.get("focused-full:20260824:jra:05:03")).toMatchObject({
    focusedFullRepairCount: 1,
    status: "success",
  });
});

test("cancelFocusedFullRaceRepair restores the active claim without promoting its waiter", async () => {
  storageMap.set("focused-full:20260824:nar:35:01", {
    doName: "predict-nar-1",
    priorityMs: 500,
    reservationId: "repair-1",
    status: "enqueued",
    timestamp: 2_000,
  });
  storageMap.set("focused-full-lane:predict-nar-1", {
    activeRaceKey: "focused-full:20260824:nar:35:01",
    startedAt: 2_000,
    waiters: ["focused-full:20260824:nar:35:04"],
  });
  const coordinator = makeCoordinator();
  await coordinator.cancelFocusedFullRaceRepair({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "35",
    raceBango: "01",
    reservationId: "repair-1",
    runYmd: "20260824",
  });
  expect(storageMap.get("focused-full:20260824:nar:35:01")).toStrictEqual({
    doName: "predict-nar-1",
    priorityMs: 500,
    status: "started",
    timestamp: 2_000,
  });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toStrictEqual({
    activeRaceKey: "focused-full:20260824:nar:35:01",
    startedAt: 2_000,
    waiters: ["focused-full:20260824:nar:35:04"],
  });
});

test("cancelFocusedFullRaceRepair ignores a superseded reservation", async () => {
  storageMap.set("focused-full:20260824:nar:35:01", {
    doName: "predict-nar-1",
    reservationId: "repair-new",
    status: "enqueued",
    timestamp: 2_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.cancelFocusedFullRaceRepair({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "35",
    raceBango: "01",
    reservationId: "repair-old",
    runYmd: "20260824",
  });
  expect(storageMock.put).not.toHaveBeenCalled();
});

test("cancelFocusedFullRaceRepair recreates a missing legacy lane for the current retry", async () => {
  storageMap.set("focused-full:20260824:nar:35:01", {
    doName: "predict-nar-1",
    reservationId: "repair-1",
    status: "enqueued",
    timestamp: 2_000,
  });
  const coordinator = makeCoordinator();
  await coordinator.cancelFocusedFullRaceRepair({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "35",
    raceBango: "01",
    reservationId: "repair-1",
    runYmd: "20260824",
  });
  expect(storageMap.get("focused-full:20260824:nar:35:01")).toMatchObject({ status: "started" });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toMatchObject({
    activeRaceKey: "focused-full:20260824:nar:35:01",
    waiters: [],
  });
});

test("cancelFocusedFullRaceRepair queues behind a newer lane owner without preempting it", async () => {
  storageMap.set("focused-full:20260824:nar:35:01", {
    doName: "predict-nar-1",
    priorityMs: 500,
    reservationId: "repair-1",
    status: "enqueued",
    timestamp: 2_000,
  });
  storageMap.set("focused-full-lane:predict-nar-1", {
    activeRaceKey: "focused-full:20260824:nar:35:04",
    startedAt: 3_000,
    waiters: [],
  });
  const coordinator = makeCoordinator();
  await coordinator.cancelFocusedFullRaceRepair({
    category: "nar",
    doName: "predict-nar-1",
    keibajoCode: "35",
    raceBango: "01",
    reservationId: "repair-1",
    runYmd: "20260824",
  });
  expect(storageMap.get("focused-full:20260824:nar:35:01")).toStrictEqual({
    doName: "predict-nar-1",
    priorityMs: 500,
    status: "queued",
    timestamp: 2_000,
  });
  expect(storageMap.get("focused-full-lane:predict-nar-1")).toStrictEqual({
    activeRaceKey: "focused-full:20260824:nar:35:04",
    startedAt: 3_000,
    waiters: ["focused-full:20260824:nar:35:01"],
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

test("claimFocusedFullRace keeps a later delivery queued behind an earlier fresh reservation", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(10_000);
  storageMap.set("focused-full:20260823:jra:04:01", {
    doName: "predict-jra-2",
    priorityMs: 100,
    reservationId: "reservation-early",
    status: "enqueued",
    timestamp: 9_500,
  });
  storageMap.set("focused-full:20260823:jra:04:10", {
    doName: "predict-jra-2",
    priorityMs: 200,
    reservationId: "reservation-late",
    status: "enqueued",
    timestamp: 9_600,
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      keibajoCode: "04",
      raceBango: "10",
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "queued" });
  expect(storageMap.get("focused-full:20260823:jra:04:10")).toStrictEqual({
    doName: "predict-jra-2",
    priorityMs: 200,
    reservationId: "reservation-late",
    status: "queued",
    timestamp: 9_600,
  });
  expect(storageMap.has("focused-full-lane:predict-jra-2")).toBe(false);

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      keibajoCode: "04",
      raceBango: "01",
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(storageMap.get("focused-full-lane:predict-jra-2")).toMatchObject({
    activeRaceKey: "focused-full:20260823:jra:04:01",
  });
  vi.useRealTimers();
});

test("claimFocusedFullRace force takes over a stale earlier lane reservation", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(10_000);
  storageMap.set("focused-full:20260823:jra:04:01", {
    doName: "predict-jra-2",
    priorityMs: 100,
    timestamp: 9_500,
  });
  storageMap.set("focused-full-lane:predict-jra-2", {
    activeRaceKey: "focused-full:20260823:jra:04:01",
    startedAt: 9_500,
    waiters: ["focused-full:20260823:jra:04:10"],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      force: true,
      keibajoCode: "04",
      raceBango: "10",
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "forced" });
  expect(storageMap.get("focused-full:20260823:jra:04:01")).toMatchObject({
    status: "error",
  });
  expect(storageMap.get("focused-full-lane:predict-jra-2")).toMatchObject({
    activeRaceKey: "focused-full:20260823:jra:04:10",
    waiters: [],
  });
  vi.useRealTimers();
});

test("claimFocusedFullRace force preserves a terminal earlier reservation", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(10_000);
  storageMap.set("focused-full:20260823:jra:04:01", {
    status: "success",
    timestamp: 9_500,
  });
  storageMap.set("focused-full-lane:predict-jra-2", {
    activeRaceKey: "focused-full:20260823:jra:04:01",
    startedAt: 9_500,
    waiters: [],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      force: true,
      keibajoCode: "04",
      raceBango: "10",
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "forced" });
  expect(storageMap.get("focused-full:20260823:jra:04:01")).toMatchObject({
    status: "success",
  });
  vi.useRealTimers();
});

test("claimFocusedFullRace force does not duplicate an active same race", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(10_000);
  const raceKey = "focused-full:20260823:jra:04:10";
  storageMap.set(raceKey, {
    doName: "predict-jra-2",
    priorityMs: 200,
    status: "started",
    timestamp: 9_500,
  });
  storageMap.set("focused-full-lane:predict-jra-2", {
    activeRaceKey: raceKey,
    startedAt: 9_500,
    waiters: [],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      force: true,
      keibajoCode: "04",
      raceBango: "10",
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "resumed" });
  expect(storageMap.get(raceKey)).toMatchObject({ status: "started", timestamp: 9_500 });
  vi.useRealTimers();
});

test("claimFocusedFullRace ignores an expired earlier reservation", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(10_000);
  storageMap.set("focused-full:20260823:jra:04:01", {
    doName: "predict-jra-2",
    priorityMs: 100,
    reservationId: "reservation-expired",
    status: "enqueued",
    timestamp: 8_999,
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      keibajoCode: "04",
      raceBango: "10",
      raceStartAtJst: "1970-01-01T00:00:00.200Z",
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
  vi.useRealTimers();
});

test("claimFocusedFullRace ignores earlier reservations on another shard or in terminal state", async () => {
  storageMap.set("focused-full:20260823:jra:01:01", {
    doName: "predict-jra-0",
    priorityMs: 100,
    reservationId: "reservation-other-shard",
    status: "enqueued",
    timestamp: Date.now(),
  });
  storageMap.set("focused-full:20260823:jra:04:01", {
    doName: "predict-jra-2",
    priorityMs: 100,
    reservationId: "reservation-complete",
    status: "success",
    timestamp: Date.now(),
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      keibajoCode: "04",
      raceBango: "10",
      raceStartAtJst: "2026-08-23T15:00:00+09:00",
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: true });
});

test("claimFocusedFullRace breaks equal post-time reservation ties by race key", async () => {
  const priorityMs = Date.now() + 60_000;
  storageMap.set("focused-full:20260823:jra:04:01", {
    doName: "predict-jra-2",
    priorityMs,
    reservationId: "reservation-lexically-first",
    status: "enqueued",
    timestamp: Date.now(),
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "jra",
      doName: "predict-jra-2",
      keibajoCode: "07",
      raceBango: "01",
      raceStartAtJst: new Date(priorityMs).toISOString(),
      runYmd: "20260823",
      staleAfterMs: 1_000,
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "queued" });
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

test("fetch exposes atomic focused-full repair reservation and cancellation", async () => {
  storageMap.set("focused-full:20260824:nar:35:01", {
    doName: "predict-nar-1",
    status: "started",
    timestamp: 1_000,
  });
  storageMap.set("focused-full-lane:predict-nar-1", {
    activeRaceKey: "focused-full:20260824:nar:35:01",
    startedAt: 1_000,
    waiters: [],
  });
  const coordinator = makeCoordinator();
  const reserveResponse = await coordinator.fetch(
    new Request("http://do/reserve-focused-full-race-repair", {
      body: JSON.stringify({
        category: "nar",
        doName: "predict-nar-1",
        keibajoCode: "35",
        raceBango: "01",
        reservationId: "repair-1",
        runYmd: "20260824",
        staleAfterMs: 1_860_000,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(reserveResponse.status).toBe(200);
  await expect(reserveResponse.json()).resolves.toStrictEqual({ proceed: true });
  const cancelResponse = await coordinator.fetch(
    new Request("http://do/cancel-focused-full-race-repair", {
      body: JSON.stringify({
        category: "nar",
        doName: "predict-nar-1",
        keibajoCode: "35",
        raceBango: "01",
        reservationId: "repair-1",
        runYmd: "20260824",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(cancelResponse.status).toBe(200);
  expect(storageMap.get("focused-full:20260824:nar:35:01")).toMatchObject({
    status: "started",
  });
});

test("fetch rejects GET for focused-full repair endpoints", async () => {
  const coordinator = makeCoordinator();
  const reserveResponse = await coordinator.fetch(
    new Request("http://do/reserve-focused-full-race-repair"),
  );
  const cancelResponse = await coordinator.fetch(
    new Request("http://do/cancel-focused-full-race-repair"),
  );
  expect(reserveResponse.status).toBe(405);
  expect(cancelResponse.status).toBe(405);
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

test("claimFocusedFullTerminalWatch claims a new watch", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(2_000);
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullTerminalWatch({
      claimId: "watch-claim-1",
      staleAfterMs: 60_000,
      watchId: "container:focused-full:20260824:jra:05:11",
    }),
  ).resolves.toStrictEqual({ proceed: true });
  expect(
    storageMap.get("focused-full-terminal-watch:container:focused-full:20260824:jra:05:11"),
  ).toStrictEqual({
    claimId: "watch-claim-1",
    status: "processing",
    timestamp: 2_000,
  });
  vi.useRealTimers();
});

test("forced focused-full recovery does not duplicate a fresh lane owner", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(20_000);
  const raceKey = "focused-full:20260826:nar:30:02";
  storageMap.set(raceKey, {
    doName: "predict-nar",
    status: "started",
    timestamp: 10_000,
  });
  storageMap.set("focused-full-lane:predict-nar", {
    activeRaceKey: raceKey,
    startedAt: 10_000,
    waiters: [],
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullRace({
      category: "nar",
      doName: "predict-nar",
      force: true,
      keibajoCode: "30",
      raceBango: "02",
      runYmd: "20260826",
      staleAfterMs: 1_860_000,
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "resumed" });
  expect(storageMap.get(raceKey)).toMatchObject({ status: "started", timestamp: 10_000 });
  expect(storageMap.get("focused-full-lane:predict-nar")).toMatchObject({ startedAt: 10_000 });
  vi.useRealTimers();
});

test("claimFocusedFullTerminalWatch lets the same owner resume", async () => {
  storageMap.set("focused-full-terminal-watch:watch-1", {
    claimId: "watch-claim-1",
    status: "processing",
    timestamp: 1_000,
  });
  storageMap.set("focused-full-active-watches", {
    "watch-1": { message: { watchId: "watch-1" } },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullTerminalWatch({
      claimId: "watch-claim-1",
      staleAfterMs: 60_000,
      watchId: "watch-1",
    }),
  ).resolves.toStrictEqual({ proceed: true });
});

test("claimFocusedFullTerminalWatch rejects a fresh competing owner", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(2_000);
  storageMap.set("focused-full-terminal-watch:watch-1", {
    claimId: "watch-claim-1",
    status: "processing",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullTerminalWatch({
      claimId: "watch-claim-2",
      staleAfterMs: 60_000,
      watchId: "watch-1",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "processing" });
  expect(storageMock.put).not.toHaveBeenCalled();
  vi.useRealTimers();
});

test("claimFocusedFullTerminalWatch transfers a stale watch", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(62_000);
  storageMap.set("focused-full-terminal-watch:watch-1", {
    claimId: "watch-claim-1",
    status: "processing",
    timestamp: 1_000,
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullTerminalWatch({
      claimId: "watch-claim-2",
      staleAfterMs: 60_000,
      watchId: "watch-1",
    }),
  ).resolves.toStrictEqual({ proceed: true, state: "stale" });
  expect(storageMap.get("focused-full-terminal-watch:watch-1")).toStrictEqual({
    claimId: "watch-claim-2",
    status: "processing",
    timestamp: 62_000,
  });
  expect(storageMap.has("focused-full-active-watches")).toBe(false);
  vi.useRealTimers();
});

test("claimFocusedFullTerminalWatch never reclaims a terminal watch", async () => {
  storageMap.set("focused-full-terminal-watch:watch-1", {
    claimId: "watch-claim-1",
    status: "terminal",
    timestamp: 1_000,
  });
  storageMap.set("focused-full-active-watches", {
    "watch-1": { message: { watchId: "watch-1" } },
    sibling: { message: { watchId: "sibling" } },
  });
  const coordinator = makeCoordinator();

  await expect(
    coordinator.claimFocusedFullTerminalWatch({
      claimId: "watch-claim-1",
      staleAfterMs: 60_000,
      watchId: "watch-1",
    }),
  ).resolves.toStrictEqual({ proceed: false, state: "terminal" });
  expect(storageMap.get("focused-full-active-watches")).toStrictEqual({
    sibling: { message: { watchId: "sibling" } },
  });
});

test("completeFocusedFullTerminalWatch marks only the current owner terminal", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(3_000);
  storageMap.set("focused-full-terminal-watch:watch-1", {
    claimId: "watch-claim-current",
    status: "processing",
    timestamp: 2_000,
  });
  storageMap.set("focused-full-active-watches", {
    "watch-1": { message: { watchId: "watch-1" } },
  });
  const coordinator = makeCoordinator();

  await coordinator.completeFocusedFullTerminalWatch({
    claimId: "watch-claim-stale",
    watchId: "watch-1",
  });
  expect(storageMap.get("focused-full-terminal-watch:watch-1")).toStrictEqual({
    claimId: "watch-claim-current",
    status: "processing",
    timestamp: 2_000,
  });

  await coordinator.completeFocusedFullTerminalWatch({
    claimId: "watch-claim-current",
    watchId: "watch-1",
  });
  expect(storageMap.get("focused-full-terminal-watch:watch-1")).toStrictEqual({
    claimId: "watch-claim-current",
    completedAt: 3_000,
    status: "terminal",
    timestamp: 3_000,
  });
  expect(storageMap.has("focused-full-active-watches")).toBe(false);
  vi.useRealTimers();
});

test("fetch exposes focused full terminal watch claim and completion", async () => {
  const coordinator = makeCoordinator();
  const claimResponse = await coordinator.fetch(
    new Request("http://do/claim-focused-full-terminal-watch", {
      body: JSON.stringify({
        claimId: "watch-claim-1",
        staleAfterMs: 60_000,
        watchId: "watch-1",
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(claimResponse.status).toBe(200);
  const completeResponse = await coordinator.fetch(
    new Request("http://do/complete-focused-full-terminal-watch", {
      body: JSON.stringify({ claimId: "watch-claim-1", watchId: "watch-1" }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  expect(completeResponse.status).toBe(200);
});
