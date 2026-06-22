// Run with bun. Tests for PredictRunCoordinator Durable Object.

import { beforeEach, expect, test, vi } from "vitest";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import type { Env } from "./types";

interface StoredRecord {
  status: string;
  timestamp: number;
  racesPredicted?: number;
  completedAt?: number;
}

const storageMap = new Map<string, StoredRecord>();

const storageMock = {
  get: vi.fn(async (key: string) => storageMap.get(key)),
  put: vi.fn(async (key: string, value: StoredRecord) => {
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
  storageMap.clear();
  storageMock.get.mockClear();
  storageMock.put.mockClear();
  blockConcurrencyWhileMock.mockClear();
  blockConcurrencyWhileMock.mockImplementation(async (fn: () => Promise<unknown>) => fn());
});

test("claim returns proceed:true for a new runYmd/category", async () => {
  const coordinator = makeCoordinator();
  const result = await coordinator.claim("20260603", "jra");
  expect(result).toStrictEqual({ proceed: true });
  expect(storageMock.put).toHaveBeenCalledTimes(1);
});

test("claim returns proceed:false when status is started", async () => {
  storageMap.set("run:20260603:jra", { status: "started", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const result = await coordinator.claim("20260603", "jra");
  expect(result).toStrictEqual({ proceed: false, state: "started" });
  expect(storageMock.put).not.toHaveBeenCalled();
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

test("fetch POST /claim returns proceed:false when already started", async () => {
  storageMap.set("run:20260603:jra", { status: "started", timestamp: 1000 });
  const coordinator = makeCoordinator();
  const request = new Request("http://do/claim", {
    body: JSON.stringify({ runYmd: "20260603", category: "jra" }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const response = await coordinator.fetch(request);
  expect(response.status).toBe(200);
  const body = (await response.json()) as { proceed: boolean; state: string };
  expect(body.proceed).toBe(false);
  expect(body.state).toBe("started");
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
