// Run with bun. Tests for the DO-backed run-state helpers.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const fetchMock = vi.fn();
const idFromNameMock = vi.fn(() => ({ name: "predict-run-coordinator" }));
const getMock = vi.fn(() => ({ fetch: fetchMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {
    get: getMock,
    idFromName: idFromNameMock,
  } as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

import { claimRescoreRace, claimRun, completeRun, getRunState } from "./do-state";

beforeEach(() => {
  fetchMock.mockClear();
  idFromNameMock.mockClear();
  getMock.mockClear();
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ proceed: true }), { status: 200 }));
});

test("claimRun calls DO /claim and returns the result", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ proceed: true }), { status: 200 }));
  const result = await claimRun({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toStrictEqual({ proceed: true });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/claim");
  expect(req.method).toBe("POST");
});

test("claimRun returns proceed:false when DO returns it", async () => {
  fetchMock.mockResolvedValue(
    new Response(JSON.stringify({ proceed: false, state: "started" }), { status: 200 }),
  );
  const result = await claimRun({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toStrictEqual({ proceed: false, state: "started" });
});

test("claimRun throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(claimRun({ category: "jra", env: makeEnv(), runYmd: "20260603" })).rejects.toThrow(
    "DO claim failed: 500",
  );
});

test("completeRun calls DO /complete with correct payload", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await completeRun({
    category: "jra",
    env: makeEnv(),
    racesPredicted: 8,
    runYmd: "20260603",
    status: "success",
  });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/complete");
  expect(req.method).toBe("POST");
  const body = (await req.json()) as { status: string; racesPredicted: number };
  expect(body.status).toBe("success");
  expect(body.racesPredicted).toBe(8);
});

test("completeRun throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    completeRun({
      category: "jra",
      env: makeEnv(),
      racesPredicted: 0,
      runYmd: "20260603",
      status: "error",
    }),
  ).rejects.toThrow("DO complete failed: 500");
});

test("getRunState calls DO /state with correct query params", async () => {
  fetchMock.mockResolvedValue(
    new Response(JSON.stringify({ state: { status: "success" } }), { status: 200 }),
  );
  const result = await getRunState({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/state?category=jra&runYmd=20260603");
  expect(result).toStrictEqual({ state: { status: "success" } });
});

test("getRunState throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    getRunState({ category: "jra", env: makeEnv(), runYmd: "20260603" }),
  ).rejects.toThrow("DO getState failed: 500");
});

test("claimRun uses singleton DO name predict-run-coordinator", async () => {
  await claimRun({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(idFromNameMock).toHaveBeenCalledWith("predict-run-coordinator");
});

test("claimRescoreRace calls DO /claim-race and returns the result", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ proceed: true }), { status: 200 }));
  const result = await claimRescoreRace({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(result).toStrictEqual({ proceed: true });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/claim-race");
  expect(req.method).toBe("POST");
});

test("claimRescoreRace sends the per-race key fields in the body", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ proceed: true }), { status: 200 }));
  await claimRescoreRace({
    category: "nar",
    env: makeEnv(),
    keibajoCode: "30",
    raceBango: "02",
    runYmd: "20260619",
  });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  const body = (await req.json()) as {
    category: string;
    keibajoCode: string;
    raceBango: string;
    runYmd: string;
  };
  expect(body.category).toBe("nar");
  expect(body.keibajoCode).toBe("30");
  expect(body.raceBango).toBe("02");
  expect(body.runYmd).toBe("20260619");
});

test("claimRescoreRace returns proceed:false when DO returns it", async () => {
  fetchMock.mockResolvedValue(
    new Response(JSON.stringify({ proceed: false, state: "enqueued" }), { status: 200 }),
  );
  const result = await claimRescoreRace({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(result).toStrictEqual({ proceed: false, state: "enqueued" });
});

test("claimRescoreRace throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    claimRescoreRace({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260619",
    }),
  ).rejects.toThrow("DO claim-race failed: 500");
});
