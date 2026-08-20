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

import {
  claimContainerSlot,
  claimFocusedFullRace,
  claimRescoreRace,
  claimRun,
  clearContainerSlot,
  completeFocusedFullRace,
  completeRun,
  getRunState,
  releaseContainerSlot,
  touchContainerSlot,
} from "./do-state";

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

test("claimFocusedFullRace calls DO /claim-focused-full-race with stale budget", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ proceed: true }), { status: 200 }));
  const result = await claimFocusedFullRace({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(result).toStrictEqual({ proceed: true });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/claim-focused-full-race");
  expect(req.method).toBe("POST");
  const body = (await req.json()) as { staleAfterMs: number; force: boolean };
  expect(body.staleAfterMs).toBe(2100000);
  expect(body.force).toBe(false);
});

test("claimFocusedFullRace sends force:true through to the DO when requested", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ proceed: true }), { status: 200 }));
  await claimFocusedFullRace({
    category: "jra",
    env: makeEnv(),
    force: true,
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  const body = (await req.json()) as { force: boolean };
  expect(body.force).toBe(true);
});

test("claimFocusedFullRace returns proceed:false when DO returns it", async () => {
  fetchMock.mockResolvedValue(
    new Response(JSON.stringify({ proceed: false, state: "started" }), { status: 200 }),
  );
  const result = await claimFocusedFullRace({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    staleAfterMs: 2100000,
  });
  expect(result).toStrictEqual({ proceed: false, state: "started" });
});

test("claimFocusedFullRace throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    claimFocusedFullRace({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260621",
      staleAfterMs: 2100000,
    }),
  ).rejects.toThrow("DO claim-focused-full-race failed: 500");
});

test("completeFocusedFullRace calls DO /complete-focused-full-race", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await completeFocusedFullRace({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    status: "success",
  });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/complete-focused-full-race");
  expect(req.method).toBe("POST");
  const body = (await req.json()) as { status: string };
  expect(body.status).toBe("success");
});

test("claimContainerSlot calls DO /claim-container-slot with the unique DO fields", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ proceed: true }), { status: 200 }));
  const result = await claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    env: makeEnv(),
    kind: "rescore",
    staleAfterMs: 1_200_000,
  });
  expect(result).toStrictEqual({ proceed: true });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/claim-container-slot");
  expect(req.method).toBe("POST");
  const body = (await req.json()) as {
    category: string;
    doName: string;
    kind: string;
    staleAfterMs: number;
  };
  expect(body.category).toBe("jra");
  expect(body.doName).toBe("predict-jra");
  expect(body.kind).toBe("rescore");
  expect(body.staleAfterMs).toBe(1200000);
});

test("claimContainerSlot returns proceed:false when the DO reports capped", async () => {
  fetchMock.mockResolvedValue(
    new Response(JSON.stringify({ proceed: false, state: "capped" }), { status: 200 }),
  );
  const result = await claimContainerSlot({
    category: "nar",
    doName: "predict-nar-2",
    env: makeEnv(),
    kind: "rescore",
    staleAfterMs: 1_200_000,
  });
  expect(result).toStrictEqual({ proceed: false, state: "capped" });
});

test("claimContainerSlot throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    claimContainerSlot({
      category: "jra",
      doName: "predict-jra",
      env: makeEnv(),
      kind: "rescore",
      staleAfterMs: 1_200_000,
    }),
  ).rejects.toThrow("DO claim-container-slot failed: 500");
});

test("releaseContainerSlot calls DO /release-container-slot", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await releaseContainerSlot({
    doName: "predict-jra",
    env: makeEnv(),
    kind: "rescore",
  });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/release-container-slot");
  expect(req.method).toBe("POST");
  const body = (await req.json()) as { doName: string; kind: string };
  expect(body.doName).toBe("predict-jra");
  expect(body.kind).toBe("rescore");
});

test("releaseContainerSlot throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 503 }));
  await expect(
    releaseContainerSlot({
      doName: "predict-nar",
      env: makeEnv(),
      kind: "focused-full",
    }),
  ).rejects.toThrow("DO release-container-slot failed: 503");
});

test("touchContainerSlot calls DO /touch-container-slot", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await touchContainerSlot({
    doName: "predict-jra-1",
    env: makeEnv(),
    staleAfterMs: 1_200_000,
  });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/touch-container-slot");
  expect(req.method).toBe("POST");
  const body = (await req.json()) as { doName: string; staleAfterMs: number };
  expect(body.doName).toBe("predict-jra-1");
  expect(body.staleAfterMs).toBe(1200000);
});

test("clearContainerSlot calls DO /clear-container-slot", async () => {
  fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await clearContainerSlot({
    doName: "predict-nar-0",
    env: makeEnv(),
  });
  const req = (fetchMock.mock.calls[0] as [Request])[0];
  expect(req.url).toBe("http://do/clear-container-slot");
  expect(req.method).toBe("POST");
  const body = (await req.json()) as { doName: string };
  expect(body.doName).toBe("predict-nar-0");
});

test("clearContainerSlot throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    clearContainerSlot({
      doName: "predict-nar-1",
      env: makeEnv(),
    }),
  ).rejects.toThrow("DO clear-container-slot failed: 500");
});

test("touchContainerSlot throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    touchContainerSlot({
      doName: "predict-ban-ei-0",
      env: makeEnv(),
      staleAfterMs: 1_200_000,
    }),
  ).rejects.toThrow("DO touch-container-slot failed: 500");
});

test("completeFocusedFullRace throws when DO returns non-200", async () => {
  fetchMock.mockResolvedValue(new Response("error", { status: 500 }));
  await expect(
    completeFocusedFullRace({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260621",
      status: "error",
    }),
  ).rejects.toThrow("DO complete-focused-full-race failed: 500");
});
