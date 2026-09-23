import { expect, test, vi } from "vitest";
import {
  handleAdminRaceDay,
  RaceDayWorkflow,
  runRaceDay,
  startRaceDay,
  syncDayBase,
} from "./race-day-workflow";
import type { Env } from "./types";

vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: vi.fn(),
}));
vi.mock("./feature-hit-prediction", () => ({
  fanOutPredictionsAfterDayBaseHit: vi.fn(async () => 2),
}));

import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";

const readiness = vi.mocked(getFocusedFullDayBaseReadiness);
const fanout = vi.mocked(fanOutPredictionsAfterDayBaseHit);

const containerGet = vi.fn(() => ({
  fetch: vi.fn(
    async () =>
      new Response(
        '{"type":"result","status":"success","parquetBase64":"YQ==","parquetKey":"feat.parquet"}\n',
      ),
  ),
}));
const workflowCreate = vi.fn(async () => ({ id: "race-day-nar-20260923" }));

const env = {
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: containerGet,
    idFromName: vi.fn(() => ({ name: "predict-nar" })),
  },
  PC_KEIBA_VIEWER: { fetch: vi.fn(async () => new Response("warmed")) },
  RACE_DAY_WORKFLOW: { create: workflowCreate },
  TRIGGER_TOKEN: "secret-token",
} as unknown as Env;

test("sync day-base asks the container to stream the parquet", async () => {
  const fetch = vi.fn(
    async (_request: Request) =>
      new Response(
        '{"type":"result","status":"success","parquetBase64":"YQ==","parquetKey":"feat.parquet"}\n',
      ),
  );
  const get = vi.fn(() => ({ fetch }));
  const local = {
    ...env,
    FINISH_POSITION_PREDICT_CONTAINER: { get, idFromName: vi.fn(() => ({ name: "predict-nar" })) },
  } as unknown as Env;
  await syncDayBase(local, "nar", "20260923");
  const request = fetch.mock.calls[0]?.[0];
  if (request === undefined) throw new Error("missing container request");
  expect(request.url).toContain("sync=1");
  expect(request.url).toContain("rebuild=1");
});

test("workflow skips the container when day-base is already ready", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = { do: vi.fn(async (_name: string, callback: () => Promise<unknown>) => callback()) };
  await expect(
    runRaceDay(env, step, { category: "nar", runYmd: "20260923", warmHeatmap: true }),
  ).resolves.toEqual({ heatmap: true, racesEnqueued: 2 });
  expect(fanout).toHaveBeenCalledOnce();
  expect(env.PC_KEIBA_VIEWER?.fetch).toHaveBeenCalledOnce();
});

test("workflow builds once when day-base is missing", async () => {
  readiness
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: vi.fn(
      async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
        (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    ),
  };
  await runRaceDay(env, step, { category: "jra", runYmd: "20260923" });
  expect(containerGet).toHaveBeenCalled();
});

test("workflow class delegates to the same runner", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const workflow = new RaceDayWorkflow({} as ExecutionContext, env);
  const step = { do: vi.fn(async (_name: string, callback: () => Promise<unknown>) => callback()) };
  await expect(
    workflow.run(
      {
        instanceId: "race-day-ban-ei-20260923",
        payload: { category: "ban-ei", runYmd: "20260923" },
        timestamp: new Date(),
        workflowName: "finish-position-race-day",
      },
      step as never,
    ),
  ).resolves.toMatchObject({ racesEnqueued: 2 });
});

test("admin race-day starts one workflow per category and warms heatmaps once", async () => {
  const response = await handleAdminRaceDay(
    new Request("https://cron.example/api/admin/race-day", {
      body: JSON.stringify({ runYmd: "20260923" }),
      headers: { authorization: "Bearer secret-token" },
      method: "POST",
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(workflowCreate).toHaveBeenCalledTimes(3);
});

test("admin race-day rejects a bad token", async () => {
  const response = await handleAdminRaceDay(
    new Request("https://cron.example/api/admin/race-day", {
      body: "{}",
      method: "POST",
    }),
    env,
  );
  expect(response.status).toBe(401);
});

test("admin race-day rejects an invalid body", async () => {
  const request = (body: string) =>
    new Request("https://cron.example/api/admin/race-day", {
      body,
      headers: { authorization: "Bearer secret-token" },
      method: "POST",
    });
  expect((await handleAdminRaceDay(request("{"), env)).status).toBe(400);
  expect((await handleAdminRaceDay(request(JSON.stringify({ runYmd: "bad" })), env)).status).toBe(
    400,
  );
  expect(
    (
      await handleAdminRaceDay(
        request(JSON.stringify({ category: "overseas", runYmd: "20260923" })),
        env,
      )
    ).status,
  ).toBe(400);
});

test("sync day-base rejects an empty or untyped result", async () => {
  const local = (body: string) =>
    ({
      ...env,
      FINISH_POSITION_PREDICT_CONTAINER: {
        get: () => ({ fetch: async () => new Response(body) }),
        idFromName: () => ({ name: "predict-nar" }),
      },
    }) as unknown as Env;
  await expect(syncDayBase(local(""), "nar", "20260923")).rejects.toThrow("status=-");
  await expect(syncDayBase(local("null"), "nar", "20260923")).rejects.toThrow("error=-");
  await expect(
    syncDayBase(local('{"status":"success","parquetBase64":1}\n'), "nar", "20260923"),
  ).rejects.toThrow("not committed");
});

test("heatmap warm is skipped when the viewer binding is absent", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
  };
  await expect(
    runRaceDay({ ...env, PC_KEIBA_VIEWER: undefined }, step, {
      category: "nar",
      runYmd: "20260923",
      warmHeatmap: true,
    }),
  ).resolves.toEqual({ heatmap: true, racesEnqueued: 2 });
});

test("startRaceDay rethrows unexpected workflow errors", async () => {
  const create = vi.fn(async () => {
    throw new Error("workflow unavailable");
  });
  await expect(
    startRaceDay({ ...env, RACE_DAY_WORKFLOW: { create } } as unknown as Env, {
      category: "nar",
      runYmd: "20260923",
    }),
  ).rejects.toThrow("workflow unavailable");
});

test("sync day-base rejects an accepted response that did not commit R2", async () => {
  const local = {
    ...env,
    FINISH_POSITION_PREDICT_CONTAINER: {
      get: () => ({
        fetch: async () =>
          new Response('{"type":"result","status":"accepted","parquetKey":"feat.parquet"}\n'),
      }),
      idFromName: () => ({ name: "predict-nar" }),
    },
  } as unknown as Env;
  await expect(syncDayBase(local, "nar", "20260923")).rejects.toThrow("not committed");
});

test("sync day-base fails closed when the container does not commit", async () => {
  const local = {
    ...env,
    FINISH_POSITION_PREDICT_CONTAINER: {
      get: () => ({ fetch: async () => new Response("no", { status: 502 }) }),
      idFromName: () => ({ name: "predict-nar" }),
    },
  } as unknown as Env;
  await expect(syncDayBase(local, "nar", "20260923")).rejects.toThrow("status=502");
});

test("startRaceDay is idempotent when the instance already exists", async () => {
  await expect(
    startRaceDay({ ...env, RACE_DAY_WORKFLOW: undefined }, { category: "nar", runYmd: "20260923" }),
  ).rejects.toThrow("binding missing");
  const create = vi.fn(async () => {
    throw new Error("instance already exists");
  });
  await expect(
    startRaceDay({ ...env, RACE_DAY_WORKFLOW: { create } } as unknown as Env, {
      category: "nar",
      runYmd: "20260923",
    }),
  ).resolves.toMatchObject({ id: expect.stringMatching(/^race-day-nar-20260923-/), ok: true });
});

test("heatmap warm failure stops the workflow", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const local = {
    ...env,
    PC_KEIBA_VIEWER: { fetch: vi.fn(async () => new Response("no", { status: 500 })) },
  } as unknown as Env;
  const step = { do: async (_name: string, callback: () => Promise<unknown>) => callback() };
  await expect(
    runRaceDay(local, step, { category: "nar", runYmd: "20260923", warmHeatmap: true }),
  ).rejects.toThrow("heatmap warm failed");
});

test("workflow does not enqueue predictions when the build still misses", async () => {
  readiness.mockResolvedValue({ ready: false, reason: "day-base-missing-or-invalid" });
  const step = {
    do: async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
      (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
  };
  await expect(runRaceDay(env, step, { category: "nar", runYmd: "20260923" })).rejects.toThrow(
    "day-base not ready",
  );
});
