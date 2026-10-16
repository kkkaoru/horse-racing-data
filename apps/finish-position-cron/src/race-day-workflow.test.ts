// Run with bun.
import { beforeEach, expect, test, vi } from "vitest";
import {
  handleAdminRaceDay,
  RaceDayWorkflow,
  runDayBasePrewarmWorkflows,
  runRaceDay,
  startRaceDay,
} from "./race-day-workflow";
import type { DayBaseRepairAcquireResult } from "./day-base-repair";
import type { Env } from "./types";

vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: vi.fn(),
}));
vi.mock("./feature-hit-prediction", () => ({
  fanOutPredictionsAfterDayBaseHit: vi.fn(async () => 2),
}));
vi.mock("./day-base-prewarm", () => ({
  prewarmCategoryWithOutcome: vi.fn(async () => "landed"),
}));
vi.mock("./cron-decision", () => ({ enumerateTodaysRaces: vi.fn() }));
vi.mock("./container-control", () => ({ consumeContainerStop: vi.fn(async () => true) }));
vi.mock("./day-base-repair", () => {
  const acquired = "acquired" satisfies DayBaseRepairAcquireResult["status"];
  return {
    acquireDayBaseRepair: vi.fn(async (params: { generationId: string }) => ({
      generationId: params.generationId,
      status: acquired,
    })),
    clearDayBaseRepairReservation: vi.fn(async () => true),
    heartbeatDayBaseRepair: vi.fn(async () => true),
  };
});

import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
import { prewarmCategoryWithOutcome } from "./day-base-prewarm";
import { enumerateTodaysRaces } from "./cron-decision";
import { consumeContainerStop } from "./container-control";
import {
  acquireDayBaseRepair,
  clearDayBaseRepairReservation,
  heartbeatDayBaseRepair,
} from "./day-base-repair";

const readiness = vi.mocked(getFocusedFullDayBaseReadiness);
const fanout = vi.mocked(fanOutPredictionsAfterDayBaseHit);
const prewarm = vi.mocked(prewarmCategoryWithOutcome);

const workflowCreate = vi.fn(async ({ id }: { id: string }) => ({ id }));
const workflowGet = vi.fn(async (id: string) => ({
  id,
  status: vi.fn(async () => ({ status: "running" })),
}));

const env = {
  PC_KEIBA_VIEWER: { fetch: vi.fn(async () => new Response("warmed")) },
  RACE_DAY_WORKFLOW: { create: workflowCreate, get: workflowGet },
  TRIGGER_TOKEN: "secret-token",
} as unknown as Env;

beforeEach(() => {
  vi.mocked(env.PC_KEIBA_VIEWER?.fetch)?.mockClear();
  readiness.mockReset();
  fanout.mockReset().mockResolvedValue(2);
  prewarm.mockReset().mockResolvedValue("landed");
  vi.mocked(consumeContainerStop).mockReset().mockResolvedValue(true);
  vi.mocked(enumerateTodaysRaces).mockReset().mockResolvedValue([]);
  vi.mocked(acquireDayBaseRepair)
    .mockReset()
    .mockImplementation(async (params) => ({
      generationId: params.generationId,
      status: "acquired",
    }));
  vi.mocked(clearDayBaseRepairReservation).mockReset().mockResolvedValue(true);
  vi.mocked(heartbeatDayBaseRepair).mockReset().mockResolvedValue(true);
  workflowCreate.mockReset().mockImplementation(async ({ id }) => ({ id }));
  workflowGet.mockReset().mockImplementation(async (id) => ({
    id,
    status: vi.fn(async () => ({ status: "running" })),
  }));
});

test("scheduled prewarm dispatch starts deduped prewarm-only workflows by category", async () => {
  vi.mocked(enumerateTodaysRaces).mockResolvedValueOnce([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "nar", keibajoCode: "43", raceBango: "02" },
    { category: "jra", keibajoCode: "06", raceBango: "03" },
  ]);

  await expect(runDayBasePrewarmWorkflows({ daysAhead: 2, env, runYmd: "20260923" })).resolves.toBe(
    true,
  );

  expect(enumerateTodaysRaces).toHaveBeenCalledWith(env.REALTIME_DB, "20260923");
  expect(workflowCreate).toHaveBeenCalledTimes(2);
  expect(workflowCreate).toHaveBeenCalledWith({
    id: expect.stringMatching(/^day-jra-20260923-prewarm-only-/u),
    params: {
      category: "jra",
      daysAhead: 2,
      generationId: expect.any(String),
      mode: "prewarm-only",
      repairOwner: true,
      runYmd: "20260923",
    },
  });
  expect(workflowCreate).toHaveBeenCalledWith({
    id: expect.stringMatching(/^day-nar-20260923-prewarm-only-/u),
    params: expect.objectContaining({ category: "nar", mode: "prewarm-only" }),
  });
});

test("scheduled prewarm reports no-work and discovery failure without starting Workflows", async () => {
  await expect(runDayBasePrewarmWorkflows({ daysAhead: 2, env, runYmd: "20260923" })).resolves.toBe(
    false,
  );
  vi.mocked(enumerateTodaysRaces).mockRejectedValueOnce(new Error("D1 unavailable"));
  await expect(runDayBasePrewarmWorkflows({ daysAhead: 2, env, runYmd: "20260923" })).resolves.toBe(
    false,
  );
  expect(workflowCreate).not.toHaveBeenCalled();
});

test("prewarm-only workflow never fans out predictions", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: async () => undefined,
  };

  await expect(
    runRaceDay(env, step, {
      category: "nar",
      generationId: "cron-owner-generation",
      mode: "prewarm-only",
      repairOwner: true,
      runYmd: "20260923",
    }),
  ).resolves.toStrictEqual({ heatmap: false, racesEnqueued: 0 });
  expect(clearDayBaseRepairReservation).toHaveBeenCalledOnce();
  expect(fanout).not.toHaveBeenCalled();
});

test("ready owner stops its exact Container before release, fanout and heatmap work", async () => {
  readiness.mockResolvedValue({ ready: true, reason: "hit" });
  const steps: string[] = [];
  const step = {
    do: async (name: string, callback: () => Promise<unknown>) => {
      steps.push(name);
      return callback();
    },
    sleep: vi.fn(async () => undefined),
  };
  await runRaceDay(env, step, {
    category: "jra",
    generationId: "owner-a",
    repairOwner: true,
    runYmd: "20261003",
    warmHeatmap: true,
  });
  expect(steps).toStrictEqual([
    "heartbeat-day-base-start",
    "day-base-readiness",
    "heartbeat-day-base-release",
    "stop-day-base-container",
    "release-day-base-owner",
    "fanout-predictions",
    "warm-heatmaps",
  ]);
  expect(consumeContainerStop).toHaveBeenCalledWith(env, {
    acceptableWorkKeys: ["day-base:20261003:jra:owner-a", "day-base-stale:20261003:jra:owner-a"],
    name: "predict-jra",
    requestedAt: expect.any(String),
    role: "legacy",
    type: "container-stop",
    workKey: "day-base:20261003:jra:owner-a",
  });
  expect(prewarm).not.toHaveBeenCalled();
  expect(step.sleep).not.toHaveBeenCalled();
});

test("observer workflows never stop another build", async () => {
  readiness.mockResolvedValue({ ready: true, reason: "hit" });
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: vi.fn(async () => undefined),
  };
  await runRaceDay(env, step, { category: "jra", runYmd: "20261003" });
  expect(consumeContainerStop).not.toHaveBeenCalled();
});

test("stop failures remain a retriable Workflow step before releasing ownership", async () => {
  readiness.mockResolvedValue({ ready: true, reason: "hit" });
  vi.mocked(consumeContainerStop).mockRejectedValueOnce(new Error("stop temporarily unavailable"));
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: vi.fn(async () => undefined),
  };
  await expect(
    runRaceDay(env, step, {
      category: "jra",
      generationId: "owner-a",
      repairOwner: true,
      runYmd: "20261003",
    }),
  ).rejects.toThrow("stop temporarily unavailable");
  expect(clearDayBaseRepairReservation).not.toHaveBeenCalled();
  expect(fanout).not.toHaveBeenCalled();
  expect(prewarm).not.toHaveBeenCalled();
});

test("an ownership-fenced skip never escalates to a forced stop", async () => {
  readiness.mockResolvedValue({ ready: true, reason: "hit" });
  vi.mocked(consumeContainerStop).mockResolvedValueOnce(false);
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: vi.fn(async () => undefined),
  };
  await expect(
    runRaceDay(env, step, {
      category: "nar",
      generationId: "owner-a",
      repairOwner: true,
      mode: "prewarm-only",
      runYmd: "20261003",
    }),
  ).resolves.toStrictEqual({ heatmap: false, racesEnqueued: 0 });
  expect(consumeContainerStop).toHaveBeenCalledOnce();
  expect(consumeContainerStop).toHaveBeenCalledWith(env, {
    acceptableWorkKeys: ["day-base:20261003:nar:owner-a", "day-base-stale:20261003:nar:owner-a"],
    name: "predict-nar",
    requestedAt: expect.any(String),
    role: "legacy",
    type: "container-stop",
    workKey: "day-base:20261003:nar:owner-a",
  });
});

test("owner workflow requires its generation id before checking readiness", async () => {
  const step = {
    do: vi.fn(async (_name: string, callback: () => Promise<unknown>) => callback()),
    sleep: vi.fn(async () => undefined),
  };

  await expect(
    runRaceDay(env, step, { category: "nar", repairOwner: true, runYmd: "20260923" }),
  ).rejects.toThrow("day-base repair owner generation missing");
  expect(readiness).not.toHaveBeenCalled();
});

test("workflow skips prewarm when day-base is already ready", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: vi.fn(async (_name: string, callback: () => Promise<unknown>) => callback()),
    sleep: vi.fn(async () => undefined),
  };
  await expect(
    runRaceDay(env, step, { category: "nar", runYmd: "20260923", warmHeatmap: true }),
  ).resolves.toEqual({ heatmap: true, racesEnqueued: 2 });
  expect(fanout).toHaveBeenCalledOnce();
  expect(env.PC_KEIBA_VIEWER?.fetch).toHaveBeenCalledOnce();
});

test("workflow uses the protected prewarm and confirms the artifact before fanout", async () => {
  fanout.mockClear();
  readiness
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: vi.fn(
      async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
        (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    ),
    sleep: vi.fn(async () => undefined),
  };
  await runRaceDay(env, step, {
    category: "jra",
    generationId: "workflow-1",
    repairOwner: true,
    runYmd: "20260923",
  });
  expect(prewarm).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    env,
    generationId: "workflow-1",
    runYmd: "20260923",
  });
  expect(step.sleep).not.toHaveBeenCalled();
  expect(fanout).toHaveBeenCalledOnce();
});

test("workflow sleeps and checks readiness again while prewarm pickup is pending", async () => {
  fanout.mockClear();
  readiness
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "hit" });
  prewarm.mockResolvedValueOnce("pickup-scheduled");
  const step = {
    do: vi.fn(
      async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
        (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    ),
    sleep: vi.fn(async () => undefined),
  };
  await runRaceDay(env, step, {
    category: "nar",
    generationId: "workflow-owner-1",
    repairOwner: true,
    runYmd: "20260923",
  });
  expect(step.sleep).toHaveBeenCalledWith("wait-day-base-0", "5 minutes");
  expect(prewarm).toHaveBeenCalledOnce();
  expect(step.do).not.toHaveBeenCalledWith(
    "prewarm-day-base-1",
    expect.anything(),
    expect.anything(),
  );
  expect(fanout).toHaveBeenCalledOnce();
});

test("workflow only polls readiness when the same generation already owns the pickup", async () => {
  readiness
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  prewarm.mockResolvedValueOnce("owned");
  const step = {
    do: vi.fn(
      async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
        (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    ),
    sleep: vi.fn(async () => undefined),
  };

  await expect(
    runRaceDay(env, step, {
      category: "nar",
      generationId: "current-generation",
      repairOwner: true,
      runYmd: "20260923",
    }),
  ).resolves.toStrictEqual({ heatmap: false, racesEnqueued: 2 });
  expect(prewarm).toHaveBeenCalledOnce();
  expect(step.sleep).toHaveBeenCalledTimes(2);
  expect(heartbeatDayBaseRepair).toHaveBeenCalledTimes(5);
  expect(fanout).toHaveBeenCalledOnce();
});

test("workflow retries transient prewarm failure with the same owner before fanout", async () => {
  readiness
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  prewarm.mockResolvedValueOnce("failed");
  const step = {
    do: async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
      (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    sleep: vi.fn(async () => undefined),
  };
  await expect(
    runRaceDay(env, step, {
      category: "nar",
      generationId: "workflow-owner-2",
      repairOwner: true,
      runYmd: "20260923",
    }),
  ).resolves.toStrictEqual({ heatmap: false, racesEnqueued: 2 });
  expect(step.sleep).toHaveBeenCalledWith("wait-day-base-0", "5 minutes");
  expect(prewarm).toHaveBeenCalledTimes(2);
  expect(clearDayBaseRepairReservation).toHaveBeenCalledOnce();
});

test("workflow observes an artifact published during the final readiness sleep", async () => {
  readiness.mockResolvedValue({ ready: false, reason: "day-base-missing-or-invalid" });
  const step = {
    do: vi.fn(async (_name: string, callback: () => Promise<unknown>) => callback()),
    sleep: vi.fn(async (name: string) => {
      if (name === "wait-day-base-7") readiness.mockResolvedValue({ ready: true, reason: "hit" });
    }),
  };

  await expect(
    runRaceDay(env, step, { category: "jra", runYmd: "20261003" }),
  ).resolves.toStrictEqual({ heatmap: false, racesEnqueued: 2 });
  expect(step.sleep).toHaveBeenCalledTimes(8);
  expect(step.do).toHaveBeenCalledWith("confirm-day-base-final", expect.any(Function));
  expect(readiness).toHaveBeenCalledTimes(10);
  expect(prewarm).not.toHaveBeenCalled();
  expect(fanout).toHaveBeenCalledOnce();
});

test("workflow stops before fanout when protected prewarm keeps failing", async () => {
  readiness.mockResolvedValue({ ready: false, reason: "day-base-missing-or-invalid" });
  prewarm.mockResolvedValue("failed");
  const step = {
    do: async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
      (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    sleep: vi.fn(async () => undefined),
  };
  await expect(
    runRaceDay(env, step, {
      category: "nar",
      generationId: "workflow-owner-2",
      repairOwner: true,
      runYmd: "20260923",
    }),
  ).rejects.toThrow("day-base not ready category=nar reason=day-base-missing-or-invalid");
  expect(readiness).toHaveBeenCalledTimes(2);
  expect(step.sleep).toHaveBeenCalledTimes(8);
  expect(consumeContainerStop).not.toHaveBeenCalled();
  expect(fanout).not.toHaveBeenCalled();
});

test("owner workflow waits for a newer day-base generation without fanout before readiness", async () => {
  fanout.mockClear();
  readiness.mockResolvedValue({ ready: false, reason: "day-base-missing-or-invalid" });
  prewarm.mockResolvedValue("superseded");
  const step = {
    do: async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
      (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    sleep: vi.fn(async () => undefined),
  };
  await expect(
    runRaceDay(env, step, {
      category: "nar",
      generationId: "workflow-owner-3",
      repairOwner: true,
      runYmd: "20260923",
    }),
  ).rejects.toThrow("day-base not ready");
  expect(step.sleep).toHaveBeenCalledTimes(8);
  expect(consumeContainerStop).not.toHaveBeenCalled();
  expect(fanout).not.toHaveBeenCalled();
});

test("workflow uses the D1 owner generation instead of its instance id", async () => {
  prewarm.mockClear();
  readiness
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "hit" });
  const workflow = new RaceDayWorkflow({} as ExecutionContext, env);
  const step = {
    do: vi.fn(
      async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
        (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    ),
    sleep: vi.fn(async () => undefined),
  };
  await expect(
    workflow.run(
      {
        instanceId: "workflow-instance-id",
        payload: {
          category: "ban-ei",
          daysAhead: 0,
          generationId: "d1-owner-generation",
          mode: "fanout",
          repairOwner: true,
          runYmd: "20260923",
        },
        timestamp: new Date(),
        workflowName: "finish-position-race-day",
      },
      step as never,
    ),
  ).resolves.toMatchObject({ racesEnqueued: 2 });
  expect(prewarm).toHaveBeenCalledWith({
    category: "ban-ei",
    daysAhead: 0,
    env,
    generationId: "d1-owner-generation",
    runYmd: "20260923",
  });
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

test("heatmap warm is skipped when the viewer binding is absent", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: async () => undefined,
  };
  await expect(
    runRaceDay({ ...env, PC_KEIBA_VIEWER: undefined }, step, {
      category: "nar",
      runYmd: "20260923",
      warmHeatmap: true,
    }),
  ).resolves.toEqual({ heatmap: true, racesEnqueued: 2 });
});

test("observer workflow waits for another dispatch owner without starting a prewarm", async () => {
  readiness
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
      (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    sleep: vi.fn(async () => undefined),
  };

  await runRaceDay(env, step, {
    category: "nar",
    generationId: "queue-owner-generation",
    repairOwner: false,
    runYmd: "20260923",
  });

  expect(prewarm).not.toHaveBeenCalled();
  expect(step.sleep).toHaveBeenCalledWith("wait-day-base-0", "5 minutes");
  expect(fanout).toHaveBeenCalledOnce();
});

test("stops owner workflow before prewarm when D1 heartbeat reports takeover", async () => {
  vi.mocked(heartbeatDayBaseRepair).mockResolvedValueOnce(false);
  readiness.mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" });
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: vi.fn(async () => undefined),
  };

  await expect(
    runRaceDay(env, step, {
      category: "jra",
      generationId: "old-owner-generation",
      repairOwner: true,
      runYmd: "20260923",
    }),
  ).rejects.toThrow("day-base repair owner lost");

  expect(prewarm).not.toHaveBeenCalled();
  expect(fanout).not.toHaveBeenCalled();
});

test("clears its D1 owner before fanout after readiness is verified", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: async () => undefined,
  };

  await runRaceDay(env, step, {
    category: "jra",
    generationId: "current-owner-generation",
    repairOwner: true,
    runYmd: "20260923",
  });

  expect(clearDayBaseRepairReservation).toHaveBeenCalledWith({
    category: "jra",
    env,
    generationId: "current-owner-generation",
    runYmd: "20260923",
  });
  expect(fanout).toHaveBeenCalledOnce();
});

test("does not fan out when D1 owner was replaced before release", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  vi.mocked(clearDayBaseRepairReservation).mockResolvedValueOnce(false);
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: async () => undefined,
  };

  await expect(
    runRaceDay(env, step, {
      category: "jra",
      generationId: "old-owner-generation",
      repairOwner: true,
      runYmd: "20260923",
    }),
  ).rejects.toThrow("day-base repair owner lost");
  expect(fanout).not.toHaveBeenCalled();
});

test("startRaceDay reuses a published artifact without acquiring a replacement owner", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "ready" });

  await expect(
    startRaceDay(env, {
      category: "ban-ei",
      generationId: "observer-generation",
      runYmd: "20990101",
    }),
  ).resolves.toStrictEqual({ id: "day-ban-ei-20990101-fanout-observer-generation", ok: true });
  expect(acquireDayBaseRepair).not.toHaveBeenCalled();
  expect(workflowCreate).toHaveBeenCalledWith({
    id: "day-ban-ei-20990101-fanout-observer-generation",
    params: {
      category: "ban-ei",
      daysAhead: 0,
      generationId: "observer-generation",
      mode: "fanout",
      repairOwner: false,
      runYmd: "20990101",
    },
  });
});

test("startRaceDay fails closed if an existing artifact cannot be checked", async () => {
  readiness.mockRejectedValueOnce(new Error("Catalog unavailable"));
  await expect(startRaceDay(env, { category: "nar", runYmd: "20990101" })).rejects.toThrow(
    "Catalog unavailable",
  );
  expect(acquireDayBaseRepair).not.toHaveBeenCalled();
  expect(workflowCreate).not.toHaveBeenCalled();
});

test("startRaceDay rejects generations that cannot fit in the Workflow ID", async () => {
  await expect(
    startRaceDay(env, {
      category: "nar",
      generationId: "x".repeat(37),
      runYmd: "20260923",
    }),
  ).rejects.toThrow("invalid day-base repair generation id");
  expect(acquireDayBaseRepair).not.toHaveBeenCalled();
});

test("startRaceDay refuses to start a Workflow for an old date", async () => {
  vi.mocked(acquireDayBaseRepair).mockResolvedValueOnce({
    generationId: "old-owner",
    status: "skipped-old-date",
  });

  await expect(
    startRaceDay(env, {
      category: "nar",
      generationId: "old-owner",
      runYmd: "20200923",
    }),
  ).rejects.toThrow("cannot start race-day workflow for old date");
  expect(workflowCreate).not.toHaveBeenCalled();
});

test("startRaceDay keeps the old-date fence even when a published artifact is ready", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "ready" });
  await expect(startRaceDay(env, { category: "nar", runYmd: "20200923" })).rejects.toThrow(
    "cannot start race-day workflow for old date",
  );
  expect(acquireDayBaseRepair).not.toHaveBeenCalled();
  expect(workflowCreate).not.toHaveBeenCalled();
});

test("scheduled prewarm returns false when any category Workflow cannot start", async () => {
  vi.mocked(enumerateTodaysRaces).mockResolvedValueOnce([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "nar", keibajoCode: "43", raceBango: "02" },
  ]);
  vi.mocked(acquireDayBaseRepair)
    .mockResolvedValueOnce({ generationId: "jra-owner", status: "acquired" })
    .mockRejectedValueOnce(new Error("D1 unavailable"));

  await expect(runDayBasePrewarmWorkflows({ daysAhead: 2, env, runYmd: "20260923" })).resolves.toBe(
    false,
  );
  expect(workflowCreate).toHaveBeenCalledOnce();
});

test("startRaceDay derives a stable workflow id from the acquired D1 owner", async () => {
  await expect(
    startRaceDay(env, {
      category: "nar",
      generationId: "owner-generation-1",
      runYmd: "20260923",
    }),
  ).resolves.toStrictEqual({ id: "day-nar-20260923-fanout-owner-generation-1", ok: true });

  expect(acquireDayBaseRepair).toHaveBeenCalledWith({
    category: "nar",
    env,
    generationId: "owner-generation-1",
    runYmd: "20260923",
  });
  expect(workflowCreate).toHaveBeenCalledWith({
    id: "day-nar-20260923-fanout-owner-generation-1",
    params: {
      category: "nar",
      daysAhead: 0,
      generationId: "owner-generation-1",
      mode: "fanout",
      repairOwner: true,
      runYmd: "20260923",
    },
  });
});

test("startRaceDay reuses a busy Queue owner's generation as an observer workflow", async () => {
  vi.mocked(acquireDayBaseRepair).mockResolvedValueOnce({
    generationId: "queue-owner-1",
    status: "busy",
  });

  await expect(
    startRaceDay(env, { category: "ban-ei", runYmd: "20260923" }),
  ).resolves.toStrictEqual({ id: "day-ban-ei-20260923-fanout-queue-owner-1", ok: true });

  expect(workflowCreate).toHaveBeenCalledWith({
    id: "day-ban-ei-20260923-fanout-queue-owner-1",
    params: {
      category: "ban-ei",
      daysAhead: 0,
      generationId: "queue-owner-1",
      mode: "fanout",
      repairOwner: false,
      runYmd: "20260923",
    },
  });
});

test("prewarm-only and fanout starts do not attach to a Workflow with the wrong mode", async () => {
  await startRaceDay(env, {
    category: "nar",
    generationId: "shared-owner-generation",
    mode: "prewarm-only",
    runYmd: "20260923",
  });
  vi.mocked(acquireDayBaseRepair).mockResolvedValueOnce({
    generationId: "shared-owner-generation",
    status: "busy",
  });

  await startRaceDay(env, {
    category: "nar",
    mode: "fanout",
    runYmd: "20260923",
  });

  expect(workflowCreate.mock.calls.map(([params]) => params.id)).toStrictEqual([
    "day-nar-20260923-prewarm-only-shared-owner-generation",
    "day-nar-20260923-fanout-shared-owner-generation",
  ]);
});

test("attaches to the owner workflow when deterministic creation reports a duplicate", async () => {
  const create = vi.fn(async () => {
    throw new Error("instance already exists");
  });
  const get = vi.fn(async () => ({ status: vi.fn(async () => ({ status: "waiting" })) }));
  const localEnv = { ...env, RACE_DAY_WORKFLOW: { create, get } } as unknown as Env;

  await expect(
    startRaceDay(localEnv, {
      category: "nar",
      generationId: "owner-generation-2",
      runYmd: "20260923",
    }),
  ).resolves.toStrictEqual({ id: "day-nar-20260923-fanout-owner-generation-2", ok: true });
  expect(get).toHaveBeenCalledWith("day-nar-20260923-fanout-owner-generation-2");
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

test("startRaceDay rejects a duplicate id that has no retrievable Workflow instance", async () => {
  await expect(
    startRaceDay({ ...env, RACE_DAY_WORKFLOW: undefined }, { category: "nar", runYmd: "20260923" }),
  ).rejects.toThrow("binding missing");
  const create = vi.fn(async () => {
    throw new Error("instance already exists");
  });
  const get = vi.fn(async () => {
    throw new Error("instance not found");
  });
  await expect(
    startRaceDay({ ...env, RACE_DAY_WORKFLOW: { create, get } } as unknown as Env, {
      category: "nar",
      generationId: "owner-generation-3",
      runYmd: "20260923",
    }),
  ).rejects.toThrow("instance already exists");
});

test("heatmap warm failure stops the workflow", async () => {
  readiness.mockResolvedValueOnce({ ready: true, reason: "hit" });
  const local = {
    ...env,
    PC_KEIBA_VIEWER: { fetch: vi.fn(async () => new Response("no", { status: 500 })) },
  } as unknown as Env;
  const step = {
    do: async (_name: string, callback: () => Promise<unknown>) => callback(),
    sleep: async () => undefined,
  };
  await expect(
    runRaceDay(local, step, { category: "nar", runYmd: "20260923", warmHeatmap: true }),
  ).rejects.toThrow("heatmap warm failed");
});

test("workflow does not enqueue predictions when the build still misses", async () => {
  readiness.mockResolvedValue({ ready: false, reason: "day-base-missing-or-invalid" });
  const step = {
    do: async (_name: string, configOrCallback: unknown, maybeCallback?: () => Promise<unknown>) =>
      (maybeCallback ?? (configOrCallback as () => Promise<unknown>))(),
    sleep: async () => undefined,
  };
  await expect(runRaceDay(env, step, { category: "nar", runYmd: "20260923" })).rejects.toThrow(
    "day-base not ready",
  );
});
