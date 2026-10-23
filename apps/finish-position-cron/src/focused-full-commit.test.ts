// Run with bun. Actual coordinator and external writers under controlled interleavings.
import { afterEach, expect, test, vi } from "vitest";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import { pickUpFocusedFullCache } from "./focused-full-cache-pickup";
import { proxyResultParquetsToR2 } from "./container-ndjson-proxy";
import { withFocusedFullCommit } from "./focused-full-commit";
import { publishFinishPositionPredictionCache } from "./prediction-kv-writer";
import type { Env } from "./types";

const { query } = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("@neondatabase/serverless", () => ({ neon: () => ({ query }) }));
const race = {
  category: "jra",
  runYmd: "20261004",
  keibajoCode: "05",
  raceBango: "11",
  doName: "predict-jra",
  staleAfterMs: 1,
};
const workKey = "focused-full:20261004:jra:05:11";
const slot = {
  category: "jra",
  doName: "predict-jra",
  kind: "focused-full",
  workKey,
  generationId: "A",
} satisfies Parameters<PredictRunCoordinator["claimContainerSlot"]>[0];
const scope = {
  category: "jra",
  runYmd: "20261004",
  keibajoCode: "05",
  raceBango: "11",
} satisfies Pick<
  Parameters<typeof pickUpFocusedFullCache>[0],
  "category" | "runYmd" | "keibajoCode" | "raceBango"
>;
const makeHarness = () => {
  const records = new Map<string, unknown>();
  const env: Env = Object.create(null);
  const storage = {
    get: async (key: string) => structuredClone(records.get(key)),
    put: async (key: string, value: unknown) => {
      records.set(key, structuredClone(value));
    },
    delete: async (key: string) => records.delete(key),
    list: async ({ prefix }: { prefix: string }) =>
      new Map([...records].filter(([key]) => key.startsWith(prefix))),
    setAlarm: async () => undefined,
  };
  const reconstruct = (): PredictRunCoordinator => {
    const instance: unknown = Reflect.construct(PredictRunCoordinator, [
      { storage, blockConcurrencyWhile: async <T>(fn: () => Promise<T>) => fn() },
      env,
    ]);
    if (!(instance instanceof PredictRunCoordinator)) throw new Error("Invalid coordinator");
    return instance;
  };
  const coordinator = reconstruct();
  const put = vi.fn(async () => undefined);
  const fetch = vi.fn(async () =>
    Response.json({ found: true, parquetKey: "shared", parquetBase64: "QQ==" }),
  );
  Object.assign(env, {
    FEATURES_CACHE: { put },
    DETAIL_SECTION_CACHE_KV: { put },
    NEON_DATABASE_URL: "offline",
    PREDICT_RUN_COORDINATOR: {
      idFromName: (name: string) => name,
      get: () => ({ fetch: (request: Request) => reconstruct().fetch(request) }),
    },
    FINISH_POSITION_PREDICT_CONTAINER: {
      idFromName: (name: string) => name,
      get: () => ({ fetch }),
    },
  });
  return { coordinator, reconstruct, records, env, put, fetch };
};
afterEach(() => vi.useRealTimers());

test.each(["pickup", "stream", "kv"])(
  "held %s put excludes B after slot clear and reconstruction",
  async (path) => {
    const h = makeHarness();
    await h.coordinator.claimFocusedFullRace(race);
    const a = await h.coordinator.claimContainerSlot(slot);
    const held = Promise.withResolvers<void>();
    const entered = Promise.withResolvers<void>();
    h.put.mockImplementation(async () => {
      entered.resolve();
      await held.promise;
    });
    query.mockResolvedValue([
      {
        umaban: "1",
        predicted_rank: 1,
        predicted_score: 0.4,
        model_version: "model",
        prediction_generated_at: "2026-10-04T01:00:00Z",
      },
    ]);
    const operation =
      path === "pickup"
        ? pickUpFocusedFullCache({ ...scope, env: h.env, executionId: a.executionId })
        : path === "stream"
          ? proxyResultParquetsToR2(
              {
                type: "result",
                category: "jra",
                racesPredicted: 1,
                parquetKey: "shared",
                parquetBase64: "QQ==",
              },
              {
                FEATURES_CACHE: h.env.FEATURES_CACHE,
                focusedFullCommit: { env: h.env, workKey, executionId: a.executionId },
              },
              false,
            )
          : publishFinishPositionPredictionCache({
              ...scope,
              env: h.env,
              executionId: a.executionId,
              bustCacheApi: false,
              nowMs: Date.parse("2026-10-04T03:00:00Z"),
            });
    await entered.promise;
    await h.coordinator.clearContainerSlot({
      doName: "predict-jra",
      workKey,
      executionId: a.executionId,
    });
    await expect(
      h.coordinator.completeFocusedFullRace({
        ...race,
        executionId: a.executionId,
        status: "success",
      }),
    ).rejects.toThrow("FOCUSED_FULL_COMMIT_UNRESOLVED");
    const rebuilt = h.reconstruct();
    await expect(rebuilt.claimFocusedFullRace({ ...race, force: true })).resolves.toStrictEqual({
      proceed: false,
      state: "commit-unresolved",
    });
    await expect(rebuilt.claimContainerSlot({ ...slot, generationId: "B" })).resolves.toStrictEqual(
      { proceed: false, state: "commit-unresolved" },
    );
    await expect(
      withFocusedFullCommit({
        env: h.env,
        workKey,
        executionId: a.executionId,
        commit: async () => undefined,
      }),
    ).rejects.toThrow("FOCUSED_FULL_COMMIT_UNRESOLVED");
    held.resolve();
    await operation;
    expect(h.put).toHaveBeenCalledOnce();
    expect((await rebuilt.claimContainerSlot({ ...slot, generationId: "B" })).proceed).toBe(true);
  },
);

test("held pickup response and deferred publication cannot commit A after B admission", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  const held = Promise.withResolvers<Response>();
  h.fetch.mockImplementation(() => held.promise);
  const operation = pickUpFocusedFullCache({ ...scope, env: h.env, executionId: a.executionId });
  await h.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey,
    executionId: a.executionId,
  });
  expect((await h.coordinator.claimContainerSlot({ ...slot, generationId: "B" })).proceed).toBe(
    true,
  );
  held.resolve(Response.json({ found: true, parquetKey: "shared", parquetBase64: "QQ==" }));
  await expect(operation).rejects.toThrow("FOCUSED_FULL_COMMIT_UNRESOLVED");
  query.mockResolvedValue([
    {
      umaban: "1",
      predicted_rank: 1,
      predicted_score: 0.4,
      model_version: "model",
      prediction_generated_at: "2026-10-04T01:00:00Z",
    },
  ]);
  await expect(
    publishFinishPositionPredictionCache({
      ...scope,
      env: h.env,
      executionId: a.executionId,
      bustCacheApi: false,
      nowMs: Date.parse("2026-10-04T03:00:00Z"),
    }),
  ).rejects.toThrow("FOCUSED_FULL_COMMIT_UNRESOLVED");
  expect(h.put).not.toHaveBeenCalled();
});

test("failed put remains unresolved across time, terminal retries and reconstruction", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  h.put.mockRejectedValue(new Error("response lost"));
  await expect(
    pickUpFocusedFullCache({ ...scope, env: h.env, executionId: a.executionId }),
  ).rejects.toThrow("FOCUSED_FULL_COMMIT_UNRESOLVED");
  await h.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey,
    executionId: a.executionId,
  });
  vi.useFakeTimers();
  vi.setSystemTime(Date.now() + 100_000_000);
  await expect(
    h.reconstruct().claimContainerSlot({ ...slot, generationId: "B" }),
  ).resolves.toStrictEqual({ proceed: false, state: "commit-unresolved" });
  await expect(
    h.reconstruct().finishExternalCommit({ workKey, executionId: a.executionId, token: "wrong" }),
  ).resolves.toBe(false);
  expect(h.records.has(`external-commit:${workKey}`)).toBe(true);
});

test("forced different race preserves live E until exact cleanup, including reconstruction", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  await expect(
    h.reconstruct().claimFocusedFullRace({ ...race, raceBango: "12", force: true }),
  ).resolves.toStrictEqual({ proceed: false, state: "queued" });
  await expect(
    h.reconstruct().completeFocusedFullRace({
      ...race,
      executionId: a.executionId,
      checkOnly: true,
      status: "error",
    }),
  ).resolves.toBe(true);
  await h.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey,
    executionId: a.executionId,
  });
  expect(
    (await h.reconstruct().claimFocusedFullRace({ ...race, raceBango: "12", force: true })).proceed,
  ).toBe(true);
});

test("failed repair send cancellation keeps stopped backup E until replacement admission", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  await h.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey,
    executionId: a.executionId,
  });
  expect(
    (
      await h.coordinator.reserveFocusedFullRaceRepair({
        ...race,
        executionId: a.executionId,
        reservationId: "R",
      })
    ).proceed,
  ).toBe(true);
  const send = vi.fn().mockRejectedValue(new Error("queue unavailable"));
  await expect(send()).rejects.toThrow("queue unavailable");
  await h.coordinator.cancelFocusedFullRaceRepair({ ...race, reservationId: "R" });
  await expect(
    h.reconstruct().completeFocusedFullRace({
      ...race,
      executionId: a.executionId,
      checkOnly: true,
      status: "success",
    }),
  ).resolves.toBe(true);
  expect(
    (
      await h
        .reconstruct()
        .reserveFocusedFullRaceRepair({ ...race, executionId: a.executionId, reservationId: "R2" })
    ).proceed,
  ).toBe(true);
  await h.reconstruct().claimFocusedFullRace({ ...race, force: true });
  await expect(
    h.reconstruct().completeFocusedFullRace({
      ...race,
      executionId: a.executionId,
      checkOnly: true,
      status: "success",
    }),
  ).resolves.toBe(true);
  const b = await h.reconstruct().claimContainerSlot({ ...slot, generationId: "B" });
  expect(b.executionId).not.toBe(a.executionId);
  await expect(
    h.reconstruct().completeFocusedFullRace({
      ...race,
      executionId: a.executionId,
      checkOnly: true,
      status: "success",
    }),
  ).resolves.toBe(false);
});

test("a landed KV write whose cache bust fails releases the barrier", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  query.mockResolvedValue([
    {
      umaban: "1",
      predicted_rank: 1,
      predicted_score: 0.4,
      model_version: "model",
      prediction_generated_at: "2026-10-04T01:00:00Z",
    },
  ]);
  const result = await publishFinishPositionPredictionCache({
    ...scope,
    env: h.env,
    executionId: a.executionId,
    bustCacheApi: true,
    nowMs: Date.parse("2026-10-04T03:00:00Z"),
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: "2026-10-04T01:00:00.000Z",
    status: "written",
  });
  expect(h.put).toHaveBeenCalledOnce();
  expect(h.records.has("external-commit:focused-full:20261004:jra:05:11")).toBe(false);
});

test("a failed prediction read before the KV put leaves no barrier", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  query.mockRejectedValue(new Error("neon unavailable"));
  await expect(
    publishFinishPositionPredictionCache({
      ...scope,
      env: h.env,
      executionId: a.executionId,
      bustCacheApi: true,
      nowMs: Date.parse("2026-10-04T03:00:00Z"),
    }),
  ).rejects.toThrow("neon unavailable");
  expect(h.put).not.toHaveBeenCalled();
  expect(h.records.has("external-commit:focused-full:20261004:jra:05:11")).toBe(false);
});

test("a lost completion request is resent so a landed write releases the barrier", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  const coordinatorFetch = vi
    .fn((request: Request) => h.reconstruct().fetch(request))
    .mockImplementationOnce((request: Request) => h.reconstruct().fetch(request))
    .mockRejectedValueOnce(new Error("Durable Object reset"));
  const env: Env = Object.create(null);
  Object.assign(env, {
    PREDICT_RUN_COORDINATOR: {
      idFromName: (name: string) => name,
      get: () => ({ fetch: coordinatorFetch }),
    },
  });
  await expect(
    withFocusedFullCommit({ env, workKey, executionId: a.executionId, commit: async () => 7 }),
  ).resolves.toBe(7);
  expect(coordinatorFetch).toHaveBeenCalledTimes(3);
  expect(h.records.has("external-commit:focused-full:20261004:jra:05:11")).toBe(false);
});

test("completion that keeps failing leaves the barrier unresolved", async () => {
  const h = makeHarness();
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot(slot);
  const coordinatorFetch = vi
    .fn((_request: Request): Promise<Response> => Promise.reject(new Error("Durable Object reset")))
    .mockImplementationOnce((request: Request) => h.reconstruct().fetch(request));
  const env: Env = Object.create(null);
  Object.assign(env, {
    PREDICT_RUN_COORDINATOR: {
      idFromName: (name: string) => name,
      get: () => ({ fetch: coordinatorFetch }),
    },
  });
  await expect(
    withFocusedFullCommit({ env, workKey, executionId: a.executionId, commit: async () => 7 }),
  ).rejects.toThrow("FOCUSED_FULL_COMMIT_UNRESOLVED");
  expect(coordinatorFetch).toHaveBeenCalledTimes(4);
  expect(h.records.has("external-commit:focused-full:20261004:jra:05:11")).toBe(true);
});
