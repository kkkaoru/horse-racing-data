// Run with bun. Tests for Queue-owned prediction Container lifecycle controls.

import { beforeEach, expect, test, vi } from "vitest";
import type { ContainerControlMessage, Env } from "./types";

const { claimContainerSlotStopMock, clearContainerSlotMock, markContainerSlotStoppedMock } =
  vi.hoisted(() => ({
    claimContainerSlotStopMock: vi.fn<
      () => Promise<{
        allowed: boolean;
        state: "blocked" | "claimed" | "destroyed" | "pending" | "resumed";
      }>
    >(async () => ({ allowed: true, state: "claimed" })),
    clearContainerSlotMock: vi.fn(async () => undefined),
    markContainerSlotStoppedMock: vi.fn(async () => undefined),
  }));

vi.mock("./do-state", () => ({
  claimContainerSlotStop: claimContainerSlotStopMock,
  clearContainerSlot: clearContainerSlotMock,
  markContainerSlotStopped: markContainerSlotStoppedMock,
}));

import {
  consumeContainerStop,
  enqueueContainerStop,
  enqueueContainerStopForRole,
  isAllowedContainerDoName,
  isContainerControlMessage,
  isContainerControlQueueMessage,
} from "./container-control";

const idFromNameMock = vi.fn(() => ({ name: "container-id" }));
const stubFetchMock = vi.fn<(...args: [Request]) => Promise<Response>>();
const getStateMock = vi.fn<() => Promise<{ status: string }>>(async () => ({ status: "running" }));
const getMock = vi.fn(() => ({ fetch: stubFetchMock, getState: getStateMock }));
const raceIdFromNameMock = vi.fn(() => ({ name: "race-container-id" }));
const raceGetMock = vi.fn(() => ({ fetch: stubFetchMock, getState: getStateMock }));

const makeEnv = (): Env =>
  ({
    FINISH_POSITION_PREDICT_CONTAINER: {
      get: getMock,
      idFromName: idFromNameMock,
    },
    FINISH_POSITION_RACE_CHAIN_CONTAINER: {
      get: raceGetMock,
      idFromName: raceIdFromNameMock,
    },
    PREDICT_RUN_COORDINATOR: {},
    TRIGGER_TOKEN: "secret-token",
  }) as unknown as Env;

const message: ContainerControlMessage = {
  name: "predict-jra-1",
  requestedAt: "2026-08-22T07:00:00.000Z",
  type: "container-stop",
  workKey: "focused-full:20260822:jra:01:01",
};

beforeEach(() => {
  claimContainerSlotStopMock.mockClear();
  claimContainerSlotStopMock.mockResolvedValue({ allowed: true, state: "claimed" });
  clearContainerSlotMock.mockClear();
  clearContainerSlotMock.mockResolvedValue(undefined);
  markContainerSlotStoppedMock.mockClear();
  markContainerSlotStoppedMock.mockResolvedValue(undefined);
  getStateMock.mockClear();
  getStateMock.mockResolvedValue({ status: "running" });
  getMock.mockClear();
  idFromNameMock.mockClear();
  raceGetMock.mockClear();
  raceIdFromNameMock.mockClear();
  stubFetchMock.mockReset();
});

test("skips a stale stop owned by different work without touching the container", async () => {
  const env = makeEnv();
  claimContainerSlotStopMock.mockResolvedValueOnce({ allowed: false, state: "blocked" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeContainerStop(env, message);

  expect(claimContainerSlotStopMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env,
    force: undefined,
    requestedAt: "2026-08-22T07:00:00.000Z",
    workKey: "focused-full:20260822:jra:01:01",
  });
  expect(idFromNameMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("logs a missing work key when an administrative stop becomes stale", async () => {
  claimContainerSlotStopMock.mockResolvedValueOnce({ allowed: false, state: "blocked" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeContainerStop(makeEnv(), { ...message, workKey: undefined });

  expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("workKey=-"));
  warnSpy.mockRestore();
});

test("stops the named container and clears its coordinator slot", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));

  await consumeContainerStop(env, message);

  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra-1");
  expect(getMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const request = (stubFetchMock.mock.calls[0] as [Request])[0];
  expect(request.url).toBe("http://do/__admin/stop-container");
  expect(request.method).toBe("POST");
  expect(request.headers.get("authorization")).toBe("Bearer secret-token");
  expect(markContainerSlotStoppedMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env,
    workKey: "focused-full:20260822:jra:01:01",
  });
  expect(clearContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env,
    workKey: "focused-full:20260822:jra:01:01",
  });
});

test("one day-base stop accepts either owner and a lease-less redelivery does not stop twice", async () => {
  const env = makeEnv();
  const dayBaseMessage: ContainerControlMessage = {
    acceptableWorkKeys: ["day-base:20260824:nar", "day-base-stale:20260824:nar"],
    name: "predict-nar",
    requestedAt: "2026-08-24T00:00:00.000Z",
    role: "legacy",
    type: "container-stop",
    workKey: "day-base:20260824:nar",
  };
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));
  claimContainerSlotStopMock
    .mockResolvedValueOnce({ allowed: true, state: "claimed" })
    .mockResolvedValueOnce({ allowed: false, state: "blocked" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeContainerStop(env, dayBaseMessage);
  await consumeContainerStop(env, dayBaseMessage);

  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(clearContainerSlotMock).toHaveBeenCalledTimes(1);
  expect(clearContainerSlotMock).toHaveBeenCalledWith({
    acceptableWorkKeys: ["day-base:20260824:nar", "day-base-stale:20260824:nar"],
    doName: "predict-nar",
    env,
    workKey: "day-base:20260824:nar",
  });
  warnSpy.mockRestore();
});

test("treats an already-stopped 204 response as an idempotent success", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

  await expect(consumeContainerStop(env, message)).resolves.toBe(true);

  expect(clearContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env,
    workKey: "focused-full:20260822:jra:01:01",
  });
});

test("stops the explicitly targeted race-chain binding", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

  await consumeContainerStop(env, {
    ...message,
    name: "race-chain-predict-jra-1",
    role: "race-chain",
  });

  expect(raceIdFromNameMock).toHaveBeenCalledWith("race-chain-predict-jra-1");
  expect(raceGetMock).toHaveBeenCalledTimes(1);
  expect(idFromNameMock).not.toHaveBeenCalled();
});

test("rejects a non-canonical target before creating coordinator or container DO state", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await consumeContainerStop(makeEnv(), {
    ...message,
    name: "predict-jra-20260824-01-01",
  });

  expect(claimContainerSlotStopMock).not.toHaveBeenCalled();
  expect(idFromNameMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[container-control] rejected non-canonical target name=predict-jra-20260824-01-01 role=legacy",
  );
  errorSpy.mockRestore();
});

test("allows only the finite legacy and race-chain target universes", () => {
  expect(isAllowedContainerDoName("predict-jra", undefined)).toBe(true);
  expect(isAllowedContainerDoName("predict-ban-ei-2", "legacy")).toBe(true);
  expect(isAllowedContainerDoName("race-chain-predict-nar-0", "race-chain")).toBe(true);
  expect(isAllowedContainerDoName("predict-jra-3", "legacy")).toBe(false);
  expect(isAllowedContainerDoName("predict-jra-0", "race-chain")).toBe(false);
  expect(isAllowedContainerDoName("race-chain-predict-jra-0", "legacy")).toBe(false);
});

test("throws on a non-2xx container response and preserves the queue retry", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response("busy", { status: 503 }));

  await expect(consumeContainerStop(env, message)).rejects.toThrow(
    "Container stop failed name=predict-jra-1 requestedAt=2026-08-22T07:00:00.000Z status=503",
  );
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
});

test("throws when the container request rejects and does not clear the slot", async () => {
  const env = makeEnv();
  stubFetchMock.mockRejectedValueOnce(new Error("container unreachable"));

  await expect(consumeContainerStop(env, message)).rejects.toThrow("container unreachable");
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
});

test("propagates a coordinator clear failure after a successful stop", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  clearContainerSlotMock.mockRejectedValueOnce(new Error("coordinator unavailable"));

  await expect(consumeContainerStop(env, message)).rejects.toThrow("coordinator unavailable");
});

test("resumes at slot clear after destroy succeeded without destroying the container twice", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  clearContainerSlotMock.mockRejectedValueOnce(new Error("coordinator unavailable"));
  claimContainerSlotStopMock
    .mockResolvedValueOnce({ allowed: true, state: "claimed" })
    .mockResolvedValueOnce({ allowed: false, state: "destroyed" });

  await expect(consumeContainerStop(env, message)).rejects.toThrow("coordinator unavailable");
  await expect(consumeContainerStop(env, message)).resolves.toBe(true);

  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(markContainerSlotStoppedMock).toHaveBeenCalledTimes(1);
  expect(clearContainerSlotMock).toHaveBeenCalledTimes(2);
});

test("keeps the destroyed fence until a caller durable marker succeeds", async () => {
  const env = makeEnv();
  const afterDestroyed = vi
    .fn<() => Promise<void>>()
    .mockRejectedValueOnce(new Error("watch marker unavailable"))
    .mockResolvedValueOnce(undefined);
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  claimContainerSlotStopMock
    .mockResolvedValueOnce({ allowed: true, state: "claimed" })
    .mockResolvedValueOnce({ allowed: false, state: "destroyed" });

  await expect(consumeContainerStop(env, message, afterDestroyed)).rejects.toThrow(
    "watch marker unavailable",
  );
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  await expect(consumeContainerStop(env, message, afterDestroyed)).resolves.toBe(true);

  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(afterDestroyed).toHaveBeenCalledTimes(2);
  expect(clearContainerSlotMock).toHaveBeenCalledTimes(1);
});

test.each(["stopped", "stopped_with_code"])(
  "finishes a pending stop from observed Container state %s without another destroy",
  async (status) => {
    const env = makeEnv();
    claimContainerSlotStopMock.mockResolvedValueOnce({ allowed: false, state: "pending" });
    getStateMock.mockResolvedValueOnce({ status });

    await expect(consumeContainerStop(env, message)).resolves.toBe(true);

    expect(stubFetchMock).not.toHaveBeenCalled();
    expect(markContainerSlotStoppedMock).toHaveBeenCalledTimes(1);
    expect(clearContainerSlotMock).toHaveBeenCalledTimes(1);
  },
);

test("keeps a pending stop durable while the Container is still running", async () => {
  claimContainerSlotStopMock.mockResolvedValueOnce({ allowed: false, state: "pending" });
  getStateMock.mockResolvedValueOnce({ status: "running" });

  await expect(consumeContainerStop(makeEnv(), message)).rejects.toThrow(
    "Container stop already in progress name=predict-jra-1 status=running",
  );

  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
});

test("does not act when a non-allowed claim has no resumable stop stage", async () => {
  claimContainerSlotStopMock.mockResolvedValueOnce({ allowed: false, state: "claimed" });
  await expect(consumeContainerStop(makeEnv(), message)).resolves.toBe(false);
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
});

test("a resumed fence observes an already-stopped Container and only marks and clears", async () => {
  claimContainerSlotStopMock.mockResolvedValueOnce({ allowed: true, state: "resumed" });
  getStateMock.mockResolvedValueOnce({ status: "stopped" });

  await expect(consumeContainerStop(makeEnv(), message)).resolves.toBe(true);

  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(markContainerSlotStoppedMock).toHaveBeenCalledTimes(1);
  expect(clearContainerSlotMock).toHaveBeenCalledTimes(1);
});

test("a resumed fence destroys a Container that is still running", async () => {
  claimContainerSlotStopMock.mockResolvedValueOnce({ allowed: true, state: "resumed" });
  getStateMock.mockResolvedValueOnce({ status: "running" });
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));

  await expect(consumeContainerStop(makeEnv(), message)).resolves.toBe(true);

  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(markContainerSlotStoppedMock).toHaveBeenCalledTimes(1);
  expect(clearContainerSlotMock).toHaveBeenCalledTimes(1);
});

test("validates container control messages and queue wrappers", () => {
  expect(isContainerControlMessage(message)).toBe(true);
  expect(
    isContainerControlMessage({
      ...message,
      acceptableWorkKeys: ["day-base:20260824:nar", "day-base-stale:20260824:nar"],
    }),
  ).toBe(true);
  expect(isContainerControlQueueMessage({ body: message } as Message<unknown>)).toBe(true);
  for (const invalid of [
    null,
    "container-stop",
    {},
    { ...message, type: "start" },
    { requestedAt: message.requestedAt, type: "container-stop" },
    { ...message, name: 1 },
    { name: message.name, type: "container-stop" },
    { ...message, requestedAt: 1 },
    { ...message, workKey: "" },
    { ...message, acceptableWorkKeys: [] },
    { ...message, acceptableWorkKeys: [""] },
    { ...message, acceptableWorkKeys: [1] },
    { ...message, role: "unknown" },
  ]) {
    expect(isContainerControlMessage(invalid)).toBe(false);
  }
});

test("enqueueContainerStop returns false without a binding and sends optional work keys", async () => {
  await expect(enqueueContainerStop(makeEnv(), "predict-jra")).resolves.toBe(false);
  const send = vi.fn(async (_message: ContainerControlMessage) => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };
  await expect(enqueueContainerStop(env, "predict-jra", "work-1")).resolves.toBe(true);
  expect(send).toHaveBeenCalledWith(
    expect.objectContaining({ name: "predict-jra", type: "container-stop", workKey: "work-1" }),
  );
  await expect(enqueueContainerStop(env, "predict-nar")).resolves.toBe(true);
  expect(send).toHaveBeenLastCalledWith(
    expect.objectContaining({ name: "predict-nar", type: "container-stop" }),
  );
  expect(send.mock.calls[1]?.[0]).not.toHaveProperty("workKey");
});

test("enqueueContainerStopForRole records an unambiguous binding role", async () => {
  await expect(
    enqueueContainerStopForRole({
      env: makeEnv(),
      name: "race-chain-predict-jra-1",
      role: "race-chain",
      workKey: "work-2",
    }),
  ).resolves.toBe(false);
  const send = vi.fn(async (_message: ContainerControlMessage) => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };

  await expect(
    enqueueContainerStopForRole({
      env,
      name: "race-chain-predict-jra-1",
      role: "race-chain",
      workKey: "work-2",
    }),
  ).resolves.toBe(true);
  expect(send).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "race-chain-predict-jra-1",
      role: "race-chain",
      type: "container-stop",
      workKey: "work-2",
    }),
  );
});
