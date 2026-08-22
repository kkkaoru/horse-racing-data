// Run with bun. Tests for Queue-owned prediction Container lifecycle controls.

import { beforeEach, expect, test, vi } from "vitest";
import type { ContainerControlMessage, Env } from "./types";

const { checkContainerSlotStopMock, clearContainerSlotMock } = vi.hoisted(() => ({
  checkContainerSlotStopMock: vi.fn(async () => true),
  clearContainerSlotMock: vi.fn(async () => undefined),
}));

vi.mock("./do-state", () => ({
  checkContainerSlotStop: checkContainerSlotStopMock,
  clearContainerSlot: clearContainerSlotMock,
}));

import {
  consumeContainerStop,
  enqueueContainerStop,
  enqueueContainerStopForRole,
  isContainerControlMessage,
  isContainerControlQueueMessage,
} from "./container-control";

const idFromNameMock = vi.fn(() => ({ name: "container-id" }));
const stubFetchMock = vi.fn<(...args: [Request]) => Promise<Response>>();
const getMock = vi.fn(() => ({ fetch: stubFetchMock }));
const raceIdFromNameMock = vi.fn(() => ({ name: "race-container-id" }));
const raceGetMock = vi.fn(() => ({ fetch: stubFetchMock }));

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
  checkContainerSlotStopMock.mockClear();
  checkContainerSlotStopMock.mockResolvedValue(true);
  clearContainerSlotMock.mockClear();
  clearContainerSlotMock.mockResolvedValue(undefined);
  getMock.mockClear();
  idFromNameMock.mockClear();
  raceGetMock.mockClear();
  raceIdFromNameMock.mockClear();
  stubFetchMock.mockReset();
});

test("skips a stale stop owned by different work without touching the container", async () => {
  const env = makeEnv();
  checkContainerSlotStopMock.mockResolvedValueOnce(false);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeContainerStop(env, message);

  expect(checkContainerSlotStopMock).toHaveBeenCalledWith({
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
  checkContainerSlotStopMock.mockResolvedValueOnce(false);
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
  expect(clearContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env,
    workKey: "focused-full:20260822:jra:01:01",
  });
});

test("treats an already-stopped 204 response as an idempotent success", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

  await expect(consumeContainerStop(env, message)).resolves.toBeUndefined();

  expect(clearContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env,
    workKey: "focused-full:20260822:jra:01:01",
  });
});

test("stops the explicitly targeted race-chain binding", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

  await consumeContainerStop(env, { ...message, role: "race-chain" });

  expect(raceIdFromNameMock).toHaveBeenCalledWith("predict-jra-1");
  expect(raceGetMock).toHaveBeenCalledTimes(1);
  expect(idFromNameMock).not.toHaveBeenCalled();
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

test("validates container control messages and queue wrappers", () => {
  expect(isContainerControlMessage(message)).toBe(true);
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
