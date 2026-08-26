// Run with bun. Queue-owned lifecycle controls for prediction Container DOs.

import { claimContainerSlotStop, clearContainerSlot, markContainerSlotStopped } from "./do-state";
import {
  resolveContainerNamespaceForRole,
  type PredictionContainerRole,
} from "./race-container-routing";
import { listAllowedPredictDoNames } from "./predict-do-shard";
import type { ContainerControlMessage, Env } from "./types";

const DO_HOST = "http://do";
const ADMIN_STOP_CONTAINER_PATH = "/__admin/stop-container";
const RACE_CHAIN_DO_NAME_PREFIX = "race-chain-";

interface EnqueueContainerStopForRoleParams {
  acceptableWorkKeys?: string[];
  env: Env;
  name: string;
  role: PredictionContainerRole;
  workKey?: string;
}

interface ContainerStateReader {
  getState(): Promise<{ status: string }>;
}

interface FinalizeContainerStopParams {
  afterDestroyed?: () => Promise<void>;
  env: Env;
  message: ContainerControlMessage;
}

interface PollContainerStoppedParams {
  deadlineAtMs: number;
  name: string;
  reader: ContainerStateReader;
}

export const CONTAINER_STOP_CONFIRM_POLL_INTERVAL_MS: number = 250;
export const CONTAINER_STOP_CONFIRM_TIMEOUT_MS: number = 15_000;

const pause = async (durationMs: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, durationMs));

const isStoppedStatus = (status: string): boolean =>
  status === "stopped" || status === "stopped_with_code";

const pollContainerStopped = async (params: PollContainerStoppedParams): Promise<string> => {
  const state = await params.reader.getState();
  if (isStoppedStatus(state.status)) return state.status;
  const remainingMs = params.deadlineAtMs - Date.now();
  if (remainingMs <= 0) {
    throw new Error(
      `Container stop confirmation timed out name=${params.name} status=${state.status}`,
    );
  }
  await pause(Math.min(CONTAINER_STOP_CONFIRM_POLL_INTERVAL_MS, remainingMs));
  return pollContainerStopped(params);
};

export const waitForContainerStopped = async (
  name: string,
  reader: ContainerStateReader,
): Promise<string> =>
  pollContainerStopped({
    deadlineAtMs: Date.now() + CONTAINER_STOP_CONFIRM_TIMEOUT_MS,
    name,
    reader,
  });

const finalizeContainerStop = async (params: FinalizeContainerStopParams): Promise<void> => {
  await markContainerSlotStopped({
    acceptableWorkKeys: params.message.acceptableWorkKeys,
    doName: params.message.name,
    env: params.env,
    workKey: params.message.workKey,
  });
  await params.afterDestroyed?.();
  await clearContainerSlot({
    acceptableWorkKeys: params.message.acceptableWorkKeys,
    doName: params.message.name,
    env: params.env,
    workKey: params.message.workKey,
  });
};

export const isAllowedContainerDoName = (
  name: string,
  role: PredictionContainerRole | undefined,
): boolean => {
  const prefix = role === "race-chain" ? RACE_CHAIN_DO_NAME_PREFIX : "";
  return listAllowedPredictDoNames().some((doName) => name === `${prefix}${doName}`);
};

export const isContainerControlMessage = (value: unknown): value is ContainerControlMessage =>
  typeof value === "object" &&
  value !== null &&
  "type" in value &&
  value.type === "container-stop" &&
  "name" in value &&
  typeof value.name === "string" &&
  "requestedAt" in value &&
  typeof value.requestedAt === "string" &&
  (!("acceptableWorkKeys" in value) ||
    (Array.isArray(value.acceptableWorkKeys) &&
      value.acceptableWorkKeys.length > 0 &&
      value.acceptableWorkKeys.every(
        (workKey) => typeof workKey === "string" && workKey.length > 0,
      ))) &&
  (!("workKey" in value) || (typeof value.workKey === "string" && value.workKey.length > 0)) &&
  (!("role" in value) || value.role === "legacy" || value.role === "race-chain") &&
  (!("force" in value) || typeof value.force === "boolean");

export const isContainerControlQueueMessage = (
  message: Message<unknown>,
): message is Message<ContainerControlMessage> => isContainerControlMessage(message.body);

export const enqueueContainerStop = async (
  env: Env,
  name: string,
  workKey?: string,
): Promise<boolean> => {
  if (env.CONTAINER_CONTROL_QUEUE === undefined) return false;
  await env.CONTAINER_CONTROL_QUEUE.send({
    name,
    requestedAt: new Date().toISOString(),
    type: "container-stop",
    ...(workKey === undefined ? {} : { workKey }),
  });
  return true;
};

export const enqueueContainerStopForRole = async (
  params: EnqueueContainerStopForRoleParams,
): Promise<boolean> => {
  if (params.env.CONTAINER_CONTROL_QUEUE === undefined) return false;
  await params.env.CONTAINER_CONTROL_QUEUE.send({
    name: params.name,
    requestedAt: new Date().toISOString(),
    role: params.role,
    type: "container-stop",
    ...(params.acceptableWorkKeys === undefined
      ? {}
      : { acceptableWorkKeys: params.acceptableWorkKeys }),
    ...(params.workKey === undefined ? {} : { workKey: params.workKey }),
  });
  return true;
};

export const consumeContainerStop = async (
  env: Env,
  message: ContainerControlMessage,
  afterDestroyed?: () => Promise<void>,
): Promise<boolean> => {
  if (!isAllowedContainerDoName(message.name, message.role)) {
    console.error(
      `[container-control] rejected non-canonical target name=${message.name} role=${message.role ?? "legacy"}`,
    );
    return false;
  }
  const claim = await claimContainerSlotStop({
    doName: message.name,
    env,
    force: message.force,
    requestedAt: message.requestedAt,
    acceptableWorkKeys: message.acceptableWorkKeys,
    workKey: message.workKey,
  });
  if (!claim.allowed) {
    if (claim.state === "destroyed") {
      await afterDestroyed?.();
      await clearContainerSlot({
        acceptableWorkKeys: message.acceptableWorkKeys,
        doName: message.name,
        env,
        workKey: message.workKey,
      });
      return true;
    }
    if (claim.state === "blocked") {
      console.warn(
        `[container-control] skipped stale stop name=${message.name} requestedAt=${message.requestedAt} workKey=${message.workKey ?? "-"}`,
      );
      return false;
    }
    const namespace = resolveContainerNamespaceForRole(env, message.role);
    const stub = namespace.get(namespace.idFromName(message.name));
    if (claim.state === "pending") {
      await waitForContainerStopped(message.name, stub);
      await finalizeContainerStop({ afterDestroyed, env, message });
      return true;
    }
    return false;
  }
  const namespace = resolveContainerNamespaceForRole(env, message.role);
  const stub = namespace.get(namespace.idFromName(message.name));
  if (claim.state === "resumed") {
    const state = await stub.getState();
    if (isStoppedStatus(state.status)) {
      await finalizeContainerStop({ afterDestroyed, env, message });
      return true;
    }
  }
  const response = await stub.fetch(
    new Request(`${DO_HOST}${ADMIN_STOP_CONTAINER_PATH}`, {
      headers: { authorization: `Bearer ${env.TRIGGER_TOKEN}` },
      method: "POST",
    }),
  );
  if (!response.ok) {
    throw new Error(
      `Container stop failed name=${message.name} requestedAt=${message.requestedAt} status=${response.status}`,
    );
  }
  const terminalStatus = await waitForContainerStopped(message.name, stub);
  await finalizeContainerStop({ afterDestroyed, env, message });
  console.log(
    `[container-control] stopped name=${message.name} requestedAt=${message.requestedAt} responseStatus=${response.status} terminalStatus=${terminalStatus}`,
  );
  return true;
};
