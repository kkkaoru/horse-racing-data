// Run with bun. Queue-owned lifecycle controls for prediction Container DOs.

import { checkContainerSlotStop, clearContainerSlot } from "./do-state";
import {
  resolveContainerNamespaceForRole,
  type PredictionContainerRole,
} from "./race-container-routing";
import type { ContainerControlMessage, Env } from "./types";

const DO_HOST = "http://do";
const ADMIN_STOP_CONTAINER_PATH = "/__admin/stop-container";

interface EnqueueContainerStopForRoleParams {
  env: Env;
  name: string;
  role: PredictionContainerRole;
  workKey?: string;
}

export const isContainerControlMessage = (value: unknown): value is ContainerControlMessage =>
  typeof value === "object" &&
  value !== null &&
  "type" in value &&
  value.type === "container-stop" &&
  "name" in value &&
  typeof value.name === "string" &&
  "requestedAt" in value &&
  typeof value.requestedAt === "string" &&
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
    ...(params.workKey === undefined ? {} : { workKey: params.workKey }),
  });
  return true;
};

export const consumeContainerStop = async (
  env: Env,
  message: ContainerControlMessage,
): Promise<void> => {
  const allowed = await checkContainerSlotStop({
    doName: message.name,
    env,
    force: message.force,
    requestedAt: message.requestedAt,
    workKey: message.workKey,
  });
  if (!allowed) {
    console.warn(
      `[container-control] skipped stale stop name=${message.name} requestedAt=${message.requestedAt} workKey=${message.workKey ?? "-"}`,
    );
    return;
  }
  const namespace = resolveContainerNamespaceForRole(env, message.role);
  const doId = namespace.idFromName(message.name);
  const stub = namespace.get(doId);
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
  await clearContainerSlot({ doName: message.name, env, workKey: message.workKey });
  console.log(
    `[container-control] stopped name=${message.name} requestedAt=${message.requestedAt} status=${response.status}`,
  );
};
