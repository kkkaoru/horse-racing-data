// Run with bun. Durable cleanup-only retries for terminal prediction Containers.

import { enqueueContainerStopForRole } from "./container-control";
import type { ContainerCleanupMessage, Env } from "./types";

interface ContainerCleanupParams {
  env: Env;
  name: string;
  role: ContainerCleanupMessage["role"];
  workKey: string;
}

interface EnqueueContainerCleanupParams extends ContainerCleanupParams {
  attempt: number;
}

interface ConsumeContainerCleanupParams {
  env: Env;
  message: ContainerCleanupMessage;
}

export const CONTAINER_CLEANUP_TYPE = "container-cleanup";
export const CONTAINER_CLEANUP_DELAY_SECONDS = 30;
export const CONTAINER_CLEANUP_FIRST_ATTEMPT = 1;
export const CONTAINER_CLEANUP_MAX_ATTEMPT = 5;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const isContainerCleanupMessage = (value: unknown): value is ContainerCleanupMessage => {
  if (!isRecord(value) || value.type !== CONTAINER_CLEANUP_TYPE) return false;
  if (typeof value.name !== "string" || value.name.length === 0) return false;
  if (value.role !== "legacy" && value.role !== "race-chain") return false;
  if (typeof value.workKey !== "string" || value.workKey.length === 0) return false;
  return typeof value.attempt === "number" && Number.isInteger(value.attempt) && value.attempt > 0;
};

export const isContainerCleanupQueueMessage = (
  message: Message<unknown>,
): message is Message<ContainerCleanupMessage> => isContainerCleanupMessage(message.body);

const enqueueContainerCleanup = async (params: EnqueueContainerCleanupParams): Promise<void> => {
  await params.env.PREDICT_QUEUE.send(
    {
      attempt: params.attempt,
      name: params.name,
      role: params.role,
      type: CONTAINER_CLEANUP_TYPE,
      workKey: params.workKey,
    },
    { delaySeconds: CONTAINER_CLEANUP_DELAY_SECONDS },
  );
  console.log(
    `[container-cleanup] scheduled name=${params.name} role=${params.role} workKey=${params.workKey} attempt=${params.attempt} delaySeconds=${CONTAINER_CLEANUP_DELAY_SECONDS}`,
  );
};

const tryEnqueueContainerStop = async (params: ContainerCleanupParams): Promise<boolean> => {
  try {
    return await enqueueContainerStopForRole(params);
  } catch (error) {
    console.error(
      `[container-cleanup] stop enqueue failed name=${params.name} role=${params.role} workKey=${params.workKey}:`,
      String(error),
    );
    return false;
  }
};

export const handOffContainerStopOrCleanup = async (
  params: ContainerCleanupParams,
): Promise<void> => {
  if (await tryEnqueueContainerStop(params)) return;
  await enqueueContainerCleanup({ ...params, attempt: CONTAINER_CLEANUP_FIRST_ATTEMPT });
};

export const consumeContainerCleanup = async (
  params: ConsumeContainerCleanupParams,
): Promise<void> => {
  if (await tryEnqueueContainerStop({ ...params.message, env: params.env })) {
    console.log(
      `[container-cleanup] handed off name=${params.message.name} role=${params.message.role} workKey=${params.message.workKey} attempt=${params.message.attempt}`,
    );
    return;
  }
  if (params.message.attempt >= CONTAINER_CLEANUP_MAX_ATTEMPT) {
    throw new Error(
      `Container cleanup exhausted name=${params.message.name} role=${params.message.role} workKey=${params.message.workKey} attempt=${params.message.attempt}`,
    );
  }
  await enqueueContainerCleanup({
    ...params.message,
    env: params.env,
    attempt: params.message.attempt + 1,
  });
};
