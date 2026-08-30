// Run with bun. Queue-draining deployment for finish-position Worker + Containers.

import { config } from "dotenv";
import { fileURLToPath } from "node:url";

import {
  buildDeploymentPredictionRequest,
  DEPLOYMENT_DRAIN_QUEUES,
  parseDeploymentRaces,
  shouldRequeueDeploymentPredictions,
} from "../src/deploy-safety";
import {
  describeUnsafePredictionContainers,
  listLivePredictionContainers,
  listUnsafePredictionContainers,
  runCommand,
  runWranglerJson,
} from "./wrangler-container-state";

const POLL_INTERVAL_MS = 10_000;
const DRAIN_TIMEOUT_MS = 45 * 60 * 1000;
const WRANGLER_DEPLOY_ATTEMPTS = 3;
const WRANGLER_DEPLOY_RETRY_WAIT_MS = 15_000;
const ADMIN_ORIGIN = "https://finish-position-cron.kaoru.workers.dev";
const ADMIN_STOP_PATH = "/api/admin/stop-predict-containers";
const ADMIN_RUN_PATH = "/api/admin/run-focused-full-race";
const pausedQueues = new Set<string>();
let resuming = false;

config({ path: fileURLToPath(new URL("../../../.env", import.meta.url)), quiet: true });

const triggerToken = process.env.FINISH_POSITION_CRON_TRIGGER_TOKEN;
if (triggerToken === undefined || triggerToken.length === 0) {
  throw new Error("FINISH_POSITION_CRON_TRIGGER_TOKEN is required for rolling deployment");
}
const requeuePredictions = shouldRequeueDeploymentPredictions(
  process.env.REQUEUE_FINISH_POSITION_PREDICTIONS,
);

const queueCommand = async (operation: "pause-delivery" | "resume-delivery", queue: string) =>
  runCommand(["bunx", "wrangler", "queues", operation, queue]);

const resumePausedQueues = async (): Promise<void> => {
  if (resuming) return;
  resuming = true;
  const queues = [...pausedQueues];
  const results = await Promise.allSettled(
    queues.map(async (queue) => {
      await queueCommand("resume-delivery", queue);
      pausedQueues.delete(queue);
    }),
  );
  resuming = false;
  const failed = results.filter((result) => result.status === "rejected");
  if (failed.length > 0) throw new Error(`Failed to resume ${failed.length} deployment queues`);
};

const pauseStartQueues = async (): Promise<void> => {
  const results = await Promise.allSettled(
    DEPLOYMENT_DRAIN_QUEUES.map(async (queue) => {
      await queueCommand("pause-delivery", queue);
      pausedQueues.add(queue);
    }),
  );
  const failed = results.filter((result) => result.status === "rejected");
  if (failed.length > 0) throw new Error(`Failed to pause ${failed.length} deployment queues`);
};

const postAdmin = async (path: string, body: unknown): Promise<void> => {
  const response = await fetch(new URL(path, ADMIN_ORIGIN), {
    body: JSON.stringify(body),
    headers: {
      authorization: `Bearer ${triggerToken}`,
      "content-type": "application/json",
    },
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(`Admin request failed path=${path} status=${response.status}`);
  }
};

const stopSupersededContainers = async (): Promise<void> => {
  const unsafe = await listUnsafePredictionContainers();
  const names = unsafe.flatMap((instance) => (instance.name === undefined ? [] : [instance.name]));
  if (names.length === 0) return;
  if (names.length !== unsafe.length) {
    throw new Error("An active prediction Container has no canonical name");
  }
  await postAdmin(ADMIN_STOP_PATH, { names, overrideActive: true });
  console.log(`[rolling-deploy] superseded old-model Containers names=${names.join(",")}`);
};

const waitForContainerDrain = async (): Promise<void> => {
  const deadline = Date.now() + DRAIN_TIMEOUT_MS;
  while (true) {
    const live = await listLivePredictionContainers();
    if (live.length === 0) {
      console.log("[rolling-deploy] Container drain complete");
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error(`Container drain timed out: ${describeUnsafePredictionContainers(live)}`);
    }
    await stopSupersededContainers();
    console.log(
      `[rolling-deploy] waiting for Container drain: ${describeUnsafePredictionContainers(live)}`,
    );
    await Bun.sleep(POLL_INTERVAL_MS);
  }
};

const deployWorkerAndContainers = async (attemptsRemaining: number): Promise<void> => {
  try {
    await runCommand(["bunx", "wrangler", "deploy"]);
  } catch (error) {
    if (attemptsRemaining <= 1) throw error;
    console.log(
      `[rolling-deploy] wrangler deploy failed, retrying remaining=${String(attemptsRemaining - 1)}`,
    );
    await Bun.sleep(WRANGLER_DEPLOY_RETRY_WAIT_MS);
    await deployWorkerAndContainers(attemptsRemaining - 1);
  }
};

const runYmdJst = (): string => {
  const parts = new Intl.DateTimeFormat("en-CA", {
    day: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  }).formatToParts(new Date());
  const value = (type: Intl.DateTimeFormatPartTypes): string =>
    parts.find((part) => part.type === type)?.value ?? "";
  return `${value("year")}${value("month")}${value("day")}`;
};

const enqueueNewModelPredictions = async (): Promise<void> => {
  const runYmd = runYmdJst();
  const sql = `select source, keibajo_code, race_bango
from realtime_race_sources
where kaisai_nen = '${runYmd.slice(0, 4)}'
  and kaisai_tsukihi = '${runYmd.slice(4)}'
  and datetime(race_start_at_jst) > datetime('now')
order by datetime(race_start_at_jst)`;
  const races = parseDeploymentRaces(
    await runWranglerJson([
      "d1",
      "execute",
      "sync-realtime-data",
      "--remote",
      "--json",
      "--command",
      sql,
    ]),
  );
  for (const race of races) {
    await postAdmin(ADMIN_RUN_PATH, buildDeploymentPredictionRequest(race, runYmd));
  }
  console.log(
    `[rolling-deploy] queued new-model predictions runYmd=${runYmd} races=${races.length}`,
  );
};

const handleTermination = (signal: NodeJS.Signals): void => {
  void resumePausedQueues()
    .catch((error: unknown) => console.error("[rolling-deploy] emergency resume failed", error))
    .finally(() => process.kill(process.pid, signal));
};

process.once("SIGINT", handleTermination);
process.once("SIGTERM", handleTermination);

try {
  await pauseStartQueues();
  console.log(
    `[rolling-deploy] paused new prediction delivery queues=${DEPLOYMENT_DRAIN_QUEUES.join(",")}`,
  );
  await stopSupersededContainers();
  await waitForContainerDrain();
  await deployWorkerAndContainers(WRANGLER_DEPLOY_ATTEMPTS);
  if (requeuePredictions) {
    await enqueueNewModelPredictions();
  } else {
    console.log("[rolling-deploy] Worker-only deployment; prediction requeue skipped");
  }
} finally {
  await resumePausedQueues();
  console.log("[rolling-deploy] prediction delivery resumed");
}
