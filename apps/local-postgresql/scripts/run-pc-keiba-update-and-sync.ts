#!/usr/bin/env bun
// Run with bun. Orchestrates independently runnable update, feature, training, and sync scripts.

import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  readEnvValue,
  resolveRealtimeAdminToken,
  resolveRealtimeDiscoveryDates,
  triggerRealtimeDiscoveryAfterReplica,
} from "../src/replica-push/realtime-discovery";
import { attestPreWeightPredictionReadiness } from "./finish-position-readiness-attestation";
import {
  createUpdateSyncRunStateStore,
  type UpdateSyncRunStateStore,
  type UpdateSyncStep,
} from "./update-sync-run-state";

export interface CommandOptions {
  captureOutput?: boolean;
  env?: Readonly<Record<string, string>>;
}

export interface CommandResult {
  exitCode: number;
  stderr: string;
  stdout: string;
}

export type CommandRunner = (
  command: readonly string[],
  options?: CommandOptions,
) => Promise<CommandResult>;

interface UpdateAndSyncOptions {
  appDir: string;
  attestFinishPosition: (runYmd: string) => Promise<void>;
  bunExecutable: string;
  log: (message: string) => void;
  now: () => Date;
  runCommand: CommandRunner;
  stateStore: UpdateSyncRunStateStore;
  triggerRealtimeDiscovery: () => Promise<void>;
  vmName: string;
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const defaultAppDir = resolve(scriptDir, "..");
const realtimeDevVarsPath = resolve(defaultAppDir, "../sync-realtime-data/.dev.vars");
const rootEnvPath = resolve(defaultAppDir, "../../.env");
const DEFAULT_REALTIME_BASE_URL = "https://sync-realtime-data.kkk4oru.com";
const DEFAULT_FINISH_POSITION_BASE_URL = "https://finish-position-cron.kaoru.workers.dev";
const REALTIME_DISCOVERY_POLL_INTERVAL_MILLISECONDS = 10_000;
const REALTIME_DISCOVERY_POLL_TIMEOUT_MILLISECONDS = 15 * 60_000;
const FINISH_POSITION_POLL_INTERVAL_MILLISECONDS = 15_000;
const FINISH_POSITION_POLL_TIMEOUT_MILLISECONDS = 45 * 60_000;
const REPLICA_SYNC_BROKEN_PIPE_EXIT_CODE = 141;
const REPLICA_SYNC_BROKEN_PIPE_RETRIES = 2;
// PC-KEIBA publishes several upcoming race days at once (not only tomorrow).
// Keep the derived-feature build bounded while covering the normal one-week
// publication horizon so future races cannot be synced without their features.
const FEATURE_BUILD_HORIZON_DAYS = 7;
const UPDATE_SYNC_STEP_INDEX: ReadonlyMap<UpdateSyncStep, number> = new Map([
  ["update", 0],
  ["verify-vm", 1],
  ["features", 2],
  ["training", 3],
  ["sync", 4],
  ["discovery", 5],
  ["readiness", 6],
]);

const commandLabel = (command: readonly string[]): string => command.join(" ");

const stepIndex = (step: UpdateSyncStep): number => {
  const index = UPDATE_SYNC_STEP_INDEX.get(step);
  if (index === undefined) throw new Error(`Unknown update-and-sync step: ${step}`);
  return index;
};

const shouldRunStep = (resumeStep: UpdateSyncStep, candidate: UpdateSyncStep): boolean =>
  stepIndex(candidate) >= stepIndex(resumeStep);

const formatJstDate = (date: Date): string =>
  new Intl.DateTimeFormat("ja-JP", {
    calendar: "iso8601",
    day: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  })
    .format(date)
    .replaceAll("/", "")
    .replaceAll("-", "");

/** Build derived corner features for the update date and the next race day. */
export const resolveFeatureBuildDateRange = (now: Date): { fromDate: string; toDate: string } => {
  const fromDate = formatJstDate(now);
  const horizonDate = new Date(now.getTime() + FEATURE_BUILD_HORIZON_DAYS * 24 * 60 * 60 * 1000);
  return { fromDate, toDate: formatJstDate(horizonDate) };
};

const requireSuccess = (command: readonly string[], result: CommandResult): void => {
  if (result.exitCode === 0) return;
  const detail = result.stderr.trim();
  throw new Error(
    detail === ""
      ? `Command failed (${result.exitCode}): ${commandLabel(command)}`
      : `Command failed (${result.exitCode}): ${commandLabel(command)}\n${detail}`,
  );
};

const runReplicaSyncWithBrokenPipeRetry = async (
  command: readonly string[],
  runCommand: CommandRunner,
  log: (message: string) => void,
): Promise<void> => {
  for (let attempt = 1; attempt <= REPLICA_SYNC_BROKEN_PIPE_RETRIES; attempt += 1) {
    const result = await runCommand(command);
    if (result.exitCode === 0) return;
    if (
      result.exitCode !== REPLICA_SYNC_BROKEN_PIPE_EXIT_CODE ||
      attempt === REPLICA_SYNC_BROKEN_PIPE_RETRIES
    ) {
      requireSuccess(command, result);
    }
    log(
      `Replica sync exited with broken pipe (141); retrying without repeating PC-KEIBA update (${attempt}/${REPLICA_SYNC_BROKEN_PIPE_RETRIES - 1})...`,
    );
  }
};

export const parseParallelsVmStatus = (output: string): string => {
  const match = /\b(running|stopped|suspended|paused)\s*$/u.exec(output.trim());
  if (match === null) {
    throw new Error(`Could not parse Parallels VM status: ${output.trim()}`);
  }
  const status = match[1];
  if (status === undefined) {
    throw new Error(`Could not parse Parallels VM status: ${output.trim()}`);
  }
  return status;
};

export const runPcKeibaUpdateAndSync = async (options: UpdateAndSyncOptions): Promise<void> => {
  const startedAt = options.now();
  const runYmd = formatJstDate(startedAt);
  const persisted = await options.stateStore.loadCheckpoint();
  const resumeStep = persisted?.runYmd === runYmd ? persisted.nextStep : "update";
  if (persisted !== null && persisted.runYmd !== runYmd) {
    options.log(
      `Ignoring stale update-and-sync checkpoint for ${persisted.runYmd}; starting ${runYmd} from the PC-KEIBA update.`,
    );
  }
  if (persisted?.runYmd === runYmd) {
    options.log(`Resuming update-and-sync run ${runYmd} from step '${resumeStep}'.`);
  }
  const saveNextStep = async (nextStep: UpdateSyncStep): Promise<void> =>
    options.stateStore.saveCheckpoint({
      nextStep,
      runYmd,
      updatedAt: options.now().toISOString(),
      version: 1,
    });
  const updateCommand = [
    options.bunExecutable,
    "run",
    "--cwd",
    options.appDir,
    "pc-keiba:update",
  ] satisfies readonly string[];
  if (shouldRunStep(resumeStep, "update")) {
    options.log("Step 1/7: updating PC-KEIBA data through the Parallels Windows VM...");
    const updateResult = await options.runCommand(updateCommand, {
      env: { PARALLELS_STOP_AFTER_SUCCESS: "1" },
    });
    requireSuccess(updateCommand, updateResult);
    // The Windows updater is lock-protected and idempotent. Persist only after
    // command success so a host crash between checkpointing and process start
    // cannot incorrectly skip an update that never ran.
    await saveNextStep("verify-vm");
  }

  const statusCommand = ["prlctl", "status", options.vmName] satisfies readonly string[];
  if (shouldRunStep(resumeStep, "verify-vm")) {
    options.log("Step 2/7: verifying that the Windows VM stopped after the update...");
    const statusResult = await options.runCommand(statusCommand, { captureOutput: true });
    requireSuccess(statusCommand, statusResult);
    const vmStatus = parseParallelsVmStatus(statusResult.stdout);
    if (vmStatus !== "stopped") {
      throw new Error(
        `Parallels VM '${options.vmName}' must be stopped before replica sync; current status: ${vmStatus}`,
      );
    }
    await saveNextStep("features");
  }

  const featureDates = resolveFeatureBuildDateRange(startedAt);
  const featureAppDir = resolve(options.appDir, "../pc-keiba-viewer");
  const cornerFeatureCommand = [
    options.bunExecutable,
    "run",
    "--cwd",
    featureAppDir,
    "dev:build-corner-features",
    "--",
    "--target",
    "local",
    "--source-scope",
    "all",
    "--from-date",
    featureDates.fromDate,
    "--to-date",
    featureDates.toDate,
  ] satisfies readonly string[];
  if (shouldRunStep(resumeStep, "features")) {
    options.log(
      `Step 3/7: materializing local corner features for ${featureDates.fromDate}-${featureDates.toDate} before replica sync...`,
    );
    const cornerFeatureResult = await options.runCommand(cornerFeatureCommand);
    requireSuccess(cornerFeatureCommand, cornerFeatureResult);
    await saveNextStep("training");
  }

  const trainingCommand = [
    options.bunExecutable,
    "run",
    "--cwd",
    options.appDir,
    "scrape:netkeiba-training",
  ] satisfies readonly string[];
  if (shouldRunStep(resumeStep, "training")) {
    options.log("Step 4/7: importing JRA training workouts from netkeiba as backup...");
    const trainingResult = await options.runCommand(trainingCommand);
    requireSuccess(trainingCommand, trainingResult);
    await saveNextStep("sync");
  }

  const syncCommand = [
    options.bunExecutable,
    "run",
    "--cwd",
    options.appDir,
    "replica:push",
  ] satisfies readonly string[];
  if (shouldRunStep(resumeStep, "sync")) {
    options.log("Step 5/7: syncing local PostgreSQL to R2 Catalog and Neon...");
    await runReplicaSyncWithBrokenPipeRetry(syncCommand, options.runCommand, options.log);
    await saveNextStep("discovery");
  }

  if (shouldRunStep(resumeStep, "discovery")) {
    options.log("Step 6/7: discovering synced races and planning premium fetches...");
    await options.triggerRealtimeDiscovery();
    await saveNextStep("readiness");
  }
  options.log(
    `Step 7/7: attesting pre-weight prediction and KV readiness for upcoming ${runYmd} races...`,
  );
  await options.attestFinishPosition(runYmd);
  await options.stateStore.recordCompletion({
    completedAt: options.now().toISOString(),
    runYmd,
    version: 1,
  });
  await options.stateStore.clearCheckpoint();
  options.log(
    "PC-KEIBA update, R2 Catalog/Neon sync, realtime discovery, and prediction readiness attestation completed successfully.",
  );
};

const runCommand: CommandRunner = async (command, options = {}) => {
  const captureOutput = options.captureOutput === true;
  const subprocess = Bun.spawn([...command], {
    env: { ...process.env, ...options.env },
    stderr: captureOutput ? "pipe" : "inherit",
    stdout: captureOutput ? "pipe" : "inherit",
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    subprocess.exited,
    captureOutput ? new Response(subprocess.stdout).text() : Promise.resolve(""),
    captureOutput ? new Response(subprocess.stderr).text() : Promise.resolve(""),
  ]);
  return { exitCode, stderr, stdout };
};

const triggerRealtimeDiscovery = async (): Promise<void> => {
  const explicitToken = process.env.REALTIME_ADMIN_TOKEN;
  const devVarsContents =
    explicitToken === undefined || explicitToken === ""
      ? await Bun.file(realtimeDevVarsPath)
          .text()
          .catch(() => "")
      : "";
  const token = resolveRealtimeAdminToken(explicitToken, devVarsContents);
  const dates = resolveRealtimeDiscoveryDates(process.env.SYNC_REALTIME_DATA_DATE, new Date());
  await triggerRealtimeDiscoveryAfterReplica({
    baseUrl: process.env.SYNC_REALTIME_DATA_BASE_URL || DEFAULT_REALTIME_BASE_URL,
    dates,
    fetcher: fetch,
    log: console.log,
    pollIntervalMilliseconds: REALTIME_DISCOVERY_POLL_INTERVAL_MILLISECONDS,
    pollTimeoutMilliseconds: REALTIME_DISCOVERY_POLL_TIMEOUT_MILLISECONDS,
    planRunningStyle: true,
    retryDelay: Bun.sleep,
    token,
  });
};

const attestFinishPosition = async (runYmd: string): Promise<void> => {
  const explicitToken = process.env.FINISH_POSITION_CRON_TRIGGER_TOKEN;
  const rootEnvContents =
    explicitToken === undefined || explicitToken === ""
      ? await Bun.file(rootEnvPath)
          .text()
          .catch(() => "")
      : "";
  const token =
    explicitToken === undefined || explicitToken === ""
      ? readEnvValue(rootEnvContents, "FINISH_POSITION_CRON_TRIGGER_TOKEN")
      : explicitToken;
  if (token === undefined || token === "") {
    throw new Error(
      "FINISH_POSITION_CRON_TRIGGER_TOKEN is required in the environment or repository .env",
    );
  }
  await attestPreWeightPredictionReadiness({
    baseUrl: process.env.FINISH_POSITION_CRON_BASE_URL || DEFAULT_FINISH_POSITION_BASE_URL,
    fetcher: fetch,
    log: console.log,
    nowMilliseconds: Date.now,
    pollIntervalMilliseconds: FINISH_POSITION_POLL_INTERVAL_MILLISECONDS,
    pollTimeoutMilliseconds: FINISH_POSITION_POLL_TIMEOUT_MILLISECONDS,
    retryDelay: Bun.sleep,
    runYmd,
    token,
  });
};

if (import.meta.main) {
  try {
    await runPcKeibaUpdateAndSync({
      appDir: defaultAppDir,
      attestFinishPosition,
      bunExecutable: process.execPath,
      log: console.log,
      now: () => new Date(),
      runCommand,
      stateStore: createUpdateSyncRunStateStore({ appDir: defaultAppDir }),
      triggerRealtimeDiscovery,
      vmName: process.env.PARALLELS_VM_NAME || "Windows 11",
    });
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
