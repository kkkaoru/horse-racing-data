#!/usr/bin/env bun

import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveRealtimeAdminToken,
  resolveRealtimeDiscoveryDates,
  triggerRealtimeDiscoveryAfterReplica,
} from "../src/replica-push/realtime-discovery";

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
  bunExecutable: string;
  log: (message: string) => void;
  runCommand: CommandRunner;
  triggerRealtimeDiscovery: () => Promise<void>;
  vmName: string;
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const defaultAppDir = resolve(scriptDir, "..");
const realtimeDevVarsPath = resolve(defaultAppDir, "../sync-realtime-data/.dev.vars");
const DEFAULT_REALTIME_BASE_URL = "https://sync-realtime-data.kkk4oru.com";
const REALTIME_DISCOVERY_POLL_INTERVAL_MILLISECONDS = 10_000;
const REALTIME_DISCOVERY_POLL_TIMEOUT_MILLISECONDS = 15 * 60_000;

const commandLabel = (command: readonly string[]): string => command.join(" ");

const requireSuccess = (command: readonly string[], result: CommandResult): void => {
  if (result.exitCode === 0) return;
  const detail = result.stderr.trim();
  throw new Error(
    detail === ""
      ? `Command failed (${result.exitCode}): ${commandLabel(command)}`
      : `Command failed (${result.exitCode}): ${commandLabel(command)}\n${detail}`,
  );
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
  const updateCommand = [
    options.bunExecutable,
    "run",
    "--cwd",
    options.appDir,
    "pc-keiba:update",
  ] as const;
  options.log("Step 1/5: updating PC-KEIBA data through the Parallels Windows VM...");
  const updateResult = await options.runCommand(updateCommand, {
    env: { PARALLELS_STOP_AFTER_SUCCESS: "1" },
  });
  requireSuccess(updateCommand, updateResult);

  const statusCommand = ["prlctl", "status", options.vmName] as const;
  options.log("Step 2/5: verifying that the Windows VM stopped after the update...");
  const statusResult = await options.runCommand(statusCommand, { captureOutput: true });
  requireSuccess(statusCommand, statusResult);
  const vmStatus = parseParallelsVmStatus(statusResult.stdout);
  if (vmStatus !== "stopped") {
    throw new Error(
      `Parallels VM '${options.vmName}' must be stopped before replica sync; current status: ${vmStatus}`,
    );
  }

  const trainingCommand = [
    options.bunExecutable,
    "run",
    "--cwd",
    options.appDir,
    "scrape:netkeiba-training",
  ] as const;
  options.log("Step 3/5: importing JRA training workouts from netkeiba as backup...");
  const trainingResult = await options.runCommand(trainingCommand);
  requireSuccess(trainingCommand, trainingResult);

  const syncCommand = [
    options.bunExecutable,
    "run",
    "--cwd",
    options.appDir,
    "replica:push",
  ] as const;
  options.log("Step 4/5: syncing local PostgreSQL to R2 Catalog and Neon...");
  const syncResult = await options.runCommand(syncCommand);
  requireSuccess(syncCommand, syncResult);

  options.log("Step 5/5: discovering synced races and planning premium fetches...");
  await options.triggerRealtimeDiscovery();
  options.log(
    "PC-KEIBA update, R2 Catalog/Neon sync, and realtime discovery completed successfully.",
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
    retryDelay: Bun.sleep,
    token,
  });
};

if (import.meta.main) {
  try {
    await runPcKeibaUpdateAndSync({
      appDir: defaultAppDir,
      bunExecutable: process.execPath,
      log: console.log,
      runCommand,
      triggerRealtimeDiscovery,
      vmName: process.env.PARALLELS_VM_NAME || "Windows 11",
    });
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
