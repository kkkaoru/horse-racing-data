// Run with bun. Persistent, day-scoped resume state for the update-and-sync orchestrator.

import { mkdir, rename, rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";

export type UpdateSyncStep =
  | "update"
  | "verify-vm"
  | "features"
  | "training"
  | "sync"
  | "discovery"
  | "readiness";

export interface UpdateSyncCheckpoint {
  runYmd: string;
  nextStep: UpdateSyncStep;
  updatedAt: string;
  version: 1;
}

export interface UpdateSyncCompletion {
  completedAt: string;
  runYmd: string;
  version: 1;
}

export interface UpdateSyncRunStateStore {
  clearCheckpoint: () => Promise<void>;
  loadCheckpoint: () => Promise<UpdateSyncCheckpoint | null>;
  recordCompletion: (completion: UpdateSyncCompletion) => Promise<void>;
  saveCheckpoint: (checkpoint: UpdateSyncCheckpoint) => Promise<void>;
}

interface RunStateFileIo {
  readText: (path: string) => Promise<string | null>;
  remove: (path: string) => Promise<void>;
  rename: (fromPath: string, toPath: string) => Promise<void>;
  writeText: (path: string, contents: string) => Promise<void>;
}

interface CreateRunStateStoreOptions {
  appDir: string;
  io?: RunStateFileIo;
  randomId?: () => string;
}

const RUN_STATE_DIRECTORY = "tmp/update-and-sync-state";
const CHECKPOINT_FILE = "active.json";
const RUN_YMD_PATTERN = /^\d{8}$/u;
const UPDATE_SYNC_STEPS: ReadonlySet<string> = new Set([
  "update",
  "verify-vm",
  "features",
  "training",
  "sync",
  "discovery",
  "readiness",
]);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isUpdateSyncStep = (value: unknown): value is UpdateSyncStep =>
  typeof value === "string" && UPDATE_SYNC_STEPS.has(value);

export const parseUpdateSyncCheckpoint = (contents: string): UpdateSyncCheckpoint => {
  const parsed: unknown = JSON.parse(contents);
  if (
    !isRecord(parsed) ||
    parsed.version !== 1 ||
    typeof parsed.runYmd !== "string" ||
    !RUN_YMD_PATTERN.test(parsed.runYmd) ||
    !isUpdateSyncStep(parsed.nextStep) ||
    typeof parsed.updatedAt !== "string" ||
    !Number.isFinite(Date.parse(parsed.updatedAt))
  ) {
    throw new Error("Update-and-sync checkpoint is invalid");
  }
  return {
    nextStep: parsed.nextStep,
    runYmd: parsed.runYmd,
    updatedAt: parsed.updatedAt,
    version: 1,
  };
};

const createDefaultFileIo = (): RunStateFileIo => ({
  readText: async (path) =>
    Bun.file(path)
      .text()
      .catch((error: unknown) => {
        const code = isRecord(error) ? error.code : undefined;
        if (code === "ENOENT") return null;
        throw error;
      }),
  remove: async (path) => {
    await rm(path, { force: true });
  },
  rename,
  writeText: async (path, contents) => {
    await mkdir(dirname(path), { recursive: true });
    await Bun.write(path, contents);
  },
});

const serialize = (value: UpdateSyncCheckpoint | UpdateSyncCompletion): string =>
  `${JSON.stringify(value, null, 2)}\n`;

export const createUpdateSyncRunStateStore = (
  options: CreateRunStateStoreOptions,
): UpdateSyncRunStateStore => {
  const directory = resolve(options.appDir, RUN_STATE_DIRECTORY);
  const checkpointPath = resolve(directory, CHECKPOINT_FILE);
  const io = options.io ?? createDefaultFileIo();
  const randomId = options.randomId ?? (() => crypto.randomUUID());
  const writeAtomically = async (path: string, contents: string): Promise<void> => {
    const temporaryPath = `${path}.${randomId()}.tmp`;
    await io.writeText(temporaryPath, contents);
    await io.rename(temporaryPath, path);
  };
  return {
    clearCheckpoint: async () => io.remove(checkpointPath),
    loadCheckpoint: async () => {
      const contents = await io.readText(checkpointPath);
      return contents === null ? null : parseUpdateSyncCheckpoint(contents);
    },
    recordCompletion: async (completion) =>
      writeAtomically(
        resolve(directory, `completed-${completion.runYmd}.json`),
        serialize(completion),
      ),
    saveCheckpoint: async (checkpoint) => writeAtomically(checkpointPath, serialize(checkpoint)),
  };
};
