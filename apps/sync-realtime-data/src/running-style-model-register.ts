// Run with bun. Register running-style flatbin models in R2 and sync them for local runs.

import { mkdtemp, rm, stat } from "node:fs/promises";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { buildRunningStyleFlatModelKey } from "./running-style-model-binary";
import type { RegisteredRaceRow } from "./running-style-cron";

export const RUNNING_STYLE_MODEL_BUCKET = "pc-keiba-finish-position-models";
export const WRANGLER_COMMAND = "bunx";

export type RunningStyleModelSource = "jra" | "nar";

export interface RunningStyleModelRegisterSpec {
  inputPath: string;
  remote: boolean;
  source: RunningStyleModelSource;
}

export interface UploadRunningStyleModelOptions {
  bucket: string;
  filePath: string;
  objectKey: string;
  remote: boolean;
}

export interface EnsureRunningStyleModelsOptions {
  bucket?: string;
  register?: ReadonlyArray<RunningStyleModelRegisterSpec>;
  sources: ReadonlyArray<RunningStyleModelSource>;
  syncLocalFromRemote?: boolean;
}

export type WranglerSpawner = (
  args: readonly string[],
) => Promise<{ exitCode: number; stderr: string }>;

const SOURCE_SET = new Set<RunningStyleModelSource>(["jra", "nar"]);

export const isRunningStyleModelSource = (value: string): value is RunningStyleModelSource =>
  SOURCE_SET.has(value as RunningStyleModelSource);

export const buildRunningStyleLatestModelKey = (source: RunningStyleModelSource): string =>
  buildRunningStyleFlatModelKey(source);

export const listRequiredRunningStyleModelSources = (
  races: ReadonlyArray<Pick<RegisteredRaceRow, "source">>,
): RunningStyleModelSource[] => {
  const sources = new Set<RunningStyleModelSource>();
  races.forEach((race) => {
    sources.add(race.source);
  });
  return [...sources].toSorted();
};

export const parseRegisterModelArg = (
  value: string,
): RunningStyleModelRegisterSpec & { inputPath: string } => {
  const separatorIndex = value.indexOf(":");
  if (separatorIndex <= 0) {
    throw new Error(
      `Invalid --register-model value "${value}". Use source:/path/to/model.json or source:/path/to/model.flatbin.`,
    );
  }
  const source = value.slice(0, separatorIndex);
  const inputPath = value.slice(separatorIndex + 1);
  if (!isRunningStyleModelSource(source)) {
    throw new Error(`--register-model source must be jra or nar. Received "${source}".`);
  }
  if (inputPath.length === 0) {
    throw new Error("--register-model requires a file path after the source prefix.");
  }
  return { inputPath, remote: false, source };
};

export const buildWranglerR2PutArgs = (
  bucket: string,
  objectKey: string,
  filePath: string,
  remote: boolean,
): string[] => {
  const baseArgs = [
    "wrangler",
    "r2",
    "object",
    "put",
    `${bucket}/${objectKey}`,
    "--file",
    filePath,
    "--content-type",
    "application/octet-stream",
  ];
  return remote ? [...baseArgs, "--remote"] : baseArgs;
};

export const buildWranglerR2GetArgs = (
  bucket: string,
  objectKey: string,
  filePath: string,
  remote: boolean,
): string[] => {
  const baseArgs = [
    "wrangler",
    "r2",
    "object",
    "get",
    `${bucket}/${objectKey}`,
    "--file",
    filePath,
  ];
  return remote ? [...baseArgs, "--remote"] : baseArgs;
};

const spawnWrangler: WranglerSpawner = async (args) => {
  const stderrChunks: Buffer[] = [];
  const child = spawn(WRANGLER_COMMAND, [...args], { stdio: ["ignore", "inherit", "pipe"] });
  child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
  const exitCode = await new Promise<number>((resolvePromise, rejectPromise) => {
    child.once("error", rejectPromise);
    child.once("close", (code) => resolvePromise(code ?? 0));
  });
  return { exitCode, stderr: Buffer.concat(stderrChunks).toString("utf8") };
};

const resolveFlatbinPath = async (
  inputPath: string,
): Promise<{ cleanup: () => Promise<void>; filePath: string }> => {
  if (inputPath.endsWith(".flatbin")) {
    return { cleanup: async () => {}, filePath: inputPath };
  }
  if (!inputPath.endsWith(".json")) {
    throw new Error(`Unsupported model input "${inputPath}". Use .json or .flatbin.`);
  }
  const tempDir = await mkdtemp(join(tmpdir(), "running-style-model-"));
  const outputPath = join(tempDir, "model.flatbin");
  const { convertRunningStyleModelFile } =
    await import("../scripts/convert-running-style-model-to-binary");
  await convertRunningStyleModelFile(inputPath, outputPath);
  return {
    cleanup: async () => {
      await rm(tempDir, { recursive: true, force: true });
    },
    filePath: outputPath,
  };
};

export const uploadRunningStyleModel = async (
  options: UploadRunningStyleModelOptions,
  spawner: WranglerSpawner = spawnWrangler,
): Promise<string> => {
  const wranglerArgs = buildWranglerR2PutArgs(
    options.bucket,
    options.objectKey,
    options.filePath,
    options.remote,
  );
  const result = await spawner(wranglerArgs);
  if (result.exitCode !== 0) {
    throw new Error(
      `wrangler upload failed for ${options.objectKey} (exit ${result.exitCode}): ${result.stderr}`,
    );
  }
  return options.objectKey;
};

export const objectExistsInR2 = async (
  bucket: string,
  objectKey: string,
  remote: boolean,
  spawner: WranglerSpawner = spawnWrangler,
): Promise<boolean> => {
  const tempDir = await mkdtemp(join(tmpdir(), "running-style-model-check-"));
  const filePath = join(tempDir, "model.flatbin");
  try {
    const result = await spawner(buildWranglerR2GetArgs(bucket, objectKey, filePath, remote));
    return result.exitCode === 0;
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
};

export const syncRunningStyleModel = async (
  source: RunningStyleModelSource,
  options: { bucket?: string; spawner?: WranglerSpawner } = {},
): Promise<string> => {
  const bucket = options.bucket ?? RUNNING_STYLE_MODEL_BUCKET;
  const spawner = options.spawner ?? spawnWrangler;
  const objectKey = buildRunningStyleLatestModelKey(source);
  const tempDir = await mkdtemp(join(tmpdir(), "running-style-model-sync-"));
  const filePath = join(tempDir, "model.flatbin");
  try {
    const remoteResult = await spawner(buildWranglerR2GetArgs(bucket, objectKey, filePath, true));
    if (remoteResult.exitCode !== 0) {
      throw new Error(
        `Remote model missing for ${source} (${objectKey}): ${remoteResult.stderr.trim()}`,
      );
    }
    const localResult = await spawner(buildWranglerR2PutArgs(bucket, objectKey, filePath, false));
    if (localResult.exitCode !== 0) {
      throw new Error(
        `Local model sync failed for ${source} (${objectKey}): ${localResult.stderr.trim()}`,
      );
    }
    return objectKey;
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
};

export const registerRunningStyleModel = async (
  spec: RunningStyleModelRegisterSpec,
  options: { bucket?: string; spawner?: WranglerSpawner } = {},
): Promise<{ objectKey: string; sizeBytes: number }> => {
  const bucket = options.bucket ?? RUNNING_STYLE_MODEL_BUCKET;
  const spawner = options.spawner ?? spawnWrangler;
  const objectKey = buildRunningStyleLatestModelKey(spec.source);
  const resolved = await resolveFlatbinPath(spec.inputPath);
  try {
    const uploadedKey = await uploadRunningStyleModel(
      {
        bucket,
        filePath: resolved.filePath,
        objectKey,
        remote: spec.remote,
      },
      spawner,
    );
    const sizeBytes = (await stat(resolved.filePath)).size;
    return { objectKey: uploadedKey, sizeBytes };
  } finally {
    await resolved.cleanup();
  }
};

export const ensureRunningStyleModels = async (
  options: EnsureRunningStyleModelsOptions,
  spawner: WranglerSpawner = spawnWrangler,
): Promise<{ registered: string[]; synced: string[] }> => {
  const bucket = options.bucket ?? RUNNING_STYLE_MODEL_BUCKET;
  const registered: string[] = [];
  const synced: string[] = [];

  for (const spec of options.register ?? []) {
    const result = await registerRunningStyleModel(spec, { bucket, spawner });
    registered.push(result.objectKey);
    const localExists = await objectExistsInR2(bucket, result.objectKey, false, spawner);
    if (!localExists && options.syncLocalFromRemote !== false) {
      if (spec.remote) {
        await syncRunningStyleModel(spec.source, { bucket, spawner });
        synced.push(result.objectKey);
      } else {
        throw new Error(
          `Local model upload did not persist for ${spec.source} (${result.objectKey}).`,
        );
      }
    }
  }

  for (const source of options.sources) {
    const objectKey = buildRunningStyleLatestModelKey(source);
    const localExists = await objectExistsInR2(bucket, objectKey, false, spawner);
    if (localExists) {
      continue;
    }
    if (options.syncLocalFromRemote === false) {
      throw new Error(
        `Local R2 model missing for ${source} (${objectKey}). Pass --register-model or enable --sync-models.`,
      );
    }
    await syncRunningStyleModel(source, { bucket, spawner });
    synced.push(objectKey);
  }

  return { registered, synced };
};
