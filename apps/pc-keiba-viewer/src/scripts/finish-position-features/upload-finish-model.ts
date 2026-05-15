// Run with: bun run src/scripts/finish-position-features/upload-finish-model.ts \
//   --category jra --model-version lambdarank-jra-20260520-v1 \
//   --model-path tmp/models/finish-jra-v1.lgb \
//   --metadata-path tmp/models/finish-jra-v1.json

import { spawn } from "node:child_process";

import type { FeatureCategory } from "./build-finish-position-features-types";

const DEFAULT_BUCKET = "pc-keiba-finish-position-models";
const WRANGLER_COMMAND = "bunx";
const MODEL_FILE_NAME = "model.lgb";
const METADATA_FILE_NAME = "metadata.json";

const CATEGORY_SET = new Set<FeatureCategory>(["ban-ei", "jra", "nar"]);

export interface UploadOptions {
  bucket: string;
  category: FeatureCategory;
  metadataPath: string | null;
  modelPath: string;
  modelVersion: string;
  remote: boolean;
}

export type WranglerSpawner = (
  args: readonly string[],
) => Promise<{ exitCode: number; stderr: string }>;

const isUploadableCategory = (value: string): value is FeatureCategory => {
  for (const candidate of CATEGORY_SET) {
    if (candidate === value) return true;
  }
  return false;
};

const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/upload-finish-model.ts \\",
    "    --category jra|nar|ban-ei --model-version <id> --model-path <file.lgb> \\",
    "    [--metadata-path <file.json>] [--bucket <name>] [--remote]",
    "",
    "Uploads the LightGBM model and optional metadata to the",
    "pc-keiba-finish-position-models R2 bucket via wrangler.",
    "Default --remote=false targets the local wrangler bucket emulation;",
    "pass --remote to push to the production R2 bucket.",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const initialOptions = (): UploadOptions => ({
  bucket: DEFAULT_BUCKET,
  category: "jra",
  metadataPath: null,
  modelPath: "",
  modelVersion: "",
  remote: false,
});

export const buildObjectKey = (
  category: FeatureCategory,
  modelVersion: string,
  filename: string,
): string => `finish-position/${category}/${modelVersion}/${filename}`;

export const buildModelObjectKey = (category: FeatureCategory, modelVersion: string): string =>
  buildObjectKey(category, modelVersion, MODEL_FILE_NAME);

export const buildMetadataObjectKey = (category: FeatureCategory, modelVersion: string): string =>
  buildObjectKey(category, modelVersion, METADATA_FILE_NAME);

const applyArg = (
  options: UploadOptions,
  name: string,
  value: string | undefined,
): { advanceBy: number } => {
  if (name === "--category") {
    const raw = requireValue(name, value);
    if (!isUploadableCategory(raw)) {
      throw new Error("--category must be jra, nar, or ban-ei.");
    }
    options.category = raw;
    return { advanceBy: 2 };
  }
  if (name === "--model-version") {
    options.modelVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-path") {
    options.modelPath = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--metadata-path") {
    options.metadataPath = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--bucket") {
    options.bucket = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--remote") {
    options.remote = true;
    return { advanceBy: 1 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

export const parseArgs = (argv: readonly string[]): UploadOptions => {
  const options = initialOptions();
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
    cursor += advanceBy;
  }
  if (options.modelVersion === "") throw new Error("--model-version is required.");
  if (options.modelPath === "") throw new Error("--model-path is required.");
  return options;
};

export const buildWranglerArgs = (
  bucket: string,
  objectKey: string,
  filePath: string,
  remote: boolean,
  contentType: string,
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
    contentType,
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

interface UploadStep {
  contentType: string;
  filePath: string;
  objectKey: string;
}

export const buildUploadSteps = (options: UploadOptions): UploadStep[] => {
  const steps: UploadStep[] = [
    {
      contentType: "application/octet-stream",
      filePath: options.modelPath,
      objectKey: buildModelObjectKey(options.category, options.modelVersion),
    },
  ];
  if (options.metadataPath !== null) {
    steps.push({
      contentType: "application/json",
      filePath: options.metadataPath,
      objectKey: buildMetadataObjectKey(options.category, options.modelVersion),
    });
  }
  return steps;
};

export const runUpload = (
  options: UploadOptions,
  spawner: WranglerSpawner,
): Promise<string[]> =>
  buildUploadSteps(options).reduce<Promise<string[]>>(
    (accPromise, step) =>
      accPromise.then(async (acc) => {
        const wranglerArgs = buildWranglerArgs(
          options.bucket,
          step.objectKey,
          step.filePath,
          options.remote,
          step.contentType,
        );
        const result = await spawner(wranglerArgs);
        if (result.exitCode !== 0) {
          throw new Error(
            `wrangler upload failed for ${step.objectKey} (exit ${result.exitCode}): ${result.stderr}`,
          );
        }
        return [...acc, step.objectKey];
      }),
    Promise.resolve([]),
  );

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const uploaded = await runUpload(options, spawnWrangler);
  console.log(
    `[upload-finish-model] bucket=${options.bucket} remote=${options.remote} uploaded=${uploaded.length}: ${uploaded.join(", ")}`,
  );
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export { buildUsageText, DEFAULT_BUCKET, initialOptions, isUploadableCategory };
