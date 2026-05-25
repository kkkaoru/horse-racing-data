// Run with:
//   bun run running-style:upload-model -- --source jra --input tmp/jra-model.json --remote
//   bun run running-style:upload-model -- --source nar --input tmp/nar-model.flatbin

import {
  buildRunningStyleLatestModelKey,
  isRunningStyleModelSource,
  registerRunningStyleModel,
  RUNNING_STYLE_MODEL_BUCKET,
  type RunningStyleModelSource,
} from "../src/running-style-model-register";

export interface UploadRunningStyleModelCliArgs {
  bucket: string;
  inputPath: string;
  remote: boolean;
  source: RunningStyleModelSource;
  syncLocal: boolean;
}

const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run running-style:upload-model -- --source jra|nar --input <model.json|model.flatbin> \\",
    "    [--remote] [--sync-local] [--bucket <name>]",
    "",
    "Converts JSON models to flatbin when needed and uploads to",
    "running-style/models/<source>/latest.flatbin in R2.",
    "Use --remote for production R2 and --sync-local to mirror into local wrangler R2.",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} requires a value.`);
  }
  return value;
};

export const parseUploadRunningStyleModelCliArgs = (
  argv: readonly string[],
): UploadRunningStyleModelCliArgs => {
  let source: RunningStyleModelSource | undefined;
  let inputPath = "";
  let remote = false;
  let syncLocal = true;
  let bucket = RUNNING_STYLE_MODEL_BUCKET;

  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index];
    const value = argv[index + 1];
    if (name === "--source") {
      const raw = requireValue(name, value);
      if (!isRunningStyleModelSource(raw)) {
        throw new Error("--source must be jra or nar.");
      }
      source = raw;
      index += 1;
      continue;
    }
    if (name === "--input") {
      inputPath = requireValue(name, value);
      index += 1;
      continue;
    }
    if (name === "--bucket") {
      bucket = requireValue(name, value);
      index += 1;
      continue;
    }
    if (name === "--remote") {
      remote = true;
      continue;
    }
    if (name === "--sync-local") {
      syncLocal = true;
      continue;
    }
    if (name === "--no-sync-local") {
      syncLocal = false;
      continue;
    }
    if (name === "--help" || name === "-h") {
      console.log(buildUsageText());
      process.exit(0);
    }
    if (name === undefined) {
      break;
    }
    throw new Error(`Unknown argument: ${name}`);
  }

  if (source === undefined) {
    throw new Error(buildUsageText());
  }
  if (inputPath.length === 0) {
    throw new Error("--input is required.");
  }

  return {
    bucket,
    inputPath,
    remote,
    source,
    syncLocal,
  };
};

export const run = async (): Promise<void> => {
  const args = parseUploadRunningStyleModelCliArgs(process.argv.slice(2));
  const objectKey = buildRunningStyleLatestModelKey(args.source);
  const uploaded = await registerRunningStyleModel(
    {
      inputPath: args.inputPath,
      remote: args.remote,
      source: args.source,
    },
    { bucket: args.bucket },
  );
  console.log(
    `[running-style:upload-model] source=${args.source} remote=${args.remote} key=${uploaded.objectKey} sizeBytes=${uploaded.sizeBytes}`,
  );

  if (args.syncLocal && args.remote) {
    const { syncRunningStyleModel } = await import("../src/running-style-model-register");
    await syncRunningStyleModel(args.source, { bucket: args.bucket });
    console.log(`[running-style:upload-model] synced-local key=${objectKey}`);
  }
};

if (import.meta.main) {
  run().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export { buildUsageText };
