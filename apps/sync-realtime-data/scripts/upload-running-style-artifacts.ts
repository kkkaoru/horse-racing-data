// Run with bun.
// Uploads the LightGBM JSON + per-horse feature JSONL to R2 under the
// key convention the Worker cron handler expects.
//
// Usage:
//   bun run apps/sync-realtime-data/scripts/upload-running-style-artifacts.ts \
//     --source nar \
//     --model-json tmp/models/nar-running-style-lgbm-prod-v1.5/model.json \
//     --features-jsonl tmp/running-style-features-nar-20260518.jsonl \
//     --features-date 20260518
//
// Requires wrangler authentication (`bunx wrangler login` once).

import { spawnSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";

interface CliArgs {
  source: "jra" | "nar";
  modelJson: string;
  featuresJsonl: string;
  featuresDate: string;
}

const REQUIRED_FLAGS = ["--source", "--model-json", "--features-jsonl", "--features-date"] as const;
const VALID_SOURCES = ["jra", "nar"] as const;
const BUCKET_NAME = "pc-keiba-finish-position-models";

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value`);
  return value;
};

const isValidSource = (value: string): value is "jra" | "nar" =>
  VALID_SOURCES.includes(value as "jra" | "nar");

const parseArgs = (argv: readonly string[]): CliArgs => {
  const map = new Map<string, string>();
  argv.forEach((token, index) => {
    if (!token.startsWith("--")) return;
    map.set(token, argv[index + 1] ?? "");
  });
  REQUIRED_FLAGS.forEach((flag) => {
    if (!map.has(flag)) throw new Error(`missing flag: ${flag}`);
  });
  const sourceRaw = requireValue("--source", map.get("--source"));
  if (!isValidSource(sourceRaw)) throw new Error(`--source must be jra or nar, got ${sourceRaw}`);
  return {
    featuresDate: requireValue("--features-date", map.get("--features-date")),
    featuresJsonl: requireValue("--features-jsonl", map.get("--features-jsonl")),
    modelJson: requireValue("--model-json", map.get("--model-json")),
    source: sourceRaw,
  };
};

const assertFileExists = (path: string): void => {
  if (!existsSync(path)) throw new Error(`file not found: ${path}`);
};

const buildModelKey = (source: string): string => `running-style/models/${source}/latest.json`;
const buildFeaturesKey = (source: string, date: string): string =>
  `running-style/features/${source}/${date}.jsonl`;

const uploadObject = (key: string, filePath: string): void => {
  const result = spawnSync(
    "bunx",
    ["wrangler", "r2", "object", "put", `${BUCKET_NAME}/${key}`, "--file", filePath, "--remote"],
    { stdio: "inherit" },
  );
  if (result.status !== 0) throw new Error(`upload failed for ${key}`);
};

const main = (): void => {
  const args = parseArgs(process.argv.slice(2));
  assertFileExists(args.modelJson);
  assertFileExists(args.featuresJsonl);
  const modelKey = buildModelKey(args.source);
  const featuresKey = buildFeaturesKey(args.source, args.featuresDate);
  const modelBytes = statSync(args.modelJson).size;
  const featureBytes = statSync(args.featuresJsonl).size;
  console.log(`[upload] model ${modelKey} (${modelBytes} bytes)`);
  uploadObject(modelKey, args.modelJson);
  console.log(`[upload] features ${featuresKey} (${featureBytes} bytes)`);
  uploadObject(featuresKey, args.featuresJsonl);
  console.log(JSON.stringify({ featuresKey, modelKey, source: args.source, status: "ok" }));
};

main();
