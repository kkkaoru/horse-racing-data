// Run with bun. Download and verify the pinned private Wasm release before deployment.
import { mkdir, rm } from "node:fs/promises";

interface CoreLock {
  assets: Record<string, string>;
  coreVersion: string;
  releaseTag: string;
  repository: string;
  schemaVersion: 1;
}

interface ReleaseManifest {
  coreVersion: string;
  credentialsEmbedded: false;
  schemaVersion: 1;
  sha256: string;
  wasmBytes: number;
}

const appRoot: URL = new URL("..", import.meta.url);
const lockFile: Bun.BunFile = Bun.file(new URL("private-core.lock.json", appRoot));
const lock: CoreLock = JSON.parse(await lockFile.text());
const generatedRoot: URL = new URL("src/generated/nvlink-compatible/", appRoot);
const generatedMetadata: URL = new URL("src/generated/nvlink-artifact.ts", appRoot);
const artifactKey: string = `artifacts/nvlink-compatible/${lock.coreVersion}/core.wasm`;
const manifestKey: string = `artifacts/nvlink-compatible/${lock.coreVersion}/manifest.json`;
const ASSET_COUNT: number = 5;

if (lock.schemaVersion !== 1 || Object.keys(lock.assets).length !== ASSET_COUNT)
  throw new Error("Private NV-Link core lock is invalid");

const sha256 = async (file: URL): Promise<string> => {
  const digest: Uint8Array = new Uint8Array(
    await crypto.subtle.digest("SHA-256", await Bun.file(file).arrayBuffer()),
  );
  return Array.from(digest, (value: number): string => value.toString(16).padStart(2, "0")).join(
    "",
  );
};

await rm(generatedRoot.pathname, { force: true, recursive: true });
await mkdir(generatedRoot.pathname, { recursive: true });
const download: Bun.SyncSubprocess = Bun.spawnSync(
  [
    "gh",
    "release",
    "download",
    lock.releaseTag,
    "--repo",
    lock.repository,
    "--dir",
    generatedRoot.pathname,
  ],
  { stderr: "inherit", stdout: "inherit" },
);
if (!download.success)
  throw new Error(`Private NV-Link core download failed (${download.exitCode})`);

await Promise.all(
  Object.entries(lock.assets).map(async ([name, expected]: [string, string]): Promise<void> => {
    const file: URL = new URL(name, generatedRoot);
    if (!(await Bun.file(file).exists()) || (await sha256(file)) !== expected)
      throw new Error(`Private NV-Link core asset verification failed: ${name}`);
  }),
);

const manifestFile: URL = new URL("manifest.json", generatedRoot);
const manifest: ReleaseManifest = JSON.parse(await Bun.file(manifestFile).text());
const wasmFile: URL = new URL("nvlink_compatible_bg.wasm", generatedRoot);
if (
  manifest.schemaVersion !== 1 ||
  manifest.coreVersion !== lock.coreVersion ||
  manifest.credentialsEmbedded !== false ||
  manifest.sha256 !== lock.assets["nvlink_compatible_bg.wasm"] ||
  manifest.wasmBytes !== Bun.file(wasmFile).size
)
  throw new Error("Private NV-Link core release manifest does not match its lock");

await Bun.write(
  manifestFile,
  `${JSON.stringify(
    {
      artifactKey,
      coreVersion: manifest.coreVersion,
      credentialsEmbedded: false,
      schemaVersion: 1,
      sha256: manifest.sha256,
    },
    null,
    2,
  )}\n`,
);
await Bun.write(
  generatedMetadata,
  `// Generated from private-core.lock.json. This file contains no credentials.\n\n` +
    `export const NVLINK_ARTIFACT_KEY: string =\n  ${JSON.stringify(artifactKey)};\n` +
    `export const NVLINK_ARTIFACT_SHA256: string =\n  ${JSON.stringify(manifest.sha256)};\n` +
    `export const NVLINK_MANIFEST_KEY: string =\n  ${JSON.stringify(manifestKey)};\n`,
);
console.log(JSON.stringify({ coreVersion: manifest.coreVersion, verifiedAssets: ASSET_COUNT }));
