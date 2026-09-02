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

const appRoot = new URL("..", import.meta.url);
const lock = JSON.parse(
  await Bun.file(new URL("private-core.lock.json", appRoot)).text(),
) as CoreLock;
const generatedRoot = new URL("src/generated/jvlink-compatible/", appRoot);
const generatedMetadata = new URL("src/generated/jvlink-artifact.ts", appRoot);
const artifactKey = `artifacts/jvlink-compatible/${lock.coreVersion}/core.wasm`;
const manifestKey = `artifacts/jvlink-compatible/${lock.coreVersion}/manifest.json`;

if (lock.schemaVersion !== 1 || Object.keys(lock.assets).length !== 5)
  throw new Error("Private core lock is invalid");

const sha256 = async (file: URL): Promise<string> => {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", await Bun.file(file).arrayBuffer()),
  );
  return Array.from(digest, (value) => value.toString(16).padStart(2, "0")).join("");
};

await rm(generatedRoot.pathname, { force: true, recursive: true });
await mkdir(generatedRoot.pathname, { recursive: true });
const download = Bun.spawnSync(
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
if (!download.success) throw new Error(`Private core download failed (${download.exitCode})`);

for (const [name, expected] of Object.entries(lock.assets)) {
  const file = new URL(name, generatedRoot);
  if (!(await Bun.file(file).exists()) || (await sha256(file)) !== expected)
    throw new Error(`Private core asset verification failed: ${name}`);
}

const manifestFile = new URL("manifest.json", generatedRoot);
const manifest = JSON.parse(await Bun.file(manifestFile).text()) as ReleaseManifest;
const wasmFile = new URL("jvlink_compatible_bg.wasm", generatedRoot);
if (
  manifest.schemaVersion !== 1 ||
  manifest.coreVersion !== lock.coreVersion ||
  manifest.credentialsEmbedded !== false ||
  manifest.sha256 !== lock.assets["jvlink_compatible_bg.wasm"] ||
  manifest.wasmBytes !== Bun.file(wasmFile).size
)
  throw new Error("Private core release manifest does not match its lock");

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
    `export const JVLINK_ARTIFACT_KEY: string =\n  ${JSON.stringify(artifactKey)};\n` +
    `export const JVLINK_ARTIFACT_SHA256: string =\n  ${JSON.stringify(manifest.sha256)};\n` +
    `export const JVLINK_MANIFEST_KEY: string =\n  ${JSON.stringify(manifestKey)};\n`,
);
console.log(JSON.stringify({ coreVersion: manifest.coreVersion, verifiedAssets: 5 }));
