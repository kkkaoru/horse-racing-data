// R2 stores a credential-free provenance copy. Workers execute the statically bundled Wasm module.

import {
  NVLINK_ARTIFACT_KEY,
  NVLINK_ARTIFACT_SHA256,
  NVLINK_MANIFEST_KEY,
} from "./generated/nvlink-artifact";
import { RUST_CORE_VERSION } from "./rust-core";

interface CompatibilityManifest {
  artifactKey: string;
  coreVersion: string;
  credentialsEmbedded: false;
  schemaVersion: 1;
  sha256: string;
}

export interface CompatibilityAttestation {
  artifactKey: string;
  coreVersion: string;
  sha256: string;
  verified: true;
}

export interface ArtifactObject {
  arrayBuffer(): Promise<ArrayBuffer>;
  readonly size: number;
  text(): Promise<string>;
}

export interface ArtifactBucket {
  get(key: string): Promise<ArtifactObject | null>;
}

const MAX_MANIFEST_BYTES: number = 4096;
const MAX_WASM_BYTES: number = 2 * 1024 * 1024;

const isManifest = (value: unknown): value is CompatibilityManifest => {
  if (typeof value !== "object" || value === null) return false;
  return (
    "artifactKey" in value &&
    "coreVersion" in value &&
    "credentialsEmbedded" in value &&
    "schemaVersion" in value &&
    "sha256" in value &&
    value.artifactKey === NVLINK_ARTIFACT_KEY &&
    value.coreVersion === RUST_CORE_VERSION &&
    value.credentialsEmbedded === false &&
    value.schemaVersion === 1 &&
    value.sha256 === NVLINK_ARTIFACT_SHA256
  );
};

const sha256 = async (bytes: ArrayBuffer): Promise<string> => {
  const digest: Uint8Array = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(digest, (value: number): string => value.toString(16).padStart(2, "0")).join(
    "",
  );
};

export const assertCompatibilityAttestation = async (
  bucket: ArtifactBucket,
): Promise<CompatibilityAttestation> => {
  const manifestObject: ArtifactObject | null = await bucket.get(NVLINK_MANIFEST_KEY);
  if (
    manifestObject === null ||
    manifestObject.size < 1 ||
    manifestObject.size > MAX_MANIFEST_BYTES
  )
    throw new Error("NV-Link compatibility manifest is unavailable");
  let parsed: unknown;
  try {
    parsed = JSON.parse(await manifestObject.text());
  } catch {
    throw new Error("NV-Link compatibility manifest is invalid");
  }
  if (!isManifest(parsed)) throw new Error("NV-Link compatibility manifest does not match build");

  const artifact: ArtifactObject | null = await bucket.get(parsed.artifactKey);
  if (artifact === null || artifact.size < 8 || artifact.size > MAX_WASM_BYTES)
    throw new Error("NV-Link compatibility Wasm artifact is unavailable");
  const bytes: ArrayBuffer = await artifact.arrayBuffer();
  if ((await sha256(bytes)) !== parsed.sha256)
    throw new Error("NV-Link compatibility Wasm digest does not match manifest");
  return {
    artifactKey: parsed.artifactKey,
    coreVersion: parsed.coreVersion,
    sha256: parsed.sha256,
    verified: true,
  };
};
