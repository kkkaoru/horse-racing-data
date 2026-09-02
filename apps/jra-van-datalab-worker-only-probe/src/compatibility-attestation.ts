// R2 stores a credential-free provenance copy. Workers execute the statically bundled Wasm module.

import {
  JVLINK_ARTIFACT_KEY,
  JVLINK_ARTIFACT_SHA256,
  JVLINK_MANIFEST_KEY,
} from "./generated/jvlink-artifact";
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

const MAX_MANIFEST_BYTES: number = 4096;
const MAX_WASM_BYTES: number = 2 * 1024 * 1024;
const isManifest = (value: unknown): value is CompatibilityManifest => {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<CompatibilityManifest>;
  return (
    candidate.artifactKey === JVLINK_ARTIFACT_KEY &&
    candidate.coreVersion === RUST_CORE_VERSION &&
    candidate.credentialsEmbedded === false &&
    candidate.schemaVersion === 1 &&
    candidate.sha256 === JVLINK_ARTIFACT_SHA256
  );
};

const sha256 = async (bytes: ArrayBuffer): Promise<string> => {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(digest, (value) => value.toString(16).padStart(2, "0")).join("");
};

export const assertCompatibilityAttestation = async (
  bucket: Pick<R2Bucket, "get">,
): Promise<CompatibilityAttestation> => {
  const manifestObject = await bucket.get(JVLINK_MANIFEST_KEY);
  if (
    manifestObject === null ||
    manifestObject.size < 1 ||
    manifestObject.size > MAX_MANIFEST_BYTES
  )
    throw new Error("JV-Link compatibility manifest is unavailable");
  let parsed: unknown;
  try {
    parsed = JSON.parse(await manifestObject.text());
  } catch {
    throw new Error("JV-Link compatibility manifest is invalid");
  }
  if (!isManifest(parsed)) throw new Error("JV-Link compatibility manifest does not match build");

  const artifact = await bucket.get(parsed.artifactKey);
  if (artifact === null || artifact.size < 8 || artifact.size > MAX_WASM_BYTES)
    throw new Error("JV-Link compatibility Wasm artifact is unavailable");
  const bytes = await artifact.arrayBuffer();
  if ((await sha256(bytes)) !== parsed.sha256)
    throw new Error("JV-Link compatibility Wasm digest does not match manifest");
  return {
    artifactKey: parsed.artifactKey,
    coreVersion: parsed.coreVersion,
    sha256: parsed.sha256,
    verified: true,
  };
};
