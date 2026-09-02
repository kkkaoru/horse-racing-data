// Run with bun.
import { expect, it, vi } from "vitest";
import { assertCompatibilityAttestation } from "./compatibility-attestation";

const EXPECTED_DIGEST: string = "859951814a4353e7dd51ccbc6cc02e511f2a1244aec9f452cd6cc8f48f12dc7c";
const MANIFEST_KEY: string = "artifacts/jvlink-compatible/0500-private-core-v2/manifest.json";
const ARTIFACT_KEY: string = "artifacts/jvlink-compatible/0500-private-core-v2/core.wasm";
const VALID_MANIFEST: string = JSON.stringify({
  artifactKey: ARTIFACT_KEY,
  coreVersion: "0500-private-core-v2",
  credentialsEmbedded: false,
  schemaVersion: 1,
  sha256: EXPECTED_DIGEST,
});

const object = (body: string | Uint8Array, size?: number): R2ObjectBody => {
  const bytes = typeof body === "string" ? new TextEncoder().encode(body) : body;
  return {
    arrayBuffer: async () =>
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
    size: size ?? bytes.length,
    text: async () => new TextDecoder().decode(bytes),
  } as R2ObjectBody;
};

it("attests the R2 provenance copy against the statically bundled Rust Wasm", async () => {
  const digest = Uint8Array.from(EXPECTED_DIGEST.match(/.{2}/g)!, (value) =>
    Number.parseInt(value, 16),
  );
  const digestSpy = vi.spyOn(crypto.subtle, "digest").mockResolvedValue(digest.buffer);
  const get = vi.fn(async (key: string) =>
    key === MANIFEST_KEY ? object(VALID_MANIFEST) : object(new Uint8Array(8)),
  );
  await expect(assertCompatibilityAttestation({ get })).resolves.toStrictEqual({
    artifactKey: "artifacts/jvlink-compatible/0500-private-core-v2/core.wasm",
    coreVersion: "0500-private-core-v2",
    sha256: "859951814a4353e7dd51ccbc6cc02e511f2a1244aec9f452cd6cc8f48f12dc7c",
    verified: true,
  });
  expect(get).toHaveBeenNthCalledWith(1, MANIFEST_KEY);
  expect(get).toHaveBeenNthCalledWith(2, ARTIFACT_KEY);
  digestSpy.mockRestore();
});

it.each([null, object("", 0), object("x", 4097)])(
  "rejects an unavailable or unbounded manifest",
  async (manifest) => {
    const get = vi.fn(async () => manifest);
    await expect(assertCompatibilityAttestation({ get })).rejects.toThrow(
      "manifest is unavailable",
    );
  },
);

it("rejects malformed manifest JSON", async () => {
  const get = vi.fn(async () => object("not-json"));
  await expect(assertCompatibilityAttestation({ get })).rejects.toThrow("manifest is invalid");
});

it.each(["null", "42"])("rejects a non-object manifest", async (body) => {
  const get = vi.fn(async () => object(body));
  await expect(assertCompatibilityAttestation({ get })).rejects.toThrow("does not match build");
});

it.each([
  { artifactKey: undefined },
  { artifactKey: "wrong" },
  { coreVersion: "wrong" },
  { credentialsEmbedded: true },
  { schemaVersion: 2 },
  { sha256: "0".repeat(64) },
  { sha256: "not-a-digest" },
])("rejects a manifest that differs from the deployed build", async (override) => {
  const get = vi.fn(async () =>
    object(JSON.stringify({ ...JSON.parse(VALID_MANIFEST), ...override })),
  );
  await expect(assertCompatibilityAttestation({ get })).rejects.toThrow("does not match build");
});

it.each([null, object(new Uint8Array(7)), object(new Uint8Array(8), 2 * 1024 * 1024 + 1)])(
  "rejects an unavailable or unbounded Wasm artifact",
  async (artifact) => {
    const get = vi.fn(async (key: string) =>
      key === MANIFEST_KEY ? object(VALID_MANIFEST) : artifact,
    );
    await expect(assertCompatibilityAttestation({ get })).rejects.toThrow(
      "artifact is unavailable",
    );
  },
);

it("rejects a Wasm artifact whose digest differs", async () => {
  const get = vi.fn(async (key: string) =>
    key === MANIFEST_KEY ? object(VALID_MANIFEST) : object(new Uint8Array(8)),
  );
  await expect(assertCompatibilityAttestation({ get })).rejects.toThrow(
    "digest does not match manifest",
  );
});
