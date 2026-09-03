// Run with bun.
import { describe, expect, it, vi } from "vitest";
import {
  assertCompatibilityAttestation,
  type ArtifactBucket,
  type ArtifactObject,
} from "./compatibility-attestation";

const EXPECTED_DIGEST: string = "c24baea58f8d2fd32c7a094ee24d1223f599e5a715b67fd919a241f59458423e";
const MANIFEST_KEY: string = "artifacts/nvlink-compatible/0354-private-core-v1.2/manifest.json";
const ARTIFACT_KEY: string = "artifacts/nvlink-compatible/0354-private-core-v1.2/core.wasm";
const VALID_MANIFEST: string = JSON.stringify({
  artifactKey: ARTIFACT_KEY,
  coreVersion: "0354-private-core-v1.2",
  credentialsEmbedded: false,
  schemaVersion: 1,
  sha256: EXPECTED_DIGEST,
});

const object = (body: string | Uint8Array, size?: number): ArtifactObject => {
  const value: Uint8Array = typeof body === "string" ? new TextEncoder().encode(body) : body;
  return {
    arrayBuffer: async (): Promise<ArrayBuffer> => value.slice().buffer,
    size: size ?? value.length,
    text: async (): Promise<string> => new TextDecoder().decode(value),
  };
};

const bucket = (get: ArtifactBucket["get"]): ArtifactBucket => ({ get });

describe("compatibility attestation", () => {
  it("attests R2 provenance against the bundled core", async () => {
    const digest: Uint8Array<ArrayBuffer> = new Uint8Array(EXPECTED_DIGEST.length / 2);
    digest.set(
      Array.from({ length: EXPECTED_DIGEST.length / 2 }, (_: unknown, index: number): number =>
        Number.parseInt(EXPECTED_DIGEST.slice(index * 2, index * 2 + 2), 16),
      ),
    );
    const digestSpy = vi.spyOn(crypto.subtle, "digest").mockResolvedValue(digest.buffer);
    const get = vi.fn(
      async (key: string): Promise<ArtifactObject> =>
        key === MANIFEST_KEY ? object(VALID_MANIFEST) : object(new Uint8Array(8)),
    );
    await expect(assertCompatibilityAttestation(bucket(get))).resolves.toEqual({
      artifactKey: ARTIFACT_KEY,
      coreVersion: "0354-private-core-v1.2",
      sha256: EXPECTED_DIGEST,
      verified: true,
    });
    expect(get).toHaveBeenNthCalledWith(1, MANIFEST_KEY);
    expect(get).toHaveBeenNthCalledWith(2, ARTIFACT_KEY);
    digestSpy.mockRestore();
  });

  it.each([null, object("", 0), object("x", 4097)])(
    "rejects an unavailable manifest",
    async (manifest) => {
      await expect(
        assertCompatibilityAttestation(
          bucket(async (): Promise<ArtifactObject | null> => manifest),
        ),
      ).rejects.toThrow("manifest is unavailable");
    },
  );

  it("rejects malformed manifest JSON", async () => {
    await expect(
      assertCompatibilityAttestation(
        bucket(async (): Promise<ArtifactObject> => object("not-json")),
      ),
    ).rejects.toThrow("manifest is invalid");
  });

  it.each(["null", "42", "{}"])("rejects mismatched manifests", async (body: string) => {
    await expect(
      assertCompatibilityAttestation(bucket(async (): Promise<ArtifactObject> => object(body))),
    ).rejects.toThrow("does not match build");
  });

  it.each([null, object(new Uint8Array(7)), object(new Uint8Array(8), 2 * 1024 * 1024 + 1)])(
    "rejects an unavailable artifact",
    async (artifact) => {
      const get = async (key: string): Promise<ArtifactObject | null> =>
        key === MANIFEST_KEY ? object(VALID_MANIFEST) : artifact;
      await expect(assertCompatibilityAttestation(bucket(get))).rejects.toThrow(
        "artifact is unavailable",
      );
    },
  );

  it("rejects a mismatched artifact digest", async () => {
    const get = async (key: string): Promise<ArtifactObject> =>
      key === MANIFEST_KEY ? object(VALID_MANIFEST) : object(new Uint8Array(8));
    await expect(assertCompatibilityAttestation(bucket(get))).rejects.toThrow(
      "digest does not match",
    );
  });
});
