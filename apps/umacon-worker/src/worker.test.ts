// Run with bun.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AcquisitionError } from "./acquisition";
import type { ArtifactBucket, CompatibilityAttestation } from "./compatibility-attestation";
import type { Env } from "./worker";

const mocks = vi.hoisted(() => ({
  attest: vi.fn(),
  stream: vi.fn(),
}));

vi.mock("./compatibility-attestation", async (importOriginal) => {
  const original = await importOriginal();
  if (typeof original !== "object" || original === null) throw new Error("Invalid test module");
  return { ...original, assertCompatibilityAttestation: mocks.attest };
});
vi.mock("./streaming", () => ({ createDataStream: mocks.stream }));

import worker from "./worker";

const ATTESTATION: CompatibilityAttestation = {
  artifactKey: "artifact",
  coreVersion: "0354-private-core-v1.2",
  sha256: "digest",
  verified: true,
};
const bucket: ArtifactBucket = {
  get: async () => null,
};
const env: Env = {
  NV_CORE_CONFIG_V1: "opaque",
  NVLINK_COMPATIBILITY_ARTIFACTS: bucket,
  UMMACON_WORKER_API_TOKEN: "token",
};
const authorized = (path: string, init?: RequestInit): Request => {
  const headers: Headers = new Headers(init?.headers);
  headers.set("Authorization", "Bearer token");
  return new Request(`https://example.test${path}`, { ...init, headers });
};

beforeEach(() => {
  mocks.attest.mockReset().mockResolvedValue(ATTESTATION);
  mocks.stream.mockReset().mockResolvedValue(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(new TextEncoder().encode('{"event":"close"}\n'));
        controller.close();
      },
    }),
  );
});

describe("UmaConn Worker router", () => {
  it("serves a public health endpoint", async () => {
    const response = await worker.fetch(new Request("https://example.test/health"), env);
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      ok: true,
      runtime: "cloudflare-workers-native-nvdata",
    });
  });

  it("protects all non-health endpoints", async () => {
    const response = await worker.fetch(new Request("https://example.test/compatibility"), env);
    expect(response.status).toBe(401);
  });

  it("reports attested stored-data compatibility", async () => {
    const response = await worker.fetch(authorized("/compatibility"), env);
    expect(response.status).toBe(200);
    const body: unknown = await response.json();
    expect(body).toEqual(
      expect.objectContaining({
        attestation: ATTESTATION,
        deploymentCompatibility: true,
        fullCompatibility: false,
        provider: "UmaConn NV-Link",
      }),
    );
  });

  it("starts a credential-free NDJSON stream", async () => {
    const response = await worker.fetch(
      authorized("/acquire/stream", {
        body: JSON.stringify({ dataSpec: "RACE", fromTime: "20260902000000", option: 1 }),
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toContain("application/x-ndjson");
    expect(await response.text()).toContain("close");
    expect(mocks.stream).toHaveBeenCalledWith("opaque", {
      dataSpec: "RACE",
      fromTime: "20260902000000",
      option: 1,
    });
  });

  it.each([
    ["not-json", "Invalid JSON request"],
    ["null", "Invalid acquisition request"],
    [JSON.stringify({ dataSpec: 1, fromTime: "x", option: 1 }), "Invalid acquisition request"],
    [JSON.stringify({ dataSpec: "RACE", fromTime: 1, option: 1 }), "Invalid acquisition request"],
    [
      JSON.stringify({ dataSpec: "RACE", fromTime: "x", option: 1.5 }),
      "Invalid acquisition request",
    ],
  ])("rejects malformed acquisition bodies", async (body: string, message: string) => {
    const response = await worker.fetch(
      authorized("/acquire/stream", { body, method: "POST" }),
      env,
    );
    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({ error: message });
  });

  it("returns not found for unsupported routes and methods", async () => {
    expect((await worker.fetch(authorized("/unknown"), env)).status).toBe(404);
    expect((await worker.fetch(authorized("/compatibility", { method: "POST" }), env)).status).toBe(
      404,
    );
  });

  it("fails closed when attestation fails", async () => {
    mocks.attest.mockRejectedValueOnce(new Error("bad artifact"));
    const response = await worker.fetch(authorized("/compatibility"), env);
    expect(response.status).toBe(502);
    await expect(response.json()).resolves.toEqual({ error: "NV-Link acquisition failed" });
  });

  it("reports only a credential-safe acquisition stage", async () => {
    mocks.stream.mockRejectedValueOnce(new AcquisitionError("license-fetch", 403, "connect", 123));
    const response = await worker.fetch(
      authorized("/acquire/stream", {
        body: JSON.stringify({ dataSpec: "RACE", fromTime: "20260902000000", option: 1 }),
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(502);
    expect(response.headers.get("X-NV-Link-Failure-Stage")).toBe("license-fetch");
    expect(response.headers.get("X-NV-Link-Upstream-Status")).toBe("403");
    expect(response.headers.get("X-NV-Link-Socket-Stage")).toBe("connect");
    expect(response.headers.get("X-NV-Link-Socket-Bytes")).toBe("123");
  });

  it("omits unavailable upstream diagnostics", async () => {
    mocks.stream.mockRejectedValueOnce(new AcquisitionError("license-build"));
    const response = await worker.fetch(
      authorized("/acquire/stream", {
        body: JSON.stringify({ dataSpec: "RACE", fromTime: "20260902000000", option: 1 }),
        method: "POST",
      }),
      env,
    );
    expect(response.headers.has("X-NV-Link-Upstream-Status")).toBe(false);
    expect(response.headers.has("X-NV-Link-Socket-Stage")).toBe(false);
  });
});
