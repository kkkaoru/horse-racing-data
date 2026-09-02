// Run with bun.
import { describe, expect, it, vi } from "vitest";
import {
  buildBootstrapBody,
  buildFileListBody,
  decodeBootstrapResponse,
  decodeFileListResponse,
  deriveDownloadPath,
  randomCredentialSeeds,
} from "./protocol";

const CORE_CONFIG = "AQACQUIAAzEyMwAOMTIzNDU2Nzg5MDEyMzQ=";
const SEEDS: readonly [number, number] = [0x12, 0x34];
const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);

const compressedEnvelope = async (application: string, payload: string): Promise<Uint8Array> => {
  const source = new ReadableStream<Uint8Array>({
    start(controller): void {
      controller.enqueue(bytes(payload));
      controller.close();
    },
  });
  const compressed = new Uint8Array(
    await new Response(source.pipeThrough(new CompressionStream("deflate"))).arrayBuffer(),
  );
  const header = bytes(`0200${application}00005001\r\n`);
  const result = new Uint8Array(header.length + compressed.length);
  result.set(header);
  result.set(compressed, header.length);
  return result;
};

describe("private core adapter", () => {
  it("uses two cryptographically generated session bytes", () => {
    const spy = vi.spyOn(crypto, "getRandomValues").mockImplementation((array) => {
      const generated = array as Uint8Array;
      generated.set([7, 9]);
      return array;
    });
    expect(randomCredentialSeeds()).toEqual([7, 9]);
    spy.mockRestore();
  });

  it("builds non-secret bootstrap and file-list payloads", () => {
    const bootstrap = buildBootstrapBody(CORE_CONFIG, SEEDS);
    const defaultOption = buildFileListBody(
      CORE_CONFIG,
      { dataSpec: "RACE", from: "20260829000000", to: "20260830235959" },
      SEEDS,
    );
    const explicitOption = buildFileListBody(
      CORE_CONFIG,
      { dataSpec: "RACE", from: "20260829000000", option: 4, to: "20260830235959" },
      SEEDS,
    );
    expect(bootstrap.length).toBeGreaterThan(0);
    expect(defaultOption.length).toBeGreaterThan(0);
    expect(explicitOption.length).toBeGreaterThan(0);
    expect(bootstrap).not.toContain(CORE_CONFIG);
  });

  it("rejects an invalid opaque configuration", () => {
    expect(() => buildBootstrapBody("invalid", [0, 0])).toThrow("Core configuration is invalid");
  });

  it("decodes opaque bootstrap and file-list responses in the private core", async () => {
    await expect(decodeBootstrapResponse(bytes("0200100700005000\r\n0\r\n\r\n"))).resolves.toEqual({
      payFlag: 0,
      status: 0,
    });
    await expect(
      decodeFileListResponse(await compressedEnvelope("1002", "RM1\r\nRT2\r\n")),
    ).resolves.toMatchObject({ files: [], rm: 1, rt: 2, status: 0 });
  });

  it("delegates bounded download path validation", () => {
    expect(deriveDownloadPath("JGAA00000000000000000000.jvd")).toBe(
      "/datalab/JGAA/JGAA00000000000000000000.jvd",
    );
    expect(() => deriveDownloadPath("../secret.jvd")).toThrow("unsafe");
  });
});
