// Run with bun.
import { describe, expect, it, vi } from "vitest";
import { encodeBase64, createDataStream } from "./streaming";
import type { NvFetch } from "./acquisition";
import { decodeBase64, LICENSE_SUCCESS_BASE64, opaqueConfig, ZIP_BASE64 } from "./test-fixtures";

const readStream = async (stream: ReadableStream<Uint8Array>): Promise<string> =>
  new Response(stream).text();

describe("NV-Link NDJSON streaming", () => {
  it("streams open, file, record, and close events", async () => {
    const fetcher: NvFetch = vi
      .fn<NvFetch>()
      .mockResolvedValueOnce(new Response(decodeBase64(LICENSE_SUCCESS_BASE64)))
      .mockResolvedValueOnce(new Response("RA20260902.txt,20260902231018,7\r\n"))
      .mockResolvedValueOnce(new Response(decodeBase64(ZIP_BASE64)));
    const stream = await createDataStream(
      opaqueConfig(),
      { dataSpec: "RACE", fromTime: "20260902000000", option: 1 },
      fetcher,
    );
    const lines: string[] = (await readStream(stream)).trim().split("\n");
    expect(lines.map((entry: string): unknown => JSON.parse(entry))).toEqual([
      {
        event: "open",
        files: 1,
        transitions: ["license-authorized", "file-list-received"],
      },
      {
        archiveBytes: decodeBase64(ZIP_BASE64).length,
        decodedBytes: 7,
        event: "file",
        filename: "RA20260902.txt",
        records: 1,
      },
      {
        bytes: 7,
        data: "SDFhYmMNCg==",
        encoding: "base64",
        event: "record",
        filename: "RA20260902.txt",
      },
      {
        event: "close",
        files: 1,
        records: 1,
        transitions: ["license-authorized", "file-list-received", "payload-decoded"],
      },
    ]);
  });

  it("propagates archive failures through the stream", async () => {
    const fetcher: NvFetch = vi
      .fn<NvFetch>()
      .mockResolvedValueOnce(new Response(decodeBase64(LICENSE_SUCCESS_BASE64)))
      .mockResolvedValueOnce(new Response("RA20260902.txt,20260902231018,7\r\n"))
      .mockResolvedValueOnce(new Response("not-zip"));
    const stream = await createDataStream(
      opaqueConfig(),
      { dataSpec: "RACE", fromTime: "20260902000000", option: 1 },
      fetcher,
    );
    await expect(readStream(stream)).rejects.toThrow();
  });

  it("base64-encodes data across bounded chunks", () => {
    const value: Uint8Array = new Uint8Array(9000).fill(65);
    expect(atob(encodeBase64(value))).toHaveLength(value.length);
  });
});
