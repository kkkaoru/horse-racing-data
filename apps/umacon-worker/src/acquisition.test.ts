// Run with bun.
import { describe, expect, it, vi } from "vitest";
import {
  authorizeAcquisition,
  downloadFile,
  fetchBounded,
  openAcquisition,
  splitRecords,
  type NvFetch,
} from "./acquisition";
import {
  bytes,
  decodeBase64,
  LICENSE_SUCCESS_BASE64,
  opaqueConfig,
  ZIP_BASE64,
} from "./test-fixtures";

const response = (body: BodyInit, status: number = 200, headers?: HeadersInit): Response =>
  new Response(body, { headers, status });

describe("NV-Link acquisition", () => {
  it("authorizes and opens an ordered file list", async () => {
    const fetcher: NvFetch = vi
      .fn<NvFetch>()
      .mockResolvedValueOnce(response(decodeBase64(LICENSE_SUCCESS_BASE64)))
      .mockResolvedValueOnce(response("RA20260902.txt,20260902231018,7\r\n"));
    const opened = await openAcquisition(
      opaqueConfig(),
      { dataSpec: "RACE", fromTime: "20260902000000", option: 1 },
      fetcher,
    );
    expect(opened.entries).toHaveLength(1);
    expect(opened.transitions).toEqual(["license-authorized", "file-list-received"]);
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(fetcher).toHaveBeenNthCalledWith(1, expect.any(String), {
      headers: { "User-Agent": "UmaConn/3.5.4" },
      redirect: "error",
    });
  });

  it("downloads and expands a listed archive", async () => {
    const fetcher: NvFetch = vi.fn<NvFetch>().mockResolvedValue(response(decodeBase64(ZIP_BASE64)));
    const file = await downloadFile(
      {
        expandedBytes: 7,
        filename: "RA20260902.txt",
        timestamp: "20260902231018",
        url: "http://example.test/archive",
      },
      fetcher,
    );
    expect(file.archiveBytes).toBeGreaterThan(8);
    expect(file.decoded).toEqual(bytes("H1abc\r\n"));
  });

  it("rejects unauthorized and invalid upstream responses", async () => {
    const denied: NvFetch = vi.fn<NvFetch>().mockResolvedValue(response("no", 403));
    await expect(authorizeAcquisition(opaqueConfig(), denied)).rejects.toMatchObject({
      stage: "license-fetch",
      upstreamStatus: 403,
    });

    const declared: NvFetch = vi
      .fn<NvFetch>()
      .mockResolvedValue(response("x", 200, { "Content-Length": "99" }));
    await expect(
      fetchBounded({
        fetcher: declared,
        maxBytes: 8,
        request: { url: "http://example.test", userAgent: "test" },
      }),
    ).rejects.toThrow("too large");

    const actual: NvFetch = vi.fn<NvFetch>().mockResolvedValue(response("123456789"));
    await expect(
      fetchBounded({
        fetcher: actual,
        maxBytes: 8,
        request: { url: "http://example.test", userAgent: "test" },
      }),
    ).rejects.toThrow("exceeded");
  });

  it("splits only complete CRLF records", () => {
    expect(splitRecords(bytes("A\r\nB\r\n"))).toEqual([bytes("A\r\n"), bytes("B\r\n")]);
    expect(() => splitRecords(bytes("A"))).toThrow("CRLF");
    expect(splitRecords(new Uint8Array())).toEqual([]);
  });
});
