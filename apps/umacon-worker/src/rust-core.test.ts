// Run with bun.
import { describe, expect, it } from "vitest";
import {
  buildFileListRequestInRust,
  buildLicenseRequestInRust,
  decodeDataArchiveInRust,
  parseCoreFileListJson,
  parseCoreRequestJson,
  parseFileListInRust,
  verifyLicenseResponseInRust,
} from "./rust-core";
import {
  bytes,
  decodeBase64,
  LICENSE_SUCCESS_BASE64,
  opaqueConfig,
  ZIP_BASE64,
} from "./test-fixtures";

describe("private NV-Link core adapter", () => {
  it("builds authorization and file-list requests", () => {
    const license = buildLicenseRequestInRust(opaqueConfig());
    expect(license.userAgent).toBe("UmaConn/3.5.4");
    const licenseUrl: URL = new URL(license.url);
    expect(licenseUrl.protocol).toBe("https:");
    expect(licenseUrl.search.length).toBeGreaterThan(1);
    const list = buildFileListRequestInRust("RACE", "20260902000000", 1);
    const listUrl: URL = new URL(list.url);
    expect(listUrl.protocol).toBe("http:");
    expect(listUrl.searchParams.size).toBe(3);
    expect(list.userAgent).toBe("UmaConn/3.5.4");
  });

  it("verifies authorization and parses validated file metadata", () => {
    expect(() => verifyLicenseResponseInRust(decodeBase64(LICENSE_SUCCESS_BASE64))).not.toThrow();
    expect(() => verifyLicenseResponseInRust(decodeBase64("UkVUVVJOPTQw"))).toThrow();
    expect(parseFileListInRust(bytes("RA20260902.txt,20260902231018,7\r\n"))).toEqual([
      {
        expandedBytes: 7,
        filename: "RA20260902.txt",
        timestamp: "20260902231018",
        url: expect.stringMatching(/^http:\/\/[^/]+\/.*\/RA\/2026\/09\/02\.txt$/),
      },
    ]);
  });

  it("expands an official-shaped ZIP archive", () => {
    expect(decodeDataArchiveInRust(decodeBase64(ZIP_BASE64))).toEqual(bytes("H1abc\r\n"));
  });

  it("rejects invalid private-core JSON boundaries", () => {
    expect(() => parseCoreRequestJson("null")).toThrow("invalid request");
    expect(() => parseCoreRequestJson('{"url":1,"userAgent":"x"}')).toThrow("invalid request");
    expect(() => parseCoreFileListJson("null")).toThrow("invalid file list");
    expect(() => parseCoreFileListJson('[{"filename":"x"}]')).toThrow("invalid file list");
  });
});
