import { describe, expect, test } from "vitest";

import { parseUploadRunningStyleModelCliArgs } from "./upload-running-style-model";

describe("parseUploadRunningStyleModelCliArgs", () => {
  test("requires --source and --input", () => {
    expect(() => parseUploadRunningStyleModelCliArgs([])).toThrow(/Usage/);
  });

  test("parses upload flags", () => {
    expect(
      parseUploadRunningStyleModelCliArgs([
        "--source",
        "jra",
        "--input",
        "tmp/jra.json",
        "--remote",
      ]),
    ).toEqual({
      bucket: "pc-keiba-finish-position-models",
      inputPath: "tmp/jra.json",
      remote: true,
      source: "jra",
      syncLocal: true,
    });
  });

  test("parses explicit object keys", () => {
    expect(
      parseUploadRunningStyleModelCliArgs([
        "--source",
        "jra",
        "--input",
        "tmp/jra.flatbin",
        "--object-key",
        "running-style/models/jra/cells/tokyo-turf.flatbin",
      ]),
    ).toEqual({
      bucket: "pc-keiba-finish-position-models",
      inputPath: "tmp/jra.flatbin",
      objectKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
      remote: false,
      source: "jra",
      syncLocal: true,
    });
  });

  test("parses variant ids as cell object keys", () => {
    expect(
      parseUploadRunningStyleModelCliArgs([
        "--source",
        "nar",
        "--input",
        "tmp/nar.flatbin",
        "--variant-id",
        "ooi-dirt",
      ]),
    ).toEqual({
      bucket: "pc-keiba-finish-position-models",
      inputPath: "tmp/nar.flatbin",
      objectKey: "running-style/models/nar/cells/ooi-dirt.flatbin",
      remote: false,
      source: "nar",
      syncLocal: true,
    });
  });
});
