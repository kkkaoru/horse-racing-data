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
});
