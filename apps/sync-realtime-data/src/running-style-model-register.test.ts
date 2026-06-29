import { describe, expect, test } from "vitest";

import {
  buildRunningStyleCellModelKey,
  buildRunningStyleLatestModelKey,
  buildWranglerR2GetArgs,
  buildWranglerR2PutArgs,
  listRequiredRunningStyleModelSources,
  parseRegisterModelArg,
  resolveRunningStyleModelObjectKey,
  RUNNING_STYLE_MODEL_BUCKET,
  validateRunningStyleModelObjectKey,
} from "../src/running-style-model-register";

describe("running-style model register helpers", () => {
  test("buildRunningStyleLatestModelKey points at latest flatbin", () => {
    expect(buildRunningStyleLatestModelKey("jra")).toBe("running-style/models/jra/latest.flatbin");
    expect(buildRunningStyleLatestModelKey("nar")).toBe("running-style/models/nar/latest.flatbin");
  });

  test("buildRunningStyleCellModelKey points at a cells flatbin", () => {
    expect(buildRunningStyleCellModelKey("jra", "tokyo-turf")).toBe(
      "running-style/models/jra/cells/tokyo-turf.flatbin",
    );
  });

  test("buildRunningStyleCellModelKey rejects path-like variant ids", () => {
    expect(() => buildRunningStyleCellModelKey("jra", "../tokyo")).toThrow(/variant-id/);
  });

  test("buildRunningStyleCellModelKey rejects ids with flatbin suffix", () => {
    expect(() => buildRunningStyleCellModelKey("jra", "tokyo.flatbin")).toThrow(/variant-id/);
  });

  test("validateRunningStyleModelObjectKey accepts source-scoped flatbin keys", () => {
    expect(
      validateRunningStyleModelObjectKey("nar", "running-style/models/nar/cells/ooi-dirt.flatbin"),
    ).toBe("running-style/models/nar/cells/ooi-dirt.flatbin");
  });

  test("validateRunningStyleModelObjectKey rejects keys outside the source prefix", () => {
    expect(() =>
      validateRunningStyleModelObjectKey("jra", "running-style/models/nar/cells/ooi.flatbin"),
    ).toThrow(/start with/);
  });

  test("validateRunningStyleModelObjectKey rejects non-flatbin keys", () => {
    expect(() =>
      validateRunningStyleModelObjectKey("jra", "running-style/models/jra/cells/tokyo.bin"),
    ).toThrow(/flatbin/);
  });

  test("validateRunningStyleModelObjectKey rejects empty path segments", () => {
    expect(() =>
      validateRunningStyleModelObjectKey("jra", "running-style/models/jra//tokyo.flatbin"),
    ).toThrow(/empty path/);
  });

  test("validateRunningStyleModelObjectKey rejects backslashes", () => {
    expect(() =>
      validateRunningStyleModelObjectKey("jra", "running-style/models/jra/cells\\tokyo.flatbin"),
    ).toThrow(/backslashes/);
  });

  test("validateRunningStyleModelObjectKey rejects traversal segments", () => {
    expect(() =>
      validateRunningStyleModelObjectKey("jra", "running-style/models/jra/../nar.flatbin"),
    ).toThrow(/traversal/);
  });

  test("validateRunningStyleModelObjectKey rejects current-directory segments", () => {
    expect(() =>
      validateRunningStyleModelObjectKey("jra", "running-style/models/jra/./tokyo.flatbin"),
    ).toThrow(/traversal/);
  });

  test("resolveRunningStyleModelObjectKey defaults to latest", () => {
    expect(resolveRunningStyleModelObjectKey({ source: "jra" })).toBe(
      "running-style/models/jra/latest.flatbin",
    );
  });

  test("resolveRunningStyleModelObjectKey validates explicit keys", () => {
    expect(
      resolveRunningStyleModelObjectKey({
        objectKey: "running-style/models/jra/cells/tokyo.flatbin",
        source: "jra",
      }),
    ).toBe("running-style/models/jra/cells/tokyo.flatbin");
  });

  test("parseRegisterModelArg accepts source:path", () => {
    expect(parseRegisterModelArg("jra:tmp/model.json")).toEqual({
      inputPath: "tmp/model.json",
      remote: false,
      source: "jra",
    });
  });

  test("parseRegisterModelArg rejects invalid source", () => {
    expect(() => parseRegisterModelArg("foo:tmp/model.json")).toThrow(/jra or nar/);
  });

  test("listRequiredRunningStyleModelSources deduplicates race sources", () => {
    expect(
      listRequiredRunningStyleModelSources([
        { source: "nar" },
        { source: "jra" },
        { source: "nar" },
      ]),
    ).toEqual(["jra", "nar"]);
  });

  test("buildWranglerR2PutArgs adds --remote when requested", () => {
    expect(
      buildWranglerR2PutArgs(
        RUNNING_STYLE_MODEL_BUCKET,
        "running-style/models/jra/latest.flatbin",
        "tmp/model.flatbin",
        true,
      ),
    ).toContain("--remote");
  });

  test("buildWranglerR2GetArgs can fetch from remote bucket", () => {
    expect(
      buildWranglerR2GetArgs(
        RUNNING_STYLE_MODEL_BUCKET,
        "running-style/models/nar/latest.flatbin",
        "tmp/model.flatbin",
        true,
      ),
    ).toEqual([
      "wrangler",
      "r2",
      "object",
      "get",
      "pc-keiba-finish-position-models/running-style/models/nar/latest.flatbin",
      "--file",
      "tmp/model.flatbin",
      "--remote",
    ]);
  });
});
