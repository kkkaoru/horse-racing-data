import { describe, expect, test } from "vitest";

import {
  buildRunningStyleLatestModelKey,
  buildWranglerR2GetArgs,
  buildWranglerR2PutArgs,
  listRequiredRunningStyleModelSources,
  parseRegisterModelArg,
  RUNNING_STYLE_MODEL_BUCKET,
} from "../src/running-style-model-register";

describe("running-style model register helpers", () => {
  test("buildRunningStyleLatestModelKey points at latest flatbin", () => {
    expect(buildRunningStyleLatestModelKey("jra")).toBe("running-style/models/jra/latest.flatbin");
    expect(buildRunningStyleLatestModelKey("nar")).toBe("running-style/models/nar/latest.flatbin");
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
