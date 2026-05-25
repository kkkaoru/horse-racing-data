// run with: bun run test
import { expect, it } from "vitest";
import {
  buildUsageText,
  parseUploadRunningStyleModelCliArgs,
} from "./upload-running-style-model";

it("parseUploadRunningStyleModelCliArgs throws when --source is not jra/nar", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs(["--source", "ban-ei", "--input", "tmp/model.flatbin"]),
  ).toThrow("--source must be jra or nar");
});

it("parseUploadRunningStyleModelCliArgs reads --bucket override", () => {
  const args = parseUploadRunningStyleModelCliArgs([
    "--source",
    "jra",
    "--input",
    "tmp/model.flatbin",
    "--bucket",
    "custom-bucket",
  ]);
  expect(args.bucket).toBe("custom-bucket");
});

it("parseUploadRunningStyleModelCliArgs accepts --sync-local and --no-sync-local toggles", () => {
  const enabled = parseUploadRunningStyleModelCliArgs([
    "--source",
    "jra",
    "--input",
    "tmp/model.flatbin",
    "--sync-local",
  ]);
  expect(enabled.syncLocal).toBe(true);
  const disabled = parseUploadRunningStyleModelCliArgs([
    "--source",
    "jra",
    "--input",
    "tmp/model.flatbin",
    "--no-sync-local",
  ]);
  expect(disabled.syncLocal).toBe(false);
});

it("parseUploadRunningStyleModelCliArgs throws on unknown argument", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs([
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--other",
    ]),
  ).toThrow("Unknown argument: --other");
});

it("parseUploadRunningStyleModelCliArgs throws when --input value missing", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs(["--source", "jra", "--input"]),
  ).toThrow("--input requires a value");
});

it("parseUploadRunningStyleModelCliArgs throws when --bucket value missing", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs([
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--bucket",
    ]),
  ).toThrow("--bucket requires a value");
});

it("buildUsageText starts with Usage:", () => {
  const text = buildUsageText();
  expect(text.startsWith("Usage:")).toBe(true);
});

it("parseUploadRunningStyleModelCliArgs throws when --input is missing", () => {
  expect(() => parseUploadRunningStyleModelCliArgs(["--source", "jra"])).toThrow(
    "--input is required",
  );
});

it("parseUploadRunningStyleModelCliArgs throws when --source has no value", () => {
  expect(() => parseUploadRunningStyleModelCliArgs(["--source"])).toThrow(
    "--source requires a value",
  );
});
