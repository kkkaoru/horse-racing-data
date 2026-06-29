// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  buildUsageText,
  parseUploadRunningStyleModelCliArgs,
  run,
} from "./upload-running-style-model";

vi.mock("../src/running-style-model-register", async () => {
  const actual = await vi.importActual<typeof import("../src/running-style-model-register")>(
    "../src/running-style-model-register",
  );
  return {
    ...actual,
    registerRunningStyleModel: vi.fn(async () => ({
      objectKey: "running-style/models/jra/latest.flatbin",
      sizeBytes: 1024,
    })),
    syncRunningStyleModel: vi.fn(async () => undefined),
  };
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

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

it("parseUploadRunningStyleModelCliArgs rejects object keys for another source", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs([
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--object-key",
      "running-style/models/nar/cells/ooi-dirt.flatbin",
    ]),
  ).toThrow("must start with running-style/models/jra/");
});

it("parseUploadRunningStyleModelCliArgs rejects object-key and variant-id together", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs([
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--object-key",
      "running-style/models/jra/cells/tokyo-turf.flatbin",
      "--variant-id",
      "tokyo-turf",
    ]),
  ).toThrow("Use either --object-key or --variant-id");
});

it("parseUploadRunningStyleModelCliArgs throws when --object-key value missing", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs([
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--object-key",
    ]),
  ).toThrow("--object-key requires a value");
});

it("parseUploadRunningStyleModelCliArgs throws when --variant-id value missing", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs([
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--variant-id",
    ]),
  ).toThrow("--variant-id requires a value");
});

it("parseUploadRunningStyleModelCliArgs rejects path-like variant ids", () => {
  expect(() =>
    parseUploadRunningStyleModelCliArgs([
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--variant-id",
      "tokyo/turf",
    ]),
  ).toThrow("--variant-id");
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
  expect(() => parseUploadRunningStyleModelCliArgs(["--source", "jra", "--input"])).toThrow(
    "--input requires a value",
  );
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

it("run uploads the model and logs the result", async () => {
  vi.stubGlobal("process", {
    ...process,
    argv: ["bun", "scripts/upload.ts", "--source", "jra", "--input", "tmp/model.json"],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await run();
  const { registerRunningStyleModel } = await import("../src/running-style-model-register");
  expect(registerRunningStyleModel).toHaveBeenCalledTimes(1);
});

it("run syncs the local bucket when --remote and --sync-local are set", async () => {
  vi.stubGlobal("process", {
    ...process,
    argv: [
      "bun",
      "scripts/upload.ts",
      "--source",
      "jra",
      "--input",
      "tmp/model.json",
      "--remote",
      "--sync-local",
    ],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await run();
  const { syncRunningStyleModel } = await import("../src/running-style-model-register");
  expect(syncRunningStyleModel).toHaveBeenCalledTimes(1);
});

it("run skips sync when --remote is set but --no-sync-local disables sync", async () => {
  vi.stubGlobal("process", {
    ...process,
    argv: [
      "bun",
      "scripts/upload.ts",
      "--source",
      "jra",
      "--input",
      "tmp/model.json",
      "--remote",
      "--no-sync-local",
    ],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { syncRunningStyleModel } = await import("../src/running-style-model-register");
  vi.mocked(syncRunningStyleModel).mockClear();
  await run();
  expect(syncRunningStyleModel).not.toHaveBeenCalled();
});

it("run uploads and syncs an explicit objectKey", async () => {
  vi.stubGlobal("process", {
    ...process,
    argv: [
      "bun",
      "scripts/upload.ts",
      "--source",
      "jra",
      "--input",
      "tmp/model.flatbin",
      "--object-key",
      "running-style/models/jra/cells/tokyo-turf.flatbin",
      "--remote",
    ],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { registerRunningStyleModel, syncRunningStyleModel } =
    await import("../src/running-style-model-register");
  vi.mocked(registerRunningStyleModel).mockResolvedValueOnce({
    objectKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
    sizeBytes: 2048,
  });
  vi.mocked(syncRunningStyleModel).mockClear();
  await run();
  expect(registerRunningStyleModel).toHaveBeenCalledWith(
    {
      inputPath: "tmp/model.flatbin",
      objectKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
      remote: true,
      source: "jra",
    },
    { bucket: "pc-keiba-finish-position-models" },
  );
  expect(syncRunningStyleModel).toHaveBeenCalledWith("jra", {
    bucket: "pc-keiba-finish-position-models",
    objectKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
  });
});

it("parseUploadRunningStyleModelCliArgs prints usage and exits on --help", () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const exitSpy = vi.spyOn(process, "exit").mockImplementation(((_code?: number): never => {
    throw new Error("process.exit called");
  }) as never);
  expect(() => parseUploadRunningStyleModelCliArgs(["--help"])).toThrow("process.exit called");
  expect(logSpy).toHaveBeenCalledTimes(1);
  expect(exitSpy).toHaveBeenCalledWith(0);
});

it("parseUploadRunningStyleModelCliArgs prints usage and exits on -h", () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const exitSpy = vi.spyOn(process, "exit").mockImplementation(((_code?: number): never => {
    throw new Error("process.exit called");
  }) as never);
  expect(() => parseUploadRunningStyleModelCliArgs(["-h"])).toThrow("process.exit called");
  expect(logSpy).toHaveBeenCalledTimes(1);
  expect(exitSpy).toHaveBeenCalledWith(0);
});
