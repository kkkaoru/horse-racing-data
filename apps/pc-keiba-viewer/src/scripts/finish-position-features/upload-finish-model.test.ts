import { expect, test } from "vitest";

import {
  buildMetadataObjectKey,
  buildModelObjectKey,
  buildObjectKey,
  buildUploadSteps,
  buildUsageText,
  buildWranglerArgs,
  DEFAULT_BUCKET,
  initialOptions,
  isJsonModel,
  isUploadableCategory,
  modelFileName,
  parseArgs,
  runUpload,
  type UploadOptions,
  type WranglerSpawner,
} from "./upload-finish-model";

test("DEFAULT_BUCKET points at the finish-position bucket", () => {
  expect(DEFAULT_BUCKET).toBe("pc-keiba-finish-position-models");
});

test("isUploadableCategory accepts JRA / NAR / ban-ei", () => {
  expect(isUploadableCategory("jra")).toBe(true);
  expect(isUploadableCategory("nar")).toBe(true);
  expect(isUploadableCategory("ban-ei")).toBe(true);
});

test("isUploadableCategory rejects 'all' because uploads are per-category", () => {
  expect(isUploadableCategory("all")).toBe(false);
});

test("buildUsageText documents the CLI", () => {
  expect(buildUsageText()).toContain("--model-version");
  expect(buildUsageText()).toContain("--remote");
});

test("initialOptions defaults to local (non-remote) jra upload", () => {
  expect(initialOptions()).toStrictEqual({
    bucket: "pc-keiba-finish-position-models",
    category: "jra",
    metadataPath: null,
    modelPath: "",
    modelVersion: "",
    remote: false,
  });
});

test("buildObjectKey lays out objects under finish-position", () => {
  expect(buildObjectKey("jra", "v1", "model.lgb")).toBe("finish-position/jra/v1/model.lgb");
});

test("buildModelObjectKey mirrors the lgb model path basename", () => {
  expect(buildModelObjectKey("nar", "v2", "tmp/models/x/model.lgb")).toBe(
    "finish-position/nar/v2/model.lgb",
  );
});

test("buildModelObjectKey mirrors the json model path basename", () => {
  expect(buildModelObjectKey("jra", "v7", "tmp/models/jra/model.json")).toBe(
    "finish-position/jra/v7/model.json",
  );
});

test("modelFileName extracts the basename from a nested path", () => {
  expect(modelFileName("tmp/models/jra-cb/model.json")).toBe("model.json");
});

test("isJsonModel is true for a .json model path", () => {
  expect(isJsonModel("tmp/models/jra/model.json")).toBe(true);
});

test("isJsonModel is false for a .lgb model path", () => {
  expect(isJsonModel("tmp/models/jra/model.lgb")).toBe(false);
});

test("buildMetadataObjectKey appends metadata.json", () => {
  expect(buildMetadataObjectKey("ban-ei", "v3")).toBe("finish-position/ban-ei/v3/metadata.json");
});

test("parseArgs requires --model-version", () => {
  expect(() => parseArgs(["--model-path", "x.lgb"])).toThrow("--model-version is required.");
});

test("parseArgs requires --model-path", () => {
  expect(() => parseArgs(["--model-version", "v"])).toThrow("--model-path is required.");
});

test("parseArgs accepts the full flag set", () => {
  expect(
    parseArgs([
      "--category",
      "nar",
      "--model-version",
      "v9",
      "--model-path",
      "tmp/x.lgb",
      "--metadata-path",
      "tmp/x.json",
      "--bucket",
      "alt-bucket",
      "--remote",
    ]),
  ).toStrictEqual({
    bucket: "alt-bucket",
    category: "nar",
    metadataPath: "tmp/x.json",
    modelPath: "tmp/x.lgb",
    modelVersion: "v9",
    remote: true,
  });
});

test("parseArgs rejects 'all' category", () => {
  expect(() =>
    parseArgs(["--category", "all", "--model-version", "v", "--model-path", "x"]),
  ).toThrow("--category must be jra, nar, or ban-ei.");
});

test("buildWranglerArgs builds a local-bucket put", () => {
  expect(
    buildWranglerArgs("bucket", "key/path", "file.lgb", false, "application/octet-stream"),
  ).toStrictEqual([
    "wrangler",
    "r2",
    "object",
    "put",
    "bucket/key/path",
    "--file",
    "file.lgb",
    "--content-type",
    "application/octet-stream",
  ]);
});

test("buildWranglerArgs appends --remote when requested", () => {
  const args = buildWranglerArgs("bucket", "k", "f", true, "application/json");
  expect(args[args.length - 1]).toBe("--remote");
});

test("buildUploadSteps yields one octet-stream step for an lgb model when no metadata is set", () => {
  const options: UploadOptions = {
    bucket: "bucket",
    category: "jra",
    metadataPath: null,
    modelPath: "tmp/model.lgb",
    modelVersion: "v1",
    remote: false,
  };
  const steps = buildUploadSteps(options);
  expect(steps.length).toBe(1);
  const first = steps[0];
  expect(first).toBeDefined();
  if (first === undefined) return;
  expect(first.objectKey).toBe("finish-position/jra/v1/model.lgb");
  expect(first.contentType).toBe("application/octet-stream");
});

test("buildUploadSteps yields a json model step with application/json content type", () => {
  const options: UploadOptions = {
    bucket: "bucket",
    category: "nar",
    metadataPath: null,
    modelPath: "tmp/models/nar/model.json",
    modelVersion: "nar-xgb-v7-lineage-wf-21y",
    remote: false,
  };
  const steps = buildUploadSteps(options);
  expect(steps.length).toBe(1);
  const first = steps[0];
  expect(first).toBeDefined();
  if (first === undefined) return;
  expect(first.objectKey).toBe("finish-position/nar/nar-xgb-v7-lineage-wf-21y/model.json");
  expect(first.contentType).toBe("application/json");
});

test("buildUploadSteps adds metadata when a path is provided", () => {
  const options: UploadOptions = {
    bucket: "bucket",
    category: "jra",
    metadataPath: "x.json",
    modelPath: "tmp/x.lgb",
    modelVersion: "v1",
    remote: false,
  };
  const steps = buildUploadSteps(options);
  expect(steps.length).toBe(2);
  const second = steps[1];
  expect(second).toBeDefined();
  if (second === undefined) return;
  expect(second.objectKey).toBe("finish-position/jra/v1/metadata.json");
  expect(second.contentType).toBe("application/json");
});

test("runUpload invokes spawner per step and collects keys", async () => {
  const calls: string[][] = [];
  const spawner: WranglerSpawner = async (args) => {
    calls.push([...args]);
    return { exitCode: 0, stderr: "" };
  };
  const uploaded = await runUpload(
    {
      bucket: "bucket",
      category: "jra",
      metadataPath: "x.json",
      modelPath: "tmp/model.lgb",
      modelVersion: "v1",
      remote: true,
    },
    spawner,
  );
  expect(uploaded).toStrictEqual([
    "finish-position/jra/v1/model.lgb",
    "finish-position/jra/v1/metadata.json",
  ]);
  expect(calls.length).toBe(2);
});

const failingSpawner: WranglerSpawner = async () => ({ exitCode: 1, stderr: "denied" });

test("runUpload propagates wrangler failures", async () => {
  await expect(
    runUpload(
      {
        bucket: "bucket",
        category: "jra",
        metadataPath: null,
        modelPath: "x.lgb",
        modelVersion: "v1",
        remote: false,
      },
      failingSpawner,
    ),
  ).rejects.toThrow("wrangler upload failed");
});
