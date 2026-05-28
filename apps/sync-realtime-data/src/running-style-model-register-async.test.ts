// run with: bun run test
import { EventEmitter } from "node:events";
import { afterEach, expect, it, vi } from "vitest";
import type { WranglerSpawner } from "./running-style-model-register";

vi.mock("../scripts/convert-running-style-model-to-binary", () => ({
  convertRunningStyleModelFile: vi.fn(async (_input: string, _output: string) => ({
    categoricalValueCount: 0,
    nodes: 1,
    sizeBytes: 1024,
    trees: 1,
  })),
}));

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return {
    ...actual,
    stat: vi.fn(async (_path: string) => ({ size: 2048 })),
  };
});

interface FakeChildProcess extends EventEmitter {
  stderr: EventEmitter;
}

const fakeChildSpawnSuccess = (): FakeChildProcess => {
  const child = new EventEmitter() as FakeChildProcess;
  child.stderr = new EventEmitter();
  queueMicrotask(() => child.emit("close", 0));
  return child;
};

vi.mock("node:child_process", () => ({
  spawn: vi.fn(() => fakeChildSpawnSuccess()),
}));

const buildSuccessSpawner = (): WranglerSpawner =>
  vi.fn(async (_args: readonly string[]) => ({ exitCode: 0, stderr: "" }));

const buildFailingSpawner = (stderr: string): WranglerSpawner =>
  vi.fn(async (_args: readonly string[]) => ({ exitCode: 1, stderr }));

afterEach(() => {
  vi.restoreAllMocks();
});

it("isRunningStyleModelSource recognizes jra and nar", async () => {
  const { isRunningStyleModelSource } = await import("./running-style-model-register");
  expect(isRunningStyleModelSource("jra")).toBe(true);
  expect(isRunningStyleModelSource("nar")).toBe(true);
  expect(isRunningStyleModelSource("ban-ei")).toBe(false);
});

it("parseRegisterModelArg throws when value has no colon", async () => {
  const { parseRegisterModelArg } = await import("./running-style-model-register");
  expect(() => parseRegisterModelArg("no-colon")).toThrow("Invalid --register-model value");
});

it("parseRegisterModelArg throws when path is empty after colon", async () => {
  const { parseRegisterModelArg } = await import("./running-style-model-register");
  expect(() => parseRegisterModelArg("jra:")).toThrow("requires a file path");
});

it("buildWranglerR2PutArgs builds local args without --remote", async () => {
  const { buildWranglerR2PutArgs } = await import("./running-style-model-register");
  expect(
    buildWranglerR2PutArgs(
      "pc-keiba-finish-position-models",
      "running-style/models/jra/latest.flatbin",
      "tmp/model.flatbin",
      false,
    ),
  ).toStrictEqual([
    "wrangler",
    "r2",
    "object",
    "put",
    "pc-keiba-finish-position-models/running-style/models/jra/latest.flatbin",
    "--file",
    "tmp/model.flatbin",
    "--content-type",
    "application/octet-stream",
  ]);
});

it("buildWranglerR2GetArgs builds local args without --remote", async () => {
  const { buildWranglerR2GetArgs } = await import("./running-style-model-register");
  expect(
    buildWranglerR2GetArgs(
      "pc-keiba-finish-position-models",
      "running-style/models/nar/latest.flatbin",
      "tmp/model.flatbin",
      false,
    ),
  ).toStrictEqual([
    "wrangler",
    "r2",
    "object",
    "get",
    "pc-keiba-finish-position-models/running-style/models/nar/latest.flatbin",
    "--file",
    "tmp/model.flatbin",
  ]);
});

it("uploadRunningStyleModel returns objectKey on successful spawn", async () => {
  const { uploadRunningStyleModel } = await import("./running-style-model-register");
  const spawner = buildSuccessSpawner();
  const key = await uploadRunningStyleModel(
    {
      bucket: "b",
      filePath: "tmp/model.flatbin",
      objectKey: "running-style/models/jra/latest.flatbin",
      remote: false,
    },
    spawner,
  );
  expect(key).toBe("running-style/models/jra/latest.flatbin");
});

it("uploadRunningStyleModel throws when wrangler exits non-zero", async () => {
  const { uploadRunningStyleModel } = await import("./running-style-model-register");
  const spawner = buildFailingSpawner("upload failed");
  await expect(
    uploadRunningStyleModel(
      {
        bucket: "b",
        filePath: "tmp/model.flatbin",
        objectKey: "running-style/models/jra/latest.flatbin",
        remote: true,
      },
      spawner,
    ),
  ).rejects.toThrow("wrangler upload failed");
});

it("objectExistsInR2 returns true when wrangler get exits 0", async () => {
  const { objectExistsInR2 } = await import("./running-style-model-register");
  const spawner = buildSuccessSpawner();
  expect(await objectExistsInR2("b", "k", false, spawner)).toBe(true);
});

it("objectExistsInR2 returns false when wrangler get exits non-zero", async () => {
  const { objectExistsInR2 } = await import("./running-style-model-register");
  const spawner = buildFailingSpawner("not found");
  expect(await objectExistsInR2("b", "k", true, spawner)).toBe(false);
});

it("syncRunningStyleModel throws when remote download fails", async () => {
  const { syncRunningStyleModel } = await import("./running-style-model-register");
  const spawner: WranglerSpawner = vi.fn(async (args: readonly string[]) => {
    if (args.includes("--remote")) {
      return { exitCode: 1, stderr: "remote 404" };
    }
    return { exitCode: 0, stderr: "" };
  });
  await expect(syncRunningStyleModel("jra", { spawner })).rejects.toThrow(
    "Remote model missing for jra",
  );
});

it("syncRunningStyleModel throws when local upload fails", async () => {
  const { syncRunningStyleModel } = await import("./running-style-model-register");
  const spawner: WranglerSpawner = vi.fn(async (args: readonly string[]) => {
    if (args.includes("--remote")) {
      return { exitCode: 0, stderr: "" };
    }
    return { exitCode: 1, stderr: "local fail" };
  });
  await expect(syncRunningStyleModel("nar", { spawner })).rejects.toThrow(
    "Local model sync failed for nar",
  );
});

it("syncRunningStyleModel returns objectKey on success", async () => {
  const { syncRunningStyleModel } = await import("./running-style-model-register");
  const spawner = buildSuccessSpawner();
  expect(await syncRunningStyleModel("jra", { spawner })).toBe(
    "running-style/models/jra/latest.flatbin",
  );
});

it("registerRunningStyleModel uploads a .flatbin path directly", async () => {
  const { registerRunningStyleModel } = await import("./running-style-model-register");
  const spawner = buildSuccessSpawner();
  const result = await registerRunningStyleModel(
    { inputPath: "tmp/model.flatbin", remote: false, source: "jra" },
    { spawner },
  );
  expect(result.objectKey).toBe("running-style/models/jra/latest.flatbin");
  expect(result.sizeBytes).toBe(2048);
});

it("registerRunningStyleModel converts a .json path before uploading", async () => {
  const { registerRunningStyleModel } = await import("./running-style-model-register");
  const { convertRunningStyleModelFile } =
    await import("../scripts/convert-running-style-model-to-binary");
  const spawner = buildSuccessSpawner();
  await registerRunningStyleModel(
    { inputPath: "tmp/model.json", remote: true, source: "nar" },
    { spawner },
  );
  expect(convertRunningStyleModelFile).toHaveBeenCalledTimes(1);
});

it("registerRunningStyleModel throws on unsupported extension", async () => {
  const { registerRunningStyleModel } = await import("./running-style-model-register");
  const spawner = buildSuccessSpawner();
  await expect(
    registerRunningStyleModel(
      { inputPath: "tmp/model.txt", remote: false, source: "jra" },
      { spawner },
    ),
  ).rejects.toThrow("Unsupported model input");
});

it("ensureRunningStyleModels registers and syncs when locally missing", async () => {
  const { ensureRunningStyleModels } = await import("./running-style-model-register");
  let localProbeCount = 0;
  const spawner: WranglerSpawner = vi.fn(async (args: readonly string[]) => {
    if (args[3] === "get" && !args.includes("--remote")) {
      localProbeCount += 1;
      return { exitCode: 1, stderr: "local 404" };
    }
    return { exitCode: 0, stderr: "" };
  });
  const result = await ensureRunningStyleModels(
    {
      register: [{ inputPath: "tmp/model.flatbin", remote: true, source: "jra" }],
      sources: ["nar"],
    },
    spawner,
  );
  expect(result.registered).toStrictEqual(["running-style/models/jra/latest.flatbin"]);
  expect(localProbeCount).toBeGreaterThan(0);
  expect(result.synced.length).toBeGreaterThan(0);
});

it("ensureRunningStyleModels throws when local missing and syncLocalFromRemote=false", async () => {
  const { ensureRunningStyleModels } = await import("./running-style-model-register");
  const spawner: WranglerSpawner = vi.fn(async () => ({ exitCode: 1, stderr: "404" }));
  await expect(
    ensureRunningStyleModels({ sources: ["jra"], syncLocalFromRemote: false }, spawner),
  ).rejects.toThrow("Local R2 model missing for jra");
});

it("ensureRunningStyleModels skips sync when local already exists", async () => {
  const { ensureRunningStyleModels } = await import("./running-style-model-register");
  const spawner: WranglerSpawner = vi.fn(async () => ({ exitCode: 0, stderr: "" }));
  const result = await ensureRunningStyleModels({ sources: ["jra", "nar"] }, spawner);
  expect(result.synced).toStrictEqual([]);
});

it("ensureRunningStyleModels skips register-sync when local exists after upload", async () => {
  const { ensureRunningStyleModels } = await import("./running-style-model-register");
  const spawner: WranglerSpawner = vi.fn(async () => ({ exitCode: 0, stderr: "" }));
  const result = await ensureRunningStyleModels(
    {
      register: [{ inputPath: "tmp/model.flatbin", remote: true, source: "jra" }],
      sources: [],
    },
    spawner,
  );
  expect(result.registered).toStrictEqual(["running-style/models/jra/latest.flatbin"]);
  expect(result.synced).toStrictEqual([]);
});

it("ensureRunningStyleModels throws when non-remote spec leaves local missing", async () => {
  const { ensureRunningStyleModels } = await import("./running-style-model-register");
  const spawner: WranglerSpawner = vi.fn(async (args: readonly string[]) => {
    if (args[3] === "get") return { exitCode: 1, stderr: "missing" };
    return { exitCode: 0, stderr: "" };
  });
  await expect(
    ensureRunningStyleModels(
      {
        register: [{ inputPath: "tmp/model.flatbin", remote: false, source: "jra" }],
        sources: [],
      },
      spawner,
    ),
  ).rejects.toThrow("Local model upload did not persist");
});

it("syncRunningStyleModel uses default spawnWrangler when spawner is not provided", async () => {
  const { syncRunningStyleModel } = await import("./running-style-model-register");
  const result = await syncRunningStyleModel("jra");
  expect(result).toBe("running-style/models/jra/latest.flatbin");
});

it("registerRunningStyleModel uses default spawnWrangler when spawner is not provided", async () => {
  const { registerRunningStyleModel } = await import("./running-style-model-register");
  const result = await registerRunningStyleModel({
    inputPath: "tmp/model.flatbin",
    remote: false,
    source: "nar",
  });
  expect(result.objectKey).toBe("running-style/models/nar/latest.flatbin");
});

it("ensureRunningStyleModels uses default spawnWrangler when spawner is not provided", async () => {
  const { ensureRunningStyleModels } = await import("./running-style-model-register");
  const result = await ensureRunningStyleModels({ sources: [] });
  expect(result.registered).toStrictEqual([]);
});
