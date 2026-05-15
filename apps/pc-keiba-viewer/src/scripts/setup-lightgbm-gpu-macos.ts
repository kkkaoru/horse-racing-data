import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const APP_DIR = resolve(SCRIPT_DIR, "../..");
const ROOT_DIR = resolve(APP_DIR, "../..");
const LIGHTGBM_VERSION = "v4.6.0";
const BUILD_DIR = resolve(APP_DIR, "tmp/lightgbm-gpu-build");
const VENV_LIGHTGBM_LIB = resolve(
  APP_DIR,
  ".venv/lib/python3.14/site-packages/lightgbm/lib/lib_lightgbm.dylib",
);

const run = async (command: string, args: string[], cwd = ROOT_DIR) => {
  console.log([command, ...args].join(" "));
  const result = await execFileAsync(command, args, {
    cwd,
    env: process.env,
    maxBuffer: 1024 * 1024 * 50,
  });
  if (result.stdout.trim()) {
    console.log(result.stdout.trim());
  }
  if (result.stderr.trim()) {
    console.error(result.stderr.trim());
  }
};

const commandOutput = async (command: string, args: string[], cwd = ROOT_DIR): Promise<string> => {
  const result = await execFileAsync(command, args, { cwd, env: process.env });
  return result.stdout.trim();
};

const main = async () => {
  if (process.platform !== "darwin") {
    throw new Error("This setup script is only for macOS.");
  }
  if (!existsSync(VENV_LIGHTGBM_LIB)) {
    throw new Error(`LightGBM library was not found: ${VENV_LIGHTGBM_LIB}`);
  }

  await run("brew", ["install", "cmake", "libomp", "boost@1.85"]);
  if (!existsSync(BUILD_DIR)) {
    await run("git", [
      "clone",
      "--recursive",
      "--branch",
      LIGHTGBM_VERSION,
      "--depth",
      "1",
      "https://github.com/microsoft/LightGBM.git",
      BUILD_DIR,
    ]);
  }

  const sdkPath = await commandOutput("xcrun", ["--sdk", "macosx", "--show-sdk-path"]);
  await run(
    "cmake",
    [
      "-B",
      "build",
      "-S",
      ".",
      "-DUSE_GPU=ON",
      "-DBOOST_ROOT=/opt/homebrew/opt/boost@1.85",
      "-DBoost_NO_SYSTEM_PATHS=ON",
      `-DOpenCL_LIBRARY=${sdkPath}/System/Library/Frameworks/OpenCL.framework/OpenCL.tbd`,
      `-DOpenCL_INCLUDE_DIR=${sdkPath}/System/Library/Frameworks/OpenCL.framework/Headers`,
    ],
    BUILD_DIR,
  );
  const cpuCount = await commandOutput("sysctl", ["-n", "hw.ncpu"]);
  await run("cmake", ["--build", "build", `-j${cpuCount}`], BUILD_DIR);

  const backupDir = resolve(APP_DIR, "tmp/lightgbm-cpu-backup");
  await mkdir(backupDir, { recursive: true });
  const timestamp = new Date().toISOString().replaceAll(/[-:.TZ]/gu, "");
  await copyFile(VENV_LIGHTGBM_LIB, resolve(backupDir, `lib_lightgbm.dylib.${timestamp}`));
  await copyFile(resolve(BUILD_DIR, "lib_lightgbm.dylib"), VENV_LIGHTGBM_LIB);

  console.log("LightGBM GPU dylib installed.");
  console.log("Use PC_KEIBA_LIGHTGBM_DEVICE=gpu when training.");
};

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
