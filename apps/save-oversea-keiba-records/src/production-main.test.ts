// This file runs with Bun and Vitest; entrypoint I/O is mocked.
import { afterEach, beforeEach, expect, test, vi } from "vitest";
const cli = vi.hoisted(() => ({ run: vi.fn(), runtime: vi.fn() }));
vi.mock("./production-cli", () => ({
  runProductionCli: cli.run,
  createProductionCliRuntime: cli.runtime,
}));
const originalExitCode: typeof process.exitCode = process.exitCode;
beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  vi.stubGlobal("Bun", { argv: ["bun", "production-main.ts", "status", "/cache"] });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  process.exitCode = originalExitCode;
});
test("prints the operator result", async () => {
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  cli.run.mockResolvedValue({ accepted: true });
  await import("./production-main");
  expect(log).toHaveBeenCalledWith('{"accepted":true}');
  expect(cli.run).toHaveBeenCalledTimes(1);
});
test("reports sanitized failure without retrying or exposing credentials", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  cli.run.mockRejectedValue(new Error("private credential"));
  await import("./production-main");
  expect(error).toHaveBeenCalledWith(
    "Production operator failed. Check arguments, manifest integrity, and tool authorization; do not blindly retry apply.",
  );
  expect(process.exitCode).toBe(1);
  expect(cli.run).toHaveBeenCalledTimes(1);
});
