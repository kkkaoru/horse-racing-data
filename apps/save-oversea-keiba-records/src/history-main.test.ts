// This file runs with Bun. Entrypoint I/O is mocked.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
const cli = vi.hoisted(() => ({ run: vi.fn(), runtime: vi.fn() }));
vi.mock("./history-cli", () => ({ createHistoryCliRuntime: cli.runtime }));
vi.mock("./history-command", () => ({ runHistoryCommand: cli.run }));
const originalExitCode: typeof process.exitCode = process.exitCode;
beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  process.exitCode = undefined;
  vi.stubGlobal("Bun", { argv: ["bun", "history-main.ts", "status", "/private/archive"] });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  process.exitCode = originalExitCode;
});

it("prints archival completion without claiming database publication", async () => {
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  cli.run.mockResolvedValue({ status: "complete", databasePublished: false });
  await import("./history-main");
  expect(log).toHaveBeenCalledWith('{"status":"complete","databasePublished":false}');
  expect(process.exitCode).toBeUndefined();
});
it("returns a non-success exit code for an incomplete bounded chunk", async () => {
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  cli.run.mockResolvedValue({ status: "paused" });
  await import("./history-main");
  expect(process.exitCode).toBe(2);
});
it("returns failure for blocked acquisition", async () => {
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  cli.run.mockResolvedValue({ status: "blocked" });
  await import("./history-main");
  expect(process.exitCode).toBe(1);
});
it("does not expose private plans or credentials on exceptions", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  cli.run.mockRejectedValue(new Error("private source URL and credentials"));
  await import("./history-main");
  expect(error).toHaveBeenCalledWith(
    "History operator failed. Inspect the private plan and archive; never treat missing or partial data as complete.",
  );
  expect(process.exitCode).toBe(1);
});
