// Run with bun.
import { afterEach, expect, test, vi } from "vitest";
import { readSyncOutput } from "./exec-output";

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test("completed sync cancels its timeout", async () => {
  vi.useFakeTimers();
  const process = {
    output: vi.fn(async () => ({
      exitCode: 0,
      stdout: new ArrayBuffer(0),
      stderr: new ArrayBuffer(0),
    })),
    kill: vi.fn(),
  };
  expect((await readSyncOutput(process)).exitCode).toBe(0);
  await vi.advanceTimersByTimeAsync(600000);
  expect(process.kill).not.toHaveBeenCalled();
});

test("timeout kills only the exec process", async () => {
  vi.useFakeTimers();
  const pending = Promise.withResolvers<ExecOutput>();
  const process = { output: () => pending.promise, kill: vi.fn() };
  const assertion = expect(readSyncOutput(process)).rejects.toThrow(
    "MLflow sync exec exceeded ten minutes",
  );
  await vi.advanceTimersByTimeAsync(600000);
  await assertion;
  expect(process.kill).toHaveBeenCalledWith(9);
});

test("kill failure still reports the timeout", async () => {
  vi.useFakeTimers();
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const pending = Promise.withResolvers<ExecOutput>();
  const process = {
    output: () => pending.promise,
    kill: vi.fn(() => {
      throw new Error("already exited");
    }),
  };
  const assertion = expect(readSyncOutput(process)).rejects.toThrow(
    "MLflow sync exec exceeded ten minutes",
  );
  await vi.advanceTimersByTimeAsync(600000);
  await assertion;
});

test("output failure cancels the timer", async () => {
  vi.useFakeTimers();
  const process = {
    output: vi.fn(async (): Promise<ExecOutput> => {
      throw new Error("exec failed");
    }),
    kill: vi.fn(),
  };
  await expect(readSyncOutput(process)).rejects.toThrow("exec failed");
  expect(vi.getTimerCount()).toBe(0);
});
