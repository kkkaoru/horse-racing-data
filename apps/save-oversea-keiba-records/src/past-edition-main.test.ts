// This file runs with Bun. Entrypoint I/O is mocked.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

const ports = vi.hoisted(() => ({
  run: vi.fn(),
  connect: vi.fn(),
  end: vi.fn(),
  withTransaction: vi.fn(),
  text: vi.fn(),
}));
vi.mock("./run-past-edition", () => ({ runPastEdition: ports.run }));
vi.mock("./storage/pg-client", () => ({
  resolvePostgresConfig: () => ({}),
  createPostgresClient: () => ({
    connect: ports.connect,
    end: ports.end,
    execute: vi.fn(),
    withTransaction: ports.withTransaction,
  }),
}));
const originalExitCode: typeof process.exitCode = process.exitCode;

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  process.exitCode = undefined;
  vi.stubGlobal("Bun", {
    argv: ["bun", "past-edition-main.ts", "/private/result.html"],
    file: () => ({ text: ports.text }),
  });
});
afterEach(() => {
  vi.unstubAllGlobals();
  process.exitCode = originalExitCode;
});

it("runs the importer with real ports and closes the client", async () => {
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  ports.text.mockResolvedValue("<html></html>");
  ports.withTransaction.mockResolvedValue(undefined);
  ports.run.mockImplementation(async ({ ports: real }) => {
    await real.readFile("/private/result.html");
    await real.withTransaction(async () => undefined);
    real.log("done");
  });
  await import("./past-edition-main");
  expect(ports.run.mock.calls[0]?.[0].argv).toStrictEqual(["/private/result.html"]);
  expect(ports.text).toHaveBeenCalledTimes(1);
  expect(ports.withTransaction).toHaveBeenCalledTimes(1);
  expect(log).toHaveBeenCalledWith("done");
  expect(ports.end).toHaveBeenCalledTimes(1);
  expect(process.exitCode).toBeUndefined();
});

it("reports failures without leaking anything but the message", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  ports.run.mockRejectedValue(new Error("Race result header could not be verified."));
  await import("./past-edition-main");
  expect(error).toHaveBeenCalledWith("Race result header could not be verified.");
  expect(process.exitCode).toBe(1);
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("reports non-Error failures generically", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  ports.run.mockRejectedValue("opaque");
  await import("./past-edition-main");
  expect(error).toHaveBeenCalledWith("Past edition import failed.");
  expect(process.exitCode).toBe(1);
});
