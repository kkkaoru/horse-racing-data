// Run with bun via vitest. The CLI delegates all I/O to the tested importer.
import { afterEach, expect, it, vi } from "vitest";

const { parse, run } = vi.hoisted(() => ({
  parse: vi.fn<() => unknown>(),
  run: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("../src/import-race-records", () => ({
  parseRaceImportOptions: parse,
  runRaceImport: run,
}));

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

it("passes command arguments and local connection configuration to the importer", async () => {
  vi.spyOn(process.argv, "slice").mockReturnValue(["--input", "race.txt"]);
  vi.stubEnv("DATABASE_URL_LOCAL", "postgresql://localhost/racing");
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  parse.mockReturnValue({ input: "race.txt" });
  run.mockResolvedValue({ applied: false });
  await import("./import-race-records");
  expect(parse).toHaveBeenCalledWith(["--input", "race.txt"], "postgresql://localhost/racing");
  expect(run).toHaveBeenCalledWith({ input: "race.txt" });
  expect(log).toHaveBeenCalledWith('{"applied":false}');
});
