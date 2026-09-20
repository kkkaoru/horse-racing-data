// This file runs with Bun. Connection, filesystem and command ports are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { runHistoryCommand, type HistoryCommandInput } from "./history-command";
const ports = vi.hoisted(() => ({
  collect: vi.fn(),
  publish: vi.fn(),
  connect: vi.fn(),
  close: vi.fn(),
}));
vi.mock("./history-cli", () => ({ runHistoryCli: ports.collect }));
vi.mock("./history-publication-operator", () => ({ runHistoryPublicationOperator: ports.publish }));
vi.mock("./storage/history-connection", () => ({ createHistoryConnection: ports.connect }));
const input: HistoryCommandInput = {
  argv: [],
  env: {},
  runtime: { read: vi.fn(), writeAtomic: vi.fn(), fetchPage: vi.fn(), wait: vi.fn() },
};
beforeEach(() => {
  vi.resetAllMocks();
  ports.connect.mockReturnValue({
    targetFingerprint: "a".repeat(64),
    database: { prepare: vi.fn(), apply: vi.fn() },
    close: ports.close,
  });
  ports.close.mockResolvedValue(undefined);
  ports.publish.mockResolvedValue({ operation: "prepared" });
});
it.each(["collect", "status"])(
  "routes %s without creating database connections",
  async (action) => {
    await runHistoryCommand({ ...input, argv: [action, "/snapshot"] });
    expect(ports.collect).toHaveBeenCalledTimes(1);
    expect(ports.connect).not.toHaveBeenCalled();
  },
);
it("routes a local prepare and closes its pool", async () => {
  await runHistoryCommand({ ...input, argv: ["prepare", "/snapshot", "local"] });
  expect(ports.connect).toHaveBeenCalledWith({}, "local");
  expect(ports.publish).toHaveBeenCalledWith(
    expect.objectContaining({ argv: ["prepare", "/snapshot"] }),
  );
  expect(ports.close).toHaveBeenCalledTimes(1);
});
it("passes explicit production apply confirmation", async () => {
  await runHistoryCommand({
    ...input,
    argv: ["apply", "/snapshot", "production", "--confirm-write"],
  });
  expect(ports.connect).toHaveBeenCalledWith({}, "production");
  expect(ports.publish).toHaveBeenCalledWith(
    expect.objectContaining({ argv: ["apply", "/snapshot", "--confirm-write"] }),
  );
  expect(ports.close).toHaveBeenCalledTimes(1);
});
it("closes the pool even if publication fails", async () => {
  ports.publish.mockRejectedValue(new Error("Publication failed"));
  await expect(
    runHistoryCommand({ ...input, argv: ["prepare", "/snapshot", "local"] }),
  ).rejects.toThrow("Publication failed");
  expect(ports.close).toHaveBeenCalledTimes(1);
});
it.each([
  { argv: [] },
  { argv: ["unknown", "/snapshot", "local"] },
  { argv: ["prepare", "", "local"] },
  { argv: ["prepare", "/snapshot", "unknown"] },
  { argv: ["apply", "/snapshot", "production"] },
  { argv: ["apply", "/snapshot", "production", "yes"] },
])("rejects invalid publication commands before connecting %j", async ({ argv }) => {
  await expect(runHistoryCommand({ ...input, argv })).rejects.toThrow(
    "Use prepare DIRECTORY TARGET or apply DIRECTORY TARGET --confirm-write; TARGET is local or production.",
  );
  expect(ports.connect).not.toHaveBeenCalled();
});
