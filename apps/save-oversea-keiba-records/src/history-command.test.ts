// This file runs with Bun. Connection, filesystem and command ports are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { runHistoryCommand, type HistoryCommandInput } from "./history-command";
const ports = vi.hoisted(() => ({
  collect: vi.fn(),
  publish: vi.fn(),
  connect: vi.fn(),
  close: vi.fn(),
  mirror: vi.fn(),
  mirrorProduction: vi.fn(),
  sourceIds: vi.fn(),
  postPositions: vi.fn(),
  raceTimes: vi.fn(),
  racingPost: vi.fn(),
  jvTimes: vi.fn(),
  jraCardTimes: vi.fn(),
  missingTimes: vi.fn(),
  ownerIds: vi.fn(),
  ancestors: vi.fn(),
}));
vi.mock("./race-time-store", () => ({ runRaceTimeStore: ports.raceTimes }));
vi.mock("./racing-post-times", () => ({ runRacingPostTimes: ports.racingPost }));
vi.mock("./jv-race-times", () => ({ runJvRaceTimes: ports.jvTimes }));
vi.mock("./jra-card-race-times", () => ({ runJraCardRaceTimes: ports.jraCardTimes }));
vi.mock("./missing-race-times", () => ({ runMissingRaceTimes: ports.missingTimes }));
vi.mock("./owner-id-store", () => ({ runOwnerIdStore: ports.ownerIds }));
vi.mock("./pedigree-store", () => ({
  runAncestorPedigreeStore: ports.ancestors,
  runPedigreeStore: vi.fn(),
}));
vi.mock("./post-position-store", () => ({ runPostPositions: ports.postPositions }));
vi.mock("./source-id-mirror", () => ({ runSourceIdMirror: ports.sourceIds }));
vi.mock("./history-cli", () => ({ runHistoryCli: ports.collect }));
vi.mock("./history-local-mirror", () => ({
  runHistoryLocalMirror: ports.mirror,
  runHistoryProductionMirror: ports.mirrorProduction,
}));
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
it.each(["collect", "status", "audit"])(
  "routes %s without creating database connections",
  async (action) => {
    await runHistoryCommand({ ...input, argv: [action, "/snapshot"] });
    expect(ports.collect).toHaveBeenCalledTimes(1);
    expect(ports.connect).not.toHaveBeenCalled();
  },
);
it("delegates a local-only mirror without opening a production connection", async () => {
  await runHistoryCommand({
    ...input,
    argv: ["mirror-local", "/source", "/new-receipts", "--confirm-write"],
  });
  expect(ports.mirror).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
it("routes explicit production mirroring without using the local entrypoint", async () => {
  await runHistoryCommand({
    ...input,
    argv: ["mirror-production", "/source", "/new", "--confirm-production"],
  });
  expect(ports.mirrorProduction).toHaveBeenCalledTimes(1);
  expect(ports.mirror).not.toHaveBeenCalled();
  expect(ports.connect).not.toHaveBeenCalled();
});
it.each(["source-ids-plan", "source-ids-production"])(
  "routes %s to the source-id mirror",
  async (action) => {
    await runHistoryCommand({ ...input, argv: [action, "2026", "1101", "A4"] });
    expect(ports.sourceIds).toHaveBeenCalledTimes(1);
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
it.each(["post-positions-fetch", "post-positions-plan", "post-positions-apply"])(
  "routes %s to the post-position store without a publication connection",
  async (action) => {
    await runHistoryCommand({ ...input, argv: [action, "/pages"] });
    expect(ports.postPositions).toHaveBeenCalledTimes(1);
    expect(ports.connect).not.toHaveBeenCalled();
  },
);
it("routes race-times-plan to the race time store", async () => {
  await runHistoryCommand({ ...input, argv: ["race-times-plan", "/h", "local"] });
  expect(ports.raceTimes).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
it("routes racing-post-races-fetch to the results-page times", async () => {
  await runHistoryCommand({ ...input, argv: ["racing-post-races-fetch", "/m", "/c", "/h"] });
  expect(ports.racingPost).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
it("routes jra-card-race-times-plan to the JRA card times", async () => {
  await runHistoryCommand({
    ...input,
    argv: ["jra-card-race-times-plan", "/c.html", "2026", "1103", "B6", "07", "local"],
  });
  expect(ports.jraCardTimes).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
it("routes missing-race-times to the missing time listing", async () => {
  await runHistoryCommand({ ...input, argv: ["missing-race-times", "2026", "1103", "B6", "/o"] });
  expect(ports.missingTimes).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
it("routes owner-ids-plan to the owner id store", async () => {
  await runHistoryCommand({
    ...input,
    argv: ["owner-ids-plan", "/h", "2026", "1103", "B6", "local"],
  });
  expect(ports.ownerIds).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
it("routes pedigree-ancestors-plan to the ancestor pedigree store", async () => {
  await runHistoryCommand({ ...input, argv: ["pedigree-ancestors-plan", "/p", "local"] });
  expect(ports.ancestors).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
it("routes jv-race-times-apply to the official JV times", async () => {
  await runHistoryCommand({ ...input, argv: ["jv-race-times-apply", "local", "--confirm-write"] });
  expect(ports.jvTimes).toHaveBeenCalledTimes(1);
  expect(ports.connect).not.toHaveBeenCalled();
});
