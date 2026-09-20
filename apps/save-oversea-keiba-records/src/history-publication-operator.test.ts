// This file runs with Bun. All filesystem and database operations are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import {
  runHistoryPublicationOperator,
  type HistoryPublicationOperatorInput,
} from "./history-publication-operator";
import {
  historyPlanDigest,
  type HistoryArchivePlan,
  type HistoryArchiveState,
} from "./history-archive";
import type { HistoryDatabase } from "./storage/history-database";

const plan: HistoryArchivePlan = {
  kind: "owner",
  sourceId: "owner1",
  initialUrl: "https://example.test/results/",
  encoding: "utf-8",
  initialHtmlPath: null,
  profile: {
    populationPattern: "Count ([0-9]+)",
    nextLabels: ["Next"],
    emptyMarker: "No results",
    markup: {
      tableMarker: "demo-results",
      racePathPrefix: "/event/",
      horsePathPrefix: "/animal/",
      jockeyPathPrefix: "/rider/",
      raceUrlTemplate: "https://example.test/event/{RACE_ID}",
      horseFields: {
        date: 0,
        venue: 1,
        raceNumber: 2,
        raceName: 3,
        finishPosition: 4,
        distance: 5,
        going: 6,
        relatedEntity: 7,
      },
      personFields: {
        date: 0,
        venue: 1,
        raceNumber: 2,
        raceName: 3,
        finishPosition: 4,
        distance: 5,
        going: 6,
        relatedEntity: 7,
      },
    },
  },
};
const state: HistoryArchiveState = {
  planDigest: historyPlanDigest(plan),
  publishedCount: 0,
  checkpoint: { pendingUrl: null, completedUrls: [plan.initialUrl], processedRows: 0 },
  rows: [],
};
const files: Map<string, string> = new Map();
const read = vi.fn<HistoryPublicationOperatorInput["files"]["read"]>();
const writeAtomic = vi.fn<HistoryPublicationOperatorInput["files"]["writeAtomic"]>();
const prepare = vi.fn<HistoryDatabase["prepare"]>();
const apply = vi.fn<HistoryDatabase["apply"]>();
const input: HistoryPublicationOperatorInput = {
  argv: ["prepare", "/snapshot"],
  targetFingerprint: "a".repeat(64),
  database: { prepare, apply },
  files: { read, writeAtomic },
};

beforeEach(() => {
  vi.resetAllMocks();
  files.clear();
  files.set("/snapshot/plan.json", JSON.stringify(plan));
  files.set("/snapshot/archive.json", JSON.stringify(state));
  read.mockImplementation(async (path) => files.get(path) ?? null);
  writeAtomic.mockImplementation(async (path, text) => {
    files.set(path, text);
  });
  prepare.mockResolvedValue([]);
  apply.mockResolvedValue({ submittedRows: 0, insertedRows: 0, verifiedRows: 0 });
});

it("pins source artifacts and target during a no-write preparation", async () => {
  expect(await runHistoryPublicationOperator(input)).toMatchObject({
    operation: "prepared",
    databasePublished: false,
    sourceComplete: true,
    canonicalCoverageComplete: false,
    missingRows: 0,
  });
  expect(apply).not.toHaveBeenCalled();
  expect(files.has("/snapshot/database-prepared.json")).toBe(true);
});
it("applies only a prepared snapshot and writes a verified receipt afterwards", async () => {
  await runHistoryPublicationOperator(input);
  expect(
    await runHistoryPublicationOperator({
      ...input,
      argv: ["apply", "/snapshot", "--confirm-write"],
    }),
  ).toMatchObject({
    operation: "applied",
    databasePublished: true,
    canonicalCoverageComplete: true,
    receipt: { submittedRows: 0, insertedRows: 0, verifiedRows: 0 },
  });
  expect(apply).toHaveBeenCalledTimes(1);
  expect(files.has("/snapshot/database-receipt.json")).toBe(true);
});
it.each([
  { argv: [] },
  { argv: ["apply", "/snapshot"] },
  { argv: ["prepare", ""] },
  { argv: ["apply", "/snapshot", "yes"] },
  { argv: ["prepare", "/snapshot", "extra"] },
])("requires explicit apply confirmation and valid arguments %j", async ({ argv }) => {
  await expect(runHistoryPublicationOperator({ ...input, argv })).rejects.toThrow(
    "Use prepare DIRECTORY or apply DIRECTORY --confirm-write.",
  );
  expect(apply).not.toHaveBeenCalled();
  expect(prepare).not.toHaveBeenCalled();
});
it("does not replay an operation with an existing durable receipt", async () => {
  files.set("/snapshot/database-receipt.json", "{}");
  await expect(runHistoryPublicationOperator(input)).rejects.toThrow(
    "History publication already has a receipt; verify it instead of replaying apply.",
  );
  expect(prepare).not.toHaveBeenCalled();
});
it("requires the source files", async () => {
  files.delete("/snapshot/archive.json");
  await expect(runHistoryPublicationOperator(input)).rejects.toThrow(
    "Required history publication artifact is missing.",
  );
});
it("rejects invalid target fingerprints before touching the database", async () => {
  await expect(runHistoryPublicationOperator({ ...input, targetFingerprint: "" })).rejects.toThrow(
    "History database target fingerprint is invalid.",
  );
  expect(prepare).not.toHaveBeenCalled();
});
it("requires a durable prepare artifact before apply", async () => {
  await expect(
    runHistoryPublicationOperator({ ...input, argv: ["apply", "/snapshot", "--confirm-write"] }),
  ).rejects.toThrow("Required history publication artifact is missing.");
  expect(apply).not.toHaveBeenCalled();
});
it.each(["/snapshot/plan.json", "/snapshot/archive.json"])(
  "rejects changed artifact %s",
  async (path) => {
    await runHistoryPublicationOperator(input);
    files.set(path, `${files.get(path)}\n`);
    await expect(
      runHistoryPublicationOperator({ ...input, argv: ["apply", "/snapshot", "--confirm-write"] }),
    ).rejects.toThrow("History source artifacts or database target changed after preparation.");
    expect(apply).not.toHaveBeenCalled();
  },
);
it("rejects target changes between prepare and apply", async () => {
  await runHistoryPublicationOperator(input);
  await expect(
    runHistoryPublicationOperator({
      ...input,
      argv: ["apply", "/snapshot", "--confirm-write"],
      targetFingerprint: "b".repeat(64),
    }),
  ).rejects.toThrow("History source artifacts or database target changed after preparation.");
  expect(apply).not.toHaveBeenCalled();
});
it.each([null, {}, { binding: null }, { binding: { version: 2 } }])(
  "rejects malformed prepared bindings %j",
  async (value) => {
    files.set("/snapshot/database-prepared.json", JSON.stringify(value));
    await expect(
      runHistoryPublicationOperator({ ...input, argv: ["apply", "/snapshot", "--confirm-write"] }),
    ).rejects.toThrow("History source artifacts or database target changed after preparation.");
  },
);
it("never writes a success receipt for failed database publication", async () => {
  await runHistoryPublicationOperator(input);
  apply.mockRejectedValue(new Error("Readback failed"));
  await expect(
    runHistoryPublicationOperator({ ...input, argv: ["apply", "/snapshot", "--confirm-write"] }),
  ).rejects.toThrow("Readback failed");
  expect(files.has("/snapshot/database-receipt.json")).toBe(false);
});
it("allows crash recovery when the database committed but receipt persistence failed", async () => {
  await runHistoryPublicationOperator(input);
  writeAtomic.mockRejectedValueOnce(new Error("Disk failed"));
  await expect(
    runHistoryPublicationOperator({ ...input, argv: ["apply", "/snapshot", "--confirm-write"] }),
  ).rejects.toThrow("Disk failed");
  expect(files.has("/snapshot/database-receipt.json")).toBe(false);
  await runHistoryPublicationOperator({
    ...input,
    argv: ["apply", "/snapshot", "--confirm-write"],
  });
  expect(apply).toHaveBeenCalledTimes(2);
  expect(files.has("/snapshot/database-receipt.json")).toBe(true);
});
it("does not claim complete canonical coverage for pending source pages", async () => {
  files.set(
    "/snapshot/archive.json",
    JSON.stringify({ ...state, checkpoint: { ...state.checkpoint, pendingUrl: plan.initialUrl } }),
  );
  await runHistoryPublicationOperator(input);
  expect(
    await runHistoryPublicationOperator({
      ...input,
      argv: ["apply", "/snapshot", "--confirm-write"],
    }),
  ).toMatchObject({ sourceComplete: false, canonicalCoverageComplete: false });
});
it("keeps genuine source-partial records out of canonical coverage claims", async () => {
  files.set(
    "/snapshot/archive.json",
    JSON.stringify({
      ...state,
      publishedCount: 1,
      checkpoint: { ...state.checkpoint, processedRows: 1 },
      rows: [
        {
          personKind: "owner",
          sourcePersonId: "owner1",
          sourceRaceId: "race1",
          raceDate: "2026-09-01",
          venue: null,
          raceNumber: "1",
          raceName: "Race",
          sourceRaceUrl: "https://example.test/event/race1",
          sourceHorseId: null,
          horseName: null,
          finishPosition: null,
          finishPositionText: "",
          surface: null,
          distanceMetres: null,
          going: null,
        },
      ],
    }),
  );
  await runHistoryPublicationOperator(input);
  expect(
    await runHistoryPublicationOperator({
      ...input,
      argv: ["apply", "/snapshot", "--confirm-write"],
    }),
  ).toMatchObject({
    sourceComplete: true,
    sourcePartialRows: 1,
    eligibleRows: 0,
    canonicalCoverageComplete: false,
  });
});
