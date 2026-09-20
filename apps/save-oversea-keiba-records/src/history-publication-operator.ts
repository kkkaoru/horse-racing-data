// This file runs with Bun. Pin source artifacts and the database target before writes.
import { createHash } from "node:crypto";
import { join } from "node:path";
import { decodeHistoryArchive, decodeHistoryPlan, isHistoryRecord } from "./history-json";
import { prepareHistoryArchive, type PreparedHistoryArchive } from "./history-preparation";
import type { HistoryCliRuntime } from "./history-cli";
import type { HistoryDatabase, HistoryDatabaseReceipt } from "./storage/history-database";

export interface HistoryPublicationOperatorInput {
  readonly argv: readonly string[];
  readonly targetFingerprint: string;
  readonly database: HistoryDatabase;
  readonly files: Pick<HistoryCliRuntime, "read" | "writeAtomic">;
}
export interface HistoryPublicationReport {
  readonly operation: "prepared" | "applied";
  readonly status: "complete";
  readonly databasePublished: boolean;
  readonly archivedRows: number;
  readonly eligibleRows: number;
  readonly sourcePartialRows: number;
  readonly sourceComplete: boolean;
  readonly canonicalCoverageComplete: boolean;
  readonly missingRows: number;
  readonly receipt: HistoryDatabaseReceipt | null;
}
interface ArtifactBinding {
  readonly version: 1;
  readonly planDigest: string;
  readonly archiveDigest: string;
  readonly targetFingerprint: string;
}
interface LoadedSnapshot {
  readonly prepared: PreparedHistoryArchive;
  readonly binding: ArtifactBinding;
}
interface OperatorContext {
  readonly input: HistoryPublicationOperatorInput;
  readonly directory: string;
  readonly snapshot: LoadedSnapshot;
}
const PLAN_FILE: string = "plan.json";
const ARCHIVE_FILE: string = "archive.json";
const PREPARED_FILE: string = "database-prepared.json";
const RECEIPT_FILE: string = "database-receipt.json";
const digest = (text: string): string => createHash("sha256").update(text).digest("hex");

const requiredFile = async (
  input: HistoryPublicationOperatorInput,
  path: string,
): Promise<string> => {
  const text: string | null = await input.files.read(path);
  if (text === null) throw new Error("Required history publication artifact is missing.");
  return text;
};

const loadSnapshot = async (
  input: HistoryPublicationOperatorInput,
  directory: string,
): Promise<LoadedSnapshot> => {
  if (!/^[a-f0-9]{64}$/u.test(input.targetFingerprint))
    throw new Error("History database target fingerprint is invalid.");
  const [planText, archiveText]: readonly [string, string] = await Promise.all([
    requiredFile(input, join(directory, PLAN_FILE)),
    requiredFile(input, join(directory, ARCHIVE_FILE)),
  ]);
  return {
    prepared: prepareHistoryArchive(decodeHistoryArchive(archiveText), decodeHistoryPlan(planText)),
    binding: {
      version: 1,
      planDigest: digest(planText),
      archiveDigest: digest(archiveText),
      targetFingerprint: input.targetFingerprint,
    },
  };
};

const makeReport = (
  prepared: PreparedHistoryArchive,
  missingRows: number,
): HistoryPublicationReport => ({
  operation: "prepared",
  status: "complete",
  databasePublished: false,
  archivedRows: prepared.archivedRows,
  eligibleRows: prepared.input.horses.length + prepared.input.people.length,
  sourcePartialRows: prepared.sourcePartialRows.length,
  sourceComplete: prepared.sourceComplete,
  canonicalCoverageComplete: false,
  missingRows,
  receipt: null,
});

const prepare = async ({
  input,
  directory,
  snapshot,
}: OperatorContext): Promise<HistoryPublicationReport> => {
  const missing = await input.database.prepare(snapshot.prepared.input);
  const report: HistoryPublicationReport = makeReport(snapshot.prepared, missing.length);
  await input.files.writeAtomic(
    join(directory, PREPARED_FILE),
    JSON.stringify({ binding: snapshot.binding, report, preparedAt: new Date().toISOString() }),
  );
  return report;
};

const sameBinding = (value: unknown, expected: ArtifactBinding): boolean =>
  isHistoryRecord(value) &&
  value.version === expected.version &&
  value.planDigest === expected.planDigest &&
  value.archiveDigest === expected.archiveDigest &&
  value.targetFingerprint === expected.targetFingerprint;

const apply = async ({
  input,
  directory,
  snapshot,
}: OperatorContext): Promise<HistoryPublicationReport> => {
  const frozen: unknown = JSON.parse(await requiredFile(input, join(directory, PREPARED_FILE)));
  if (!isHistoryRecord(frozen) || !sameBinding(frozen.binding, snapshot.binding)) {
    throw new Error("History source artifacts or database target changed after preparation.");
  }
  const receipt: HistoryDatabaseReceipt = await input.database.apply(snapshot.prepared.input);
  const report: HistoryPublicationReport = {
    ...makeReport(snapshot.prepared, receipt.submittedRows),
    operation: "applied",
    databasePublished: true,
    canonicalCoverageComplete:
      snapshot.prepared.sourceComplete && snapshot.prepared.sourcePartialRows.length === 0,
    receipt,
  };
  // COMMIT and all-column verification precede this atomic filesystem receipt.
  // If this write fails, retry reselects the DB delta, not the old insert batch.
  await input.files.writeAtomic(
    join(directory, RECEIPT_FILE),
    JSON.stringify({ binding: snapshot.binding, report, verifiedAt: new Date().toISOString() }),
  );
  return report;
};

export const runHistoryPublicationOperator = async (
  input: HistoryPublicationOperatorInput,
): Promise<HistoryPublicationReport> => {
  const [action, directory, confirmation]: readonly (string | undefined)[] = input.argv;
  const preparing: boolean = action === "prepare" && input.argv.length === 2;
  const applying: boolean =
    action === "apply" && input.argv.length === 3 && confirmation === "--confirm-write";
  if ((!preparing && !applying) || !directory)
    throw new Error("Use prepare DIRECTORY or apply DIRECTORY --confirm-write.");
  if ((await input.files.read(join(directory, RECEIPT_FILE))) !== null) {
    throw new Error(
      "History publication already has a receipt; verify it instead of replaying apply.",
    );
  }
  const snapshot: LoadedSnapshot = await loadSnapshot(input, directory);
  const context: OperatorContext = { input, directory, snapshot };
  return preparing ? prepare(context) : apply(context);
};
