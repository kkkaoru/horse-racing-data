// This file runs with Bun. Explicit-target, insert-only snapshot mirroring with full readback.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { decodeHistoryArchive, decodeHistoryPlan } from "./history-json";
import {
  historyRowsToInput,
  historyRowFingerprint,
  prepareHistoryArchive,
  type PreparedHistoryArchive,
} from "./history-preparation";
import type { HistorySourceRow } from "./sources/history-source-page";
import {
  createHistoryConnection,
  type HistoryConnection,
  type HistoryDatabaseTarget,
} from "./storage/history-connection";
import type { HistoryDatabaseReceipt } from "./storage/history-database";
import type { HistoryPublicationReport } from "./history-publication-operator";

export interface HistoryLocalMirrorInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface HistoryLocalMirrorReport extends HistoryPublicationReport {
  readonly existingConflictingRows: number;
}
interface MirrorChunksResult {
  readonly receipt: HistoryDatabaseReceipt;
  readonly existingConflictingRows: number;
}
interface MirrorChunksInput {
  readonly connection: HistoryConnection;
  readonly chunks: readonly MirrorChunk[];
  readonly directory: string;
  readonly preserveExistingVariants: boolean;
}
interface MirrorChunk {
  readonly index: number;
  readonly rows: readonly HistorySourceRow[];
}
const CHUNK_SIZE: number = 250;
const DIRECTORY_MODE: number = 0o700;
const FILE_MODE: number = 0o600;
const LOOPBACK_HOSTS: ReadonlySet<string> = new Set(["127.0.0.1", "localhost", "[::1]"]);
const POSTGRES_KEYS: readonly string[] = [
  "POSTGRES_PORT",
  "POSTGRES_USER",
  "POSTGRES_PASSWORD",
  "POSTGRES_DB",
];
const digest = (text: string): string => createHash("sha256").update(text).digest("hex");
const save = (path: string, value: unknown): Promise<void> =>
  writeFile(path, JSON.stringify(value), { flag: "wx", mode: FILE_MODE });

export const localHistoryEnvironment = (
  env: HistoryLocalMirrorInput["env"],
): HistoryLocalMirrorInput["env"] => {
  const supplied: string | undefined = env.OVERSEA_HISTORY_LOCAL_DATABASE_URL;
  const url: URL = new URL(supplied ?? "postgresql://127.0.0.1");
  if (supplied === undefined) {
    assert(
      POSTGRES_KEYS.every((key) => Boolean(env[key])),
      "Explicit local PostgreSQL configuration is required.",
    );
    url.port = String(env.POSTGRES_PORT);
    url.username = String(env.POSTGRES_USER);
    url.password = String(env.POSTGRES_PASSWORD);
    url.pathname = `/${String(env.POSTGRES_DB)}`;
  }
  assert(LOOPBACK_HOSTS.has(url.hostname), "Local mirroring requires a loopback database host.");
  assert(
    [...url.searchParams.keys()].every((key) => key === "sslmode"),
    "Database endpoint overrides are forbidden.",
  );
  return { ...env, OVERSEA_HISTORY_LOCAL_DATABASE_URL: url.href };
};

const mirrorChunks = async ({
  connection,
  chunks,
  directory,
  preserveExistingVariants,
}: MirrorChunksInput): Promise<MirrorChunksResult> => {
  const receipts: HistoryDatabaseReceipt[] = [];
  const accepted: MirrorChunk[] = [];
  const conflicts: HistorySourceRow[] = [];
  for (const chunk of chunks) {
    const variants: readonly HistorySourceRow[] = preserveExistingVariants
      ? await connection.database.conflicts(historyRowsToInput(chunk.rows))
      : [];
    const excluded: ReadonlySet<string> = new Set(variants.map(historyRowFingerprint));
    const rows: readonly HistorySourceRow[] = chunk.rows.filter(
      (row) => !excluded.has(historyRowFingerprint(row)),
    );
    conflicts.push(...variants);
    accepted.push({ index: chunk.index, rows });
    await save(join(directory, `chunk-${chunk.index}-pending.json`), {
      inputSha256: digest(JSON.stringify(rows)),
      rows: rows.length,
      existingVariantsPreserved: variants,
    });
    const receipt: HistoryDatabaseReceipt = await connection.database.apply(
      historyRowsToInput(rows),
    );
    assert.equal(receipt.verifiedRows, rows.length, "Chunk verification count differs.");
    receipts.push(receipt);
    await save(join(directory, `chunk-${chunk.index}-receipt.json`), receipt);
  }
  // A separate post-commit pass detects receipt loss, missing rows, and changed fields.
  for (const chunk of accepted)
    assert.equal(
      (await connection.database.prepare(historyRowsToInput(chunk.rows))).length,
      0,
      "Local post-commit readback differs.",
    );
  return {
    existingConflictingRows: conflicts.length,
    receipt: {
      submittedRows: receipts.reduce((sum, receipt) => sum + receipt.submittedRows, 0),
      insertedRows: receipts.reduce((sum, receipt) => sum + receipt.insertedRows, 0),
      verifiedRows: receipts.reduce((sum, receipt) => sum + receipt.verifiedRows, 0),
    },
  };
};

const runHistoryMirror = async (
  { argv, env }: HistoryLocalMirrorInput,
  target: HistoryDatabaseTarget,
): Promise<HistoryLocalMirrorReport> => {
  const [action, source, destination, confirmation, policy]: readonly (string | undefined)[] = argv;
  assert(
    action === `mirror-${target}` &&
      (argv.length === 4 || (argv.length === 5 && policy === "--preserve-existing-variants")) &&
      source &&
      destination &&
      confirmation === (target === "local" ? "--confirm-write" : "--confirm-production"),
    "Use mirror-local SOURCE_DIRECTORY NEW_RECEIPT_DIRECTORY --confirm-write or mirror-production SOURCE_DIRECTORY NEW_RECEIPT_DIRECTORY --confirm-production.",
  );
  assert.notEqual(
    resolve(source),
    resolve(destination),
    "Never write publication evidence into the live source directory.",
  );
  const localEnv: HistoryLocalMirrorInput["env"] =
    target === "local" ? localHistoryEnvironment(env) : env;
  const [planText, archiveText]: [string, string] = await Promise.all([
    readFile(join(source, "plan.json"), "utf8"),
    readFile(join(source, "archive.json"), "utf8"),
  ]);
  const prepared: PreparedHistoryArchive = prepareHistoryArchive(
    decodeHistoryArchive(archiveText),
    decodeHistoryPlan(planText),
  );
  assert.equal(
    await readFile(join(source, "plan.json"), "utf8"),
    planText,
    "Source plan changed while freezing the checkpoint.",
  );
  // The source archive is an atomic checkpoint; later appended rows belong to the next pass.
  await mkdir(destination, { mode: DIRECTORY_MODE });
  await writeFile(join(destination, "plan.json"), planText, { flag: "wx", mode: FILE_MODE });
  await writeFile(join(destination, "archive.json"), archiveText, { flag: "wx", mode: FILE_MODE });
  const connection: HistoryConnection = createHistoryConnection(localEnv, target);
  try {
    const binding = {
      target,
      targetFingerprint: connection.targetFingerprint,
      planSha256: digest(planText),
      archiveSha256: digest(archiveText),
    };
    await save(join(destination, "pending.json"), {
      ...binding,
      startedAt: new Date().toISOString(),
      sourceComplete: prepared.sourceComplete,
    });
    const rows: readonly HistorySourceRow[] = [...prepared.input.horses, ...prepared.input.people];
    const chunks: readonly MirrorChunk[] = Array.from(
      { length: Math.ceil(rows.length / CHUNK_SIZE) },
      (_, index) => ({ index, rows: rows.slice(index * CHUNK_SIZE, (index + 1) * CHUNK_SIZE) }),
    );
    const { receipt, existingConflictingRows }: MirrorChunksResult = await mirrorChunks({
      connection,
      chunks,
      directory: destination,
      preserveExistingVariants: policy === "--preserve-existing-variants",
    });
    const report: HistoryLocalMirrorReport = {
      operation: "applied",
      status: "complete",
      databasePublished: true,
      archivedRows: prepared.archivedRows,
      eligibleRows: rows.length,
      sourcePartialRows: prepared.sourcePartialRows.length,
      sourceComplete: prepared.sourceComplete,
      existingConflictingRows,
      canonicalCoverageComplete:
        prepared.sourceComplete &&
        prepared.sourcePartialRows.length === 0 &&
        existingConflictingRows === 0,
      missingRows: receipt.submittedRows,
      receipt,
    };
    await save(join(destination, "receipt.json"), {
      ...binding,
      verifiedAt: new Date().toISOString(),
      report,
    });
    return report;
  } finally {
    await connection.close();
  }
};

export const runHistoryLocalMirror = (
  input: HistoryLocalMirrorInput,
): Promise<HistoryLocalMirrorReport> => runHistoryMirror(input, "local");
export const runHistoryProductionMirror = (
  input: HistoryLocalMirrorInput,
): Promise<HistoryLocalMirrorReport> => runHistoryMirror(input, "production");
