// This file runs with Bun. Read-before-write excludes already verified rows.
import { isHistorySourceRow } from "../history-json";
import { historyRowFingerprint, historyRowsToInput } from "../history-preparation";
import type { HistorySourceRow } from "../sources/history-source-page";
import type { SqlStatement } from "../types";
import {
  buildHistoryStatements,
  buildHistoryVerificationStatements,
  buildMissingHistoryStatements,
  publishHistory,
  type HistoryPublicationInput,
} from "./history-publication";
import type { PostgresPool, PostgresPoolClient, PostgresQueryResult } from "./pg-client";

export interface HistoryDatabaseReceipt {
  readonly submittedRows: number;
  readonly insertedRows: number;
  readonly verifiedRows: number;
}
export interface HistoryDatabase {
  readonly prepare: (input: HistoryPublicationInput) => Promise<readonly HistorySourceRow[]>;
  readonly apply: (input: HistoryPublicationInput) => Promise<HistoryDatabaseReceipt>;
}
interface QueryTarget {
  readonly query: PostgresPoolClient["query"];
}
interface TransactionProgress {
  began: boolean;
  insertedRows: number;
}

const mismatchCount = (result: PostgresQueryResult): number => {
  const value: unknown = result.rows[0]?.mismatches;
  if (
    result.rows.length !== 1 ||
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < 0
  ) {
    throw new Error("History readback returned an invalid mismatch count.");
  }
  return value;
};

const readMissing = async (
  input: HistoryPublicationInput,
  target: QueryTarget,
): Promise<readonly HistorySourceRow[]> => {
  const statements: readonly SqlStatement[] = buildMissingHistoryStatements(input);
  const permitted: ReadonlySet<string> = new Set(
    [...input.horses, ...input.people].map(historyRowFingerprint),
  );
  const rows: HistorySourceRow[] = [];
  const seen: Set<string> = new Set();
  for (const statement of statements) {
    const result: PostgresQueryResult = await target.query(statement.text, statement.values);
    for (const record of result.rows) {
      const row: unknown = record.row;
      if (!isHistorySourceRow(row) || !permitted.has(historyRowFingerprint(row))) {
        throw new Error("Database returned a history candidate outside the validated input.");
      }
      const fingerprint: string = historyRowFingerprint(row);
      if (seen.has(fingerprint)) throw new Error("Database returned duplicate history candidates.");
      seen.add(fingerprint);
      rows.push(row);
    }
  }
  return rows;
};

const verifyAll = async (input: HistoryPublicationInput, target: QueryTarget): Promise<void> => {
  for (const statement of buildHistoryVerificationStatements(input)) {
    if (mismatchCount(await target.query(statement.text, statement.values)) !== 0) {
      throw new Error("Full history readback differs; transaction must roll back.");
    }
  }
};

const apply = async (
  input: HistoryPublicationInput,
  pool: PostgresPool,
): Promise<HistoryDatabaseReceipt> => {
  buildHistoryStatements(input);
  const client: PostgresPoolClient = await pool.connect();
  const progress: TransactionProgress = { began: false, insertedRows: 0 };
  try {
    await client.query("BEGIN");
    progress.began = true;
    const missing: readonly HistorySourceRow[] = await readMissing(input, client);
    await publishHistory(historyRowsToInput(missing), {
      withTransaction: async (run): Promise<void> =>
        run({
          write: async (statement): Promise<void> => {
            const result: PostgresQueryResult = await client.query(
              statement.text,
              statement.values,
            );
            if (
              result.rowCount === null ||
              !Number.isSafeInteger(result.rowCount) ||
              result.rowCount < 0
            ) {
              throw new Error("History insert returned an invalid affected-row count.");
            }
            progress.insertedRows += result.rowCount;
          },
          countMismatches: async (statement): Promise<number> =>
            mismatchCount(await client.query(statement.text, statement.values)),
        }),
    });
    await verifyAll(input, client);
    await client.query("COMMIT");
    return {
      submittedRows: missing.length,
      insertedRows: progress.insertedRows,
      verifiedRows: input.horses.length + input.people.length,
    };
  } catch (error: unknown) {
    if (progress.began) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError: unknown) {
        throw new AggregateError(
          [error, rollbackError],
          "History transaction and rollback failed.",
        );
      }
    }
    throw error;
  } finally {
    client.release();
  }
};

export const createHistoryDatabase = (pool: PostgresPool): HistoryDatabase => ({
  prepare: (input): Promise<readonly HistorySourceRow[]> => readMissing(input, pool),
  apply: (input): Promise<HistoryDatabaseReceipt> => apply(input, pool),
});
