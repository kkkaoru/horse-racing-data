// Run with bun. Checkout one Neon client and run a single write transaction.
// SET TRANSACTION READ WRITE applies only to this txn. Session-level SET+RESET
// is unsafe with Neon transaction pooling, so this helper never RESET.

import type { Pool, PoolClient } from "pg";

interface TransactionReadOnlyRow {
  transaction_read_only: string;
}

const BEGIN_SQL = "BEGIN";
const SET_READ_WRITE_SQL = "SET TRANSACTION READ WRITE";
const SHOW_READ_ONLY_SQL = "SHOW transaction_read_only";
const COMMIT_SQL = "COMMIT";
const ROLLBACK_SQL = "ROLLBACK";

export class NeonTransactionReadOnlyError extends Error {
  constructor(actual: string | undefined) {
    super(`Neon transaction_read_only is ${actual ?? "missing"}; refusing DML`);
    this.name = "NeonTransactionReadOnlyError";
  }
}

const rollbackQuietly = async (client: PoolClient): Promise<void> => {
  try {
    await client.query(ROLLBACK_SQL);
  } catch {
    // Preserve the original error; rollback failure must not hide it.
  }
};

export const withWritableClient = async <T>(
  pool: Pool,
  fn: (client: PoolClient) => Promise<T>,
): Promise<T> => {
  const client = await pool.connect();
  try {
    await client.query(BEGIN_SQL);
    await client.query(SET_READ_WRITE_SQL);
    const showResult = await client.query<TransactionReadOnlyRow>(SHOW_READ_ONLY_SQL);
    const transactionReadOnly = showResult.rows?.[0]?.transaction_read_only;
    if (transactionReadOnly !== "off") {
      throw new NeonTransactionReadOnlyError(transactionReadOnly);
    }
    const result = await fn(client);
    await client.query(COMMIT_SQL);
    return result;
  } catch (error: unknown) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
};
