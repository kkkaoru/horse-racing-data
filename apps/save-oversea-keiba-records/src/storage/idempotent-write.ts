// This file runs with Bun.
import type { JvdSeRow, SqlStatement } from "../types";
import { buildJvdSeUpsert } from "./upsert-sql";

export interface QueryOutcome {
  readonly rowCount: number;
  readonly rows?: readonly Readonly<Record<string, string>>[];
}

export interface SqlExecutor {
  readonly execute: (statement: SqlStatement) => Promise<QueryOutcome>;
}

export interface JvdRaceKey {
  readonly kaisai_nen: string;
  readonly kaisai_tsukihi: string;
  readonly keibajo_code: string;
  readonly race_bango: string;
}

export interface IdentityConflict {
  readonly raceKey: JvdRaceKey;
  readonly umaban: string;
  readonly storedKettoTorokuBango: string;
  readonly incomingKettoTorokuBango: string;
}

export interface WriteSummary {
  readonly migrated: number;
  readonly inserted: number;
  readonly updated: number;
  readonly skipped: number;
  readonly conflicts: readonly IdentityConflict[];
}

export interface IdempotentWriteInput {
  readonly raceKey: JvdRaceKey;
  readonly runners: readonly JvdSeRow[];
  readonly executor: SqlExecutor;
}

interface KeyMigrationInput {
  readonly raceKey: JvdRaceKey;
  readonly umaban: string;
  readonly realKettoTorokuBango: string;
}

interface ExactPkExistsInput {
  readonly raceKey: JvdRaceKey;
  readonly umaban: string;
  readonly kettoTorokuBango: string;
}

interface RaceUmabanKey {
  readonly raceKey: JvdRaceKey;
  readonly umaban: string;
}

interface WriteOneInput {
  readonly raceKey: JvdRaceKey;
  readonly runner: JvdSeRow;
  readonly executor: SqlExecutor;
  readonly summary: WriteSummary;
}

interface ConflictSummaryInput {
  readonly summary: WriteSummary;
  readonly raceKey: JvdRaceKey;
  readonly umaban: string;
  readonly storedKettos: readonly string[];
  readonly incomingKetto: string;
}

interface UpsertAndCountInput {
  readonly writeInput: WriteOneInput;
  readonly rowExists: boolean;
  readonly summary: WriteSummary;
}

const PLACEHOLDER_KETTO_TOROKU_BANGO: string = "0000000000";
const JVD_SE_TABLE: string = "jvd_se";
const ZERO_ROWS: number = 0;
const ONE_COUNT: number = 1;
const MISSING_IDENTITY_ROWS_MESSAGE: string =
  "Identity lookup returned a positive row count without identity rows.";
const MISSING_KETTO_COLUMN_MESSAGE: string =
  "Identity lookup returned a row without ketto_toroku_bango.";

const EMPTY_SUMMARY: WriteSummary = {
  migrated: 0,
  inserted: 0,
  updated: 0,
  skipped: 0,
  conflicts: [],
};

const isPlaceholderKetto = (kettoTorokuBango: string): boolean =>
  kettoTorokuBango === PLACEHOLDER_KETTO_TOROKU_BANGO;

export const buildKeyMigrationStatement = ({
  raceKey,
  umaban,
  realKettoTorokuBango,
}: KeyMigrationInput): SqlStatement => ({
  text: `UPDATE ${JVD_SE_TABLE} SET ketto_toroku_bango = $6 WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 AND ketto_toroku_bango = $7`,
  values: [
    raceKey.kaisai_nen,
    raceKey.kaisai_tsukihi,
    raceKey.keibajo_code,
    raceKey.race_bango,
    umaban,
    realKettoTorokuBango,
    PLACEHOLDER_KETTO_TOROKU_BANGO,
  ],
});

export const buildExactPkExistsStatement = ({
  raceKey,
  umaban,
  kettoTorokuBango,
}: ExactPkExistsInput): SqlStatement => ({
  text: `SELECT 1 FROM ${JVD_SE_TABLE} WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 AND ketto_toroku_bango = $6 LIMIT 1`,
  values: [
    raceKey.kaisai_nen,
    raceKey.kaisai_tsukihi,
    raceKey.keibajo_code,
    raceKey.race_bango,
    umaban,
    kettoTorokuBango,
  ],
});

export const buildRealKettoExistsStatement = ({
  raceKey,
  umaban,
}: RaceUmabanKey): SqlStatement => ({
  text: `SELECT 1 FROM ${JVD_SE_TABLE} WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 AND ketto_toroku_bango <> $6 LIMIT 1`,
  values: [
    raceKey.kaisai_nen,
    raceKey.kaisai_tsukihi,
    raceKey.keibajo_code,
    raceKey.race_bango,
    umaban,
    PLACEHOLDER_KETTO_TOROKU_BANGO,
  ],
});

export const buildExistingRunnerKeysStatement = ({
  raceKey,
  umaban,
}: RaceUmabanKey): SqlStatement => ({
  text: `SELECT ketto_toroku_bango FROM ${JVD_SE_TABLE} WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 ORDER BY ketto_toroku_bango FOR UPDATE`,
  values: [
    raceKey.kaisai_nen,
    raceKey.kaisai_tsukihi,
    raceKey.keibajo_code,
    raceKey.race_bango,
    umaban,
  ],
});

const withMigrated = (summary: WriteSummary, count: number): WriteSummary => ({
  migrated: summary.migrated + count,
  inserted: summary.inserted,
  updated: summary.updated,
  skipped: summary.skipped,
  conflicts: summary.conflicts,
});

const withInserted = (summary: WriteSummary): WriteSummary => ({
  migrated: summary.migrated,
  inserted: summary.inserted + ONE_COUNT,
  updated: summary.updated,
  skipped: summary.skipped,
  conflicts: summary.conflicts,
});

const withUpdated = (summary: WriteSummary): WriteSummary => ({
  migrated: summary.migrated,
  inserted: summary.inserted,
  updated: summary.updated + ONE_COUNT,
  skipped: summary.skipped,
  conflicts: summary.conflicts,
});

const withSkipped = (summary: WriteSummary): WriteSummary => ({
  migrated: summary.migrated,
  inserted: summary.inserted,
  updated: summary.updated,
  skipped: summary.skipped + ONE_COUNT,
  conflicts: summary.conflicts,
});

const withConflicts = ({
  summary,
  raceKey,
  umaban,
  storedKettos,
  incomingKetto,
}: ConflictSummaryInput): WriteSummary => ({
  migrated: summary.migrated,
  inserted: summary.inserted,
  updated: summary.updated,
  skipped: summary.skipped,
  conflicts: [
    ...summary.conflicts,
    ...storedKettos.map(
      (storedKettoTorokuBango: string): IdentityConflict => ({
        raceKey,
        umaban,
        storedKettoTorokuBango,
        incomingKettoTorokuBango: incomingKetto,
      }),
    ),
  ],
});

const storedKettosFromOutcome = (outcome: QueryOutcome): readonly string[] => {
  if (outcome.rows === undefined) {
    if (outcome.rowCount === ZERO_ROWS) {
      return [];
    }
    throw new Error(MISSING_IDENTITY_ROWS_MESSAGE);
  }

  return outcome.rows.map((row: Readonly<Record<string, string>>): string => {
    const storedKetto: string | undefined = row.ketto_toroku_bango;
    if (storedKetto === undefined) {
      throw new Error(MISSING_KETTO_COLUMN_MESSAGE);
    }
    return storedKetto;
  });
};

const upsertAndCount = async ({
  writeInput,
  rowExists,
  summary,
}: UpsertAndCountInput): Promise<WriteSummary> => {
  await writeInput.executor.execute(buildJvdSeUpsert(writeInput.runner));
  if (rowExists) {
    return withUpdated(summary);
  }
  return withInserted(summary);
};

const writePlaceholderRunner = (
  input: WriteOneInput,
  storedKettos: readonly string[],
): Promise<WriteSummary> => {
  // A placeholder never supersedes a real identity. It is skipped when any real
  // key already exists, including already-corrupted placeholder-plus-real states.
  if (storedKettos.some((storedKetto: string): boolean => !isPlaceholderKetto(storedKetto))) {
    return Promise.resolve(withSkipped(input.summary));
  }
  return upsertAndCount({
    writeInput: input,
    rowExists: storedKettos.length > ZERO_ROWS,
    summary: input.summary,
  });
};

const migratePlaceholderAndUpsert = async (input: WriteOneInput): Promise<WriteSummary> => {
  const migrationOutcome: QueryOutcome = await input.executor.execute(
    buildKeyMigrationStatement({
      raceKey: input.raceKey,
      umaban: input.runner.umaban,
      realKettoTorokuBango: input.runner.ketto_toroku_bango,
    }),
  );
  return upsertAndCount({
    writeInput: input,
    rowExists: true,
    summary: withMigrated(input.summary, migrationOutcome.rowCount),
  });
};

const identityConflict = (input: WriteOneInput, storedKettos: readonly string[]): WriteSummary =>
  withConflicts({
    summary: input.summary,
    raceKey: input.raceKey,
    umaban: input.runner.umaban,
    storedKettos,
    incomingKetto: input.runner.ketto_toroku_bango,
  });

const writeRealRunner = (
  input: WriteOneInput,
  storedKettos: readonly string[],
): Promise<WriteSummary> => {
  if (storedKettos.length === ZERO_ROWS) {
    return upsertAndCount({ writeInput: input, rowExists: false, summary: input.summary });
  }
  if (storedKettos.length > ONE_COUNT) {
    return Promise.resolve(identityConflict(input, storedKettos));
  }
  if (
    storedKettos.some(
      (storedKetto: string): boolean => storedKetto === input.runner.ketto_toroku_bango,
    )
  ) {
    return upsertAndCount({ writeInput: input, rowExists: true, summary: input.summary });
  }
  if (storedKettos.some(isPlaceholderKetto)) {
    return migratePlaceholderAndUpsert(input);
  }
  return Promise.resolve(identityConflict(input, storedKettos));
};

const writeOneRunner = async (input: WriteOneInput): Promise<WriteSummary> => {
  const existingOutcome: QueryOutcome = await input.executor.execute(
    buildExistingRunnerKeysStatement({
      raceKey: input.raceKey,
      umaban: input.runner.umaban,
    }),
  );
  const storedKettos: readonly string[] = storedKettosFromOutcome(existingOutcome);
  if (isPlaceholderKetto(input.runner.ketto_toroku_bango)) {
    return writePlaceholderRunner(input, storedKettos);
  }
  return writeRealRunner(input, storedKettos);
};

// Caller must wrap this sequence in one database transaction. Identity conflicts
// fail closed for their runner and remain visible in the returned summary while
// independent runners continue processing.
export const writeJvdSeRunnersIdempotently = (input: IdempotentWriteInput): Promise<WriteSummary> =>
  input.runners.reduce(
    async (summaryPromise: Promise<WriteSummary>, runner: JvdSeRow): Promise<WriteSummary> => {
      const summary: WriteSummary = await summaryPromise;
      return writeOneRunner({
        raceKey: input.raceKey,
        runner,
        executor: input.executor,
        summary,
      });
    },
    Promise.resolve(EMPTY_SUMMARY),
  );
