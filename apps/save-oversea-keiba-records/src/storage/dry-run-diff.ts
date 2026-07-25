// This file runs with Bun.
import type { SqlStatement } from "../types";

export type DiffClassification = "unchanged" | "enriched" | "changed" | "REGRESSION";
export type GateVerdict = "safe" | "blocked";

export interface DryRunRaceKey {
  readonly kaisai_nen: string;
  readonly kaisai_tsukihi: string;
  readonly keibajo_code: string;
  readonly race_bango: string;
}

export interface DiffableRow extends Readonly<Record<string, string>> {
  readonly umaban: string;
}

export interface DryRunQueryOutcome {
  readonly rows: readonly Readonly<Record<string, string>>[];
}

export interface DryRunSqlExecutor {
  readonly execute: (statement: SqlStatement) => Promise<DryRunQueryOutcome>;
}

export interface ClassifyColumnChangeInput {
  readonly dbValue: string;
  readonly incomingValue: string;
}

export interface ColumnDiff {
  readonly umaban: string;
  readonly column: string;
  readonly dbValue: string;
  readonly incomingValue: string;
  readonly classification: DiffClassification;
}

export interface CompareRowsInput {
  readonly dbRow: DiffableRow;
  readonly incomingRow: DiffableRow;
}

export interface RowDiff {
  readonly umaban: string;
  readonly columns: readonly ColumnDiff[];
  readonly hasRegression: boolean;
}

export interface EvaluateDryRunDiffInput {
  readonly dbRows: readonly DiffableRow[];
  readonly incomingRows: readonly DiffableRow[];
}

export interface DryRunDiffResult {
  readonly verdict: GateVerdict;
  readonly hasRegression: boolean;
  readonly regressions: readonly ColumnDiff[];
  readonly changes: readonly ColumnDiff[];
  readonly enrichments: readonly ColumnDiff[];
  readonly unchanged: readonly ColumnDiff[];
  readonly inserts: readonly string[];
  readonly missingFromIncoming: readonly string[];
  readonly rowDiffs: readonly RowDiff[];
}

export interface RunDryRunDiffGateInput {
  readonly raceKey: DryRunRaceKey;
  readonly incomingRows: readonly DiffableRow[];
  readonly executor: DryRunSqlExecutor;
}

export interface CompareColumnDiffInput {
  readonly left: ColumnDiff;
  readonly right: ColumnDiff;
}

export interface CompareUmabanInput {
  readonly left: string;
  readonly right: string;
}

// Documented JV placeholder field widths (detection itself is shape-based: empty or all zeros).
export const JV_PLACEHOLDER_WIDTHS: Readonly<Record<string, number>> = {
  ketto_toroku_bango: 10,
  kishu_code: 5,
  chokyoshi_code: 5,
  banushi_code: 6,
  tansho_odds: 4,
  tansho_ninkijun: 2,
  hasso_jikoku: 4,
  kakutei_chakujun: 2,
  soha_time: 4,
};

const JVD_SE_TABLE: string = "jvd_se";
const UMABAN_COLUMN: string = "umaban";
const EMPTY_STRING: string = "";
const ASCII_SPACE: string = " ";
const IDEOGRAPHIC_SPACE: string = "\u3000";
const ZERO_DIGIT: string = "0";
const SORT_EQUAL: number = 0;

const LEADING_JV_PADDING_PATTERN: RegExp = /^[ \u3000]+/;
const TRAILING_JV_PADDING_PATTERN: RegExp = /[ \u3000]+$/;
const ONLY_ASCII_ZEROS_PATTERN: RegExp = /^0+$/;

const CURRENT_STATE_SELECT_TEXT: string = `SELECT * FROM ${JVD_SE_TABLE} WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 ORDER BY umaban`;

const CLASSIFICATION_UNCHANGED: DiffClassification = "unchanged";
const CLASSIFICATION_ENRICHED: DiffClassification = "enriched";
const CLASSIFICATION_CHANGED: DiffClassification = "changed";
const CLASSIFICATION_REGRESSION: DiffClassification = "REGRESSION";

const VERDICT_SAFE: GateVerdict = "safe";
const VERDICT_BLOCKED: GateVerdict = "blocked";

const REPORT_HEADER_VERDICT_PREFIX: string = "VERDICT ";
const REPORT_REGRESSION_PREFIX: string = "REGRESSION ";
const REPORT_CHANGED_PREFIX: string = "CHANGED ";
const REPORT_ENRICHED_PREFIX: string = "ENRICHED ";
const REPORT_INSERT_PREFIX: string = "INSERT umaban=";
const REPORT_DB_ONLY_PREFIX: string = "DB_ONLY umaban=";

const trimJvPadding = (value: string): string =>
  value
    .replace(LEADING_JV_PADDING_PATTERN, EMPTY_STRING)
    .replace(TRAILING_JV_PADDING_PATTERN, EMPTY_STRING);

const isOnlyAsciiZeros = (value: string): boolean => ONLY_ASCII_ZEROS_PATTERN.test(value);

// A value is placeholder-like when, after trimming ASCII space and ideographic space (U+3000),
// it is empty or consists solely of the digit zero.
export const isPlaceholderLike = (value: string): boolean => {
  const trimmed: string = trimJvPadding(value);
  if (trimmed.length === 0) {
    return true;
  }
  return isOnlyAsciiZeros(trimmed);
};

export const classifyColumnChange = (input: ClassifyColumnChangeInput): DiffClassification => {
  if (input.dbValue === input.incomingValue) {
    return CLASSIFICATION_UNCHANGED;
  }
  const dbIsPlaceholder: boolean = isPlaceholderLike(input.dbValue);
  const incomingIsPlaceholder: boolean = isPlaceholderLike(input.incomingValue);
  if (dbIsPlaceholder && !incomingIsPlaceholder) {
    return CLASSIFICATION_ENRICHED;
  }
  if (!dbIsPlaceholder && incomingIsPlaceholder) {
    return CLASSIFICATION_REGRESSION;
  }
  if (!dbIsPlaceholder && !incomingIsPlaceholder) {
    return CLASSIFICATION_CHANGED;
  }
  // Both sides are placeholder-like but not byte-identical (e.g. "00" vs "0000").
  return CLASSIFICATION_UNCHANGED;
};

const compareUmaban = ({ left, right }: CompareUmabanInput): number => left.localeCompare(right);

const compareColumnDiff = ({ left, right }: CompareColumnDiffInput): number => {
  const umabanOrder: number = compareUmaban({ left: left.umaban, right: right.umaban });
  if (umabanOrder !== SORT_EQUAL) {
    return umabanOrder;
  }
  return left.column.localeCompare(right.column);
};

const sortColumnDiffs = (diffs: readonly ColumnDiff[]): readonly ColumnDiff[] =>
  [...diffs].sort((left: ColumnDiff, right: ColumnDiff): number =>
    compareColumnDiff({ left, right }),
  );

const sortUmabans = (umabans: readonly string[]): readonly string[] =>
  [...umabans].sort((left: string, right: string): number => compareUmaban({ left, right }));

const readCell = (row: Readonly<Record<string, string>>, column: string): string => {
  const value: string | undefined = row[column];
  if (value === undefined) {
    return EMPTY_STRING;
  }
  return value;
};

const toDiffableRow = (row: Readonly<Record<string, string>>): DiffableRow | null => {
  const umaban: string | undefined = row[UMABAN_COLUMN];
  if (umaban === undefined) {
    return null;
  }
  return {
    ...row,
    umaban,
  };
};

const indexRowsByUmaban = (rows: readonly DiffableRow[]): ReadonlyMap<string, DiffableRow> =>
  new Map(rows.map((row: DiffableRow): [string, DiffableRow] => [row.umaban, row]));

export const compareRows = (input: CompareRowsInput): RowDiff => {
  const columns: readonly string[] = Object.keys(input.incomingRow).sort(
    (left: string, right: string): number => left.localeCompare(right),
  );
  const columnDiffs: readonly ColumnDiff[] = columns.map((column: string): ColumnDiff => {
    const dbValue: string = readCell(input.dbRow, column);
    const incomingValue: string = readCell(input.incomingRow, column);
    return {
      umaban: input.incomingRow.umaban,
      column,
      dbValue,
      incomingValue,
      classification: classifyColumnChange({ dbValue, incomingValue }),
    };
  });
  const hasRegression: boolean = columnDiffs.some(
    (diff: ColumnDiff): boolean => diff.classification === CLASSIFICATION_REGRESSION,
  );
  return {
    umaban: input.incomingRow.umaban,
    columns: columnDiffs,
    hasRegression,
  };
};

const partitionByClassification = (
  diffs: readonly ColumnDiff[],
  classification: DiffClassification,
): readonly ColumnDiff[] =>
  sortColumnDiffs(
    diffs.filter((diff: ColumnDiff): boolean => diff.classification === classification),
  );

export const evaluateDryRunDiff = (input: EvaluateDryRunDiffInput): DryRunDiffResult => {
  const dbByUmaban: ReadonlyMap<string, DiffableRow> = indexRowsByUmaban(input.dbRows);
  const incomingByUmaban: ReadonlyMap<string, DiffableRow> = indexRowsByUmaban(input.incomingRows);

  const inserts: readonly string[] = sortUmabans(
    input.incomingRows
      .filter((row: DiffableRow): boolean => !dbByUmaban.has(row.umaban))
      .map((row: DiffableRow): string => row.umaban),
  );

  const missingFromIncoming: readonly string[] = sortUmabans(
    input.dbRows
      .filter((row: DiffableRow): boolean => !incomingByUmaban.has(row.umaban))
      .map((row: DiffableRow): string => row.umaban),
  );

  // flatMap (not pre-filter) so the missing-DB arm is exercised by pure inserts.
  const rowDiffs: readonly RowDiff[] = input.incomingRows
    .flatMap((incomingRow: DiffableRow): readonly RowDiff[] => {
      const dbRow: DiffableRow | undefined = dbByUmaban.get(incomingRow.umaban);
      if (dbRow === undefined) {
        return [];
      }
      return [compareRows({ dbRow, incomingRow })];
    })
    .sort((left: RowDiff, right: RowDiff): number =>
      compareUmaban({ left: left.umaban, right: right.umaban }),
    );

  const allColumnDiffs: readonly ColumnDiff[] = rowDiffs.flatMap(
    (rowDiff: RowDiff): readonly ColumnDiff[] => rowDiff.columns,
  );

  const regressions: readonly ColumnDiff[] = partitionByClassification(
    allColumnDiffs,
    CLASSIFICATION_REGRESSION,
  );
  const changes: readonly ColumnDiff[] = partitionByClassification(
    allColumnDiffs,
    CLASSIFICATION_CHANGED,
  );
  const enrichments: readonly ColumnDiff[] = partitionByClassification(
    allColumnDiffs,
    CLASSIFICATION_ENRICHED,
  );
  const unchanged: readonly ColumnDiff[] = partitionByClassification(
    allColumnDiffs,
    CLASSIFICATION_UNCHANGED,
  );
  const hasRegression: boolean = regressions.length > 0;
  const verdict: GateVerdict = hasRegression ? VERDICT_BLOCKED : VERDICT_SAFE;

  return {
    verdict,
    hasRegression,
    regressions,
    changes,
    enrichments,
    unchanged,
    inserts,
    missingFromIncoming,
    rowDiffs,
  };
};

// Pure SELECT builder. Values use $1..$n placeholders only — never string-interpolated.
// This module emits SELECT only; never mutating write statements or schema changes.
export const buildCurrentStateSelect = (raceKey: DryRunRaceKey): SqlStatement => ({
  text: CURRENT_STATE_SELECT_TEXT,
  values: [raceKey.kaisai_nen, raceKey.kaisai_tsukihi, raceKey.keibajo_code, raceKey.race_bango],
});

const formatColumnDiffLine = (prefix: string, diff: ColumnDiff): string =>
  `${prefix}umaban=${diff.umaban} column=${diff.column} db=${diff.dbValue} incoming=${diff.incomingValue}`;

// Deterministic, sorted by umaban then column name. Pure — returns string lines for the CLI.
export const formatDryRunDiffReport = (result: DryRunDiffResult): readonly string[] => {
  const verdictLine: string = `${REPORT_HEADER_VERDICT_PREFIX}${result.verdict}`;
  const regressionLines: readonly string[] = result.regressions.map((diff: ColumnDiff): string =>
    formatColumnDiffLine(REPORT_REGRESSION_PREFIX, diff),
  );
  const changeLines: readonly string[] = result.changes.map((diff: ColumnDiff): string =>
    formatColumnDiffLine(REPORT_CHANGED_PREFIX, diff),
  );
  const enrichmentLines: readonly string[] = result.enrichments.map((diff: ColumnDiff): string =>
    formatColumnDiffLine(REPORT_ENRICHED_PREFIX, diff),
  );
  const insertLines: readonly string[] = result.inserts.map(
    (umaban: string): string => `${REPORT_INSERT_PREFIX}${umaban}`,
  );
  const dbOnlyLines: readonly string[] = result.missingFromIncoming.map(
    (umaban: string): string => `${REPORT_DB_ONLY_PREFIX}${umaban}`,
  );
  return [
    verdictLine,
    ...regressionLines,
    ...changeLines,
    ...enrichmentLines,
    ...insertLines,
    ...dbOnlyLines,
  ];
};

// Thin runner: injected executor only. No pg import, no connection, no filesystem, no network.
export const runDryRunDiffGate = async (
  input: RunDryRunDiffGateInput,
): Promise<DryRunDiffResult> => {
  const statement: SqlStatement = buildCurrentStateSelect(input.raceKey);
  const outcome: DryRunQueryOutcome = await input.executor.execute(statement);
  const dbRows: readonly DiffableRow[] = outcome.rows.flatMap(
    (row: Readonly<Record<string, string>>): readonly DiffableRow[] => {
      const diffable: DiffableRow | null = toDiffableRow(row);
      if (diffable === null) {
        return [];
      }
      return [diffable];
    },
  );
  return evaluateDryRunDiff({
    dbRows,
    incomingRows: input.incomingRows,
  });
};

// Re-export padding markers so tests can pin the exact characters without magic literals scatter.
export const JV_PADDING_CHARS: Readonly<{
  readonly asciiSpace: string;
  readonly ideographicSpace: string;
  readonly zeroDigit: string;
}> = {
  asciiSpace: ASCII_SPACE,
  ideographicSpace: IDEOGRAPHIC_SPACE,
  zeroDigit: ZERO_DIGIT,
};
