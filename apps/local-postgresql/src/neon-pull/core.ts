// Run with bun. Backup (break-glass) path: pull the race-dated training tables that production writes to Neon
// (daily-keiba-sync, finish-position-cron) back into the local PostgreSQL training DB.
//
// Local PG is no longer fed by the disabled legacy writer, so training silently reads
// weeks-old results, corner features and running-style predictions. For each table the
// pull upserts Neon's rows of a trailing race-date window on the table's primary key, so
// re-runs also pick up results for races that were still upcoming at the previous run and
// local-only rows (e.g. predictions of local experiment models) are kept.
// Only tables where Neon is the newer source are listed: odds, payouts and master tables
// are newer locally than in Neon and stay untouched.

const DATE_EXPRESSION = "kaisai_nen || kaisai_tsukihi";
const MS_PER_DAY = 86_400_000;
const YYYYMMDD_LENGTH = 8;

export const PULL_TABLES: readonly string[] = [
  "jvd_ra",
  "jvd_se",
  "nvd_ra",
  "nvd_se",
  "netkeiba_training_workouts",
  "race_entry_corner_features",
  "race_running_style_model_predictions",
  "race_finish_position_model_predictions",
];
export const DEFAULT_LOOKBACK_DAYS = 14;
export const DEFAULT_INSERT_BATCH_ROWS = 500;

export interface PullWindow {
  from: string;
  to: string;
}

export interface ResolveWindowInput {
  localMax: string | null;
  neonMax: string | null;
  lookbackDays: number;
}

export type Row = Record<string, unknown>;

export interface PullDatabase {
  query: (sql: string, params?: readonly unknown[]) => Promise<Row[]>;
}

export interface PullTarget extends PullDatabase {
  transaction: <T>(work: () => Promise<T>) => Promise<T>;
}

export interface PullOptions {
  tables: readonly string[];
  lookbackDays: number;
  batchRows: number;
}

export interface TablePullResult {
  table: string;
  window: PullWindow | null;
  upserted: number;
}

const toUtcDate = (yyyymmdd: string): Date =>
  new Date(
    Date.UTC(
      Number(yyyymmdd.slice(0, 4)),
      Number(yyyymmdd.slice(4, 6)) - 1,
      Number(yyyymmdd.slice(6, 8)),
    ),
  );

const formatUtcDate = (date: Date): string => date.toISOString().slice(0, 10).replaceAll("-", "");

export const subtractDays = (yyyymmdd: string, days: number): string =>
  formatUtcDate(new Date(toUtcDate(yyyymmdd).getTime() - days * MS_PER_DAY));

const isYyyymmdd = (value: string | null): value is string =>
  value !== null && value.length === YYYYMMDD_LENGTH && /^\d{8}$/u.test(value);

// Window to replace locally: from (local max - lookback) through Neon's max. Without a
// local max the whole Neon range is unknown territory, so the caller must bound it.
export const resolvePullWindow = (input: ResolveWindowInput): PullWindow | null =>
  isYyyymmdd(input.neonMax) && isYyyymmdd(input.localMax)
    ? { from: subtractDays(input.localMax, input.lookbackDays), to: input.neonMax }
    : null;

export const quoteIdentifier = (identifier: string): string => {
  if (!/^[a-z_][a-z0-9_]*$/u.test(identifier)) {
    throw new Error(`Unsafe SQL identifier: ${identifier}`);
  }
  return `"${identifier}"`;
};

export const commonColumns = (
  localColumns: readonly string[],
  neonColumns: readonly string[],
): string[] => {
  const neon = new Set(neonColumns);
  return localColumns.filter((column) => neon.has(column));
};

export const buildMaxDateSql = (table: string): string =>
  `select max(${DATE_EXPRESSION}) as max_date from ${quoteIdentifier(table)}`;

export const buildColumnsSql = (): string =>
  "select column_name from information_schema.columns where table_schema = 'public' and table_name = $1 order by ordinal_position";

export const buildSelectWindowSql = (table: string, columns: readonly string[]): string =>
  `select ${columns.map(quoteIdentifier).join(", ")} from ${quoteIdentifier(table)} where ${DATE_EXPRESSION} between $1 and $2`;

export const buildPrimaryKeySql = (): string =>
  "select kcu.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kcu on kcu.constraint_name = tc.constraint_name and kcu.table_schema = tc.table_schema where tc.table_schema = 'public' and tc.table_name = $1 and tc.constraint_type = 'PRIMARY KEY' order by kcu.ordinal_position";

export interface UpsertSqlInput {
  table: string;
  columns: readonly string[];
  primaryKey: readonly string[];
  rowCount: number;
}

export const buildUpsertSql = (input: UpsertSqlInput): string => {
  const width = input.columns.length;
  const tuples = Array.from(
    { length: input.rowCount },
    (_, row) =>
      `(${input.columns.map((_column, index) => `$${row * width + index + 1}`).join(", ")})`,
  );
  const keys = new Set(input.primaryKey);
  const updates = input.columns
    .filter((column) => !keys.has(column))
    .map((column) => `${quoteIdentifier(column)} = excluded.${quoteIdentifier(column)}`);
  const conflict = `on conflict (${input.primaryKey.map(quoteIdentifier).join(", ")}) ${
    updates.length > 0 ? `do update set ${updates.join(", ")}` : "do nothing"
  }`;
  return `insert into ${quoteIdentifier(input.table)} (${input.columns.map(quoteIdentifier).join(", ")}) values ${tuples.join(", ")} ${conflict}`;
};

export const chunk = <T>(items: readonly T[], size: number): T[][] =>
  Array.from({ length: Math.ceil(items.length / size) }, (_, index) =>
    items.slice(index * size, (index + 1) * size),
  );

const maxDate = async (db: PullDatabase, table: string): Promise<string | null> => {
  const [row] = await db.query(buildMaxDateSql(table));
  const value = row?.max_date;
  return typeof value === "string" ? value : null;
};

const columnsOf = async (db: PullDatabase, table: string): Promise<string[]> =>
  (await db.query(buildColumnsSql(), [table])).map((row) => String(row.column_name));

const primaryKeyOf = async (db: PullDatabase, table: string): Promise<string[]> => {
  const primaryKey = (await db.query(buildPrimaryKeySql(), [table])).map((row) =>
    String(row.column_name),
  );
  if (primaryKey.length === 0) {
    throw new Error(`Table has no primary key to upsert on: ${table}`);
  }
  return primaryKey;
};

interface UpsertRowsInput {
  target: PullTarget;
  table: string;
  columns: readonly string[];
  primaryKey: readonly string[];
  rows: readonly Row[];
  batchRows: number;
}

const upsertRows = async (input: UpsertRowsInput): Promise<number> => {
  await chunk(input.rows, input.batchRows).reduce<Promise<void>>(
    (previous, batch) =>
      previous.then(async () => {
        await input.target.query(
          buildUpsertSql({
            table: input.table,
            columns: input.columns,
            primaryKey: input.primaryKey,
            rowCount: batch.length,
          }),
          batch.flatMap((row) => input.columns.map((column) => row[column])),
        );
      }),
    Promise.resolve(),
  );
  return input.rows.length;
};

export const pullTable = async (
  source: PullDatabase,
  target: PullTarget,
  table: string,
  options: PullOptions,
): Promise<TablePullResult> => {
  const window = resolvePullWindow({
    localMax: await maxDate(target, table),
    neonMax: await maxDate(source, table),
    lookbackDays: options.lookbackDays,
  });
  if (window === null) {
    return { table, window, upserted: 0 };
  }
  const columns = commonColumns(await columnsOf(target, table), await columnsOf(source, table));
  const primaryKey = await primaryKeyOf(target, table);
  const rows = await source.query(buildSelectWindowSql(table, columns), [window.from, window.to]);
  const upserted = await target.transaction(() =>
    upsertRows({ target, table, columns, primaryKey, rows, batchRows: options.batchRows }),
  );
  return { table, window, upserted };
};

export const runPull = async (
  source: PullDatabase,
  target: PullTarget,
  options: PullOptions,
): Promise<TablePullResult[]> =>
  options.tables.reduce<Promise<TablePullResult[]>>(
    async (previous, table) => [
      ...(await previous),
      await pullTable(source, target, table, options),
    ],
    Promise.resolve([]),
  );
