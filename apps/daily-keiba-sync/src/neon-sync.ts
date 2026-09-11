import { neon } from "@neondatabase/serverless";
import type { NeonQueryFunction } from "@neondatabase/serverless";
import { permanentFailure, transientFailure } from "./errors";
import { layoutByTable } from "./layouts";
import type { Env, RecordRow, TableStage } from "./types";

const MAX_QUERY_PARAMETERS = 10_000;
const WARM_ATTEMPTS = 3;
const WARM_DELAY_MS = 1_000;

const quoteIdentifier = (value: string): string => `"${value.replaceAll('"', '""')}"`;

const postgresFailure = (error: unknown): Error => {
  const code =
    typeof error === "object" && error !== null && "code" in error && typeof error.code === "string"
      ? error.code
      : undefined;
  if (code?.startsWith("42") === true) return permanentFailure("neon-schema", error);
  if (code?.startsWith("22") === true) return permanentFailure("neon-data", error);
  if (code?.startsWith("23") === true) return permanentFailure("neon-integrity", error);
  return transientFailure("neon-upsert", error);
};

const sleep = async (milliseconds: number): Promise<void> =>
  await new Promise((resolve) => setTimeout(resolve, milliseconds));

const orderedValues = (
  columns: readonly string[],
  rows: readonly RecordRow[],
): Array<null | number | string> => {
  const values: Array<null | number | string> = [];
  for (const row of rows) {
    for (const column of columns) {
      const value = row[column];
      if (value === undefined) throw new Error("Staged Neon row is missing a column");
      values.push(value);
    }
  }
  return values;
};

const buildUpsert = (
  tableName: string,
  columns: readonly string[],
  primaryKey: readonly string[],
  rowCount: number,
): string => {
  const names = columns.map(quoteIdentifier).join(", ");
  let parameter = 1;
  const values: string[] = [];
  for (let row = 0; row < rowCount; row += 1) {
    const placeholders: string[] = [];
    for (let column = 0; column < columns.length; column += 1) {
      placeholders.push(`$${parameter}`);
      parameter += 1;
    }
    values.push(`(${placeholders.join(", ")})`);
  }
  const updateColumns = columns.filter((column) => !primaryKey.includes(column));
  const updates = updateColumns
    .map((column) => `${quoteIdentifier(column)} = excluded.${quoteIdentifier(column)}`)
    .join(", ");
  const conflict = `on conflict (${primaryKey.map(quoteIdentifier).join(", ")}) do update set ${updates}`;
  if (tableName !== "jvd_se") {
    return `insert into ${quoteIdentifier(tableName)} (${names}) values ${values.join(", ")} ${conflict}`;
  }
  const selected = columns.map((column) => `incoming.${quoteIdentifier(column)}`).join(", ");
  return `with incoming (${names}) as (values ${values.join(", ")}), deleted_provisional as (
  delete from "jvd_se" existing using incoming
  where existing."kaisai_nen" = incoming."kaisai_nen"
    and existing."kaisai_tsukihi" = incoming."kaisai_tsukihi"
    and existing."keibajo_code" = incoming."keibajo_code"
    and existing."race_bango" = incoming."race_bango"
    and existing."ketto_toroku_bango" = incoming."ketto_toroku_bango"
    and existing."umaban" = '00'
    and incoming."umaban" ~ '^(0[1-9]|1[0-8])$'
    and incoming."keibajo_code" ~ '^[0-9]{2}$'
    and incoming."ketto_toroku_bango" ~ '^[0-9]{10}$'
    and incoming."ketto_toroku_bango" <> '0000000000'
  returning 1
)
insert into "jvd_se" (${names}) select ${selected} from incoming ${conflict}`;
};

const warmNeon = async (query: NeonQueryFunction<false, false>): Promise<void> => {
  let latestError: unknown;
  for (let attempt = 1; attempt <= WARM_ATTEMPTS; attempt += 1) {
    try {
      await query.query("select 1");
      return;
    } catch (error: unknown) {
      latestError = error;
      if (attempt < WARM_ATTEMPTS) await sleep(WARM_DELAY_MS * attempt);
    }
  }
  throw new Error("Neon compute did not become ready", { cause: latestError });
};

export const syncNeonTable = async (
  stage: TableStage,
  env: Pick<Env, "NEON_DATABASE_URL">,
): Promise<number> => {
  if (stage.records.length === 0) throw new Error("Neon table stage has no records");
  const layout = layoutByTable(stage.tableName);
  const columns = layout.columns.map((column) => column.name);
  const batchRows = Math.floor(MAX_QUERY_PARAMETERS / columns.length);
  if (batchRows < 1) throw new Error("Table has too many columns for Neon upsert");
  const query = neon(env.NEON_DATABASE_URL);
  try {
    await warmNeon(query);
  } catch (error: unknown) {
    throw transientFailure("neon-connect", error);
  }
  for (let offset = 0; offset < stage.records.length; offset += batchRows) {
    const records = stage.records.slice(offset, offset + batchRows);
    const values = orderedValues(columns, records);
    try {
      await query.query(
        buildUpsert(stage.tableName, columns, layout.primaryKey, records.length),
        values,
      );
    } catch (error: unknown) {
      throw postgresFailure(error);
    }
  }
  return stage.records.length;
};
