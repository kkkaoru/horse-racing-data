// Run with bun. Imports Wine JV-Link UTF-8 exports with the production record parser.
import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { encode } from "iconv-lite";
import { Pool } from "pg";
import {
  catalogDeleteRowKeys,
  optionalLayoutForRecord,
  parseFixedRecord,
  rowKey,
} from "../../daily-keiba-sync/src/layouts";
import type { RecordLayout, RecordRow } from "../../daily-keiba-sync/src/layouts";

export interface RaceImportOptions {
  input: string;
  fromDate: string;
  toDate: string;
  apply: boolean;
  databaseUrl: string;
}
export interface RaceImportTable {
  layout: RecordLayout;
  rows: Map<string, RecordRow>;
}
export interface RaceImportPlan {
  tables: Map<string, RaceImportTable>;
  skippedTypes: Record<string, number>;
  outsideRange: number;
}

const LOCAL_DATABASE_URL: string =
  "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing";
const SUPPORTED_TABLES: ReadonlySet<string> = new Set(["jvd_ra", "jvd_se"]);
const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
const validateDate = (value: string | undefined): string => {
  if (value === undefined || !/^\d{4}-\d{2}-\d{2}$/.test(value))
    throw new Error("Dates must use YYYY-MM-DD");
  const date = new Date(`${value}T00:00:00Z`);
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== value)
    throw new Error("Invalid calendar date");
  return value.replaceAll("-", "");
};

export const parseRaceImportOptions = (
  args: string[],
  databaseUrl: string = LOCAL_DATABASE_URL,
): RaceImportOptions => {
  const { values } = parseArgs({
    args,
    options: {
      input: { type: "string" },
      "from-date": { type: "string" },
      "to-date": { type: "string" },
      apply: { type: "boolean", default: false },
    },
  });
  if (!values.input) throw new Error("--input is required");
  const fromDate = validateDate(values["from-date"]);
  const toDate = validateDate(values["to-date"]);
  if (fromDate > toDate) throw new Error("--from-date must not exceed --to-date");
  const url = new URL(databaseUrl);
  if (
    !["postgres:", "postgresql:"].includes(url.protocol) ||
    !["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)
  )
    throw new Error("Only a local PostgreSQL database is allowed");
  return { input: values.input, fromDate, toDate, apply: values.apply, databaseUrl };
};

export const planRaceImport = (
  text: string,
  range: Pick<RaceImportOptions, "fromDate" | "toDate">,
): RaceImportPlan => {
  const plan: RaceImportPlan = { tables: new Map(), skippedTypes: {}, outsideRange: 0 };
  for (const line of text.split(/\r?\n/).filter(Boolean)) {
    const bytes = encode(`${line}\r\n`, "cp932");
    const layout = optionalLayoutForRecord("jv", bytes);
    if (layout === undefined || !SUPPORTED_TABLES.has(layout.tableName)) {
      const type = line.slice(0, 2);
      plan.skippedTypes[type] = (plan.skippedTypes[type] ?? 0) + 1;
      continue;
    }
    const row = parseFixedRecord(layout, bytes);
    const date = `${row.kaisai_nen}${row.kaisai_tsukihi}`;
    if (date < range.fromDate || date > range.toDate) {
      plan.outsideRange += 1;
      continue;
    }
    const table = plan.tables.get(layout.tableName) ?? {
      layout,
      rows: new Map<string, RecordRow>(),
    };
    const key = rowKey(layout, row);
    const prior = table.rows.get(key);
    // Do not let an older card overwrite later results in an overlapping export.
    if (
      prior === undefined ||
      `${row.data_sakusei_nengappi}${row.data_kubun}` >=
        `${prior.data_sakusei_nengappi}${prior.data_kubun}`
    )
      table.rows.set(key, row);
    plan.tables.set(layout.tableName, table);
  }
  for (const { layout, rows } of plan.tables.values()) {
    for (const row of rows.values()) {
      catalogDeleteRowKeys(layout, row)
        .slice(1)
        .forEach((key) => rows.delete(key));
    }
  }
  if (plan.tables.size === 0) throw new Error("No RA/SE records in the requested date range");
  return plan;
};

export const buildRaceImportSql = (layout: RecordLayout): string => {
  const columns = layout.columns.map((column) => column.name);
  const updates = columns.filter((column) => !layout.primaryKey.includes(column));
  return `insert into ${quote(layout.tableName)} (${columns.map(quote).join(",")}) values (${columns.map((_, index) => `$${index + 1}`).join(",")}) on conflict (${layout.primaryKey.map(quote).join(",")}) do update set ${updates.map((column) => `${quote(column)}=excluded.${quote(column)}`).join(",")} where (excluded.data_sakusei_nengappi,excluded.data_kubun) >= (${quote(layout.tableName)}.data_sakusei_nengappi,${quote(layout.tableName)}.data_kubun)`;
};

export const runRaceImport = async (options: RaceImportOptions) => {
  const plan = planRaceImport(await readFile(options.input, "utf8"), options);
  const summary = {
    applied: options.apply,
    records: Object.fromEntries([...plan.tables].map(([name, table]) => [name, table.rows.size])),
    skippedTypes: plan.skippedTypes,
    outsideRange: plan.outsideRange,
  };
  if (!options.apply) return summary;
  const pool = new Pool({ connectionString: options.databaseUrl, connectionTimeoutMillis: 5000 });
  try {
    const client = await pool.connect();
    try {
      await client.query("begin");
      await client.query("set local statement_timeout = '30s'");
      for (const { layout, rows } of plan.tables.values()) {
        const sql = buildRaceImportSql(layout);
        for (const row of rows.values()) {
          await client.query(
            sql,
            layout.columns.map((column) => row[column.name]),
          );
          // Match production ingestion: remove provisional horse-number 00 rows.
          if (catalogDeleteRowKeys(layout, row).length > 1)
            await client.query(
              "delete from jvd_se where kaisai_nen=$1 and kaisai_tsukihi=$2 and keibajo_code=$3 and race_bango=$4 and ketto_toroku_bango=$5 and umaban='00'",
              [
                row.kaisai_nen,
                row.kaisai_tsukihi,
                row.keibajo_code,
                row.race_bango,
                row.ketto_toroku_bango,
              ],
            );
        }
      }
      await client.query("commit");
    } catch (error: unknown) {
      await client.query("rollback");
      throw error;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
  return summary;
};
