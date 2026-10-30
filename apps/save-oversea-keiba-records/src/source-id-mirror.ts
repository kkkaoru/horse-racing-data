// This file runs with Bun. Insert-or-fill-only production mirroring of oversea_runner_source_id for one meeting.
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { localHistoryEnvironment } from "./history-local-mirror";
import { createTargetPool, type TargetPool } from "./storage/history-connection";
import type { PostgresPoolClient, PostgresQueryResult } from "./storage/pg-client";

export interface SourceIdMirrorInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface SourceIdRow {
  readonly [column: string]: string | null;
}
export interface SourceIdFill {
  readonly row: SourceIdRow;
  readonly columns: readonly string[];
}
export interface SourceIdMirrorPlan {
  readonly inserts: readonly SourceIdRow[];
  readonly fills: readonly SourceIdFill[];
  // Today's or a future meeting only: scratches shift later gates, so local gates win.
  readonly gateRefreshes: readonly SourceIdRow[];
  readonly conflicts: readonly string[];
  readonly unchanged: number;
}
export interface SourceIdMirrorReport {
  readonly status: "complete";
  readonly operation: "planned" | "applied";
  readonly localRows: number;
  readonly inserted: number;
  readonly filled: number;
  readonly gatesRefreshed: number;
  readonly unchanged: number;
  readonly conflicts: readonly string[];
}
interface MeetingKey {
  readonly year: string;
  readonly monthDay: string;
  readonly venue: string;
}

const KEY_COLUMNS: readonly string[] = [
  "race_source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "umaban",
  "source",
];
const VALUE_COLUMNS: readonly string[] = [
  "source_horse_id",
  "source_jockey_id",
  "source_trainer_id",
  "source_owner_id",
  "source_url",
  "gate_number",
];
const COLUMNS: readonly string[] = [...KEY_COLUMNS, ...VALUE_COLUMNS];
const SELECT_SQL: string = `SELECT ${COLUMNS.map((column) => `${column}::text AS ${column}`).join(", ")}
FROM oversea_runner_source_id
WHERE race_source = 'jra' AND source = 'netkeiba' AND kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3
ORDER BY race_bango, umaban`;
const NULLABLE_COLUMNS: ReadonlySet<string> = new Set(VALUE_COLUMNS.slice(1));
// Nullable values travel as '' and become NULL in SQL, so no synthetic text is stored.
const placeholder = (column: string, index: number): string =>
  NULLABLE_COLUMNS.has(column)
    ? `NULLIF($${String(index + 1)}, '')${column === "gate_number" ? "::smallint" : ""}`
    : `$${String(index + 1)}`;
const INSERT_SQL: string = `INSERT INTO oversea_runner_source_id (${COLUMNS.join(", ")})
VALUES (${COLUMNS.map(placeholder).join(", ")})`;
const KEY_PREDICATE: string = KEY_COLUMNS.map(
  (column, index) => `${column} = $${String(index + 2)}`,
).join(" AND ");
const GATE_COLUMN: string = "gate_number";
const JST_OFFSET_MS: number = 9 * 60 * 60 * 1000;
const MEETING_PATTERN: RegExp = /^(\d{4}) (\d{4}) ([0-9A-Z]{2})$/u;
const DIRECTORY_MODE: number = 0o700;
const FILE_MODE: number = 0o600;
const USAGE: string =
  "Use source-ids-plan YEAR MMDD VENUE or source-ids-production YEAR MMDD VENUE NEW_RECEIPT_DIRECTORY --confirm-production.";

const rowKey = (row: SourceIdRow): string => KEY_COLUMNS.map((column) => row[column]).join("/");

const toRows = (result: PostgresQueryResult): SourceIdRow[] =>
  result.rows.map((row) =>
    Object.fromEntries(
      COLUMNS.map((column) => [column, typeof row[column] === "string" ? row[column] : null]),
    ),
  );

const differingColumns = (local: SourceIdRow, remote: SourceIdRow): string[] =>
  VALUE_COLUMNS.filter(
    (column) =>
      local[column] !== null && remote[column] !== null && local[column] !== remote[column],
  );

const fillableColumns = (local: SourceIdRow, remote: SourceIdRow): string[] =>
  VALUE_COLUMNS.filter((column) => local[column] !== null && remote[column] === null);

const jstToday = (nowMs: number): string =>
  new Date(nowMs + JST_OFFSET_MS).toISOString().slice(0, 10).replaceAll("-", "");

export const isCurrentMeeting = (meeting: MeetingKey, nowMs: number): boolean =>
  `${meeting.year}${meeting.monthDay}` >= jstToday(nowMs);

export const planSourceIdMirror = (
  local: readonly SourceIdRow[],
  remote: readonly SourceIdRow[],
  refreshGates: boolean,
): SourceIdMirrorPlan => {
  const remoteByKey: ReadonlyMap<string, SourceIdRow> = new Map(
    remote.map((row) => [rowKey(row), row]),
  );
  const existing: readonly [SourceIdRow, SourceIdRow][] = local.flatMap(
    (row): [SourceIdRow, SourceIdRow][] => {
      const match: SourceIdRow | undefined = remoteByKey.get(rowKey(row));
      return match === undefined ? [] : [[row, match]];
    },
  );
  const fills: SourceIdFill[] = existing
    .map(([row, match]) => ({
      row,
      columns: fillableColumns(row, match).filter(
        (column) => !refreshGates || column !== GATE_COLUMN,
      ),
    }))
    .filter((fill) => fill.columns.length > 0);
  const gateRefreshes: SourceIdRow[] = refreshGates
    ? existing.filter(([row, match]) => row[GATE_COLUMN] !== match[GATE_COLUMN]).map(([row]) => row)
    : [];
  const refreshed: ReadonlySet<string> = new Set(gateRefreshes.map(rowKey));
  return {
    inserts: local.filter((row) => !remoteByKey.has(rowKey(row))),
    fills,
    gateRefreshes,
    conflicts: existing.flatMap(([row, match]) =>
      differingColumns(row, match)
        .filter((column) => !refreshGates || column !== GATE_COLUMN)
        .map((column) => `${rowKey(row)}:${column}`),
    ),
    unchanged: existing.filter(
      ([row]) => !refreshed.has(rowKey(row)) && !fills.some((fill) => fill.row === row),
    ).length,
  };
};

const parseMeeting = (args: readonly string[]): MeetingKey => {
  const match: RegExpExecArray | null = MEETING_PATTERN.exec(args.join(" "));
  assert(match !== null, USAGE);
  return { year: String(match[1]), monthDay: String(match[2]), venue: String(match[3]) };
};

const selectRows = async (
  client: Pick<PostgresPoolClient, "query">,
  meeting: MeetingKey,
): Promise<SourceIdRow[]> =>
  toRows(await client.query(SELECT_SQL, [meeting.year, meeting.monthDay, meeting.venue]));

const applyPlan = async (
  client: Pick<PostgresPoolClient, "query">,
  plan: SourceIdMirrorPlan,
): Promise<void> => {
  for (const row of plan.inserts) {
    await client.query(
      INSERT_SQL,
      COLUMNS.map((column) => row[column] ?? ""),
    );
  }
  for (const row of plan.gateRefreshes) {
    await client.query(
      `UPDATE oversea_runner_source_id SET gate_number = NULLIF($1, '')::smallint, updated_at = now() WHERE ${KEY_PREDICATE}`,
      [row[GATE_COLUMN] ?? "", ...KEY_COLUMNS.map((key) => String(row[key]))],
    );
  }
  for (const fill of plan.fills) {
    for (const column of fill.columns) {
      await client.query(
        `UPDATE oversea_runner_source_id SET ${column} = $1${column === "gate_number" ? "::smallint" : ""}, updated_at = now() WHERE ${KEY_PREDICATE} AND ${column} IS NULL`,
        [String(fill.row[column]), ...KEY_COLUMNS.map((key) => String(fill.row[key]))],
      );
    }
  }
};

const summary = (
  operation: SourceIdMirrorReport["operation"],
  local: readonly SourceIdRow[],
  plan: SourceIdMirrorPlan,
): SourceIdMirrorReport => ({
  status: "complete",
  operation,
  localRows: local.length,
  inserted: plan.inserts.length,
  filled: plan.fills.length,
  gatesRefreshed: plan.gateRefreshes.length,
  unchanged: plan.unchanged,
  conflicts: plan.conflicts,
});

const writeTransaction = async (
  production: TargetPool,
  local: readonly SourceIdRow[],
  meeting: MeetingKey,
  refreshGates: boolean,
): Promise<SourceIdMirrorPlan> => {
  const client: PostgresPoolClient = await production.pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("LOCK TABLE oversea_runner_source_id IN SHARE ROW EXCLUSIVE MODE");
    const plan: SourceIdMirrorPlan = planSourceIdMirror(
      local,
      await selectRows(client, meeting),
      refreshGates,
    );
    assert.equal(
      plan.conflicts.length,
      0,
      "Production has conflicting source ids; nothing written.",
    );
    await applyPlan(client, plan);
    const readback: SourceIdMirrorPlan = planSourceIdMirror(
      local,
      await selectRows(client, meeting),
      refreshGates,
    );
    assert.equal(
      readback.inserts.length +
        readback.fills.length +
        readback.gateRefreshes.length +
        readback.conflicts.length,
      0,
      "Production readback differs from local source ids.",
    );
    await client.query("COMMIT");
    return plan;
  } catch (error: unknown) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

export const runSourceIdMirror = async ({
  argv,
  env,
}: SourceIdMirrorInput): Promise<SourceIdMirrorReport> => {
  const [action, ...rest]: readonly string[] = argv;
  const apply: boolean = action === "source-ids-production";
  assert(
    (action === "source-ids-plan" && rest.length === 3) ||
      (apply && rest.length === 5 && rest[4] === "--confirm-production"),
    USAGE,
  );
  const meeting: MeetingKey = parseMeeting(rest.slice(0, 3));
  const refreshGates: boolean = isCurrentMeeting(meeting, Date.now());
  const localPool: TargetPool = createTargetPool(localHistoryEnvironment(env), "local");
  const production: TargetPool = createTargetPool(env, "production");
  try {
    const local: readonly SourceIdRow[] = await selectRows(localPool.pool, meeting);
    assert(local.length > 0, "No local source ids exist for this meeting.");
    if (!apply)
      return summary(
        "planned",
        local,
        planSourceIdMirror(local, await selectRows(production.pool, meeting), refreshGates),
      );
    const directory: string = String(rest[3]);
    await mkdir(directory, { mode: DIRECTORY_MODE });
    await writeFile(
      join(directory, "pending.json"),
      JSON.stringify({ meeting, startedAt: new Date().toISOString(), localRows: local }),
      { flag: "wx", mode: FILE_MODE },
    );
    const report: SourceIdMirrorReport = summary(
      "applied",
      local,
      await writeTransaction(production, local, meeting, refreshGates),
    );
    await writeFile(
      join(directory, "receipt.json"),
      JSON.stringify({ meeting, verifiedAt: new Date().toISOString(), report }),
      { flag: "wx", mode: FILE_MODE },
    );
    return report;
  } finally {
    await Promise.all([localPool.pool.end(), production.pool.end()]);
  }
};
