// This file runs with Bun. Stores pedigrees parsed from archived horse pages (already fetched through the
// production proxy) into oversea_horse_pedigree: insert-only; stored rows are never overwritten.
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { localHistoryEnvironment } from "./history-local-mirror";
import {
  parseSecondaryHorsePedigreeHtml,
  type SecondaryHorsePedigree,
  type SecondaryPedigreeMarkupProfile,
} from "./sources/secondary-pedigree-parser";
import { createTargetPool, type TargetPool } from "./storage/history-connection";
import type { PostgresPoolClient, PostgresQueryResult } from "./storage/pg-client";

export interface PedigreeStoreInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface PedigreeRow {
  readonly [column: string]: string;
}
export interface PedigreeStorePlan {
  readonly inserts: readonly PedigreeRow[];
  readonly conflicts: readonly string[];
  readonly unchanged: number;
}
export interface PedigreeStoreReport {
  readonly status: "complete";
  readonly operation: "planned" | "applied";
  readonly target: "local" | "production";
  readonly archivedHorses: number;
  readonly parsed: number;
  readonly withoutPedigree: number;
  readonly inserted: number;
  readonly unchanged: number;
  readonly conflicts: readonly string[];
}

const SOURCE: string = "netkeiba";
const PROFILE_ENV: string = "OVERSEA_PEDIGREE_MARKUP_PROFILE_PATH";
const HORSE_DIRECTORY_PATTERN: RegExp = /^horse-([0-9A-Za-z]+)(?:-v\d+)?$/u;
const RAW_PAGE_SUFFIX: string = ".raw";
const VALUE_COLUMNS: readonly string[] = [
  "sire_source_id",
  "sire_name",
  "sire_sire_source_id",
  "sire_sire_name",
  "dam_source_id",
  "dam_name",
  "dam_sire_source_id",
  "dam_sire_name",
  "source_url",
];
const COLUMNS: readonly string[] = ["source", "source_horse_id", ...VALUE_COLUMNS];
const SELECT_SQL: string = `SELECT ${COLUMNS.join(", ")} FROM oversea_horse_pedigree
WHERE source = $1 AND source_horse_id = ANY($2::text[]) ORDER BY source_horse_id`;
const INSERT_SQL: string = `INSERT INTO oversea_horse_pedigree (${COLUMNS.join(", ")})
VALUES (${COLUMNS.map((_column, index) => `$${String(index + 1)}`).join(", ")})`;
const USAGE: string =
  "Use pedigree-plan HISTORY_ROOT TARGET or pedigree-apply HISTORY_ROOT TARGET --confirm-write; TARGET is local or production.";

export const pedigreeRow = (pedigree: SecondaryHorsePedigree): PedigreeRow => ({
  source: SOURCE,
  source_horse_id: pedigree.sourceHorseId,
  sire_source_id: pedigree.sire.sourceHorseId,
  sire_name: pedigree.sire.name,
  sire_sire_source_id: pedigree.sireSire.sourceHorseId,
  sire_sire_name: pedigree.sireSire.name,
  dam_source_id: pedigree.dam.sourceHorseId,
  dam_name: pedigree.dam.name,
  dam_sire_source_id: pedigree.damSire.sourceHorseId,
  dam_sire_name: pedigree.damSire.name,
  source_url: pedigree.sourceUrl,
});

export const planPedigreeStore = (
  parsed: readonly PedigreeRow[],
  existing: readonly PedigreeRow[],
): PedigreeStorePlan => {
  const existingById: ReadonlyMap<string, PedigreeRow> = new Map(
    existing.map((row) => [String(row.source_horse_id), row]),
  );
  const conflicts: string[] = parsed.flatMap((row) => {
    const current: PedigreeRow | undefined = existingById.get(String(row.source_horse_id));
    return current === undefined
      ? []
      : VALUE_COLUMNS.filter((column) => current[column] !== row[column]).map(
          (column) => `${String(row.source_horse_id)}:${column}`,
        );
  });
  return {
    inserts: parsed.filter((row) => !existingById.has(String(row.source_horse_id))),
    conflicts,
    unchanged: parsed.filter((row) => existingById.has(String(row.source_horse_id))).length,
  };
};

const isProfile = (value: unknown): value is SecondaryPedigreeMarkupProfile =>
  typeof value === "object" &&
  value !== null &&
  "tableMarker" in value &&
  typeof value.tableMarker === "string" &&
  "horsePathPrefix" in value &&
  typeof value.horsePathPrefix === "string" &&
  "sourceUrlTemplate" in value &&
  typeof value.sourceUrlTemplate === "string";

const loadProfile = async (
  env: PedigreeStoreInput["env"],
): Promise<SecondaryPedigreeMarkupProfile> => {
  const path: string | undefined = env[PROFILE_ENV];
  assert(path !== undefined && path !== "", `${PROFILE_ENV} is required.`);
  const profile: unknown = JSON.parse(await readFile(path, "utf8"));
  assert(isProfile(profile), "The pedigree markup profile is incomplete.");
  return profile;
};

// A horse page without the pedigree table (or with an incomplete one) is a source gap: skipped, never filled.
const parseHorseDirectory = async (
  root: string,
  directory: string,
  profile: SecondaryPedigreeMarkupProfile,
): Promise<PedigreeRow | null> => {
  const sourceHorseId: string = String(HORSE_DIRECTORY_PATTERN.exec(directory)?.[1]);
  const pages: string[] = (await readdir(join(root, directory, "pages"))).filter((name) =>
    name.endsWith(RAW_PAGE_SUFFIX),
  );
  const parsed: (PedigreeRow | null)[] = await Promise.all(
    pages.toSorted().map(async (page) => {
      const html: string = await readFile(join(root, directory, "pages", page), "utf8");
      try {
        return pedigreeRow(parseSecondaryHorsePedigreeHtml(html, sourceHorseId, profile));
      } catch {
        return null;
      }
    }),
  );
  return parsed.find((row): row is PedigreeRow => row !== null) ?? null;
};

const toRows = (result: PostgresQueryResult): PedigreeRow[] =>
  result.rows.map((row) =>
    Object.fromEntries(COLUMNS.map((column) => [column, String(row[column] ?? "")])),
  );

const selectExisting = async (
  client: Pick<PostgresPoolClient, "query">,
  rows: readonly PedigreeRow[],
): Promise<PedigreeRow[]> =>
  toRows(await client.query(SELECT_SQL, [SOURCE, rows.map((row) => String(row.source_horse_id))]));

const writeInserts = async (
  target: TargetPool,
  rows: readonly PedigreeRow[],
): Promise<PedigreeStorePlan> => {
  const client: PostgresPoolClient = await target.pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("LOCK TABLE oversea_horse_pedigree IN SHARE ROW EXCLUSIVE MODE");
    const plan: PedigreeStorePlan = planPedigreeStore(rows, await selectExisting(client, rows));
    // A stored row that differs is kept as-is (never overwritten) and only reported.
    for (const row of plan.inserts) {
      await client.query(
        INSERT_SQL,
        COLUMNS.map((column) => String(row[column])),
      );
    }
    const readback: PedigreeStorePlan = planPedigreeStore(rows, await selectExisting(client, rows));
    assert.equal(readback.inserts.length, 0, "Pedigree readback is missing inserted rows.");
    assert.deepEqual(readback.conflicts, plan.conflicts, "Pedigree readback changed stored rows.");
    await client.query("COMMIT");
    return plan;
  } catch (error: unknown) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

export const runPedigreeStore = async ({
  argv,
  env,
}: PedigreeStoreInput): Promise<PedigreeStoreReport> => {
  const [action, root, target, confirmation]: readonly (string | undefined)[] = argv;
  const apply: boolean = action === "pedigree-apply";
  assert(
    root !== undefined &&
      (target === "local" || target === "production") &&
      ((action === "pedigree-plan" && argv.length === 3) ||
        (apply && argv.length === 4 && confirmation === "--confirm-write")),
    USAGE,
  );
  const profile: SecondaryPedigreeMarkupProfile = await loadProfile(env);
  const directories: string[] = (await readdir(root)).filter((name) =>
    HORSE_DIRECTORY_PATTERN.test(name),
  );
  const parsed: (PedigreeRow | null)[] = await Promise.all(
    directories.map((directory) => parseHorseDirectory(root, directory, profile)),
  );
  // Several snapshots (e.g. -v2) of one horse resolve to one row.
  const rows: PedigreeRow[] = [
    ...new Map(
      parsed
        .filter((row): row is PedigreeRow => row !== null)
        .map((row) => [String(row.source_horse_id), row]),
    ).values(),
  ];
  const archivedHorses: number = new Set(
    directories.map((name) => HORSE_DIRECTORY_PATTERN.exec(name)?.[1]),
  ).size;
  const pool: TargetPool = createTargetPool(
    target === "local" ? localHistoryEnvironment(env) : env,
    target,
  );
  try {
    const plan: PedigreeStorePlan =
      apply && rows.length > 0
        ? await writeInserts(pool, rows)
        : planPedigreeStore(rows, rows.length === 0 ? [] : await selectExisting(pool.pool, rows));
    return {
      status: "complete",
      operation: apply ? "applied" : "planned",
      target,
      archivedHorses,
      parsed: rows.length,
      withoutPedigree: archivedHorses - rows.length,
      inserted: plan.inserts.length,
      unchanged: plan.unchanged,
      conflicts: plan.conflicts,
    };
  } finally {
    await pool.pool.end();
  }
};
