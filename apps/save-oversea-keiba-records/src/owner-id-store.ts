// This file runs with Bun. Fills a meeting's missing oversea_runner_source_id.source_owner_id values
// from the owner link of each runner's archived horse page (already fetched through the proxy).
// The owner-link pattern lives in an operator-owned private profile, never in this repository.
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { localHistoryEnvironment } from "./history-local-mirror";
import { createTargetPool, type TargetPool } from "./storage/history-connection";

export interface OwnerIdStoreInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface HorseOwnerId {
  readonly sourceHorseId: string;
  readonly sourceOwnerId: string;
}
export interface OwnerIdStoreReport {
  readonly status: "complete";
  readonly operation: "planned" | "applied";
  readonly target: "local" | "production";
  readonly archivedOwners: number;
  readonly runnersWithoutOwner: number;
  readonly filled: number;
}

const PROFILE_ENV: string = "OVERSEA_OWNER_ID_PROFILE_PATH";
const HORSE_DIRECTORY_PATTERN: RegExp = /^horse-([0-9A-Za-z]+)(?:-v\d+)?$/u;
const RAW_PAGE_SUFFIX: string = ".raw";
const OWNER_ID_PATTERN: RegExp = /^[0-9A-Za-z]+$/u;
const KEY_PATTERN: RegExp = /^\d{4}$/u;
const VENUE_PATTERN: RegExp = /^[0-9A-Z]{2}$/u;
const MISSING_SQL: string = `SELECT DISTINCT source_horse_id FROM oversea_runner_source_id
WHERE source = 'netkeiba' AND kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3
  AND source_owner_id IS NULL`;
// A horse page shows the current owner, so only this meeting's rows are filled, never past ones.
const UPDATE_SQL: string = `UPDATE oversea_runner_source_id SET source_owner_id = $5, updated_at = now()
WHERE source = 'netkeiba' AND kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3
  AND source_horse_id = $4 AND source_owner_id IS NULL`;
const USAGE: string =
  "Use owner-ids-plan HISTORY_ROOT YYYY MMDD VENUE TARGET or owner-ids-apply HISTORY_ROOT YYYY MMDD VENUE TARGET --confirm-write; TARGET is local or production.";

export const ownerIdFromPage = (html: string, pattern: RegExp): string | null => {
  const owner: string | undefined = pattern.exec(html)?.[1];
  return owner !== undefined && OWNER_ID_PATTERN.test(owner) ? owner : null;
};

const loadPattern = async (env: OwnerIdStoreInput["env"]): Promise<RegExp> => {
  const path: string | undefined = env[PROFILE_ENV];
  assert(path !== undefined && path !== "", `${PROFILE_ENV} is required.`);
  const profile: unknown = JSON.parse(await readFile(path, "utf8"));
  assert(
    typeof profile === "object" &&
      profile !== null &&
      "ownerIdPattern" in profile &&
      typeof profile.ownerIdPattern === "string",
    "The owner id profile needs an ownerIdPattern.",
  );
  return new RegExp(profile.ownerIdPattern, "u");
};

const directoryOwner = async (
  root: string,
  directory: string,
  pattern: RegExp,
): Promise<HorseOwnerId | null> => {
  const pages: string[] = (await readdir(join(root, directory, "pages")))
    .filter((name) => name.endsWith(RAW_PAGE_SUFFIX))
    .toSorted();
  const owners: (string | null)[] = await Promise.all(
    pages.map(async (page) =>
      ownerIdFromPage(await readFile(join(root, directory, "pages", page), "utf8"), pattern),
    ),
  );
  const owner: string | undefined = owners.find((value): value is string => value !== null);
  return owner === undefined
    ? null
    : { sourceHorseId: String(HORSE_DIRECTORY_PATTERN.exec(directory)?.[1]), sourceOwnerId: owner };
};

export const archivedOwnerIds = async (
  root: string,
  pattern: RegExp,
): Promise<Map<string, string>> => {
  const directories: string[] = (await readdir(root)).filter((name) =>
    HORSE_DIRECTORY_PATTERN.test(name),
  );
  const owners: (HorseOwnerId | null)[] = await Promise.all(
    directories.map((directory) => directoryOwner(root, directory, pattern)),
  );
  return new Map(
    owners
      .filter((owner): owner is HorseOwnerId => owner !== null)
      .map((owner) => [owner.sourceHorseId, owner.sourceOwnerId]),
  );
};

const fillOwners = async (
  pool: TargetPool,
  key: readonly string[],
  fills: readonly HorseOwnerId[],
): Promise<void> => {
  const client = await pool.pool.connect();
  try {
    await client.query("BEGIN");
    for (const fill of fills) {
      await client.query(UPDATE_SQL, [...key, fill.sourceHorseId, fill.sourceOwnerId]);
    }
    const remaining = await client.query(MISSING_SQL, [...key]);
    const filledIds: ReadonlySet<string> = new Set(fills.map((fill) => fill.sourceHorseId));
    assert(
      remaining.rows.every((row) => !filledIds.has(String(row.source_horse_id))),
      "Owner id readback is missing filled values.",
    );
    await client.query("COMMIT");
  } catch (error: unknown) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

export const runOwnerIdStore = async ({
  argv,
  env,
}: OwnerIdStoreInput): Promise<OwnerIdStoreReport> => {
  const [action, root, year, monthDay, venue, target, confirmation] = argv;
  const apply: boolean =
    action === "owner-ids-apply" && argv.length === 7 && confirmation === "--confirm-write";
  assert(
    root !== undefined &&
      KEY_PATTERN.test(String(year)) &&
      KEY_PATTERN.test(String(monthDay)) &&
      VENUE_PATTERN.test(String(venue)) &&
      (target === "local" || target === "production") &&
      ((action === "owner-ids-plan" && argv.length === 6) || apply),
    USAGE,
  );
  const owners: Map<string, string> = await archivedOwnerIds(root, await loadPattern(env));
  const key: readonly string[] = [String(year), String(monthDay), String(venue)];
  const pool: TargetPool = createTargetPool(
    target === "local" ? localHistoryEnvironment(env) : env,
    target,
  );
  try {
    const missing = await pool.pool.query(MISSING_SQL, [...key]);
    const fills: HorseOwnerId[] = missing.rows.flatMap((row) => {
      const owner: string | undefined = owners.get(String(row.source_horse_id));
      return owner === undefined
        ? []
        : [{ sourceHorseId: String(row.source_horse_id), sourceOwnerId: owner }];
    });
    if (apply) await fillOwners(pool, key, fills);
    return {
      status: "complete",
      operation: apply ? "applied" : "planned",
      target,
      archivedOwners: owners.size,
      runnersWithoutOwner: missing.rows.length,
      filled: fills.length,
    };
  } finally {
    await pool.pool.end();
  }
};
