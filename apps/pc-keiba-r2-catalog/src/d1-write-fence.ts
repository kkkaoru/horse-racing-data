// Runs with bun; prepare a fail-closed DML fence. Installation/activation need a durable input handoff.
import { createHash } from "node:crypto";
import type { D1SnapshotQuery } from "./d1-snapshot";

export interface D1WriteFenceIdentity {
  databaseId: string;
  owner: string;
  epoch: string;
}
export interface D1WriteFenceInput extends D1WriteFenceIdentity {
  tables: readonly string[];
}
export interface D1WriteFenceTrigger {
  name: string;
  sql: string;
}
export interface D1WriteFencePlan {
  createFence: string;
  initialize: D1SnapshotQuery;
  triggers: readonly D1WriteFenceTrigger[];
  planHash: string;
}
export const D1_WRITE_FENCE_TABLE: string = "__pc_keiba_catalog_write_fence_v1";
const UUID: RegExp = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/u;
const OWNER: RegExp = /^[a-zA-Z0-9_-]{1,128}$/u;
const MAX_EPOCH: bigint = 9223372036854775807n;
const MAX_TABLES: number = 100;
const OPERATIONS: readonly string[] = ["INSERT", "UPDATE", "DELETE"];
const digest = (value: string): string => createHash("sha256").update(value).digest("hex");
const quote = (value: string): string => `"${value.replaceAll('"', '""')}"`;
const validateIdentity = (input: D1WriteFenceIdentity): void => {
  if (
    !UUID.test(input.databaseId) ||
    input.databaseId.length !== 36 ||
    !OWNER.test(input.owner) ||
    input.owner.trim() !== input.owner ||
    !/^[1-9]\d{0,18}$/u.test(input.epoch) ||
    BigInt(input.epoch).toString() !== input.epoch ||
    BigInt(input.epoch) > MAX_EPOCH
  )
    throw new Error("Invalid D1 source fence identity");
};
const validTable = (table: string): boolean =>
  table.length > 0 &&
  table.length <= 256 &&
  !table.includes("\0") &&
  !/^(?:_cf_|sqlite_|__pc_keiba_)/iu.test(table);
const tableTriggers = (table: string, identity: D1WriteFenceIdentity): D1WriteFenceTrigger[] =>
  OPERATIONS.map((operation) => {
    const name: string = `__pc_keiba_source_fence_${digest(table)}_${operation.toLowerCase()}`;
    return {
      name,
      sql: `CREATE TRIGGER ${quote(name)} BEFORE ${operation} ON ${quote(table)} WHEN COALESCE((SELECT mode FROM ${quote(D1_WRITE_FENCE_TABLE)} WHERE id = 1 AND database_id = '${identity.databaseId}'), 'frozen') <> 'open' BEGIN SELECT RAISE(ABORT, 'Catalog cutover source is fenced'); END`,
    };
  });

/** Install the complete plan atomically only after validating existing schema/objects and ownership. */
export const buildD1WriteFencePlan = (input: D1WriteFenceInput): D1WriteFencePlan => {
  validateIdentity(input);
  if (
    input.tables.length < 1 ||
    input.tables.length > MAX_TABLES ||
    input.tables.some((table) => !validTable(table)) ||
    new Set(input.tables.map((table) => table.toLowerCase())).size !== input.tables.length
  )
    throw new Error("Invalid D1 source fence table set");
  const createFence: string = `CREATE TABLE ${quote(D1_WRITE_FENCE_TABLE)} (id INTEGER PRIMARY KEY CHECK (id = 1), database_id TEXT NOT NULL, owner TEXT NOT NULL, epoch INTEGER NOT NULL CHECK (epoch > 0), mode TEXT NOT NULL CHECK (mode IN ('open', 'frozen')))`;
  const initialize: D1SnapshotQuery = {
    sql: `INSERT INTO ${quote(D1_WRITE_FENCE_TABLE)} (id, database_id, owner, epoch, mode) VALUES (1, ?, ?, CAST(? AS INTEGER), 'open')`,
    params: [input.databaseId, input.owner, input.epoch],
  };
  const triggers: D1WriteFenceTrigger[] = [...input.tables]
    .sort()
    .flatMap((table) => tableTriggers(table, input));
  return {
    createFence,
    initialize,
    triggers,
    planHash: digest(JSON.stringify({ createFence, initialize, triggers })),
  };
};

/** Compare owner+epoch before closing writes. This module deliberately provides no reopen/prune API. */
export const buildD1FreezeQuery = (input: D1WriteFenceIdentity): D1SnapshotQuery => {
  validateIdentity(input);
  return {
    sql: `UPDATE ${quote(D1_WRITE_FENCE_TABLE)} SET mode = 'frozen' WHERE id = 1 AND database_id = ? AND owner = ? AND epoch = CAST(? AS INTEGER) AND mode = 'open' RETURNING CAST(epoch AS TEXT) AS fence_epoch`,
    params: [input.databaseId, input.owner, input.epoch],
  };
};
