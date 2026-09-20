// Runs with bun; private RPC database for an atomic ingestion-control graph, not domain data.
import { DurableObject } from "cloudflare:workers";
import { createHash } from "node:crypto";

export interface ControlStatement {
  sql: string;
  params: (string | number | null)[];
}
export interface ControlCommand {
  requestId: string;
  statements: ControlStatement[];
}
export interface ControlEvent extends Record<string, SqlStorageValue> {
  sequence: number;
  request_id: string;
  statements: string;
  created_at: string;
}
const SCHEMA_KEY: string = "control/schema-v1";
const RESERVED: RegExp = /__control_/iu;
const encoder: TextEncoder = new TextEncoder();
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const fingerprint = (value: string): string => createHash("sha256").update(value).digest("hex");
const boundedSql = (value: unknown): string => {
  if (
    typeof value !== "string" ||
    value.trim().length === 0 ||
    encoder.encode(value).byteLength > 65536 ||
    RESERVED.test(value)
  )
    throw new Error("Invalid control-plane SQL");
  return value;
};
export const parseControlCommand = (value: unknown): ControlCommand => {
  if (
    !isRecord(value) ||
    typeof value.requestId !== "string" ||
    !/^[a-zA-Z0-9_-]{1,128}$/u.test(value.requestId) ||
    !Array.isArray(value.statements) ||
    value.statements.length === 0 ||
    value.statements.length > 100
  )
    throw new Error("Invalid control-plane command");
  if (encoder.encode(JSON.stringify(value)).byteLength > 1024 * 1024)
    throw new Error("Control-plane command exceeds byte limit");
  const statements: ControlStatement[] = value.statements.map((statement: unknown) => {
    if (!isRecord(statement) || !Array.isArray(statement.params) || statement.params.length > 1000)
      throw new Error("Invalid control-plane parameters");
    const params = statement.params.map((param: unknown) => {
      if (
        param === null ||
        typeof param === "string" ||
        (typeof param === "number" && Number.isFinite(param))
      )
        return param;
      throw new Error("Unsupported control-plane binding");
    });
    return { sql: boundedSql(statement.sql), params };
  });
  return { requestId: value.requestId, statements };
};

/**
 * One instance per control graph whose tables require cross-table transactions.
 * Do not put odds/weather/domain data here: those remain Catalog records.
 * No public HTTP SQL proxy is exposed. All mutations retain a Catalog export outbox.
 */
export class IngestionControlDatabase extends DurableObject<unknown> {
  bootstrap(schema: string[]): boolean {
    if (!Array.isArray(schema) || schema.length === 0 || schema.length > 100)
      throw new Error("Invalid control-plane schema");
    const statements = schema.map((sql) => boundedSql(sql));
    if (statements.some((sql) => !/^\s*create\s+(?:table|(?:unique\s+)?index)\b/iu.test(sql)))
      throw new Error("Bootstrap only accepts schema creation");
    const encoded: string = JSON.stringify(statements);
    if (encoder.encode(encoded).byteLength > 1024 * 1024)
      throw new Error("Control-plane schema exceeds byte limit");
    const signature: string = fingerprint(encoded);
    return this.ctx.storage.transactionSync(() => {
      const current: string | undefined = this.ctx.storage.kv.get<string>(SCHEMA_KEY);
      if (current !== undefined) {
        if (current !== signature)
          throw new Error("Control-plane schema already initialized differently");
        return false;
      }
      this.ctx.storage.sql.exec(
        "CREATE TABLE __control_receipts (request_id TEXT PRIMARY KEY, digest TEXT NOT NULL, response TEXT NOT NULL); CREATE TABLE __control_outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL UNIQUE, statements TEXT NOT NULL, created_at TEXT NOT NULL)",
      );
      for (const sql of statements) this.ctx.storage.sql.exec(sql);
      this.ctx.storage.kv.put(SCHEMA_KEY, signature);
      return true;
    });
  }

  execute(value: unknown): Response {
    const command: ControlCommand = parseControlCommand(value);
    const descriptor: string = JSON.stringify(command.statements);
    const digest: string = fingerprint(descriptor);
    const body: string = this.ctx.storage.transactionSync(() => {
      if (this.ctx.storage.kv.get(SCHEMA_KEY) === undefined)
        throw new Error("Control-plane schema is not initialized");
      const previous = this.ctx.storage.sql
        .exec<{ digest: string; response: string }>(
          "SELECT digest, response FROM __control_receipts WHERE request_id = ?",
          command.requestId,
        )
        .toArray()[0];
      if (previous !== undefined) {
        if (previous.digest !== digest)
          throw new Error("Control request identifier reused with different statements");
        return previous.response;
      }
      const results = command.statements.map((statement) => this.#executeStatement(statement));
      const encoded: string = JSON.stringify(results);
      if (encoder.encode(encoded).byteLength > 1024 * 1024)
        throw new Error("Control-plane result exceeds byte limit");
      // Retain even no-op decisions: a failed compare-and-set must not succeed on replay.
      // Fresh reads use fresh request IDs; receipts are not a shared result cache.
      this.ctx.storage.sql.exec(
        "INSERT INTO __control_receipts VALUES (?, ?, ?)",
        command.requestId,
        digest,
        encoded,
      );
      if (results.some((result) => result.meta.rows_written > 0)) {
        this.ctx.storage.sql.exec(
          "INSERT INTO __control_outbox (request_id, statements, created_at) VALUES (?, ?, ?)",
          command.requestId,
          descriptor,
          new Date().toISOString(),
        );
      }
      return encoded;
    });
    return new Response(body, {
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    });
  }

  #executeStatement(statement: ControlStatement): D1Result<Record<string, SqlStorageValue>> {
    const started: number = performance.now();
    const cursor = this.ctx.storage.sql.exec(statement.sql, ...statement.params);
    const rows: Record<string, SqlStorageValue>[] = [];
    for (const row of cursor) {
      if (rows.length >= 10000) throw new Error("Control-plane result exceeds row limit");
      if (Object.values(row).some((cell) => cell instanceof ArrayBuffer))
        throw new Error("Binary control results require an explicit SQL encoding");
      rows.push(row);
    }
    const changes: number =
      cursor.rowsWritten === 0
        ? 0
        : this.ctx.storage.sql.exec<{ count: number }>("SELECT changes() AS count").one().count;
    const lastRowId: number = this.ctx.storage.sql
      .exec<{ value: number }>("SELECT last_insert_rowid() AS value")
      .one().value;
    return {
      success: true,
      results: rows,
      meta: {
        changes,
        changed_db: cursor.rowsWritten > 0,
        duration: performance.now() - started,
        last_row_id: lastRowId,
        rows_read: cursor.rowsRead,
        rows_written: cursor.rowsWritten,
        size_after: this.ctx.storage.sql.databaseSize,
        served_by_primary: true,
      },
    };
  }

  outbox(afterSequence: number, limit: number): Response {
    if (
      !Number.isSafeInteger(afterSequence) ||
      afterSequence < 0 ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 100
    )
      throw new Error("Invalid control outbox cursor");
    const cursor = this.ctx.storage.sql.exec<ControlEvent>(
      "SELECT sequence, request_id, statements, created_at FROM __control_outbox WHERE sequence > ? ORDER BY sequence LIMIT ?",
      afterSequence,
      limit,
    );
    const events: ControlEvent[] = [];
    let bytes: number = 2;
    for (const event of cursor) {
      const size: number = encoder.encode(JSON.stringify(event)).byteLength + 1;
      if (bytes + size > 4 * 1024 * 1024) break;
      events.push(event);
      bytes += size;
    }
    // Stream the JSON body through RPC rather than returning a large structured value.
    return Response.json(events, { headers: { "Cache-Control": "no-store" } });
  }
}
