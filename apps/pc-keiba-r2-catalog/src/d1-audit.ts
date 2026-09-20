// Runs with bun; authenticated callers audit immutable, completed baseline partitions.
import { createHash } from "node:crypto";
import { parseD1SnapshotPage } from "./d1-snapshot";
import type { CacheStore, Fetcher, KvStore } from "./types";

interface AuditFilter {
  column: string;
  value: string | null;
}
export interface D1AuditInput {
  operation: "manifest" | "query";
  snapshotId: string;
  databaseName: string;
  tableName: string;
  manifestId: string;
  afterRowId: string | null;
  limit: number;
  filters: AuditFilter[];
}
export interface D1AuditDependencies {
  namespace: string;
  cacheScope: string;
  cacheOrigin: string;
  cache: CacheStore;
  kv: KvStore;
  query(sql: string): Promise<Record<string, unknown>[]>;
}
interface Manifest {
  id: string;
  columns: { name: string }[];
  copiedRows: number;
}
export class D1AuditInputError extends Error {}
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const text = (value: unknown): string => {
  if (typeof value !== "string" || !/^[a-zA-Z0-9_-]{1,128}$/u.test(value))
    throw new D1AuditInputError("Invalid audit identity");
  return value;
};
const quote = (value: string): string => `'${value.replaceAll("'", "''")}'`;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const CACHE_SECONDS: number = 600;
const KV_SECONDS: number = 86400;
const MAX_CACHE_BYTES: number = 2 * 1024 * 1024;
const encoder: TextEncoder = new TextEncoder();

export const boundedAuditFetch =
  (fetchImpl: Fetcher): Fetcher =>
  async (input, init) => {
    const result: Response = await fetchImpl(input, {
      ...init,
      signal: AbortSignal.timeout(30000),
    });
    if (result.body === null) return result;
    const size = { bytes: 0 };
    const stream = result.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, controller) {
          size.bytes += chunk.byteLength;
          if (size.bytes > 8 * 1024 * 1024)
            throw new Error("Audit provider response exceeds byte limit");
          controller.enqueue(chunk);
        },
      }),
    );
    return new Response(stream, { status: result.status, headers: result.headers });
  };

export const parseD1AuditInput = (body: Record<string, unknown>): D1AuditInput => {
  if (body.operation !== "manifest" && body.operation !== "query")
    throw new D1AuditInputError("Invalid audit operation");
  const manifestId: string = body.operation === "manifest" ? "" : text(body.manifestId);
  if (body.operation === "query" && !/^[a-f0-9]{64}$/u.test(manifestId))
    throw new D1AuditInputError("Invalid manifest revision");
  const after: unknown = body.afterRowId ?? null;
  if (
    after !== null &&
    (typeof after !== "string" ||
      !/^-?\d+$/u.test(after) ||
      BigInt(after) < -9223372036854775808n ||
      BigInt(after) > 9223372036854775807n)
  )
    throw new D1AuditInputError("Invalid audit cursor");
  const limit: unknown = body.limit ?? 25;
  if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1 || limit > 100)
    throw new D1AuditInputError("Invalid audit page limit");
  const filters: unknown = body.filters ?? [];
  if (!Array.isArray(filters) || filters.length > 8)
    throw new D1AuditInputError("Invalid audit filters");
  return {
    operation: body.operation,
    snapshotId: text(body.snapshotId),
    databaseName: text(body.databaseName),
    tableName: text(body.tableName),
    manifestId,
    afterRowId: after,
    limit,
    filters: filters.map((filter: unknown) => {
      if (
        !isRecord(filter) ||
        (filter.value !== null && (typeof filter.value !== "string" || filter.value.length > 512))
      )
        throw new D1AuditInputError("Invalid audit filter value");
      return { column: text(filter.column), value: filter.value };
    }),
  };
};

const scope = (input: D1AuditInput): string =>
  `snapshot_id = ${quote(input.snapshotId)} AND database_name = ${quote(input.databaseName)} AND table_name = ${quote(input.tableName)}`;
const response = (body: unknown, status = 200): Response =>
  Response.json(body, {
    status,
    headers: { "Cache-Control": "no-store", "X-Catalog-Read-Mode": "baseline-audit" },
  });

const parseManifest = (row: Record<string, unknown>, input: D1AuditInput): Manifest => {
  if (typeof row.batch_id !== "string" || typeof row.payload !== "string")
    throw new Error("Invalid Catalog manifest row");
  const identity: string = `[${[input.snapshotId, input.databaseName, input.tableName].map((part) => JSON.stringify(part)).join(", ")}]`;
  if (hash(identity + JSON.stringify(["0", row.payload]) + "\n") !== row.batch_id)
    throw new Error("Catalog manifest fingerprint mismatch");
  const payload: unknown = JSON.parse(row.payload);
  if (
    !isRecord(payload) ||
    payload.kind !== "d1-baseline-manifest-v1" ||
    payload.promoted !== false ||
    payload.verification !== "batch_readback_and_catalog_count" ||
    typeof payload.copied_rows !== "number" ||
    !Number.isSafeInteger(payload.copied_rows) ||
    payload.copied_rows < 0 ||
    !Array.isArray(payload.source_schema) ||
    payload.source_schema.length === 0
  )
    throw new Error("Invalid baseline manifest");
  const columns = payload.source_schema.map((column: unknown) => {
    if (!isRecord(column) || typeof column.name !== "string")
      throw new Error("Invalid baseline column");
    return { name: column.name };
  });
  return { id: row.batch_id, columns, copiedRows: payload.copied_rows };
};

const cachedBody = (value: string | null, manifestId: string): string | null => {
  if (value === null || encoder.encode(value).byteLength > MAX_CACHE_BYTES) return null;
  try {
    const body: unknown = JSON.parse(value);
    return isRecord(body) &&
      body.manifestId === manifestId &&
      body.promoted === false &&
      Array.isArray(body.rows)
      ? value
      : null;
  } catch {
    return null;
  }
};
const fromBody = (body: string, source: "edge" | "kv" | "origin"): Response =>
  new Response(body, {
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      "X-Catalog-Read-Mode": "baseline-audit",
      "X-Catalog-Cache": source,
    },
  });

export const queryD1Audit = async (
  input: D1AuditInput,
  dependencies: D1AuditDependencies,
): Promise<Response> => {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/u.test(dependencies.namespace))
    throw new Error("Invalid Catalog namespace");
  const key: string = `d1-audit/v1/${hash(JSON.stringify({ scope: dependencies.cacheScope, namespace: dependencies.namespace, input }))}`;
  const cacheRequest: Request = new Request(
    `${new URL(dependencies.cacheOrigin).origin}/__internal-cache/${key}`,
  );
  if (input.operation === "query") {
    const hit: Response | undefined = await dependencies.cache
      .match(cacheRequest)
      .catch(() => undefined);
    const edge: string | null = cachedBody(
      hit === undefined || !hit.ok ? null : await hit.text().catch(() => null),
      input.manifestId,
    );
    if (edge !== null) return fromBody(edge, "edge");
    const kv: string | null = cachedBody(
      await dependencies.kv.get(key).catch(() => null),
      input.manifestId,
    );
    if (kv !== null) {
      await dependencies.cache
        .put(
          cacheRequest,
          new Response(kv, {
            headers: { "Cache-Control": `public, max-age=${String(CACHE_SECONDS)}` },
          }),
        )
        .catch(() => undefined);
      return fromBody(kv, "kv");
    }
  }
  const manifests = await dependencies.query(
    `SELECT batch_id, payload FROM ${dependencies.namespace}.d1_snapshot_manifests WHERE ${scope(input)}${input.operation === "query" ? ` AND batch_id = ${quote(input.manifestId)}` : ""} LIMIT 2`,
  );
  const first = manifests[0];
  if (first === undefined)
    return response({ error: "Completed baseline manifest unavailable" }, 409);
  if (manifests.length !== 1) throw new Error("Ambiguous baseline manifest");
  const manifest: Manifest = parseManifest(first, input);
  if (input.operation === "query" && manifest.id !== input.manifestId)
    throw new Error("Catalog manifest revision mismatch");
  if (input.operation === "manifest")
    return response({
      manifestId: manifest.id,
      columns: manifest.columns,
      copiedRows: manifest.copiedRows,
      promoted: false,
    });
  const filters: string[] = input.filters.map((filter) => {
    if (!manifest.columns.some((column) => column.name === filter.column))
      throw new D1AuditInputError("Unknown baseline column");
    const cell: string = `json_get_json(payload, ${quote(filter.column)})`;
    return filter.value === null
      ? `json_get_str(${cell}, 'type') = 'null'`
      : `json_get_str(${cell}, 'value') = ${quote(filter.value)}`;
  });
  const conditions: string[] = [scope(input), ...filters];
  if (input.afterRowId !== null)
    conditions.push(`CAST(row_key AS BIGINT) > CAST(${quote(input.afterRowId)} AS BIGINT)`);
  const raw = await dependencies.query(
    `SELECT row_key, payload FROM ${dependencies.namespace}.d1_snapshot_rows WHERE ${conditions.join(" AND ")} ORDER BY CAST(row_key AS BIGINT) LIMIT ${String(input.limit + 1)}`,
  );
  const rows = parseD1SnapshotPage(raw, {
    afterRowId: input.afterRowId,
    columns: manifest.columns,
    limit: input.limit + 1,
    table: input.tableName,
  });
  const page = rows.slice(0, input.limit);
  const nextAfterRowId: string | null =
    rows.length > input.limit ? (page.at(-1)?.row_key ?? null) : null;
  const body: string = JSON.stringify({
    manifestId: manifest.id,
    rows: page,
    nextAfterRowId,
    promoted: false,
  });
  if (encoder.encode(body).byteLength <= MAX_CACHE_BYTES) {
    await dependencies.cache
      .put(
        cacheRequest,
        new Response(body, {
          headers: { "Cache-Control": `public, max-age=${String(CACHE_SECONDS)}` },
        }),
      )
      .catch(() => undefined);
    await dependencies.kv.put(key, body, { expirationTtl: KV_SECONDS }).catch(() => undefined);
  }
  return fromBody(body, "origin");
};
