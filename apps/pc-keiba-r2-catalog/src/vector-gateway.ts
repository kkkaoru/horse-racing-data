// Runs with bun; deployed as the Cloudflare Worker entrypoint.
export { IngestionInbox } from "./ingestion-inbox";
export { IngestionArchiveJournal } from "./ingestion-archive-journal";
export { IngestionBufferService } from "./ingestion-service";
import { timingSafeEqual } from "node:crypto";
import catalogWorker from "./worker";
import { handleIngestionDeadLetters } from "./ingestion-dead-letter";
import {
  INGESTION_MONITOR_CRON,
  monitorIngestionArchives,
  readIngestionMonitorStatus,
} from "./ingestion-monitor";
import { boundedAuditFetch, D1AuditInputError, parseD1AuditInput, queryD1Audit } from "./d1-audit";
import { executeR2Sql } from "./r2-sql";
import { handleRaceDetailRead } from "./race-detail-service";
import { handleRaceCalendarRead } from "./race-calendar-service";
import { handleRaceYearsRead } from "./race-years-service";
import { handleRaceDayListRead, handleRaceDayListWithJockeysRead } from "./race-day-list-service";
export { RaceDetailReadService } from "./race-detail-service";
import type { Env } from "./types";
import {
  queryHistoricalVectors,
  upsertHistoricalVectors,
  type HistoricalVectorQuery,
} from "./vector-search";

interface GatewayEnv
  extends Env, Pick<CatalogBindings, "CORNER_VECTORS" | "INGESTION_ARCHIVE_JOURNAL"> {
  VECTOR_ADMIN_TOKEN?: string;
  D1_AUDIT_TOKEN?: string;
}
interface MonitorTickLog {
  event: "ingestion_monitor_tick";
  completed: true;
  coverageVerified: false;
  notificationDeliveryVerified: false;
}
interface QueryInput {
  query: HistoricalVectorQuery;
}

const INGESTION_STATUS_PATH: string = "/v1/internal/ingestion/status";
const D1_AUDIT_PATH: string = "/v1/internal/d1/audit";
const RACE_DETAIL_AUDIT_PATH: string = "/v1/internal/race-detail";
const RACE_CALENDAR_AUDIT_PATH: string = "/v1/internal/race-calendar";
const RACE_YEARS_AUDIT_PATH: string = "/v1/internal/race-years";
const RACE_DAY_LIST_AUDIT_PATH: string = "/v1/internal/race-day-list";
const RACE_DAY_JOCKEYS_AUDIT_PATH: string = "/v1/internal/race-day-list-with-jockeys";
const QUERY_PATH: string = "/v1/internal/vectors/query";
const UPSERT_PATH: string = "/v1/internal/vectors/upsert";
const MAX_BODY_BYTES: number = 1024 * 1024;
const encoder: TextEncoder = new TextEncoder();
const READ_METHOD: string = "POST";

const json = (value: unknown, status: number): Response =>
  Response.json(value, {
    status,
    headers: { "Cache-Control": "no-store" },
  });

const authorized = (request: Request, token: string | undefined): boolean => {
  if (!token) return false;
  const expected: Uint8Array = encoder.encode(`Bearer ${token}`);
  const supplied: Uint8Array = encoder.encode(request.headers.get("Authorization") ?? "");
  return expected.byteLength === supplied.byteLength && timingSafeEqual(expected, supplied);
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const numbers = (value: unknown): number[] => {
  if (
    !Array.isArray(value) ||
    !value.every((item: unknown) => typeof item === "number" && Number.isFinite(item))
  )
    throw new Error("Invalid vector values");
  return value;
};

const stringValue = (value: unknown): string => {
  if (typeof value !== "string") throw new Error("Invalid string field");
  return value;
};

const numericValue = (value: unknown): number => {
  if (typeof value !== "number" || !Number.isFinite(value))
    throw new Error("Invalid numeric field");
  return value;
};

const metadata = (value: unknown): Record<string, VectorizeVectorMetadata> => {
  if (!isRecord(value)) throw new Error("Invalid vector metadata");
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => {
      if (
        typeof item !== "string" &&
        typeof item !== "boolean" &&
        !(typeof item === "number" && Number.isFinite(item))
      )
        throw new Error("Invalid metadata value");
      return [key, item];
    }),
  );
};

const readBody = async (request: Request): Promise<Record<string, unknown>> => {
  if (request.body === null) throw new Error("Missing vector request body");
  const reader: ReadableStreamDefaultReader<Uint8Array> = request.body.getReader();
  const chunks: Uint8Array[] = [];
  const size: { bytes: number } = { bytes: 0 };
  try {
    while (true) {
      const chunk: ReadableStreamReadResult<Uint8Array> = await reader.read();
      if (chunk.done) break;
      size.bytes += chunk.value.byteLength;
      if (size.bytes > MAX_BODY_BYTES) {
        await reader.cancel();
        throw new Error("Vector request body exceeds limit");
      }
      chunks.push(chunk.value);
    }
  } finally {
    reader.releaseLock();
  }
  const value: unknown = JSON.parse(await new Blob(chunks).text());
  if (!isRecord(value)) throw new Error("Invalid vector request body");
  return value;
};

const parseQuery = (body: Record<string, unknown>): QueryInput => {
  const source: unknown = body.source;
  if (source !== "jra" && source !== "nar") throw new Error("Invalid vector source");
  const filters: VectorizeVectorMetadataFilter = {};
  if (body.venue !== undefined) filters.venue = stringValue(body.venue);
  if (body.excludeVenue !== undefined) {
    if (body.venue !== undefined) throw new Error("Conflicting venue filters");
    filters.venue = { $ne: stringValue(body.excludeVenue) };
  }
  if (body.trackPrefix !== undefined) filters.trackPrefix = stringValue(body.trackPrefix);
  if (body.hasFinish !== undefined) {
    if (typeof body.hasFinish !== "boolean") throw new Error("Invalid finish filter");
    filters.hasFinish = body.hasFinish;
  }
  if (body.distanceMin !== undefined || body.distanceMax !== undefined) {
    const minimum: number = numericValue(body.distanceMin);
    const maximum: number = numericValue(body.distanceMax);
    if (minimum > maximum) throw new Error("Invalid distance interval");
    filters.distance = { $gte: minimum, $lte: maximum };
  }
  return {
    query: {
      dimensions: 8,
      earliestDate: stringValue(body.earliestDate),
      filters,
      namespace: stringValue(body.namespace),
      raceDate: stringValue(body.raceDate),
      source,
      topK: numericValue(body.topK),
      values: numbers(body.values),
    },
  };
};

const parseVectors = (value: unknown): VectorizeVector[] => {
  if (!Array.isArray(value)) throw new Error("Invalid vector records");
  return value.map((item: unknown) => {
    if (!isRecord(item)) throw new Error("Invalid vector record");
    return {
      id: stringValue(item.id),
      namespace: stringValue(item.namespace),
      values: numbers(item.values),
      metadata: metadata(item.metadata),
    };
  });
};

const vectorResponse = async (
  request: Request,
  env: GatewayEnv,
  path: string,
): Promise<Response> => {
  if (!authorized(request, env.VECTOR_ADMIN_TOKEN)) return json({ error: "Unauthorized" }, 401);
  if (request.method !== READ_METHOD) return json({ error: "Method not allowed" }, 405);
  // Keep errors from the provider separate from input validation without exposing private details.
  const operation = await (async () => {
    try {
      const body: Record<string, unknown> = await readBody(request);
      if (path === QUERY_PATH) {
        const input: QueryInput = parseQuery(body);
        return async () =>
          json({ neighbors: await queryHistoricalVectors(env.CORNER_VECTORS, input.query) }, 200);
      }
      const namespace: string = stringValue(body.namespace);
      const vectors: VectorizeVector[] = parseVectors(body.vectors);
      return async () =>
        json(
          await upsertHistoricalVectors(env.CORNER_VECTORS, { dimensions: 8, namespace, vectors }),
          202,
        );
    } catch {
      return null;
    }
  })();
  if (operation === null) return json({ error: "Invalid vector request" }, 400);
  try {
    return await operation();
  } catch {
    return json({ error: "Vector operation unavailable or invalid" }, 503);
  }
};

const auditResponse = async (request: Request, env: GatewayEnv): Promise<Response> => {
  if (!authorized(request, env.D1_AUDIT_TOKEN)) return json({ error: "Unauthorized" }, 401);
  if (request.method !== READ_METHOD) return json({ error: "Method not allowed" }, 405);
  const input = await (async () => {
    try {
      return parseD1AuditInput(await readBody(request));
    } catch {
      return null;
    }
  })();
  if (input === null) return json({ error: "Invalid audit request" }, 400);
  try {
    return await queryD1Audit(input, {
      namespace: env.R2_SQL_NAMESPACE,
      cacheScope: `${env.R2_SQL_ACCOUNT_ID}/${env.R2_SQL_BUCKET_NAME}`,
      cacheOrigin: new URL(request.url).origin,
      cache: caches.default,
      kv: env.CATALOG_KV,
      query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
    });
  } catch (error) {
    if (error instanceof D1AuditInputError) return json({ error: "Invalid audit request" }, 400);
    return json({ error: "Catalog audit unavailable" }, 503);
  }
};

const raceReadAuditResponse = async (request: Request, env: GatewayEnv): Promise<Response> => {
  if (!authorized(request, env.D1_AUDIT_TOKEN)) return json({ error: "Unauthorized" }, 401);
  const path: string = new URL(request.url).pathname;
  if (path === RACE_DAY_JOCKEYS_AUDIT_PATH)
    return await handleRaceDayListWithJockeysRead(request, env);
  if (path === RACE_DAY_LIST_AUDIT_PATH) return await handleRaceDayListRead(request, env);
  if (path === RACE_YEARS_AUDIT_PATH) return await handleRaceYearsRead(request, env);
  return path === RACE_CALENDAR_AUDIT_PATH
    ? await handleRaceCalendarRead(request, env)
    : await handleRaceDetailRead(request, env);
};

const ingestionStatusResponse = async (request: Request, env: GatewayEnv): Promise<Response> => {
  if (!authorized(request, env.ADMIN_TOKEN)) return json({ error: "Unauthorized" }, 401);
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  try {
    return json(await readIngestionMonitorStatus(env), 200);
  } catch {
    return json({ error: "Ingestion monitor status unavailable" }, 503);
  }
};

export default {
  queue: handleIngestionDeadLetters,
  async scheduled(
    controller: ScheduledController,
    env: Pick<
      CatalogBindings,
      "INGESTION_ARCHIVE_JOURNAL" | "INGESTION_DLQ_METRICS" | "INGESTION_ALERTS"
    >,
  ): Promise<void> {
    if (controller.cron !== INGESTION_MONITOR_CRON)
      throw new Error("Unexpected ingestion monitor schedule");
    await monitorIngestionArchives(env);
    console.info(
      JSON.stringify({
        event: "ingestion_monitor_tick",
        completed: true,
        coverageVerified: false,
        notificationDeliveryVerified: false,
      } satisfies MonitorTickLog),
    );
  },
  async fetch(request: Request, env: GatewayEnv): Promise<Response> {
    const path: string = new URL(request.url).pathname;
    if (path === INGESTION_STATUS_PATH) return await ingestionStatusResponse(request, env);
    if (path === D1_AUDIT_PATH) return await auditResponse(request, env);
    if (
      path === RACE_DETAIL_AUDIT_PATH ||
      path === RACE_CALENDAR_AUDIT_PATH ||
      path === RACE_YEARS_AUDIT_PATH ||
      path === RACE_DAY_LIST_AUDIT_PATH ||
      path === RACE_DAY_JOCKEYS_AUDIT_PATH
    )
      return await raceReadAuditResponse(request, env);
    if (path !== QUERY_PATH && path !== UPSERT_PATH) return await catalogWorker.fetch(request, env);
    return await vectorResponse(request, env, path);
  },
};
