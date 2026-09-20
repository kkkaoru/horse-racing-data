// Runs with bun; this handler is reachable only through the trusted binding.
import { boundedAuditFetch } from "./d1-audit";
import { notifyReadFailure } from "./read-failure-alert";
import {
  buildRaceHistoryReadSql,
  readOverseasRaceHistory,
  readRaceHistory,
  type RaceHistoryReadInput,
} from "./race-history-read";
import { executeR2Sql } from "./r2-sql";
import type { R2SqlCatalogConfig } from "./types";

const REQUIRED_PARAMETERS: readonly string[] = ["horseIds", "beforeDate"];
const OPTIONAL_PARAMETERS: readonly string[] = ["minDate", "limit", "source"];
const DEFAULT_LIMIT: number = 2000;
const json = (value: unknown, status: number): Response =>
  Response.json(value, { status, headers: { "Cache-Control": "no-store" } });

const readLimit = (value: string | null): number | null => {
  if (value === null) return DEFAULT_LIMIT;
  const parsed: number = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
};

// Defaults to the JRA pair so existing callers keep working unchanged.
const readSource = (value: string | null): "jra" | "nar" | "overseas" | null => {
  if (value === null) return "jra";
  return value === "jra" || value === "nar" || value === "overseas" ? value : null;
};

export const handleRaceHistoryRead = async (
  request: Request,
  env: R2SqlCatalogConfig,
): Promise<Response> => {
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  const params: URLSearchParams = new URL(request.url).searchParams;
  const horseIds: string | null = params.get("horseIds");
  const beforeDate: string | null = params.get("beforeDate");
  const minDate: string | null = params.get("minDate");
  const limit: number | null = readLimit(params.get("limit"));
  const source: "jra" | "nar" | "overseas" | null = readSource(params.get("source"));
  if (
    horseIds === null ||
    beforeDate === null ||
    limit === null ||
    source === null ||
    REQUIRED_PARAMETERS.some((key) => params.getAll(key).length !== 1) ||
    OPTIONAL_PARAMETERS.some((key) => params.getAll(key).length > 1) ||
    [...params.keys()].some(
      (key) => !REQUIRED_PARAMETERS.includes(key) && !OPTIONAL_PARAMETERS.includes(key),
    )
  )
    return json({ error: "Invalid race history request" }, 400);
  const input: RaceHistoryReadInput = {
    namespace: env.R2_SQL_NAMESPACE,
    source,
    horseIds: horseIds === "" ? [] : horseIds.split(","),
    beforeDate,
    minDate,
    limit,
  };
  try {
    buildRaceHistoryReadSql(input);
  } catch {
    return json({ error: "Invalid race history request" }, 400);
  }
  try {
    if (input.source === "overseas") {
      const overseas = await readOverseasRaceHistory({
        input,
        query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
      });
      return json({ rows: overseas }, 200);
    }
    const rows: Record<string, string | null>[] = await readRaceHistory({
      input,
      query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
    });
    return json({ rows }, 200);
  } catch {
    console.error(JSON.stringify({ event: "race_history_read_failed" }));
    await notifyReadFailure(env.INGESTION_ALERTS, { event: "race_history_read_failed" });
    return json({ error: "Catalog race history unavailable" }, 503);
  }
};
