// Run with bun. Reads the canonical paged history from the R2 Catalog service binding.
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";

export interface RaceEntityCatalogQuery {
  cursor: string | null;
  date: string;
  entityType: string;
  horseNumber: string;
  keibajoCode: string;
  limit: string | null;
  raceNumber: string;
  source: string;
}

export interface RaceEntityCatalogResult {
  status: number;
  value: unknown;
}

const CATALOG_ORIGIN: string = "https://pc-keiba-r2-catalog.internal";
const MAX_CATALOG_RESPONSE_BYTES: number = 64 * 1024;
const CATALOG_TIMEOUT_MS: number = 90_000;

export const buildRaceEntityCatalogUrl = (query: RaceEntityCatalogQuery): URL => {
  const url = new URL("/v1/race-entity-recent-results", CATALOG_ORIGIN);
  url.searchParams.set("date", query.date);
  url.searchParams.set("keibajoCode", query.keibajoCode);
  url.searchParams.set("raceBango", query.raceNumber);
  url.searchParams.set("source", query.source);
  url.searchParams.set("horseNumber", query.horseNumber);
  url.searchParams.set("entityType", query.entityType);
  if (query.limit !== null) url.searchParams.set("limit", query.limit);
  if (query.cursor !== null) url.searchParams.set("cursor", query.cursor);
  return url;
};

const errorValue = (code: string, message: string): RaceEntityCatalogResult => ({
  status: code === "TIMEOUT" ? 504 : 502,
  value: { error: { code, message } },
});

const isTimeout = (error: unknown): boolean =>
  error instanceof Error &&
  (error.name === "AbortError" ||
    error.name === "TimeoutError" ||
    error.message.toLowerCase().includes("timeout"));

export const fetchRaceEntityRecentResultsCatalog = async (
  query: RaceEntityCatalogQuery,
): Promise<RaceEntityCatalogResult> => {
  const env = await safeGetCloudflareEnv();
  if (env?.R2_CATALOG === undefined) {
    return errorValue("UPSTREAM_ERROR", "The R2 Catalog binding is unavailable.");
  }
  try {
    const response = await env.R2_CATALOG.fetch(
      new Request(buildRaceEntityCatalogUrl(query), {
        method: "GET",
        signal: AbortSignal.timeout(CATALOG_TIMEOUT_MS),
      }),
    );
    const body = await response.arrayBuffer();
    if (body.byteLength > MAX_CATALOG_RESPONSE_BYTES) {
      return errorValue(
        "MALFORMED_HISTORY_DATA",
        "The R2 Catalog response exceeded the hard size limit.",
      );
    }
    try {
      const value: unknown = JSON.parse(new TextDecoder().decode(body));
      return { status: response.status, value };
    } catch {
      return errorValue("MALFORMED_HISTORY_DATA", "The R2 Catalog returned invalid JSON.");
    }
  } catch (error) {
    return isTimeout(error)
      ? errorValue("TIMEOUT", "The R2 Catalog request timed out.")
      : errorValue("UPSTREAM_ERROR", "The R2 Catalog request failed.");
  }
};
