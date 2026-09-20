// Runs with bun; this handler is reachable only through the trusted binding.
import { boundedAuditFetch } from "./d1-audit";
import { notifyReadFailure } from "./read-failure-alert";
import { executeR2Sql } from "./r2-sql";
import {
  buildRaceRunnersReadSql,
  readRaceRunners,
  type RaceRunnersReadInput,
  type RaceRunnersResult,
} from "./race-runners-read";
import type { R2SqlCatalogConfig } from "./types";

const PARAMETERS: readonly string[] = ["source", "date", "keibajoCode", "raceBango"];
const json = (value: unknown, status: number): Response =>
  Response.json(value, { status, headers: { "Cache-Control": "no-store" } });

export const handleRaceRunnersRead = async (
  request: Request,
  env: R2SqlCatalogConfig,
): Promise<Response> => {
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  const params: URLSearchParams = new URL(request.url).searchParams;
  const source: string | null = params.get("source");
  const date: string | null = params.get("date");
  const keibajoCode: string | null = params.get("keibajoCode");
  const raceBango: string | null = params.get("raceBango");
  if (
    date === null ||
    keibajoCode === null ||
    raceBango === null ||
    (source !== "jra" && source !== "nar") ||
    PARAMETERS.some((key) => params.getAll(key).length !== 1) ||
    [...params.keys()].some((key) => !PARAMETERS.includes(key))
  )
    return json({ error: "Invalid race runners request" }, 400);
  const input: RaceRunnersReadInput = {
    namespace: env.R2_SQL_NAMESPACE,
    source,
    date,
    keibajoCode,
    raceBango,
  };
  try {
    buildRaceRunnersReadSql(input);
  } catch {
    return json({ error: "Invalid race runners request" }, 400);
  }
  try {
    const result: RaceRunnersResult = await readRaceRunners({
      input,
      query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
    });
    return json(result, 200);
  } catch {
    console.error(JSON.stringify({ event: "race_runners_read_failed" }));
    await notifyReadFailure(env.INGESTION_ALERTS, { event: "race_runners_read_failed" });
    return json({ error: "Catalog race runners unavailable" }, 503);
  }
};
