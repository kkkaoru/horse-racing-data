// Runs with bun; this named entrypoint exposes only race-detail/calendar reads to trusted binding holders.
import { WorkerEntrypoint } from "cloudflare:workers";
import { boundedAuditFetch } from "./d1-audit";
import { executeR2Sql } from "./r2-sql";
import { handleRaceCalendarRead } from "./race-calendar-service";
import { handleRaceYearsRead } from "./race-years-service";
import { handleRaceDayListRead, handleRaceDayListWithJockeysRead } from "./race-day-list-service";
import {
  buildRaceDetailReadSql,
  readRaceDetail,
  type RaceDetailReadInput,
} from "./race-detail-read";
import type { R2SqlCatalogConfig } from "./types";

const PARAMETERS: readonly string[] = ["source", "date", "keibajoCode", "raceBango"];
const SERVICE_PATH: string = "/v1/race-detail";
const json = (value: unknown, status: number): Response =>
  Response.json(value, { status, headers: { "Cache-Control": "no-store" } });

export const handleRaceDetailRead = async (
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
    return json({ error: "Invalid race detail request" }, 400);
  const input: RaceDetailReadInput = {
    namespace: env.R2_SQL_NAMESPACE,
    source,
    date,
    keibajoCode,
    raceBango,
  };
  try {
    buildRaceDetailReadSql(input);
  } catch {
    return json({ error: "Invalid race detail request" }, 400);
  }
  try {
    const row: Record<string, string | null> | null = await readRaceDetail({
      input,
      query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
    });
    return json({ row }, 200);
  } catch {
    console.error(JSON.stringify({ event: "race_detail_read_failed" }));
    return json({ error: "Catalog race detail unavailable" }, 503);
  }
};

/** No public route or SQL input. Existing default HTTP/Queue/Cron handlers remain separate. */
export class RaceDetailReadService extends WorkerEntrypoint<R2SqlCatalogConfig> {
  override async fetch(request: Request): Promise<Response> {
    const path: string = new URL(request.url).pathname;
    if (path === "/v1/race-day-list-with-jockeys")
      return await handleRaceDayListWithJockeysRead(request, this.env);
    if (path === "/v1/race-day-list") return await handleRaceDayListRead(request, this.env);
    if (path === "/v1/race-years") return await handleRaceYearsRead(request, this.env);
    if (path === "/v1/race-calendar") return await handleRaceCalendarRead(request, this.env);
    if (path !== SERVICE_PATH) return json({ error: "Not found" }, 404);
    return await handleRaceDetailRead(request, this.env);
  }
}
