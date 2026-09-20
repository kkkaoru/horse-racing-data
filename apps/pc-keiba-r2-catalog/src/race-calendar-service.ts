// Runs with bun; only fixed, bounded, read-only calendar queries are accepted.
import { boundedAuditFetch } from "./d1-audit";
import { executeR2Sql } from "./r2-sql";
import {
  buildRaceCalendarReadSql,
  readRaceCalendar,
  type RaceCalendarInput,
} from "./race-calendar-read";
import type { R2SqlCatalogConfig } from "./types";

const json = (value: unknown, status: number): Response =>
  Response.json(value, { status, headers: { "Cache-Control": "no-store" } });

export const handleRaceCalendarRead = async (
  request: Request,
  env: R2SqlCatalogConfig,
): Promise<Response> => {
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  const params: URLSearchParams = new URL(request.url).searchParams;
  const year: string | null = params.get("year");
  if (
    year === null ||
    params.getAll("year").length !== 1 ||
    [...params.keys()].some((key) => key !== "year")
  ) {
    return json({ error: "Invalid race calendar request" }, 400);
  }
  const input: RaceCalendarInput = { namespace: env.R2_SQL_NAMESPACE, year };
  try {
    buildRaceCalendarReadSql(input);
  } catch {
    return json({ error: "Invalid race calendar request" }, 400);
  }
  try {
    return json(
      {
        days: await readRaceCalendar({
          input,
          query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
        }),
      },
      200,
    );
  } catch {
    console.error(JSON.stringify({ event: "race_calendar_read_failed" }));
    return json({ error: "Catalog race calendar unavailable" }, 503);
  }
};
