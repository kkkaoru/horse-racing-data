// Runs with bun; trusted readers can request only the fixed year-summary projection.
import { boundedAuditFetch } from "./d1-audit";
import { executeR2Sql } from "./r2-sql";
import { readRaceYears } from "./race-years-read";
import type { R2SqlCatalogConfig } from "./types";

const json = (value: unknown, status: number): Response =>
  Response.json(value, { status, headers: { "Cache-Control": "no-store" } });

export const handleRaceYearsRead = async (
  request: Request,
  env: R2SqlCatalogConfig,
): Promise<Response> => {
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  if (new URL(request.url).searchParams.size !== 0) {
    return json({ error: "Invalid race years request" }, 400);
  }
  try {
    return json(
      {
        years: await readRaceYears({
          namespace: env.R2_SQL_NAMESPACE,
          query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
        }),
      },
      200,
    );
  } catch {
    console.error(JSON.stringify({ event: "race_years_read_failed" }));
    return json({ error: "Catalog race years unavailable" }, 503);
  }
};
