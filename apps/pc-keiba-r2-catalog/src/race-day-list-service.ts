// Runs with bun; fixed single-day read, bounded provider I/O and no shared request state.
import { boundedAuditFetch } from "./d1-audit";
import { executeR2Sql } from "./r2-sql";
import {
  buildRaceDayListReadSql,
  readRaceDayList,
  type RaceDayListInput,
  type RaceDayListReader,
  type RaceDayListRow,
} from "./race-day-list-read";
import type { R2SqlCatalogConfig } from "./types";
import { readRaceDayListWithJockeys } from "./race-day-jockeys-read";

interface DayListRequest {
  request: Request;
  env: R2SqlCatalogConfig;
  read: (reader: RaceDayListReader) => Promise<RaceDayListRow[]>;
  failureEvent: string;
}

const json = (value: unknown, status: number): Response =>
  Response.json(value, { status, headers: { "Cache-Control": "no-store" } });

const handleDayListRequest = async ({
  request,
  env,
  read,
  failureEvent,
}: DayListRequest): Promise<Response> => {
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  const params: URLSearchParams = new URL(request.url).searchParams;
  const date: string | null = params.get("date");
  if (
    date === null ||
    params.getAll("date").length !== 1 ||
    [...params.keys()].some((key) => key !== "date")
  ) {
    return json({ error: "Invalid race day list request" }, 400);
  }
  const input: RaceDayListInput = { namespace: env.R2_SQL_NAMESPACE, date };
  try {
    buildRaceDayListReadSql(input);
  } catch {
    return json({ error: "Invalid race day list request" }, 400);
  }
  try {
    return json(
      {
        races: await read({
          input,
          query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
        }),
      },
      200,
    );
  } catch {
    console.error(JSON.stringify({ event: failureEvent }));
    return json({ error: "Catalog race day list unavailable" }, 503);
  }
};

export const handleRaceDayListRead = (
  request: Request,
  env: R2SqlCatalogConfig,
): Promise<Response> =>
  handleDayListRequest({
    request,
    env,
    read: readRaceDayList,
    failureEvent: "race_day_list_read_failed",
  });

// The router must enforce private service access or diagnostic authentication.
export const handleRaceDayListWithJockeysRead = (
  request: Request,
  env: R2SqlCatalogConfig,
): Promise<Response> =>
  handleDayListRequest({
    request,
    env,
    read: readRaceDayListWithJockeys,
    failureEvent: "race_day_list_with_jockeys_read_failed",
  });
