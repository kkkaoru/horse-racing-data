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

const TRANSIENT_R2_SQL_STATUSES: ReadonlySet<number> = new Set([408, 425, 429, 500, 502, 503, 504]);
const READ_ATTEMPTS: number = 2;
const RETRY_DELAY_MS: number = 250;

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// Only provider-transient failures are retried; validation failures, 4xx and
// malformed data must fail fast so real corruption is never masked.
const isTransientReadError = (error: unknown): boolean => {
  if (error instanceof DOMException)
    return error.name === "AbortError" || error.name === "TimeoutError";
  if (error instanceof TypeError) return true;
  if (typeof error !== "object" || error === null) return false;
  const status: unknown = Reflect.get(error, "status");
  return typeof status === "number" && TRANSIENT_R2_SQL_STATUSES.has(status);
};

const describeReadError = (error: unknown): Record<string, unknown> => {
  if (typeof error !== "object" || error === null) return {};
  const name: unknown = Reflect.get(error, "name");
  const status: unknown = Reflect.get(error, "status");
  return {
    ...(typeof name === "string" ? { errorName: name } : {}),
    ...(typeof status === "number" ? { status } : {}),
  };
};

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
  let lastError: unknown = null;
  for (let attempt = 0; attempt < READ_ATTEMPTS; attempt += 1) {
    try {
      const races: RaceDayListRow[] = await read({
        input,
        query: async (sql) => executeR2Sql(env, sql, boundedAuditFetch(fetch)),
      });
      return json({ races }, 200);
    } catch (error: unknown) {
      lastError = error;
      if (attempt + 1 >= READ_ATTEMPTS || !isTransientReadError(error)) break;
      await sleep(RETRY_DELAY_MS);
    }
  }
  console.error(JSON.stringify({ event: failureEvent, ...describeReadError(lastError) }));
  return json({ error: "Catalog race day list unavailable" }, 503);
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
