// Run with bun. On-demand HTTP trigger helpers: authorize the request and parse
// the requested RUN_DATE so a manual run can be started without waiting for the
// "0 18 * * *" cron. Pure + unit-tested; the Worker wires these to the Container
// start() call (see worker.ts).

import type { RunDates } from "./types";

const TRIGGER_PATH = "/run";
const TRIGGER_METHOD = "POST";
const BEARER_PREFIX = "Bearer ";
const YMD_LENGTH = 8;
const YMD_PATTERN = /^\d{8}$/;
const YEAR_END = 4;
const MONTH_END = 6;
const DATE_SEPARATOR = "-";

// True only for the authenticated on-demand trigger route (POST /run). Any other
// method/path is treated as the health endpoint by the caller.
export const isTriggerRequest = (method: string, pathname: string): boolean =>
  method === TRIGGER_METHOD && pathname === TRIGGER_PATH;

// Constant-time-ish bearer check. The token is a Worker secret (TRIGGER_TOKEN);
// an empty configured token denies every request so a missing secret can never
// expose the trigger.
export const isAuthorized = (header: string | null, token: string): boolean => {
  if (!token) {
    return false;
  }
  if (!header) {
    return false;
  }
  return header === `${BEARER_PREFIX}${token}`;
};

// Convert a "YYYYMMDD" run date to the {runDate (ISO), runYmd (8-digit)} pair the
// dispatch + audit builders expect. Throws on malformed input so a bad manual
// trigger fails loudly instead of building features for a garbage date.
export const parseRunDates = (ymd: string): RunDates => {
  if (ymd.length !== YMD_LENGTH || !YMD_PATTERN.test(ymd)) {
    throw new Error("RUN_DATE must be 8 digits (YYYYMMDD)");
  }
  const year = ymd.slice(0, YEAR_END);
  const month = ymd.slice(YEAR_END, MONTH_END);
  const day = ymd.slice(MONTH_END, YMD_LENGTH);
  return { runDate: [year, month, day].join(DATE_SEPARATOR), runYmd: ymd };
};
