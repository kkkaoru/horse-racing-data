// Run with bun via the package scripts.
import type { R2SqlCatalogConfig } from "./types";

export interface CourseInfo {
  courseKaishuNengappi: string;
  courseSetsumei: string | null;
}

const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const CODE_PATTERN: RegExp = /^\d{2}$/u;
const VENUE_PATTERN: RegExp = /^[0-9A-Z]{2}$/u;
const DISTANCE_PATTERN: RegExp = /^\d{4}$/u;

export const buildRaceCourseQuery = (env: R2SqlCatalogConfig, url: URL): string => {
  const venue = url.searchParams.get("keibajoCode") ?? "";
  const distance = url.searchParams.get("kyori") ?? "";
  const track = url.searchParams.get("trackCode") ?? "";
  if (!IDENTIFIER_PATTERN.test(env.R2_SQL_NAMESPACE))
    throw new Error("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  if (!VENUE_PATTERN.test(venue))
    throw new Error("keibajoCode must contain two uppercase alphanumeric characters");
  if (!DISTANCE_PATTERN.test(distance)) throw new Error("kyori must contain four digits");
  if (!CODE_PATTERN.test(track)) throw new Error("trackCode must contain two digits");
  // Preserve the Viewer contract: null legacy markers survive, explicit tombstones do not.
  return `SELECT course_kaishu_nengappi, course_setsumei
FROM ${env.R2_SQL_NAMESPACE}.jvd_cs
WHERE keibajo_code = '${venue}' AND kyori = '${distance}' AND track_code = '${track}'
  AND (data_kubun IS NULL OR data_kubun <> '0')
ORDER BY course_kaishu_nengappi DESC
LIMIT 1`;
};

export const normaliseRaceCourse = (
  rows: readonly Record<string, unknown>[],
): CourseInfo | null => {
  if (rows.length === 0) return null;
  const row = rows[0];
  if (
    rows.length !== 1 ||
    typeof row?.course_kaishu_nengappi !== "string" ||
    !(row.course_setsumei === null || typeof row.course_setsumei === "string")
  )
    throw new Error("Malformed R2 course result");
  return {
    courseKaishuNengappi: row.course_kaishu_nengappi,
    courseSetsumei: row.course_setsumei,
  };
};
