// Run with bun. Course reads fail closed without PostgreSQL fallback.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";
import type { CatalogRaceDetailBinding } from "./race-detail-catalog";
import type { CourseInfo } from "./race-types";

export interface CourseQuery {
  keibajoCode: string;
  kyori: string;
  trackCode: string;
}

const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-course";
const FAILURE: string = "Catalog course unavailable";
const TIMEOUT_MS: number = 35_000;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const readCatalogRaceCourse = async (
  binding: CatalogRaceDetailBinding | undefined,
  query: CourseQuery,
): Promise<CourseInfo | null> => {
  if (binding === undefined) throw new Error(FAILURE);
  const url: URL = new URL(ENDPOINT);
  url.searchParams.set("keibajoCode", query.keibajoCode);
  url.searchParams.set("kyori", query.kyori);
  url.searchParams.set("trackCode", query.trackCode);
  try {
    const response: Response = await binding.fetch(
      new Request(url, {
        method: "GET",
        redirect: "manual",
        signal: AbortSignal.timeout(TIMEOUT_MS),
      }),
    );
    if (response.status !== 200 || response.headers.get("cache-control") !== "no-store") {
      await response.body?.cancel();
      throw new Error(FAILURE);
    }
    const payload: unknown = await readBoundedCatalogBody(response);
    if (!isRecord(payload)) throw new Error(FAILURE);
    if (payload.course === null) return null;
    const course: unknown = payload.course;
    if (
      !isRecord(course) ||
      typeof course.courseKaishuNengappi !== "string" ||
      !(course.courseSetsumei === null || typeof course.courseSetsumei === "string")
    )
      throw new Error(FAILURE);
    return {
      courseKaishuNengappi: course.courseKaishuNengappi,
      courseSetsumei: course.courseSetsumei,
    };
  } catch {
    throw new Error(FAILURE);
  }
};
