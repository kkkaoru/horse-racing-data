import type { RaceSource } from "./codes";
import { isOverseasKeibajoCode } from "./runner-format";

export const DETAIL_SECTION_CACHE_WARM_PARAM = "__cacheWarm";
export const PREDICTION_REFRESH_PARAM = "__predictionRefresh";

// Bumped v2->v3 on 2026-07-18 for cherry-picked commit a8f5ad1d (training
// section: emit a placeholder row per entrant with no jvd_hc/wc match). Same
// stale-cache trap as query-cache.ts / finish-prediction-inputs-cache.server.ts:
// any race whose "training" (or any other section sharing this version)
// payload had already been cached before this deploy would otherwise keep
// serving the pre-fix shape for up to DETAIL_SECTION_CACHE_AFTER_START_SECONDS
// (6 hours) past post time, and the per-race cache-bust endpoint would need
// to be called race-by-race to work around it. Bumping here invalidates every
// section's cache at once instead.
export const DETAIL_SECTION_CACHE_VERSION = "v3";
const DOMESTIC_PERSON_STATS_DETAIL_SECTION_CACHE_VERSION = "v4";
const OVERSEAS_HISTORY_DETAIL_SECTION_CACHE_VERSION = "v8";
const PREMIUM_DATA_TOP_DETAIL_SECTION_CACHE_VERSION = "v2";

export const DETAIL_SECTION_CACHE_AFTER_START_SECONDS = 6 * 60 * 60;

export const DETAIL_SECTION_CACHEABLE_SECTIONS = [
  "ability",
  "bloodline",
  "condition",
  "premium-data-top",
  "overall-score",
  "pace-prediction",
  "results",
  "similar",
  "time-score",
  "training",
] as const;

export const DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS = [
  "time-score",
  "results",
  "training",
  "condition",
] as const;

export type DetailSectionCacheableSection = (typeof DETAIL_SECTION_CACHEABLE_SECTIONS)[number];

const usesOverseasHistory = (section: DetailSectionCacheableSection): boolean =>
  section === "bloodline" ||
  section === "overall-score" ||
  section === "results" ||
  section === "similar" ||
  section === "time-score";

const getDetailSectionCacheVersion = (
  section: DetailSectionCacheableSection,
  keibajoCode: string,
): string => {
  if (section === "premium-data-top") {
    return PREMIUM_DATA_TOP_DETAIL_SECTION_CACHE_VERSION;
  }
  if ((section === "similar" || section === "time-score") && !isOverseasKeibajoCode(keibajoCode)) {
    return DOMESTIC_PERSON_STATS_DETAIL_SECTION_CACHE_VERSION;
  }
  if (isOverseasKeibajoCode(keibajoCode) && usesOverseasHistory(section)) {
    return OVERSEAS_HISTORY_DETAIL_SECTION_CACHE_VERSION;
  }
  return DETAIL_SECTION_CACHE_VERSION;
};

export interface DetailSectionCacheWarmMessage {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  section: DetailSectionCacheableSection;
  source: RaceSource;
  year: string;
}

export const isDetailSectionCacheableSection = (
  value: string,
): value is DetailSectionCacheableSection =>
  DETAIL_SECTION_CACHEABLE_SECTIONS.some((section) => section === value);

export const buildDetailSectionCacheKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  section,
  year,
}: Omit<DetailSectionCacheWarmMessage, "source">): string =>
  [
    "race-detail-section",
    getDetailSectionCacheVersion(section, keibajoCode),
    year,
    month,
    day,
    keibajoCode,
    raceNumber,
    section,
    "default",
  ].join(":");

export const buildDetailSectionApiPath = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  section,
  year,
}: Omit<DetailSectionCacheWarmMessage, "source">): string =>
  `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/sections/${section}`;

export const stripDetailSectionCacheWarmParams = (
  searchParams: URLSearchParams,
): URLSearchParams => {
  const next = new URLSearchParams(searchParams);
  next.delete(DETAIL_SECTION_CACHE_WARM_PARAM);
  next.delete(PREDICTION_REFRESH_PARAM);
  return next;
};

export const isDefaultDetailSectionCacheRequest = (
  section: string,
  searchParams: URLSearchParams,
): section is DetailSectionCacheableSection =>
  isDetailSectionCacheableSection(section) &&
  stripDetailSectionCacheWarmParams(searchParams).toString() === "";

export const getJstDateParts = (date: Date): { day: string; month: string; year: string } => {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    day: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date).map((part) => [part.type, part.value]),
  );
  return {
    day: parts.day ?? "01",
    month: parts.month ?? "01",
    year: parts.year ?? "1970",
  };
};

export const getTomorrowJstDateParts = (
  baseDate = new Date(),
): { day: string; month: string; year: string } => {
  const tomorrow = new Date(baseDate.getTime() + 24 * 60 * 60 * 1000);
  return getJstDateParts(tomorrow);
};

export const parseIsoDateParts = (
  value: string | null,
): { day: string; month: string; year: string } | null => {
  const match = value?.match(/^(\d{4})-(\d{2})-(\d{2})$/u);
  if (!match) {
    return null;
  }
  const [, year, month, day] = match;
  return year && month && day ? { day, month, year } : null;
};
