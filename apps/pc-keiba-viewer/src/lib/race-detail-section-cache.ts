import type { RaceSource } from "./codes";

export const DETAIL_SECTION_CACHE_WARM_PARAM = "__cacheWarm";

export const DETAIL_SECTION_CACHE_VERSION = "v1";
const FINISH_PREDICTION_DETAIL_SECTION_CACHE_VERSION = "v3";

export const DETAIL_SECTION_CACHE_AFTER_START_SECONDS = 6 * 60 * 60;

export const DETAIL_SECTION_CACHEABLE_SECTIONS = [
  "ability",
  "bloodline",
  "condition",
  "finish-prediction",
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
  "finish-prediction",
] as const;

export type DetailSectionCacheableSection = (typeof DETAIL_SECTION_CACHEABLE_SECTIONS)[number];

const getDetailSectionCacheVersion = (section: DetailSectionCacheableSection): string =>
  section === "finish-prediction"
    ? FINISH_PREDICTION_DETAIL_SECTION_CACHE_VERSION
    : DETAIL_SECTION_CACHE_VERSION;

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
    getDetailSectionCacheVersion(section),
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
