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
// section's cache at once instead. Bumped v3->v4 on 2026-08-22 because the
// training section now reads the official+netkeiba union from R2 Catalog;
// cached pre-Catalog payloads must not survive the deployment.
export const DETAIL_SECTION_CACHE_VERSION = "v4";
// Bumped v4->v5 on 2026-08-23 because training now unions netkeiba backup
// workouts (including intermediate type=1 pages). Cached official-only
// placeholder payloads must not survive.
const TRAINING_DETAIL_SECTION_CACHE_VERSION = "v5";
// Bumped v10->v11 on 2026-08-23 because Catalog condition history now filters
// by cell-matching class, age, condition-key, race-title, and ungraded-OP
// empty-grade. Cached mixed-class payloads must not survive.
// Bumped v11->v12 on 2026-08-23 because includeGrade now matches empty grades
// for 2歳未勝利/新馬. Cached empty 枠 payloads must not survive.
// Bumped v12->v13 on 2026-08-23 because includeGrade now applies only to
// listed-or-higher codes. Cached 特別 (E) zero-history payloads must not survive.
// Bumped v13->v14 on 2026-08-23 because Catalog finish-position stats now
// include horse-level details. Cached empty-details payloads must not survive.
// Bumped v14->v15 because the viewer parser now keeps those details instead of
// dropping them. Cached empty-details v14 payloads must not survive.
const CONDITION_DETAIL_SECTION_CACHE_VERSION = "v15";
// Bumped v7->v9 on 2026-08-23 because similar/bloodline/time-score now prefer
// R2 Catalog rows with empty details. Cached Neon jsonb payloads must not
// survive. v8 remains the overseas history version.
const DOMESTIC_RATE_STATS_DETAIL_SECTION_CACHE_VERSION = "v9";
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

export const DETAIL_SECTION_QUEUE_HEATMAP_SECTION = "win-rate-heatmap";

export const DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS = [
  "time-score",
  "results",
  "training",
  "condition",
  "similar",
  DETAIL_SECTION_QUEUE_HEATMAP_SECTION,
  "bloodline",
  "overall-score",
] as const;

export type DetailSectionCacheableSection = (typeof DETAIL_SECTION_CACHEABLE_SECTIONS)[number];

export type DetailSectionQueueWarmSection =
  | DetailSectionCacheableSection
  | typeof DETAIL_SECTION_QUEUE_HEATMAP_SECTION;

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
  if (section === "condition") {
    return CONDITION_DETAIL_SECTION_CACHE_VERSION;
  }
  if (section === "training") {
    return TRAINING_DETAIL_SECTION_CACHE_VERSION;
  }
  if (
    (section === "bloodline" || section === "similar" || section === "time-score") &&
    !isOverseasKeibajoCode(keibajoCode)
  ) {
    return DOMESTIC_RATE_STATS_DETAIL_SECTION_CACHE_VERSION;
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
  section: DetailSectionQueueWarmSection;
  source: RaceSource;
  year: string;
}

export interface RaceDetailSsrCacheWarmMessage {
  day: string;
  keibajoCode: string;
  kind: "race-detail-ssr";
  month: string;
  raceNumber: string;
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
}: Omit<DetailSectionCacheWarmMessage, "section" | "source"> & {
  section: DetailSectionCacheableSection;
}): string =>
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
