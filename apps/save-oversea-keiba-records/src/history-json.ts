// This file runs with Bun. Validate private manifests and archived JSON before use.
import type { HistoryArchivePlan, HistoryArchiveState } from "./history-archive";
import type { HistorySourceRow } from "./sources/history-source-page";
import type {
  SecondaryResultFieldIndexes,
  SecondaryResultMarkupProfile,
} from "./sources/secondary-result-parser";

const FIELD_NAMES: readonly string[] = [
  "date",
  "venue",
  "raceNumber",
  "raceName",
  "finishPosition",
  "distance",
  "going",
  "relatedEntity",
];
const PROFILE_TEXT: readonly string[] = [
  "tableMarker",
  "racePathPrefix",
  "horsePathPrefix",
  "jockeyPathPrefix",
  "raceUrlTemplate",
];
const COMMON_TEXT: readonly string[] = [
  "sourceRaceId",
  "raceDate",
  "raceName",
  "sourceRaceUrl",
  "finishPositionText",
];
const HORSE_TEXT: readonly string[] = ["sourceHorseId", "venue", "jockeyName", "surface", "going"];
const PERSON_NULLABLE: readonly string[] = [
  "sourceHorseId",
  "horseName",
  "venue",
  "surface",
  "going",
];

export const isHistoryRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const strings = (value: Record<string, unknown>, keys: readonly string[]): boolean =>
  keys.every((key: string): boolean => typeof value[key] === "string");
const nullableText = (value: unknown): boolean => value === null || typeof value === "string";
const nullableNumber = (value: unknown): boolean =>
  value === null || (typeof value === "number" && Number.isFinite(value));
const personKind = (value: unknown): boolean =>
  value === "owner" || value === "jockey" || value === "trainer";
const stringList = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item: unknown): boolean => typeof item === "string");
const nonnegativeInteger = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const fields = (value: unknown): value is SecondaryResultFieldIndexes =>
  isHistoryRecord(value) &&
  FIELD_NAMES.every((key: string): boolean => nonnegativeInteger(value[key]));
const markup = (value: unknown): value is SecondaryResultMarkupProfile =>
  isHistoryRecord(value) &&
  strings(value, PROFILE_TEXT) &&
  fields(value.horseFields) &&
  fields(value.personFields);

export const isHistorySourceRow = (value: unknown): value is HistorySourceRow => {
  if (
    !isHistoryRecord(value) ||
    !strings(value, COMMON_TEXT) ||
    !nullableNumber(value.finishPosition)
  )
    return false;
  if ("personKind" in value) {
    return (
      personKind(value.personKind) &&
      strings(value, ["sourcePersonId", "raceNumber"]) &&
      PERSON_NULLABLE.every((key: string): boolean => nullableText(value[key])) &&
      nullableNumber(value.distanceMetres)
    );
  }
  return (
    strings(value, HORSE_TEXT) &&
    nonnegativeInteger(value.raceDaySequence) &&
    typeof value.distanceMetres === "number" &&
    Number.isFinite(value.distanceMetres) &&
    nullableText(value.sourceJockeyId)
  );
};

const isPlan = (value: unknown): value is HistoryArchivePlan =>
  isHistoryRecord(value) &&
  (value.kind === "horse" || personKind(value.kind)) &&
  strings(value, ["sourceId", "initialUrl", "encoding"]) &&
  nullableText(value.initialHtmlPath) &&
  isHistoryRecord(value.profile) &&
  markup(value.profile.markup) &&
  typeof value.profile.populationPattern === "string" &&
  typeof value.profile.emptyMarker === "string" &&
  stringList(value.profile.nextLabels);

const isState = (value: unknown): value is HistoryArchiveState =>
  isHistoryRecord(value) &&
  typeof value.planDigest === "string" &&
  (value.publishedCount === null || nonnegativeInteger(value.publishedCount)) &&
  isHistoryRecord(value.checkpoint) &&
  nullableText(value.checkpoint.pendingUrl) &&
  stringList(value.checkpoint.completedUrls) &&
  nonnegativeInteger(value.checkpoint.processedRows) &&
  Array.isArray(value.rows) &&
  value.rows.every(isHistorySourceRow);

export const decodeHistoryPlan = (text: string): HistoryArchivePlan => {
  const value: unknown = JSON.parse(text);
  if (!isPlan(value)) throw new Error("Private history plan is invalid.");
  const url: URL = new URL(value.initialUrl);
  if (
    url.protocol !== "https:" ||
    url.username !== "" ||
    url.password !== "" ||
    url.hash !== "" ||
    !/^[a-zA-Z0-9]+$/u.test(value.sourceId)
  ) {
    throw new Error("History plan URL or source identity is invalid.");
  }
  return value;
};

export const decodeHistoryArchive = (text: string): HistoryArchiveState => {
  const value: unknown = JSON.parse(text);
  if (!isState(value)) throw new Error("History archive structure is invalid.");
  return value;
};
