// Run with bun (bunx vitest).
import type { CatalogSource } from "./types";

export type PartnershipKind =
  | "horseJockey"
  | "jockeyVenue"
  | "jockeyTrainerVenue"
  | "ownerVenue"
  | "jockeyTrainerOwner";

export interface PartnershipScope {
  date: string;
  keibajoCode: string;
  kind: PartnershipKind;
  source: CatalogSource;
  revision: string;
  surface?: string;
}

export interface PartnershipHistory {
  surface: string;
  date: string;
  finishPosition: number | null;
  horseId: string | null;
  jockeyId: string | null;
  keibajoCode: string;
  raceId: string;
  resultId: string;
  source: CatalogSource;
  trainerId: string | null;
  ownerId?: string | null;
}

export interface PartnershipCounts {
  entityKey: string;
  places: number;
  shows: number;
  starts: number;
  wins: number;
}

export interface PartnershipAggregate {
  raceIds: string[];
  rows: PartnershipCounts[];
}

interface AggregatePartnershipInput {
  history: readonly PartnershipHistory[];
  scope: PartnershipScope;
}

const DATE_PATTERN: RegExp = /^\d{8}$/u;
const VENUE_PATTERN: RegExp = /^\d{2}$/u;
const WINDOWS: Record<PartnershipKind, number | null> = {
  horseJockey: null,
  jockeyTrainerVenue: 10,
  jockeyVenue: 3,
  ownerVenue: 30,
  jockeyTrainerOwner: 30,
};
const CACHE_VERSION: string = "heatmap-partnership-v2";
const SURFACES: readonly string[] = ["芝", "ダート", "障害", "サンド", "ばんえい"];

export const partnershipSurface = (scope: PartnershipScope): string => {
  if (scope.kind === "horseJockey") return "all-surfaces";
  if (scope.surface === undefined || !SURFACES.includes(scope.surface))
    throw new Error("Partnership target surface is unavailable");
  return scope.surface;
};

const validDate = (date: string): boolean => {
  if (!DATE_PATTERN.test(date)) return false;
  const parsed: Date = new Date(
    `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6)}T00:00:00Z`,
  );
  return (
    Number.isFinite(parsed.getTime()) &&
    parsed.toISOString().slice(0, 10).replaceAll("-", "") === date
  );
};

export const validatePartnershipScope = (scope: PartnershipScope): void => {
  if (!validDate(scope.date)) throw new Error("Partnership date must be a valid YYYYMMDD date");
  if (!VENUE_PATTERN.test(scope.keibajoCode))
    throw new Error("Partnership venue must contain two digits");
  if (scope.revision.trim() === "") throw new Error("Partnership source revision is required");
};

export const partnershipStartDate = (scope: PartnershipScope): string | null => {
  validatePartnershipScope(scope);
  const years: number | null = WINDOWS[scope.kind];
  if (years === null) return null;
  const year: string = String(Number(scope.date.slice(0, 4)) - years).padStart(4, "0");
  const candidate: string = `${year}${scope.date.slice(4)}`;
  // Clamp leap-day anniversaries to February 28, rather than drifting into March.
  return validDate(candidate) ? candidate : `${year}0228`;
};

export const partnershipCacheKey = (scope: PartnershipScope): string => {
  validatePartnershipScope(scope);
  // No race number: races on the same date, venue and surface share this cohort.
  const venue: string =
    scope.kind === "horseJockey" || scope.kind === "jockeyTrainerOwner"
      ? "all-venues"
      : scope.keibajoCode;
  return [
    CACHE_VERSION,
    scope.revision,
    scope.source,
    scope.date,
    venue,
    scope.kind === "jockeyTrainerOwner" ? "jockeyTrainerOwner-all-sources" : scope.kind,
    partnershipSurface(scope),
  ]
    .map(encodeURIComponent)
    .join(":");
};

const entityKey = (row: PartnershipHistory, kind: PartnershipKind): string | null => {
  if (kind === "ownerVenue") return row.ownerId?.trim() ? JSON.stringify([row.ownerId]) : null;
  if (row.jockeyId === null || row.jockeyId.trim() === "") return null;
  if (kind === "jockeyTrainerOwner") {
    return row.trainerId?.trim() && row.ownerId?.trim()
      ? JSON.stringify([row.trainerId, row.jockeyId, row.ownerId])
      : null;
  }
  if (kind === "jockeyVenue") return JSON.stringify([row.jockeyId]);
  const partner: string | null = kind === "horseJockey" ? row.horseId : row.trainerId;
  return partner === null || partner.trim() === "" ? null : JSON.stringify([partner, row.jockeyId]);
};

const compareCounts = (left: PartnershipCounts, right: PartnershipCounts): number =>
  left.entityKey.localeCompare(right.entityKey);

export const aggregatePartnership = (input: AggregatePartnershipInput): PartnershipAggregate => {
  const start: string | null = partnershipStartDate(input.scope);
  const surface: string = partnershipSurface(input.scope);
  const eligible: PartnershipHistory[] = input.history.filter(
    (row) =>
      validDate(row.date) &&
      row.date < input.scope.date &&
      (start === null || row.date >= start) &&
      (input.scope.kind === "jockeyTrainerOwner" || row.source === input.scope.source) &&
      (input.scope.kind === "horseJockey" ||
        ((input.scope.kind === "jockeyTrainerOwner" ||
          row.keibajoCode === input.scope.keibajoCode) &&
          row.surface === surface)),
  );
  const unique: Map<string, PartnershipHistory> = new Map(
    eligible.map((row) => [row.resultId, row]),
  );
  const counts: Map<string, PartnershipCounts> = new Map();
  [...unique.values()].forEach((row) => {
    const key: string | null = entityKey(row, input.scope.kind);
    const finish: number | null = row.finishPosition;
    if (key === null || finish === null || !Number.isInteger(finish) || finish <= 0) return;
    const prior: PartnershipCounts = counts.get(key) ?? {
      entityKey: key,
      places: 0,
      shows: 0,
      starts: 0,
      wins: 0,
    };
    counts.set(key, {
      entityKey: key,
      places: prior.places + (finish <= 2 ? 1 : 0),
      shows: prior.shows + (finish <= 3 ? 1 : 0),
      starts: prior.starts + 1,
      wins: prior.wins + (finish === 1 ? 1 : 0),
    });
  });
  return {
    raceIds: [...new Set(eligible.map((row) => row.raceId))].toSorted(),
    rows: [...counts.values()].toSorted(compareCounts),
  };
};
