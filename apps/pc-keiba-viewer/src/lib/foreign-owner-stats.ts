// Run with bun.
import type { SimilarRaceStatsSettings } from "./race-types";
import type { WinRateHeatmapPartnershipRow } from "./win-rate-heatmap";
export interface ForeignOwnerMetadata extends Record<string, unknown> {
  statsSource?: "netkeiba" | "jv";
  sourcePersonId?: string;
  statsScope?:
    | "stored-results-10y"
    | "stored-results-race-10y"
    | "stored-results-venue-30y"
    | "all-published-results"
    | "japan-all-venues-10y";
}
export const foreignOwnerMetadata = (row: object): ForeignOwnerMetadata => {
  if (!("statsSource" in row) && !("statsScope" in row) && !("sourcePersonId" in row)) return {};
  if (
    "statsSource" in row &&
    "statsScope" in row &&
    ((row.statsSource === "jv" && row.statsScope === "japan-all-venues-10y") ||
      (row.statsSource === "netkeiba" && row.statsScope === "all-published-results"))
  ) {
    return { statsSource: row.statsSource, statsScope: row.statsScope };
  }
  if (
    !("statsSource" in row) ||
    row.statsSource !== "netkeiba" ||
    !("sourcePersonId" in row) ||
    typeof row.sourcePersonId !== "string" ||
    !/^(?!0+$)[a-zA-Z0-9]{6}$/.test(row.sourcePersonId) ||
    !("statsScope" in row) ||
    (row.statsScope !== "stored-results-10y" &&
      row.statsScope !== "stored-results-race-10y" &&
      row.statsScope !== "stored-results-venue-30y")
  )
    throw new Error("Foreign owner metadata is malformed");
  const category = "category" in row ? row.category : "kind" in row ? row.kind : undefined;
  if (
    category !== undefined &&
    category !== (row.statsScope === "stored-results-venue-30y" ? "ownerVenue" : "owner")
  )
    throw new Error("Foreign owner metadata is malformed");
  return {
    statsSource: row.statsSource,
    sourcePersonId: row.sourcePersonId,
    statsScope: row.statsScope,
  };
};
export const isUnfilteredForeignOwnerSettings = (settings: SimilarRaceStatsSettings): boolean =>
  (settings.years === null || settings.years === 10) &&
  settings.sourceScope === "all" &&
  settings.includeVenue === settings.includeRaceTitle &&
  Object.entries(settings).every(
    ([key, value]) =>
      !key.startsWith("include") ||
      key === "includeBloodlineAncestors" ||
      key === "includeVenue" ||
      key === "includeRaceTitle" ||
      value === false,
  );

const horseNumbers = (value: string): string[] =>
  value.split(",").map((part) => {
    if (!/^0*[1-9]\d?$/.test(part.trim()))
      throw new Error("Foreign owner horse number is malformed");
    return String(Number(part.trim()));
  });
export const mergeForeignOwnerVenueRows = (
  native: WinRateHeatmapPartnershipRow[],
  foreign: WinRateHeatmapPartnershipRow[],
): WinRateHeatmapPartnershipRow[] => {
  const protectedNumbers = new Set(
    native
      .filter((row) => row.category === "ownerVenue" && row.starts > 0)
      .flatMap((row) => horseNumbers(row.currentHorseNumbers)),
  );
  const assigned = new Set<string>();
  const eligible = foreign.flatMap((row) => {
    foreignOwnerMetadata(row);
    if (
      row.category !== "ownerVenue" ||
      row.statsScope !== "stored-results-venue-30y" ||
      ![row.starts, row.winCount, row.quinellaCount, row.showCount].every(
        (count) => Number.isSafeInteger(count) && count >= 0,
      ) ||
      row.winCount > row.quinellaCount ||
      row.quinellaCount > row.showCount ||
      row.showCount > row.starts
    )
      throw new Error("Foreign owner counts are malformed");
    const numbers = horseNumbers(row.currentHorseNumbers);
    numbers.forEach((number) => {
      if (assigned.has(number)) throw new Error("Duplicate foreign owner assignment");
      assigned.add(number);
    });
    return row.starts === 0
      ? []
      : numbers
          .filter((number) => !protectedNumbers.has(number))
          .map((number) => Object.assign({}, row, { currentHorseNumbers: number }));
  });
  const replacements = new Set(eligible.map((row) => row.currentHorseNumbers));
  return [
    ...native.flatMap((row) => {
      if (row.category !== "ownerVenue" || row.starts > 0) return [row];
      const remaining = horseNumbers(row.currentHorseNumbers).filter(
        (number) => !replacements.has(number),
      );
      return remaining.length === 0 ? [] : [{ ...row, currentHorseNumbers: remaining.join(",") }];
    }),
    ...eligible,
  ];
};
