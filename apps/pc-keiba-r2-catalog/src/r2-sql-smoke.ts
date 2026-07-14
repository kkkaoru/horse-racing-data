import { buildRaceFeaturesQuery, buildRaceKeysQuery, executeR2Sql } from "./r2-sql";
import type { Fetcher, R2SqlCatalogConfig, RaceFeatureFilters, SourceScope } from "./types";

export interface R2SqlSmokeResult {
  featureRows: number;
  raceKeyRows: number;
}

const isSourceScope = (value: string): value is SourceScope =>
  value === "all" || value === "ban-ei" || value === "jra" || value === "nar";

const optionalCode = (value: string | undefined, label: string): string | undefined => {
  if (value === undefined) return undefined;
  if (!/^\d{1,2}$/u.test(value)) throw new Error(`${label} must contain one or two digits`);
  return value.padStart(2, "0");
};

export const parseR2SqlSmokeArgs = (args: string[]): RaceFeatureFilters => {
  const date = args[0];
  if (date === undefined) {
    throw new Error("usage: smoke:r2-sql YYYYMMDD [source] [keibajoCode] [raceBango]");
  }
  const source = args[1] ?? "jra";
  if (!isSourceScope(source)) throw new Error("source must be jra, nar, ban-ei, or all");
  return {
    date,
    keibajoCode: optionalCode(args[2], "keibajoCode"),
    raceBango: optionalCode(args[3], "raceBango"),
    source,
  };
};

export const runR2SqlSmoke = async (
  config: R2SqlCatalogConfig,
  filters: RaceFeatureFilters,
  fetchImpl: Fetcher,
): Promise<R2SqlSmokeResult> => {
  const raceKeyRows = await executeR2Sql(
    config,
    buildRaceKeysQuery(config, filters.date),
    fetchImpl,
  );
  const featureRows = await executeR2Sql(
    config,
    buildRaceFeaturesQuery(config, filters),
    fetchImpl,
  );
  return { featureRows: featureRows.length, raceKeyRows: raceKeyRows.length };
};
