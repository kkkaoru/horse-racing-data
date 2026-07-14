// Run with bun. Per-race features derived by pc-keiba-r2-catalog from Iceberg raw tables.
// Catalog failure is fatal: feature archives and PostgreSQL are never input fallbacks.

import { fetchCatalogRows, isRecord } from "../catalog-client";
import type { DailyRaceEntryRow, Env, RaceJobKey } from "../types";
import { normaliseDailyRaceEntryRow } from "./normalise";

const RACE_FEATURES_URL = "https://pc-keiba-r2-catalog/v1/race-features";

const buildRaceFeaturesUrl = (job: RaceJobKey): URL => {
  const url = new URL(RACE_FEATURES_URL);
  url.searchParams.set("date", `${job.kaisaiNen}${job.kaisaiTsukihi}`);
  url.searchParams.set("source", job.source);
  url.searchParams.set("keibajoCode", job.keibajoCode.padStart(2, "0"));
  url.searchParams.set("raceBango", job.raceBango.padStart(2, "0"));
  return url;
};

const toDailyRaceEntryRow = (value: unknown): DailyRaceEntryRow => {
  if (!isRecord(value)) {
    throw new Error("PC_KEIBA_R2_CATALOG /v1/race-features returned an invalid row");
  }
  return normaliseDailyRaceEntryRow(value);
};

export const buildRaceFeatures = async (
  job: RaceJobKey,
  env: Pick<Env, "PC_KEIBA_R2_CATALOG">,
): Promise<DailyRaceEntryRow[]> => {
  const rows = await fetchCatalogRows(env.PC_KEIBA_R2_CATALOG, buildRaceFeaturesUrl(job));
  return rows.map(toDailyRaceEntryRow);
};
