// Run with bun. Resolves running-style race lists from the raw Iceberg catalog.

import { fetchRunningStyleRaceKeysFromCatalog } from "./running-style-catalog-client";
import type { RegisteredRaceRow } from "./running-style-cron";
import type { Env } from "./types";

export interface RunningStyleRaceListResult {
  races: RegisteredRaceRow[];
  source: "catalog" | "d1" | "features";
}

export const listRunningStyleRacesByDate = async (
  env: Env,
  date: string,
): Promise<RunningStyleRaceListResult> => ({
  races: await fetchRunningStyleRaceKeysFromCatalog(env.PC_KEIBA_R2_CATALOG, date),
  source: "catalog",
});
