// Run with bun. Resolves running-style race lists from the raw Iceberg catalog.

import { fetchRunningStyleRaceKeysFromCatalog } from "./running-style-catalog-client";
import type { RegisteredRaceRow } from "./running-style-cron";
import type { Env } from "./types";

export interface RunningStyleRaceListResult {
  races: RegisteredRaceRow[];
  source: "catalog" | "d1" | "features";
}

const DOMESTIC_JRA_VENUE_PATTERN: RegExp = /^(?:0[1-9]|10)$/u;

export const listRunningStyleRacesByDate = async (
  env: Env,
  date: string,
): Promise<RunningStyleRaceListResult> => ({
  // Overseas sales races remain in the public Catalog, but have no domestic
  // running-style job. Waiting for them would permanently block the R2 export.
  races: (await fetchRunningStyleRaceKeysFromCatalog(env.PC_KEIBA_R2_CATALOG, date)).filter(
    (race) => race.source !== "jra" || DOMESTIC_JRA_VENUE_PATTERN.test(race.keibajo_code),
  ),
  source: "catalog",
});
