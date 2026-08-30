// Run with bun. `bun run --filter pc-keiba-viewer test`
import { expect, it } from "vitest";

import { buildRaceEntityCacheWarmPath } from "./race-entity-cache-warm";

it("builds the internal Catalog warm path for one race", () => {
  expect(
    buildRaceEntityCacheWarmPath({
      day: "27",
      keibajoCode: "50",
      kind: "race-entity-results",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    }),
  ).toBe(
    "/v1/internal/race-entity-recent-results/warm?date=20260827&keibajoCode=50&raceBango=05&source=nar",
  );
});
