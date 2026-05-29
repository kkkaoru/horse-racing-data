// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { buildRaceParquetPrefix, buildRaceParquetR2Key } from "./r2-key";

it("builds nested key path", () => {
  expect(
    buildRaceParquetR2Key({
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
    }),
  ).toBe("features/by-race/2026/05/29/nar/30/08.parquet");
});

it("zero-pads single-digit keibajoCode", () => {
  expect(
    buildRaceParquetR2Key({
      source: "jra",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "5",
      raceBango: "1",
    }),
  ).toBe("features/by-race/2026/05/29/jra/05/01.parquet");
});

it("builds prefix path", () => {
  expect(
    buildRaceParquetPrefix({
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
    }),
  ).toBe("features/by-race/2026/05/29/nar/30/");
});
