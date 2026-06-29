// run with: bun run test
import { expect, it } from "vitest";

import {
  DEFAULT_RUNNING_STYLE_VARIANT_ID,
  deriveRunningStyleCell,
  deriveRunningStyleCategory,
  deriveRunningStyleDistanceBand,
  deriveRunningStyleSeason,
  deriveRunningStyleSurface,
  isBanEiRunningStyleKeibajoCode,
  resolveRunningStyleCellDimension,
  resolveRunningStyleCellRoute,
  runningStyleCellConditionMatches,
  runningStyleCellRuleMatches,
  type RunningStyleCellRoutingConfig,
} from "./running-style-cell-router";

const BASE_INPUT = {
  gradeCode: "A",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0712",
  keibajoCode: "5",
  kyori: 1800,
  kyosoJokenCode: "703",
  narSubClass: null,
  raceBango: "9",
  shussoTosu: 16,
  source: "jra" as const,
  trackCode: "10",
};

const ROUTING = {
  jra: {
    defaultVariantId: "latest",
    rules: [
      {
        conditions: [
          { dimension: "venue", values: ["05"] },
          { dimension: "surface", values: ["turf"] },
        ],
        variantId: "tokyo-turf",
      },
    ],
    variants: {
      latest: { modelKey: "running-style/models/jra/latest.flatbin" },
      "tokyo-turf": { modelKey: "running-style/models/jra/cells/tokyo-turf.flatbin" },
    },
  },
} satisfies RunningStyleCellRoutingConfig;

it("deriveRunningStyleCategory maps jra source to jra", () => {
  expect(deriveRunningStyleCategory({ keibajoCode: "83", source: "jra" })).toBe("jra");
});

it("deriveRunningStyleCategory maps nar ban-ei venue codes to ban-ei", () => {
  expect(deriveRunningStyleCategory({ keibajoCode: "83", source: "nar" })).toBe("ban-ei");
  expect(deriveRunningStyleCategory({ keibajoCode: "65", source: "nar" })).toBe("ban-ei");
  expect(isBanEiRunningStyleKeibajoCode("5")).toBe(false);
});

it("deriveRunningStyleCategory maps nar non-ban-ei venues to nar", () => {
  expect(deriveRunningStyleCategory({ keibajoCode: "44", source: "nar" })).toBe("nar");
});

it("deriveRunningStyleSurface derives jra turf dirt other and missing track", () => {
  expect(deriveRunningStyleSurface("10", "jra")).toBe("turf");
  expect(deriveRunningStyleSurface("20", "jra")).toBe("dirt");
  expect(deriveRunningStyleSurface("90", "jra")).toBe("other");
  expect(deriveRunningStyleSurface(null, "jra")).toBeNull();
});

it("deriveRunningStyleSurface maps non-jra categories to dirt", () => {
  expect(deriveRunningStyleSurface(null, "nar")).toBe("dirt");
  expect(deriveRunningStyleSurface("10", "ban-ei")).toBe("dirt");
});

it("deriveRunningStyleDistanceBand covers every distance band and missing distance", () => {
  expect(deriveRunningStyleDistanceBand(null)).toBeNull();
  expect(deriveRunningStyleDistanceBand(1000)).toBe("sprint");
  expect(deriveRunningStyleDistanceBand(1400)).toBe("mile");
  expect(deriveRunningStyleDistanceBand(1800)).toBe("intermediate");
  expect(deriveRunningStyleDistanceBand(2200)).toBe("long");
  expect(deriveRunningStyleDistanceBand(2600)).toBe("extended");
});

it("deriveRunningStyleSeason covers all seasons and malformed month", () => {
  expect(deriveRunningStyleSeason("0412")).toBe("spring");
  expect(deriveRunningStyleSeason("0712")).toBe("summer");
  expect(deriveRunningStyleSeason("1012")).toBe("autumn");
  expect(deriveRunningStyleSeason("0112")).toBe("winter");
  expect(deriveRunningStyleSeason("xx12")).toBeNull();
});

it("deriveRunningStyleCell normalizes identity and keeps race metadata", () => {
  const cell = deriveRunningStyleCell(BASE_INPUT);
  expect(cell.raceKey).toBe("jra:20260712:05:09");
  expect(cell.venue).toBe("05");
  expect(cell.racetrack).toBe("05");
  expect(cell.class).toBe("A");
  expect(cell.subgroup).toBe("703");
  expect(cell.shussoTosu).toBe(16);
});

it("deriveRunningStyleCell derives nar subgroup from narSubClass", () => {
  const cell = deriveRunningStyleCell({
    ...BASE_INPUT,
    category: "nar",
    gradeCode: "",
    keibajoCode: "44",
    kyori: Number.NaN,
    kyosoJokenCode: "999",
    narSubClass: "C",
    shussoTosu: Number.POSITIVE_INFINITY,
    source: "nar",
    trackCode: "",
  });
  expect(cell.category).toBe("nar");
  expect(cell.class).toBe("unknown");
  expect(cell.distanceBand).toBeNull();
  expect(cell.shussoTosu).toBeNull();
  expect(cell.subgroup).toBe("C");
  expect(cell.surface).toBe("dirt");
  expect(cell.trackCode).toBeNull();
});

it("resolveRunningStyleCellDimension resolves known aliases and unknown dimensions", () => {
  const cell = deriveRunningStyleCell(BASE_INPUT);
  expect(resolveRunningStyleCellDimension(cell, "category")).toBe("jra");
  expect(resolveRunningStyleCellDimension(cell, "keibajo_code")).toBe("05");
  expect(resolveRunningStyleCellDimension(cell, "distance_band")).toBe("intermediate");
  expect(resolveRunningStyleCellDimension(cell, "season")).toBe("summer");
  expect(resolveRunningStyleCellDimension(cell, "class")).toBe("A");
  expect(resolveRunningStyleCellDimension(cell, "grade_code")).toBe("A");
  expect(resolveRunningStyleCellDimension(cell, "subgroup")).toBe("703");
  expect(resolveRunningStyleCellDimension(cell, "kyoso_joken_code")).toBe("703");
  expect(resolveRunningStyleCellDimension(cell, "shusso_tosu")).toBe("16");
  expect(resolveRunningStyleCellDimension(cell, "unknown")).toBeNull();
});

it("resolveRunningStyleCellDimension returns null for nullable dimensions", () => {
  const cell = deriveRunningStyleCell({
    ...BASE_INPUT,
    gradeCode: null,
    kyori: null,
    kyosoJokenCode: null,
    shussoTosu: null,
    trackCode: null,
  });
  expect(resolveRunningStyleCellDimension(cell, "surface")).toBeNull();
  expect(resolveRunningStyleCellDimension(cell, "distance_band")).toBeNull();
  expect(resolveRunningStyleCellDimension(cell, "grade_code")).toBeNull();
  expect(resolveRunningStyleCellDimension(cell, "kyoso_joken_code")).toBeNull();
  expect(resolveRunningStyleCellDimension(cell, "shusso_tosu")).toBeNull();
});

it("runningStyleCellConditionMatches returns false when the dimension is missing", () => {
  const cell = deriveRunningStyleCell(BASE_INPUT);
  expect(runningStyleCellConditionMatches(cell, { dimension: "nar_subclass", values: ["C"] })).toBe(
    false,
  );
});

it("runningStyleCellRuleMatches requires every condition to match", () => {
  const cell = deriveRunningStyleCell(BASE_INPUT);
  expect(
    runningStyleCellRuleMatches(cell, {
      conditions: [
        { dimension: "venue", values: ["05"] },
        { dimension: "surface", values: ["turf"] },
      ],
      variantId: "tokyo-turf",
    }),
  ).toBe(true);
  expect(
    runningStyleCellRuleMatches(cell, {
      conditions: [
        { dimension: "venue", values: ["05"] },
        { dimension: "season", values: ["winter"] },
      ],
      variantId: "winter",
    }),
  ).toBe(false);
});

it("resolveRunningStyleCellRoute returns the historical source model when no routing exists", () => {
  const route = resolveRunningStyleCellRoute(BASE_INPUT);
  expect(route.variantId).toBe(DEFAULT_RUNNING_STYLE_VARIANT_ID);
  expect(route.modelKey).toBe("running-style/models/jra/latest.flatbin");
  expect(route.cell.raceKey).toBe("jra:20260712:05:09");
});

it("resolveRunningStyleCellRoute returns the matched configured variant", () => {
  const route = resolveRunningStyleCellRoute(BASE_INPUT, ROUTING);
  expect(route.variantId).toBe("tokyo-turf");
  expect(route.modelKey).toBe("running-style/models/jra/cells/tokyo-turf.flatbin");
});

it("resolveRunningStyleCellRoute falls back to the configured default variant", () => {
  const route = resolveRunningStyleCellRoute({ ...BASE_INPUT, keibajoCode: "06" }, ROUTING);
  expect(route.variantId).toBe("latest");
  expect(route.modelKey).toBe("running-style/models/jra/latest.flatbin");
});

it("resolveRunningStyleCellRoute throws when the selected variant is missing", () => {
  expect(() =>
    resolveRunningStyleCellRoute(BASE_INPUT, {
      jra: {
        defaultVariantId: "latest",
        rules: [{ conditions: [{ dimension: "venue", values: ["05"] }], variantId: "missing" }],
        variants: { latest: { modelKey: "running-style/models/jra/latest.flatbin" } },
      },
    }),
  ).toThrow("running-style cell route variant is not configured: missing");
});
