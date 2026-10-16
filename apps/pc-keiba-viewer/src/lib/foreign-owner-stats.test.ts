// Run with bun.
import { expect, it } from "vitest";

import {
  foreignOwnerMetadata,
  isUnfilteredForeignOwnerSettings,
  mergeForeignOwnerVenueRows,
} from "./foreign-owner-stats";
it("retains strict stored metadata and rejects partial provenance", () => {
  expect(foreignOwnerMetadata({})).toStrictEqual({});
  expect(
    foreignOwnerMetadata({
      statsSource: "netkeiba",
      sourcePersonId: "a00762",
      statsScope: "stored-results-10y",
    }),
  ).toStrictEqual({
    statsSource: "netkeiba",
    sourcePersonId: "a00762",
    statsScope: "stored-results-10y",
  });
  expect(() => foreignOwnerMetadata({ statsSource: "netkeiba" })).toThrow(
    "Foreign owner metadata is malformed",
  );
  expect(mergeForeignOwnerVenueRows([], [])).toStrictEqual([]);
});

it.each(["000000", "123", "a0006f0", "", "a00-6f"])(
  "rejects invalid owner ID %s",
  (sourcePersonId) => {
    expect(() =>
      foreignOwnerMetadata({
        statsSource: "netkeiba",
        sourcePersonId,
        statsScope: "stored-results-10y",
      }),
    ).toThrow(/Foreign owner|Duplicate foreign/);
  },
);
it.each(["stored-results-10y", "stored-results-race-10y"])(
  "accepts real alpha ID for %s",
  (statsScope) => {
    expect(
      foreignOwnerMetadata({
        category: "owner",
        statsSource: "netkeiba",
        sourcePersonId: "a0006f",
        statsScope,
      }).sourcePersonId,
    ).toBe("a0006f");
  },
);
it.each([
  { category: "trainer", statsScope: "stored-results-10y" },
  { category: "owner", statsScope: "stored-results-venue-30y" },
  { category: "ownerVenue", statsScope: "stored-results-race-10y" },
  { category: "owner", statsScope: "wrong" },
])("rejects category/scope mismatch %j", (fields) => {
  expect(() =>
    foreignOwnerMetadata({ ...fields, statsSource: "netkeiba", sourcePersonId: "a00762" }),
  ).toThrow(/Foreign owner|Duplicate foreign/);
});
it("retains legacy domestic provenance", () => {
  expect(
    foreignOwnerMetadata({ statsSource: "jv", statsScope: "japan-all-venues-10y" }),
  ).toStrictEqual({ statsSource: "jv", statsScope: "japan-all-venues-10y" });
});

const settings = {
  cellMatching: true,
  classConditionName: null,
  includeAge: false,
  includeBloodlineAncestors: true,
  includeClass: false,
  includeConditionKey: false,
  includeDistance: false,
  includeFrame: false,
  includeGrade: false,
  includeMonthWindow: false,
  includeNarOnly: false,
  includeRaceNumber: false,
  includeRaceSubtitle: false,
  includeRaceTitle: true,
  includeRunnerCount: false,
  includeSex: false,
  includeSurface: false,
  includeTrackCode: false,
  includeTurn: false,
  includeVenue: true,
  includeWeight: false,
  runnerCount: null,
  sourceScope: "all",
  years: null,
} satisfies import("./race-types").SimilarRaceStatsSettings;
it.each([null, 10])("accepts the default and unfiltered effective ten years %s", (years) => {
  expect(isUnfilteredForeignOwnerSettings({ ...settings, years })).toBe(true);
  expect(
    isUnfilteredForeignOwnerSettings({
      ...settings,
      years,
      includeVenue: false,
      includeRaceTitle: false,
    }),
  ).toBe(true);
});
it.each([
  { years: 5 },
  { sourceScope: "jra" },
  { includeVenue: false },
  { includeRaceTitle: false },
  { includeDistance: true },
  { includeFrame: true },
  { includeWeight: true },
  { includeMonthWindow: true },
] satisfies Partial<import("./race-types").SimilarRaceStatsSettings>[])(
  "rejects unsupported full settings %j",
  (change) => {
    expect(isUnfilteredForeignOwnerSettings({ ...settings, ...change })).toBe(false);
  },
);
const venue = {
  category: "ownerVenue",
  currentHorseNumbers: "01",
  name: "netkeiba:a00762",
  details: [],
  horseCount: 0,
  starts: 3,
  winCount: 0,
  quinellaCount: 0,
  showCount: 0,
  winRate: 0,
  quinellaRate: 0,
  showRate: 0,
  statsSource: "netkeiba",
  sourcePersonId: "a00762",
  statsScope: "stored-results-venue-30y",
} satisfies import("./win-rate-heatmap").WinRateHeatmapPartnershipRow;
it("protects both grouped native horses with positive starts and zero wins", () => {
  expect(
    mergeForeignOwnerVenueRows(
      [{ ...venue, currentHorseNumbers: "1,11" }],
      [venue, { ...venue, currentHorseNumbers: "011" }],
    ).map((r) => r.currentHorseNumbers),
  ).toStrictEqual(["1,11"]);
});
it("replaces only unknown native horses and canonicalizes padding", () => {
  expect(
    mergeForeignOwnerVenueRows([{ ...venue, starts: 0, currentHorseNumbers: "1,11" }], [venue]).map(
      (r) => r.currentHorseNumbers,
    ),
  ).toStrictEqual(["11", "1"]);
  expect(mergeForeignOwnerVenueRows([], [{ ...venue, starts: 0 }])).toStrictEqual([]);
});
it.each([
  { starts: -1 },
  { starts: 1.5 },
  { winCount: 2 },
  { showCount: 4 },
  { currentHorseNumbers: "x" },
  { category: "horseJockey" },
] satisfies Partial<import("./win-rate-heatmap").WinRateHeatmapPartnershipRow>[])(
  "rejects malformed foreign assignment %j",
  (change) => {
    expect(() => mergeForeignOwnerVenueRows([], [{ ...venue, ...change }])).toThrow(
      /Foreign owner|Duplicate foreign/,
    );
  },
);
it("rejects duplicate and conflicting canonical horse assignments", () => {
  expect(() =>
    mergeForeignOwnerVenueRows(
      [],
      [venue, { ...venue, currentHorseNumbers: "1", sourcePersonId: "a0006f" }],
    ),
  ).toThrow(/Foreign owner|Duplicate foreign/);
});
