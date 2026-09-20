// Run with bun (bunx vitest).
import { expect, it } from "vitest";
import {
  aggregatePartnership,
  partnershipCacheKey,
  partnershipStartDate,
  validatePartnershipScope,
  type PartnershipHistory,
  type PartnershipScope,
} from "./heatmap-partnership";

const scope: PartnershipScope = {
  surface: "芝",
  date: "20260913",
  keibajoCode: "06",
  kind: "jockeyVenue",
  revision: "snapshot-1",
  source: "jra",
};
const history: PartnershipHistory = {
  surface: "芝",
  date: "20230913",
  finishPosition: 1,
  horseId: "horse-1",
  jockeyId: "jockey-1",
  keibajoCode: "06",
  raceId: "race-1",
  resultId: "result-1",
  source: "jra",
  trainerId: "trainer-1",
};

it.each(["jockeyVenue", "jockeyTrainerVenue"] satisfies PartnershipScope["kind"][])(
  "separates turf, dirt and jumps for %s",
  (kind) => {
    const mixed: PartnershipHistory[] = [
      history,
      { ...history, surface: "ダート", resultId: "dirt", raceId: "dirt", finishPosition: 4 },
      { ...history, surface: "障害", resultId: "jump", raceId: "jump", finishPosition: 2 },
    ];
    expect(
      aggregatePartnership({ scope: { ...scope, kind, surface: "芝" }, history: mixed }).rows.map(
        (row) => [row.starts, row.wins],
      ),
    ).toStrictEqual([[1, 1]]);
    expect(
      aggregatePartnership({
        scope: { ...scope, kind, surface: "ダート" },
        history: mixed,
      }).rows.map((row) => [row.starts, row.wins]),
    ).toStrictEqual([[1, 0]]);
    expect(
      aggregatePartnership({ scope: { ...scope, kind, surface: "障害" }, history: mixed }).raceIds,
    ).toStrictEqual(["jump"]);
    expect(
      new Set(
        ["芝", "ダート", "障害"].map((surface) => partnershipCacheKey({ ...scope, kind, surface })),
      ).size,
    ).toBe(3);
  },
);

it.each([undefined, "", "unknown", "芝' OR TRUE"])(
  "rejects missing or invalid cohort surface %j",
  (surface) => {
    expect(() => partnershipCacheKey({ ...scope, surface })).toThrow(
      "target surface is unavailable",
    );
  },
);

it("uses independent thirty-year owner cohorts with exact identities and surfaces", () => {
  const ownerHistory: PartnershipHistory[] = [
    { ...history, ownerId: "o1", date: "19960913", jockeyId: null },
    { ...history, ownerId: "o1", resultId: "away", keibajoCode: "09", finishPosition: 2 },
    { ...history, ownerId: "o2", resultId: "other-owner", finishPosition: 3 },
    { ...history, ownerId: "o1", resultId: "dirt", surface: "ダート" },
    { ...history, ownerId: "o1", resultId: "old", date: "19960912" },
    { ...history, ownerId: "o1", resultId: "today", date: "20260913" },
    { ...history, ownerId: null, resultId: "missing" },
  ];
  expect(partnershipStartDate({ ...scope, kind: "ownerVenue" })).toBe("19960913");
  expect(partnershipStartDate({ ...scope, kind: "jockeyTrainerOwner" })).toBe("19960913");
  expect(
    aggregatePartnership({ scope: { ...scope, kind: "ownerVenue" }, history: ownerHistory }).rows,
  ).toStrictEqual([
    { entityKey: '["o1"]', starts: 1, wins: 1, places: 1, shows: 1 },
    { entityKey: '["o2"]', starts: 1, wins: 0, places: 0, shows: 1 },
  ]);
  expect(
    aggregatePartnership({ scope: { ...scope, kind: "jockeyTrainerOwner" }, history: ownerHistory })
      .rows,
  ).toStrictEqual([
    { entityKey: '["trainer-1","jockey-1","o1"]', starts: 1, wins: 0, places: 1, shows: 1 },
    { entityKey: '["trainer-1","jockey-1","o2"]', starts: 1, wins: 0, places: 0, shows: 1 },
  ]);
  expect(partnershipCacheKey({ ...scope, kind: "jockeyTrainerOwner", keibajoCode: "09" })).toBe(
    "heatmap-partnership-v2:snapshot-1:jra:20260913:all-venues:jockeyTrainerOwner-all-sources:%E8%8A%9D",
  );
});

it("includes the identical triple in both JRA and NAR, but excludes other surfaces and owners", () => {
  expect(
    aggregatePartnership({
      scope: { ...scope, kind: "jockeyTrainerOwner" },
      history: [
        { ...history, ownerId: "o1" },
        {
          ...history,
          ownerId: "o1",
          source: "nar",
          keibajoCode: "44",
          resultId: "nar",
          finishPosition: 2,
        },
        {
          ...history,
          ownerId: "o1",
          source: "nar",
          keibajoCode: "44",
          resultId: "dirt",
          surface: "ダート",
        },
      ],
    }).rows,
  ).toStrictEqual([
    { entityKey: '["trainer-1","jockey-1","o1"]', starts: 2, wins: 1, places: 2, shows: 2 },
  ]);
});

it("uses fixed windows and clamps leap-day anniversaries", () => {
  expect(partnershipStartDate(scope)).toBe("20230913");
  expect(partnershipStartDate({ ...scope, kind: "jockeyTrainerVenue" })).toBe("20160913");
  expect(partnershipStartDate({ ...scope, kind: "horseJockey" })).toBeNull();
  expect(partnershipStartDate({ ...scope, date: "20240229" })).toBe("20210228");
});

it("rejects invalid dates, venues and missing data revisions", () => {
  expect(() => validatePartnershipScope({ ...scope, date: "20260229" })).toThrow("valid YYYYMMDD");
  expect(() => validatePartnershipScope({ ...scope, date: "not-date" })).toThrow("valid YYYYMMDD");
  expect(() => validatePartnershipScope({ ...scope, date: "20269999" })).toThrow("valid YYYYMMDD");
  expect(() => validatePartnershipScope({ ...scope, keibajoCode: "6" })).toThrow("two digits");
  expect(() => validatePartnershipScope({ ...scope, revision: " " })).toThrow(
    "revision is required",
  );
});

it("keys shared cohorts independently of target race and separates revisions and scopes", () => {
  expect(partnershipCacheKey(scope)).toBe(
    "heatmap-partnership-v2:snapshot-1:jra:20260913:06:jockeyVenue:%E8%8A%9D",
  );
  expect(partnershipCacheKey({ ...scope, kind: "horseJockey", keibajoCode: "05" })).toBe(
    "heatmap-partnership-v2:snapshot-1:jra:20260913:all-venues:horseJockey:all-surfaces",
  );
  expect(partnershipCacheKey({ ...scope, revision: "snapshot:2" })).toBe(
    "heatmap-partnership-v2:snapshot%3A2:jra:20260913:06:jockeyVenue:%E8%8A%9D",
  );
});

it("includes the anniversary, excludes target date, other venues, sources and duplicate results", () => {
  expect(
    aggregatePartnership({
      scope,
      history: [
        history,
        history,
        { ...history, date: "20230912", resultId: "old", raceId: "old" },
        { ...history, date: "20260913", resultId: "today", raceId: "today" },
        { ...history, date: "20270913", resultId: "future", raceId: "future" },
        { ...history, keibajoCode: "05", resultId: "other", raceId: "other" },
        { ...history, source: "nar", resultId: "nar", raceId: "nar" },
        { ...history, date: "invalid", resultId: "invalid", raceId: "invalid" },
      ],
    }),
  ).toStrictEqual({
    raceIds: ["race-1"],
    rows: [{ entityKey: '["jockey-1"]', places: 1, shows: 1, starts: 1, wins: 1 }],
  });
});

it("counts partnerships by IDs, not names, and retains race targets with unavailable results", () => {
  expect(
    aggregatePartnership({
      scope: { ...scope, kind: "jockeyTrainerVenue" },
      history: [
        { ...history, date: "20160913" },
        { ...history, finishPosition: 2, resultId: "second" },
        { ...history, finishPosition: 3, resultId: "third" },
        { ...history, finishPosition: 4, resultId: "fourth", trainerId: "trainer-2" },
        { ...history, finishPosition: null, resultId: "missing", raceId: "race-2" },
        { ...history, finishPosition: 0, resultId: "cancelled" },
        { ...history, finishPosition: 1.5, resultId: "invalid-rank" },
        { ...history, jockeyId: null, resultId: "missing-jockey" },
        { ...history, jockeyId: " ", resultId: "empty-jockey" },
        { ...history, trainerId: null, resultId: "missing-trainer" },
        { ...history, trainerId: "", resultId: "empty-trainer" },
      ],
    }),
  ).toStrictEqual({
    raceIds: ["race-1", "race-2"],
    rows: [
      { entityKey: '["trainer-1","jockey-1"]', places: 2, shows: 3, starts: 3, wins: 1 },
      { entityKey: '["trainer-2","jockey-1"]', places: 0, shows: 0, starts: 1, wins: 0 },
    ],
  });
});

it("uses horse-jockey lifetime history across venues without including the target day", () => {
  expect(
    aggregatePartnership({
      scope: { ...scope, kind: "horseJockey" },
      history: [
        { ...history, date: "20000101", keibajoCode: "05" },
        { ...history, horseId: null, resultId: "unknown" },
        { ...history, date: "20260913", resultId: "today" },
      ],
    }),
  ).toStrictEqual({
    raceIds: ["race-1"],
    rows: [{ entityKey: '["horse-1","jockey-1"]', places: 1, shows: 1, starts: 1, wins: 1 }],
  });
  expect(aggregatePartnership({ scope, history: [] })).toStrictEqual({ raceIds: [], rows: [] });
});
