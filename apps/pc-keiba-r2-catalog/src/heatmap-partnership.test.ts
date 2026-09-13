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
  date: "20260913",
  keibajoCode: "06",
  kind: "jockeyVenue",
  revision: "snapshot-1",
  source: "jra",
};
const history: PartnershipHistory = {
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
    "heatmap-partnership-v1:snapshot-1:jra:20260913:06:jockeyVenue",
  );
  expect(partnershipCacheKey({ ...scope, kind: "horseJockey", keibajoCode: "05" })).toBe(
    "heatmap-partnership-v1:snapshot-1:jra:20260913:all-venues:horseJockey",
  );
  expect(partnershipCacheKey({ ...scope, revision: "snapshot:2" })).toBe(
    "heatmap-partnership-v1:snapshot%3A2:jra:20260913:06:jockeyVenue",
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
