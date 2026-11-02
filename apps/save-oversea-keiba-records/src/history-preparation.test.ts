// This file runs with Bun.
import { expect, it } from "vitest";
import {
  prepareHistoryArchive,
  historyRowsToInput,
  historyRowFingerprint,
} from "./history-preparation";
import {
  historyPlanDigest,
  type HistoryArchivePlan,
  type HistoryArchiveState,
} from "./history-archive";
import type { SecondaryPersonResult } from "./sources/secondary-result-parser";

const plan: HistoryArchivePlan = {
  kind: "owner",
  sourceId: "owner1",
  initialUrl: "https://example.test/results/",
  encoding: "utf-8",
  initialHtmlPath: null,
  profile: {
    populationPattern: "Count ([0-9]+)",
    nextLabels: ["Next"],
    emptyMarker: "No results",
    markup: {
      tableMarker: "demo-results",
      racePathPrefix: "/event/",
      horsePathPrefix: "/animal/",
      jockeyPathPrefix: "/rider/",
      raceUrlTemplate: "https://example.test/event/{RACE_ID}",
      horseFields: {
        date: 0,
        venue: 1,
        raceNumber: 2,
        raceName: 3,
        finishPosition: 4,
        distance: 5,
        going: 6,
        relatedEntity: 7,
      },
      personFields: {
        date: 0,
        venue: 1,
        raceNumber: 2,
        raceName: 3,
        finishPosition: 4,
        distance: 5,
        going: 6,
        relatedEntity: 7,
      },
    },
  },
};
const row: SecondaryPersonResult = {
  personKind: "owner",
  sourcePersonId: "owner1",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: "Venue",
  raceNumber: "1",
  raceName: "Race",
  sourceRaceUrl: "https://example.test/event/race1",
  sourceHorseId: "horse1",
  horseName: "Horse",
  finishPosition: 2,
  finishPositionText: "2",
  surface: "Dirt",
  distanceMetres: 1800,
  going: "Good",
};
const state: HistoryArchiveState = {
  planDigest: historyPlanDigest(plan),
  publishedCount: 1,
  checkpoint: { pendingUrl: null, completedUrls: [plan.initialUrl], processedRows: 1 },
  rows: [row],
};

it("prepares scoped source-complete rows without claiming database publication", () => {
  const result = prepareHistoryArchive(state, plan);
  expect(result.input.people).toHaveLength(1);
  expect(result.sourceComplete).toBe(true);
  expect(result.sourcePartialRows).toStrictEqual([]);
});
it("keeps pending archival coverage incomplete", () => {
  const result = prepareHistoryArchive(
    {
      ...state,
      checkpoint: { ...state.checkpoint, pendingUrl: "https://example.test/results/?page=2" },
    },
    plan,
  );
  expect(result.sourceComplete).toBe(false);
});
it("preserves genuine missing finish fields outside canonical publication", () => {
  const result = prepareHistoryArchive(
    { ...state, rows: [{ ...row, finishPosition: null, finishPositionText: "" }] },
    plan,
  );
  expect(result.input.people).toStrictEqual([]);
  expect(result.sourcePartialRows).toHaveLength(1);
  expect(result.archivedRows).toBe(1);
});
it.each([null, "", "  "])(
  "preserves missing runner identity %j without fabrication",
  (horseName) => {
    const result = prepareHistoryArchive(
      { ...state, rows: [{ ...row, sourceHorseId: null, horseName }] },
      plan,
    );
    expect(result.sourcePartialRows).toHaveLength(1);
    expect(result.input.people).toStrictEqual([]);
  },
);
it("preserves a person row without a published race number outside publication", () => {
  const result = prepareHistoryArchive({ ...state, rows: [{ ...row, raceNumber: " " }] }, plan);
  expect(result.sourcePartialRows).toHaveLength(1);
  expect(result.input.people).toStrictEqual([]);
});
it("preserves a horse row without a published rider outside publication", () => {
  const horsePlan: HistoryArchivePlan = { ...plan, kind: "horse", sourceId: "horse1" };
  const horseRow = {
    sourceHorseId: "horse1",
    sourceRaceId: "race1",
    raceDate: "2026-09-01",
    venue: "Venue",
    raceDaySequence: 1,
    raceName: "Race",
    sourceRaceUrl: "https://example.test/event/race1",
    finishPosition: 2,
    finishPositionText: "2",
    jockeyName: "",
    sourceJockeyId: null,
    surface: "ダ",
    distanceMetres: 1800,
    going: "良",
  };
  const result = prepareHistoryArchive(
    {
      ...state,
      planDigest: historyPlanDigest(horsePlan),
      rows: [horseRow, { ...horseRow, sourceRaceId: "race2", jockeyName: "Rider" }],
      publishedCount: 2,
      checkpoint: { ...state.checkpoint, processedRows: 2 },
    },
    horsePlan,
  );
  expect(result.sourcePartialRows).toStrictEqual([horseRow]);
  expect(result.input.horses).toHaveLength(1);
});
it("keeps a horse row without a published distance as source-partial", () => {
  const horsePlan: HistoryArchivePlan = { ...plan, kind: "horse", sourceId: "horse1" };
  const horseRow = {
    sourceHorseId: "horse1",
    sourceRaceId: "race1",
    raceDate: "2025-06-27",
    venue: "Venue",
    raceDaySequence: 1,
    raceName: "Race",
    sourceRaceUrl: "https://example.test/event/race1",
    finishPosition: 1,
    finishPositionText: "1",
    jockeyName: "Rider",
    sourceJockeyId: null,
    surface: "芝",
    distanceMetres: null,
    going: "稍",
  };
  const result = prepareHistoryArchive(
    {
      ...state,
      planDigest: historyPlanDigest(horsePlan),
      rows: [horseRow, { ...horseRow, sourceRaceId: "race2", distanceMetres: 2800 }],
      publishedCount: 2,
      checkpoint: { ...state.checkpoint, processedRows: 2 },
    },
    horsePlan,
  );
  expect(result.sourcePartialRows).toStrictEqual([horseRow]);
  expect(result.input.horses).toHaveLength(1);
});
it("accepts a published horse name when no source horse ID is available", () => {
  const result = prepareHistoryArchive({ ...state, rows: [{ ...row, sourceHorseId: null }] }, plan);
  expect(result.input.people).toHaveLength(1);
  expect(result.sourcePartialRows).toStrictEqual([]);
});
it("does not hide invalid canonical dates as source-partial", () => {
  expect(() =>
    prepareHistoryArchive({ ...state, rows: [{ ...row, raceDate: "2026-02-30" }] }, plan),
  ).toThrow("History contains an invalid source identity or calendar date.");
});
it("rejects a different actor even when its rows otherwise validate", () => {
  expect(() =>
    prepareHistoryArchive({ ...state, rows: [{ ...row, sourcePersonId: "other" }] }, plan),
  ).toThrow("History archive contains a different source entity.");
});
it("rejects a different kind of actor", () => {
  expect(() =>
    prepareHistoryArchive({ ...state, rows: [{ ...row, personKind: "trainer" }] }, plan),
  ).toThrow("History archive contains a different source entity.");
});
it("deduplicates exact records but does not claim distinct source coverage", () => {
  const result = prepareHistoryArchive(
    {
      ...state,
      publishedCount: 2,
      checkpoint: { ...state.checkpoint, processedRows: 2 },
      rows: [row, { ...row }],
    },
    plan,
  );
  expect(result.input.people).toHaveLength(1);
  expect(result.duplicateRows).toBe(1);
  expect(result.sourceComplete).toBe(false);
});
it("rejects conflicting data for one natural key", () => {
  expect(() =>
    prepareHistoryArchive(
      {
        ...state,
        publishedCount: 2,
        checkpoint: { ...state.checkpoint, processedRows: 2 },
        rows: [row, { ...row, raceName: "Other" }],
      },
      plan,
    ),
  ).toThrow("History archive contains conflicting records for one natural key.");
});
it("supports strict horse archives and rejects a horse under a person plan", () => {
  const horsePlan: HistoryArchivePlan = { ...plan, kind: "horse", sourceId: "horse1" };
  const horse = {
    sourceHorseId: "horse1",
    sourceRaceId: "race1",
    raceDate: "2026-09-01",
    venue: "Venue",
    raceDaySequence: 1,
    raceName: "Race",
    sourceRaceUrl: "https://example.test/event/race1",
    finishPosition: 2,
    finishPositionText: "2",
    jockeyName: "Rider",
    sourceJockeyId: null,
    surface: "Dirt",
    distanceMetres: 1800,
    going: "Good",
  };
  expect(
    prepareHistoryArchive(
      { ...state, planDigest: historyPlanDigest(horsePlan), rows: [horse] },
      horsePlan,
    ).input.horses,
  ).toHaveLength(1);
  expect(() => prepareHistoryArchive({ ...state, rows: [horse] }, plan)).toThrow(
    "History archive contains a different source entity.",
  );
  expect(() =>
    prepareHistoryArchive(
      {
        ...state,
        planDigest: historyPlanDigest(horsePlan),
        rows: [{ ...horse, sourceHorseId: "other" }],
      },
      horsePlan,
    ),
  ).toThrow("History archive contains a different source entity.");
});
it("uses deterministic field ordering for database candidate membership", () => {
  expect(historyRowFingerprint(row)).toMatch(
    /^\[\["distanceMetres",1800\],\["finishPosition",2\]/u,
  );
});

it("converts empty source collections without inventing data", () => {
  expect(historyRowsToInput([])).toStrictEqual({ horses: [], people: [] });
});
it("publishes a source finish of 0 as an unavailable placing with its text", () => {
  const result = prepareHistoryArchive(
    { ...state, rows: [{ ...row, finishPosition: 0, finishPositionText: "0" }] },
    plan,
  );
  expect(result.input.people).toStrictEqual([
    { ...row, finishPosition: null, finishPositionText: "0" },
  ]);
});
