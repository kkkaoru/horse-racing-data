// This file runs with Bun.
import { expect, it } from "vitest";
import { decodeHistoryArchive, decodeHistoryPlan, isHistorySourceRow } from "./history-json";
import type { HistoryArchivePlan } from "./history-archive";

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
const person = {
  personKind: "owner",
  sourcePersonId: "owner1",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: null,
  raceNumber: "1",
  raceName: "Race",
  sourceRaceUrl: "https://example.test/event/race1",
  sourceHorseId: null,
  horseName: "Horse",
  finishPosition: null,
  finishPositionText: "",
  surface: null,
  distanceMetres: null,
  going: null,
};
const horse = {
  sourceHorseId: "horse1",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: "Venue",
  raceDaySequence: 1,
  raceName: "Race",
  sourceRaceUrl: "https://example.test/event/race1",
  finishPosition: 1,
  finishPositionText: "1",
  jockeyName: "Rider",
  sourceJockeyId: null,
  surface: "Dirt",
  distanceMetres: 1800,
  going: "Good",
};

it("decodes a private source plan", () => {
  expect(decodeHistoryPlan(JSON.stringify(plan))).toMatchObject({
    kind: "owner",
    sourceId: "owner1",
    encoding: "utf-8",
    initialHtmlPath: null,
  });
});

it("supports horse plans with a normal-browser HTML override", () => {
  expect(
    decodeHistoryPlan(
      JSON.stringify({ ...plan, kind: "horse", initialHtmlPath: "/private/rendered.html" }),
    ),
  ).toMatchObject({ kind: "horse", initialHtmlPath: "/private/rendered.html" });
});

it.each([
  null,
  [],
  {},
  { ...plan, kind: "unsupported" },
  { ...plan, profile: null },
  { ...plan, initialHtmlPath: 1 },
])("rejects malformed plans %j", (value) => {
  expect(() => decodeHistoryPlan(JSON.stringify(value))).toThrow(
    "Private history plan is invalid.",
  );
});

it.each([
  "http://example.test/results/",
  "https://user@example.test/results/",
  "https://:secret@example.test/results/",
  "https://example.test/results/#fragment",
])("rejects unsafe initial URL %s", (initialUrl) => {
  expect(() => decodeHistoryPlan(JSON.stringify({ ...plan, initialUrl }))).toThrow(
    "History plan URL or source identity is invalid.",
  );
});

it("rejects unsafe source IDs", () => {
  expect(() => decodeHistoryPlan(JSON.stringify({ ...plan, sourceId: "../other" }))).toThrow(
    "History plan URL or source identity is invalid.",
  );
});

it("rejects invalid field indexes", () => {
  const value = structuredClone(plan);
  Reflect.set(value.profile.markup.personFields, "date", -1);
  expect(() => decodeHistoryPlan(JSON.stringify(value))).toThrow(
    "Private history plan is invalid.",
  );
});

it("rejects absent field maps", () => {
  const value = structuredClone(plan);
  Reflect.set(value.profile.markup, "horseFields", null);
  expect(() => decodeHistoryPlan(JSON.stringify(value))).toThrow(
    "Private history plan is invalid.",
  );
});

it("rejects non-string next labels", () => {
  const value = structuredClone(plan);
  Reflect.set(value.profile, "nextLabels", [1]);
  expect(() => decodeHistoryPlan(JSON.stringify(value))).toThrow(
    "Private history plan is invalid.",
  );
});

it("decodes archive rows without inventing missing source values", () => {
  const value = decodeHistoryArchive(
    JSON.stringify({
      planDigest: "digest",
      publishedCount: 2,
      checkpoint: {
        pendingUrl: null,
        completedUrls: ["https://example.test/results/"],
        processedRows: 2,
      },
      rows: [person, horse],
    }),
  );
  expect(value.rows).toHaveLength(2);
  expect(value.rows[0]).toMatchObject({
    sourceHorseId: null,
    finishPosition: null,
    finishPositionText: "",
  });
});

it("decodes a not-yet-completed archive", () => {
  expect(
    decodeHistoryArchive(
      JSON.stringify({
        planDigest: "digest",
        publishedCount: null,
        checkpoint: {
          pendingUrl: "https://example.test/results/",
          completedUrls: [],
          processedRows: 0,
        },
        rows: [],
      }),
    ),
  ).toMatchObject({ publishedCount: null, rows: [] });
});

it.each([
  null,
  {},
  { planDigest: "digest", publishedCount: -1 },
  { planDigest: "digest", publishedCount: 0, checkpoint: null },
])("rejects malformed archive %j", (value) => {
  expect(() => decodeHistoryArchive(JSON.stringify(value))).toThrow(
    "History archive structure is invalid.",
  );
});

it.each([
  null,
  {},
  { ...person, finishPosition: "1" },
  { ...person, personKind: "unknown" },
  { ...person, sourcePersonId: null },
  { ...person, going: 1 },
  { ...person, distanceMetres: "1800" },
  { ...horse, venue: null },
  { ...horse, raceDaySequence: 0.5 },
  { ...horse, distanceMetres: null },
  { ...horse, sourceJockeyId: 1 },
])("rejects invalid source row shape %j", (value) => {
  expect(isHistorySourceRow(value)).toBe(false);
});

it.each(["jockey", "trainer"])("recognizes supported person kind %s", (personKind) => {
  expect(
    isHistorySourceRow({
      ...person,
      personKind,
      sourceHorseId: "horse1",
      finishPosition: 2,
      distanceMetres: 1800,
    }),
  ).toBe(true);
});
