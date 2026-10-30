// This test runs with Bun and Vitest. The profile and markup are synthetic.
import { expect, it } from "vitest";
import {
  parseSecondaryRaceResult,
  type SecondaryRaceResultProfile,
} from "./secondary-race-result-parser";

const PROFILE: SecondaryRaceResultProfile = {
  tableMarker: 'data-demo="result"',
  horsePathPrefix: "/animal/",
  jockeyPathPrefix: "/rider/",
  trainerPathPrefix: "/coach/",
  datePattern: "Date (\\d{4})/(\\d{1,2})/(\\d{1,2})",
  coursePattern: "Course (芝|ダ)(左|右|直) (\\d+)m",
  trainerPrefixes: ["[X]"],
  fields: {
    finishPosition: 0,
    gate: 1,
    horseNumber: 2,
    horseName: 3,
    sexAge: 4,
    weightCarried: 5,
    jockey: 6,
    time: 7,
    winOdds: 8,
    popularity: 9,
    trainer: 10,
  },
};
const WINNER: string =
  '<tr><td>1</td><td>3</td><td>5</td><td><a href="/animal/h1/">Alpha</a></td><td>セ5</td><td>57</td><td><a href="/rider/r1/">ライダ</a></td><td>2:00.19</td><td>3.4</td><td>1</td><td><a href="/coach/c1/">[X] コーチ</a></td></tr>';
const SCRATCHED: string =
  "<tr><td>取</td><td></td><td>7</td><td>Beta</td><td>牝4</td><td></td><td>ライダ２</td><td></td><td></td><td></td><td>コーチ２</td></tr>";
const page = (rows: string, header: string = "Date 2025/11/1 Course ダ左 2000m"): string =>
  `<html>${header}<table data-demo="result"><tr><th>head</th></tr>${rows}</table></html>`;

it("parses the local date, course and every listed runner", () => {
  expect(parseSecondaryRaceResult(page(`${WINNER}${SCRATCHED}`), PROFILE)).toStrictEqual({
    date: "2025-11-01",
    surface: "ダート",
    direction: "左",
    distanceMetres: 2000,
    runners: [
      {
        finishPosition: 1,
        finishPositionText: "1",
        gate: 3,
        horseNumber: 5,
        horseName: "Alpha",
        sourceHorseId: "h1",
        sex: "せん",
        age: 5,
        weightCarriedKg: 57,
        jockeyName: "ライダ",
        sourceJockeyId: "r1",
        timeText: "2:00.19",
        winOdds: 3.4,
        popularity: 1,
        trainerName: "コーチ",
        sourceTrainerId: "c1",
      },
      {
        finishPosition: null,
        finishPositionText: "取",
        gate: 0,
        horseNumber: 7,
        horseName: "Beta",
        sourceHorseId: null,
        sex: "牝",
        age: 4,
        weightCarriedKg: 0,
        jockeyName: "ライダ２",
        sourceJockeyId: null,
        timeText: "",
        winOdds: null,
        popularity: null,
        trainerName: "コーチ２",
        sourceTrainerId: null,
      },
    ],
  });
});

it("keeps turf and straight labels that need no translation", () => {
  const result = parseSecondaryRaceResult(page(WINNER, "Date 2024/1/2 Course 芝直 1000m"), PROFILE);
  expect([result.date, result.surface, result.direction]).toStrictEqual([
    "2024-01-02",
    "芝",
    "直線",
  ]);
});

it("keeps a malformed published age unknown instead of guessing", () => {
  expect(
    parseSecondaryRaceResult(page(WINNER.replace("<td>セ5</td>", "<td>牝145</td>")), PROFILE)
      .runners[0]?.age,
  ).toBe(0);
});

it("rejects a page without a verifiable header", () => {
  expect(() => parseSecondaryRaceResult(page(WINNER, "Course 芝左 1600m"), PROFILE)).toThrow(
    "Race result header could not be verified.",
  );
});

it("rejects a page without the result table", () => {
  expect(() =>
    parseSecondaryRaceResult("<html>Date 2025/11/1 Course ダ左 2000m</html>", PROFILE),
  ).toThrow("Race result table marker was not found.");
});

it("rejects an unclosed result table", () => {
  expect(() =>
    parseSecondaryRaceResult(
      `<html>Date 2025/11/1 Course ダ左 2000m<table data-demo="result">${WINNER}</html>`,
      PROFILE,
    ),
  ).toThrow("Race result table is not closed.");
});

it("rejects a table without runner rows", () => {
  expect(() => parseSecondaryRaceResult(page(""), PROFILE)).toThrow(
    "Race result table has no runners.",
  );
});

it("rejects a row without a horse number", () => {
  expect(() =>
    parseSecondaryRaceResult(page(WINNER.replace("<td>5</td>", "<td></td>")), PROFILE),
  ).toThrow("Race result row has invalid horse number.");
});

it("rejects a row without sex and age", () => {
  expect(() =>
    parseSecondaryRaceResult(page(WINNER.replace("<td>セ5</td>", "<td>?</td>")), PROFILE),
  ).toThrow("Race result row has invalid sex and age.");
});

it("rejects a row missing a configured cell", () => {
  expect(() =>
    parseSecondaryRaceResult(page("<tr><td>1</td><td>3</td><td>5</td></tr>"), PROFILE),
  ).toThrow("Race result row is missing cell 3.");
});
