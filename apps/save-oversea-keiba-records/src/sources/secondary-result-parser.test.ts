import { expect, test } from "vitest";
import {
  parseSecondaryHorseResults,
  parseSecondaryPersonResults,
  type SecondaryResultMarkupProfile,
} from "./secondary-result-parser";

const profile: SecondaryResultMarkupProfile = {
  tableMarker: 'summary="result-marker"',
  racePathPrefix: "/event/",
  horsePathPrefix: "/runner/",
  jockeyPathPrefix: "/rider/",
  raceUrlTemplate: "https://example.test/event/{RACE_ID}/",
  horseFields: {
    date: 0,
    venue: 1,
    raceNumber: 2,
    raceName: 3,
    finishPosition: 4,
    relatedEntity: 5,
    distance: 6,
    going: 7,
  },
  personFields: {
    date: 0,
    venue: 1,
    raceNumber: 2,
    raceName: 3,
    finishPosition: 4,
    relatedEntity: 5,
    distance: 6,
    going: 7,
  },
};

const row = (finish = "1", date = "2026/08/01"): string => `<tr>
<td>${date}</td><td>Venue&nbsp;A</td><td>4</td>
<td><a href="/event/R100/">Race &amp; Cup</a></td><td>${finish}</td>
<td><a href="/runner/H100/">Horse A</a><a href="/rider/J100/">Rider A</a></td>
<td>Turf1600</td><td>Good</td></tr>`;
const html = (rows: string): string =>
  `<table summary="result-marker"><tbody>${rows}</tbody></table>`;

test("parses horse results and sequences same-day rows", () => {
  expect(parseSecondaryHorseResults(html(`${row()}${row("SCR")}`), "H100", profile)).toStrictEqual([
    {
      sourceHorseId: "H100",
      sourceRaceId: "R100",
      raceDate: "2026-08-01",
      venue: "Venue A",
      raceDaySequence: 1,
      raceName: "Race & Cup",
      sourceRaceUrl: "https://example.test/event/R100/",
      finishPosition: 1,
      finishPositionText: "1",
      jockeyName: "Horse A Rider A",
      sourceJockeyId: "J100",
      surface: "Turf",
      distanceMetres: 1600,
      going: "Good",
    },
    {
      sourceHorseId: "H100",
      sourceRaceId: "R100",
      raceDate: "2026-08-01",
      venue: "Venue A",
      raceDaySequence: 2,
      raceName: "Race & Cup",
      sourceRaceUrl: "https://example.test/event/R100/",
      finishPosition: null,
      finishPositionText: "SCR",
      jockeyName: "Horse A Rider A",
      sourceJockeyId: "J100",
      surface: "Turf",
      distanceMetres: 1600,
      going: "Good",
    },
  ]);
});

test("parses person result identities", () => {
  expect(parseSecondaryPersonResults(html(row()), "jockey", "J100", profile)).toStrictEqual([
    {
      personKind: "jockey",
      sourcePersonId: "J100",
      sourceRaceId: "R100",
      raceDate: "2026-08-01",
      venue: "Venue A",
      raceNumber: "4",
      raceName: "Race & Cup",
      sourceRaceUrl: "https://example.test/event/R100/",
      sourceHorseId: "H100",
      horseName: "Horse A Rider A",
      finishPosition: 1,
      finishPositionText: "1",
      surface: "Turf",
      distanceMetres: 1600,
      going: "Good",
    },
  ]);
});

test("allows a horse result without a rider identity", () => {
  const noRider = row().replace('<a href="/rider/J100/">Rider A</a>', "Rider A");
  expect(parseSecondaryHorseResults(html(noRider), "H100", profile)[0]?.sourceJockeyId).toBeNull();
});

test("rejects missing or malformed result structures", () => {
  expect(() => parseSecondaryHorseResults("<html></html>", "H100", profile)).toThrow(
    "table marker",
  );
  expect(() =>
    parseSecondaryHorseResults('<table summary="result-marker">', "H100", profile),
  ).toThrow("not closed");
  expect(() =>
    parseSecondaryHorseResults(html(row().replace("2026/08/01", "bad")), "H100", profile),
  ).toThrow("invalid date");
  expect(() =>
    parseSecondaryHorseResults(html(row().replace("Turf1600", "unknown")), "H100", profile),
  ).toThrow("invalid distance");
  expect(() =>
    parseSecondaryHorseResults(
      html(row().replace("/event/R100/", "/missing/R100/")),
      "H100",
      profile,
    ),
  ).toThrow("missing race ID");
  expect(
    parseSecondaryPersonResults(
      html(row().replace("/runner/H100/", "/missing/H100/")),
      "owner",
      "O100",
      profile,
    )[0]?.sourceHorseId,
  ).toBeNull();
  expect(() =>
    parseSecondaryHorseResults(html(row()), "H100", {
      ...profile,
      horseFields: { ...profile.horseFields, going: 99 },
    }),
  ).toThrow("missing cell 99");
});
