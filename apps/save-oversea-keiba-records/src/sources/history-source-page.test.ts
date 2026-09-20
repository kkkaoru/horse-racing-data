// This file runs with Bun.
import { expect, it } from "vitest";
import { parseHistorySourcePage, type HistorySourcePageProfile } from "./history-source-page";

const profile: HistorySourcePageProfile = {
  markup: {
    tableMarker: '<table data-example="results">',
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
  populationPattern: "Published \\(([0-9,]+)\\)",
  nextLabels: ["Next"],
  emptyMarker: "No published results",
};
const table: string =
  '<table data-example="results"><tr><td>2026/09/01</td><td>Venue</td><td>1</td><td><a href="/event/race1">Race</a></td><td>2</td><td>Dirt1800</td><td>Good</td><td><a href="/animal/horse1">Horse</a><a href="/rider/rider1">Rider</a></td></tr></table>';

it("parses person rows and deduplicates identical top/bottom next links", () => {
  const result = parseHistorySourcePage({
    html: `Published (2) ${table}<a href="?page=2&amp;sort=date">Next</a><a href="?page=2&amp;sort=date"><span>Next</span></a><a href="?page=0">Previous</a></html>`,
    url: "https://example.test/results/",
    kind: "owner",
    sourceId: "owner1",
    profile,
  });
  expect(result.publishedCount).toBe(2);
  expect(result.rows).toHaveLength(1);
  expect(result.rows[0]).toMatchObject({
    personKind: "owner",
    sourcePersonId: "owner1",
    sourceHorseId: "horse1",
    sourceRaceId: "race1",
  });
  expect(result.nextUrl).toBe("https://example.test/results/?page=2&sort=date");
});

it("parses horse rows without assuming absence of a next link proves population completion", () => {
  const result = parseHistorySourcePage({
    html: `Published (2) ${table}</html>`,
    url: "https://example.test/results/",
    kind: "horse",
    sourceId: "horse1",
    profile,
  });
  expect(result.rows[0]).toMatchObject({
    sourceHorseId: "horse1",
    sourceJockeyId: "rider1",
    distanceMetres: 1800,
  });
  expect(result.nextUrl).toBeNull();
  expect(result.publishedCount).toBe(2);
});

it("accepts zero published rows only with the explicit empty-page marker", () => {
  expect(
    parseHistorySourcePage({
      html: "Published (0) No published results</html>",
      url: "https://example.test/results/",
      kind: "trainer",
      sourceId: "trainer1",
      profile,
    }),
  ).toStrictEqual({ rows: [], nextUrl: null, publishedCount: 0 });
});

it("does not treat an AJAX shell or login page as zero results", () => {
  expect(() =>
    parseHistorySourcePage({
      html: "Published (0) Sign in</html>",
      url: "https://example.test/results/",
      kind: "trainer",
      sourceId: "trainer1",
      profile,
    }),
  ).toThrow("Secondary result table marker was not found.");
});

it("requires a complete document", () => {
  expect(() =>
    parseHistorySourcePage({
      html: "Published (1)",
      url: "https://example.test/results/",
      kind: "horse",
      sourceId: "horse1",
      profile,
    }),
  ).toThrow("History source document is incomplete.");
});

it.each(["Missing population</html>", "Published (999999999999999999999)</html>"])(
  "rejects an unverified population %s",
  (html) => {
    expect(() =>
      parseHistorySourcePage({
        html,
        url: "https://example.test/results/",
        kind: "owner",
        sourceId: "owner1",
        profile,
      }),
    ).toThrow("Published history population could not be verified.");
  },
);

it("rejects competing next-page links", () => {
  expect(() =>
    parseHistorySourcePage({
      html: `Published (2) ${table}<a href="?page=2">Next</a><a href="?page=3">Next</a></html>`,
      url: "https://example.test/results/",
      kind: "owner",
      sourceId: "owner1",
      profile,
    }),
  ).toThrow("Published history next-page links disagree.");
});

it("does not accept a zero-population page with a next page", () => {
  expect(() =>
    parseHistorySourcePage({
      html: 'Published (0) No published results<a href="?page=2">Next</a></html>',
      url: "https://example.test/results/",
      kind: "owner",
      sourceId: "owner1",
      profile,
    }),
  ).toThrow("Empty published history unexpectedly has pagination.");
});

it("rejects empty tables with a positive published population", () => {
  expect(() =>
    parseHistorySourcePage({
      html: 'Published (1)<table data-example="results"></table></html>',
      url: "https://example.test/results/",
      kind: "owner",
      sourceId: "owner1",
      profile,
    }),
  ).toThrow("Parsed history rows disagree with the published population.");
});

it("rejects rows contradicting a zero population, including disabled empty markers", () => {
  expect(() =>
    parseHistorySourcePage({
      html: `Published (0) ${table}</html>`,
      url: "https://example.test/results/",
      kind: "owner",
      sourceId: "owner1",
      profile: { ...profile, emptyMarker: "" },
    }),
  ).toThrow("Parsed history rows disagree with the published population.");
});

it("accepts comma-separated published counts without rounding", () => {
  expect(
    parseHistorySourcePage({
      html: `Published (1,234) ${table}</html>`,
      url: "https://example.test/results/",
      kind: "owner",
      sourceId: "owner1",
      profile,
    }).publishedCount,
  ).toBe(1234);
});
