// This file runs with Bun and Vitest. The filesystem, proxy and database writer are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import {
  candidateRaceLinks,
  marginLengths,
  parseRacingPostRace,
  raceDistanceMetres,
  matchRacingPostTime,
  normaliseHorseName,
  overseasDates,
  parseRacingPostDay,
  runRacingPostTimes,
  winnerTimeSeconds,
  type MissingTimeRow,
  type RacingPostRace,
} from "./racing-post-times";

const ports = vi.hoisted(() => ({
  access: vi.fn(),
  mkdir: vi.fn(),
  readFile: vi.fn(),
  readdir: vi.fn(),
  writeFile: vi.fn(),
  applyRaceTimeRows: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({
  access: ports.access,
  mkdir: ports.mkdir,
  readFile: ports.readFile,
  readdir: ports.readdir,
  writeFile: ports.writeFile,
}));
vi.mock("./race-time-store", () => ({ applyRaceTimeRows: ports.applyRaceTimeRows }));

const PROFILE = {
  dayUrlTemplate: "https://results.test/day/{DATE}",
  dataScriptId: "data",
  racesPath: ["props", "groups"],
  raceUrlOrigin: "https://results.test",
  raceResultPath: ["props", "race"],
};
const ENV = { OVERSEA_RACING_POST_PROFILE_PATH: "/private/rp.json" };
const RACE: RacingPostRace = {
  winnerTime: "1m 47.91s",
  runners: [
    { position: "1st", horseName: "Sovereignty", distanceToWinner: null },
    { position: "2nd", horseName: "Magnitude (USA)", distanceToWinner: "4¾" },
    { position: "3rd", horseName: "Odd Margin", distanceToWinner: "a distance" },
  ],
};
const DAY_HTML = `<html><script id="data" type="application/json">${JSON.stringify({
  props: { groups: [{ races: [RACE, { title: "no runners" }] }, { label: "no races" }] },
})}</script></html>`;
const ROW: MissingTimeRow = {
  source_horse_id: "h1",
  source_race_id: "r1",
  d: "2026-08-08",
  venue: "サラトガ",
  finish_position: 2,
  distance_metres: 1800,
};
const JRA_ROW: MissingTimeRow = { ...ROW, d: "2020-08-23", venue: "2小倉4" };
const UNPLACED_ROW: MissingTimeRow = { ...ROW, d: "2026-07-01", finish_position: null };

beforeEach(() => {
  vi.resetAllMocks();
  ports.mkdir.mockResolvedValue(undefined);
  ports.writeFile.mockResolvedValue(undefined);
  ports.applyRaceTimeRows.mockResolvedValue({ fills: [{}], conflicts: [], unmatched: 0 });
});

it("normaliseHorseName drops country suffixes, accents and punctuation", () => {
  expect(normaliseHorseName("Magnitude (USA)")).toBe("magnitude");
  expect(normaliseHorseName("Rosé d'Or")).toBe("rosedor");
});

it("winnerTimeSeconds reads minute and second-only times", () => {
  expect(winnerTimeSeconds("1m 47.91s")).toBe(107.91);
  expect(winnerTimeSeconds("58.12s")).toBe(58.12);
  expect(winnerTimeSeconds(null)).toBeNull();
  expect(winnerTimeSeconds("n/a")).toBeNull();
});

it("marginLengths reads lengths, fractions and short margins", () => {
  expect(marginLengths("4¾")).toBe(4.75);
  expect(marginLengths("1¼l")).toBe(1.25);
  expect(marginLengths("½")).toBe(0.5);
  expect(marginLengths("nk")).toBe(0.3);
  expect(marginLengths("dht")).toBe(0);
  expect(marginLengths("")).toBeNull();
  expect(marginLengths(null)).toBeNull();
  expect(marginLengths("dist")).toBeNull();
});

it("parseRacingPostDay returns every race with runners", () => {
  expect(parseRacingPostDay(DAY_HTML, PROFILE)).toStrictEqual([RACE]);
  expect(() => parseRacingPostDay("<html></html>", PROFILE)).toThrow(
    "Results day page has no data script.",
  );
  expect(() => parseRacingPostDay('<script id="data">{"props":{}}</script>', PROFILE)).toThrow(
    "Results day data has no race groups.",
  );
});

it("matchRacingPostTime stores the winner time as measured and a placed time as estimated", () => {
  expect(matchRacingPostTime(ROW, "Sovereignty", [RACE])).toStrictEqual({
    sourceHorseId: "h1",
    sourceRaceId: "r1",
    raceTimeText: "1:47.91",
    raceTimeSeconds: "107.91",
    lastThreeFurlongsSeconds: null,
    raceTimeSource: "racingpost",
    raceTimeKind: "measured",
  });
  expect(matchRacingPostTime(ROW, "Magnitude", [RACE])).toStrictEqual({
    sourceHorseId: "h1",
    sourceRaceId: "r1",
    raceTimeText: "1:48.86",
    raceTimeSeconds: "108.86",
    lastThreeFurlongsSeconds: null,
    raceTimeSource: "racingpost",
    raceTimeKind: "estimated",
  });
});

it("matchRacingPostTime leaves unknown runners, margins and times unset", () => {
  expect(matchRacingPostTime(ROW, "Not Here", [RACE])).toBeNull();
  expect(matchRacingPostTime(ROW, "Odd Margin", [RACE])).toBeNull();
  expect(matchRacingPostTime(ROW, "Sovereignty", [{ ...RACE, winnerTime: null }])).toBeNull();
  expect(
    matchRacingPostTime(ROW, "Sprinter", [
      {
        winnerTime: "58.12s",
        runners: [{ position: "1st", horseName: "Sprinter", distanceToWinner: null }],
      },
    ])?.raceTimeText,
  ).toBe("58.12");
});

it("overseasDates keeps finished overseas runs only", () => {
  expect(overseasDates([ROW, ROW, JRA_ROW, UNPLACED_ROW])).toStrictEqual(["2026-08-08"]);
});

it("fetch mode requests uncached days serially through the proxy", async () => {
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/rp.json"
      ? JSON.stringify(PROFILE)
      : JSON.stringify([ROW, { ...ROW, d: "2026-08-09" }, { ...ROW, d: "2026-08-10" }]),
  );
  ports.access.mockImplementation(async (path: string) => {
    if (!path.endsWith("2026-08-08.html")) throw new Error("ENOENT");
  });
  const fetchImpl = vi.fn(
    async (_url: string, _init: RequestInit) => new Response(DAY_HTML, { status: 200 }),
  );
  const sleep = vi.fn(async () => undefined);
  const report = await runRacingPostTimes({
    argv: ["racing-post-times-fetch", "/m.json", "/cache"],
    env: ENV,
    fetchImpl,
    sleep,
  });
  expect(report.status).toBe("complete");
  expect(report.fetched).toBe(2);
  expect(fetchImpl.mock.calls.map(([url]) => String(url))).toStrictEqual([
    "https://reverse-proxy.api.kkk4oru.com/playwright?url=https%3A%2F%2Fresults.test%2Fday%2F2026-08-09",
    "https://reverse-proxy.api.kkk4oru.com/playwright?url=https%3A%2F%2Fresults.test%2Fday%2F2026-08-10",
  ]);
  expect(sleep).toHaveBeenCalledTimes(1);
  expect(sleep).toHaveBeenCalledWith(6000);
});

it("fetch mode stops at a challenge page and keeps it aside", async () => {
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/rp.json" ? JSON.stringify(PROFILE) : JSON.stringify([ROW]),
  );
  ports.access.mockRejectedValue(new Error("ENOENT"));
  const report = await runRacingPostTimes({
    argv: ["racing-post-times-fetch", "/m.json", "/cache"],
    env: ENV,
    fetchImpl: vi.fn(
      async (_url: string, _init: RequestInit) =>
        new Response("<iframe>challenge</iframe>", { status: 200 }),
    ),
    sleep: vi.fn(async () => undefined),
  });
  expect(report.status).toBe("blocked");
  expect(report.fetched).toBe(0);
  expect(ports.writeFile).toHaveBeenCalledWith(
    "/cache/2026-08-08.rejected.html",
    "<iframe>challenge</iframe>",
    { mode: 0o600 },
  );
});

it("apply mode matches cached days to archived English names and writes fill-only", async () => {
  ports.readFile.mockImplementation(async (path: string) => {
    if (path === "/private/rp.json") return JSON.stringify(PROFILE);
    if (path === "/m.json")
      return JSON.stringify([
        { ...ROW, source_horse_id: "h1" },
        { ...ROW, source_horse_id: "h2" },
        { ...ROW, source_horse_id: "h3", d: "2026-08-09" },
        { ...ROW, source_horse_id: "h4" },
      ]);
    if (path.endsWith("2026-08-08.html")) return DAY_HTML;
    if (path.includes("horse-h1")) return "<title>Sovereignty | data</title>";
    if (path.includes("horse-h2")) return "<title>Magnitude (マグニチュード) | data</title>";
    return "<title>名前なし | data</title>";
  });
  ports.readdir.mockImplementation(async (path: string) => {
    if (path === "/history")
      return ["horse-h1", "horse-h2-v2", "horse-h3", "horse-h4", "jockey-j1"];
    return path.includes("horse-h3") ? [] : ["b.html", "a.html", "a.raw"];
  });
  ports.access.mockImplementation(async (path: string) => {
    if (!path.endsWith("2026-08-08.html")) throw new Error("ENOENT");
  });
  const report = await runRacingPostTimes({
    argv: [
      "racing-post-times-apply",
      "/m.json",
      "/cache",
      "/history",
      "production",
      "--confirm-write",
    ],
    env: ENV,
  });
  expect(report).toStrictEqual({
    status: "complete",
    operation: "applied",
    days: 2,
    fetched: 0,
    candidates: 4,
    measured: 1,
    estimated: 1,
    unmatched: 2,
    filled: 1,
    conflicts: [],
  });
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].target).toBe("production");
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].apply).toBe(true);
});

it("plan mode targets local without writing", async () => {
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/rp.json" ? JSON.stringify(PROFILE) : "[]",
  );
  ports.readdir.mockResolvedValue([]);
  const report = await runRacingPostTimes({
    argv: ["racing-post-times-plan", "/m.json", "/cache", "/history", "local"],
    env: ENV,
  });
  expect(report.operation).toBe("planned");
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].apply).toBe(false);
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].target).toBe("local");
});

it("rejects bad arguments and an incomplete or missing profile", async () => {
  await expect(
    runRacingPostTimes({
      argv: ["racing-post-times-apply", "/m.json", "/cache", "/h", "local"],
      env: ENV,
    }),
  ).rejects.toThrow("Use racing-post-times-fetch");
  await expect(
    runRacingPostTimes({ argv: ["racing-post-times-fetch", "/m.json", "/cache"], env: {} }),
  ).rejects.toThrow("OVERSEA_RACING_POST_PROFILE_PATH is required.");
  ports.readFile.mockResolvedValue("{}");
  await expect(
    runRacingPostTimes({ argv: ["racing-post-times-fetch", "/m.json", "/cache"], env: ENV }),
  ).rejects.toThrow("The results profile is incomplete.");
});

const DAY_RACE: RacingPostRace = {
  winnerTime: "1m 47.91s",
  runners: [{ position: "1st", horseName: "Sovereignty", distanceToWinner: null }],
  courseKey: "saratoga",
  countryCode: "USA",
  distance: "1m1f",
  runnersCount: 7,
  fullResultLink: "/results/445/saratoga/2026-08-08/926195",
};
const RACE_HTML = `<script id="data">${JSON.stringify({
  props: {
    race: {
      details: { winningTime: "1m 47.91s" },
      analysis: {
        paragraphs: [[{ value: "Closing 3f sectional: 37.60 (102.4%); winner 37.00 (104.1%)." }]],
      },
      runners: [
        { outcomeCode: "1", horseName: "Sovereignty", beatenDistanceToWinner: "" },
        { outcomeCode: "7", horseName: "White Abarrio", beatenDistanceToWinner: "17¾" },
        { note: "not a runner" },
      ],
    },
  },
})}</script>`;

it("raceDistanceMetres reads miles, furlongs and yards", () => {
  expect(raceDistanceMetres("1m1f")).toBeCloseTo(1810.512);
  expect(raceDistanceMetres("7½f")).toBeCloseTo(1508.76);
  expect(raceDistanceMetres("1m110y")).toBeCloseTo(1709.928);
  expect(raceDistanceMetres("")).toBeNull();
  expect(raceDistanceMetres(undefined)).toBeNull();
  expect(raceDistanceMetres("about a mile")).toBeNull();
});

it("candidateRaceLinks lists same course and distance results for an unlisted runner", () => {
  expect(
    candidateRaceLinks({ ...ROW, finish_position: 7 }, "White Abarrio", [DAY_RACE]),
  ).toStrictEqual([
    { date: "2026-08-08", raceId: "926195", link: "/results/445/saratoga/2026-08-08/926195" },
  ]);
  expect(candidateRaceLinks(ROW, "Sovereignty", [DAY_RACE])).toStrictEqual([]);
  expect(
    candidateRaceLinks({ ...ROW, finish_position: 9 }, "White Abarrio", [DAY_RACE]),
  ).toStrictEqual([]);
  expect(
    candidateRaceLinks({ ...ROW, distance_metres: 1400 }, "White Abarrio", [DAY_RACE]),
  ).toStrictEqual([]);
  expect(
    candidateRaceLinks({ ...ROW, venue: "アメリカ" }, "White Abarrio", [
      DAY_RACE,
      { ...DAY_RACE, countryCode: "GB", fullResultLink: "/results/1/ascot/2026-08-08/1" },
      { ...DAY_RACE, fullResultLink: undefined },
    ]),
  ).toStrictEqual([
    { date: "2026-08-08", raceId: "926195", link: "/results/445/saratoga/2026-08-08/926195" },
  ]);
  expect(
    candidateRaceLinks({ ...ROW, venue: "ロンシャン" }, "White Abarrio", [DAY_RACE]),
  ).toStrictEqual([]);
});

it("parseRacingPostRace reads every finisher of a full result page", () => {
  expect(parseRacingPostRace(RACE_HTML, PROFILE)).toStrictEqual({
    winnerTime: "1m 47.91s",
    winnerClosingThree: "37.0",
    runners: [
      { position: "1st", horseName: "Sovereignty", distanceToWinner: "" },
      { position: "7", horseName: "White Abarrio", distanceToWinner: "17¾" },
    ],
  });
  expect(() => parseRacingPostRace("<html></html>", PROFILE)).toThrow(
    "Race result page has no data script.",
  );
  expect(() => parseRacingPostRace('<script id="data">{"props":{}}</script>', PROFILE)).toThrow(
    "Race result data has no runners.",
  );
  expect(
    parseRacingPostRace(
      '<script id="data">{"props":{"race":{"details":{},"runners":[]}}}</script>',
      PROFILE,
    ).winnerTime,
  ).toBeNull();
});

it("races-fetch mode requests the full results of unlisted runners and apply uses them", async () => {
  const dayHtml = `<script id="data">${JSON.stringify({ props: { groups: [{ races: [DAY_RACE] }] } })}</script>`;
  ports.readFile.mockImplementation(async (path: string) => {
    if (path === "/private/rp.json") return JSON.stringify(PROFILE);
    if (path === "/m.json") return JSON.stringify([{ ...ROW, finish_position: 7 }]);
    if (path === "/cache/2026-08-08.html") return dayHtml;
    if (path.endsWith("926195.html")) return RACE_HTML;
    return "<title>White Abarrio | data</title>";
  });
  ports.readdir.mockImplementation(async (path: string) => {
    if (path === "/history") return ["horse-h1"];
    if (path === "/cache/races/2026-08-08") return ["926195.html", "1.rejected.html"];
    return ["a.html"];
  });
  ports.access.mockImplementation(async (path: string) => {
    if (path !== "/cache/2026-08-08.html") throw new Error("ENOENT");
  });
  const fetchImpl = vi.fn(
    async (_url: string, _init: RequestInit) => new Response(RACE_HTML, { status: 200 }),
  );
  const fetched = await runRacingPostTimes({
    argv: ["racing-post-races-fetch", "/m.json", "/cache", "/history"],
    env: ENV,
    fetchImpl,
    sleep: vi.fn(async () => undefined),
  });
  expect(fetched.candidates).toBe(1);
  expect(fetchImpl.mock.calls.map(([url]) => String(url))).toStrictEqual([
    "https://reverse-proxy.api.kkk4oru.com/playwright?url=https%3A%2F%2Fresults.test%2Fresults%2F445%2Fsaratoga%2F2026-08-08%2F926195",
  ]);
  expect(ports.writeFile).toHaveBeenCalledWith("/cache/races/2026-08-08/926195.html", RACE_HTML, {
    flag: "wx",
    mode: 0o600,
  });
  ports.access.mockResolvedValue(undefined);
  const report = await runRacingPostTimes({
    argv: ["racing-post-times-plan", "/m.json", "/cache", "/history", "local"],
    env: ENV,
  });
  expect(report.estimated).toBe(1);
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].rows[0].raceTimeText).toBe("1:51.46");
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].rows[0].lastThreeFurlongsSeconds).toBeNull();
});

it("matchRacingPostTime keeps the winner's published closing 3f as measured", () => {
  expect(
    matchRacingPostTime(ROW, "Sovereignty", [{ ...RACE, winnerClosingThree: "37.0" }])
      ?.lastThreeFurlongsSeconds,
  ).toBe("37.0");
  expect(
    matchRacingPostTime(ROW, "Magnitude", [{ ...RACE, winnerClosingThree: "37.0" }])
      ?.lastThreeFurlongsSeconds,
  ).toBeNull();
});
