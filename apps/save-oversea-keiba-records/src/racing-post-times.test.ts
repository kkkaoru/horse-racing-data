// This file runs with Bun and Vitest. The filesystem, proxy and database writer are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import {
  marginLengths,
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
    if (path.includes("horse-h1")) return "<title>ソヴリンティ (Sovereignty) | data</title>";
    if (path.includes("horse-h2")) return "<title>マグニチュード (Magnitude) | data</title>";
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
