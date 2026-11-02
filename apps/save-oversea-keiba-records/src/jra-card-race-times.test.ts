// This file runs with Bun and Vitest. The profile, card markup and database pools are synthetic mocks.
import { beforeEach, expect, it, vi } from "vitest";
import {
  matchJraCardPastRuns,
  parseJraCardPastRuns,
  runJraCardRaceTimes,
  type JraCardRaceTimeProfile,
} from "./jra-card-race-times";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  query: vi.fn(),
  end: vi.fn(),
  applyRaceTimeRows: vi.fn(),
  readFile: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({ readFile: ports.readFile }));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));
vi.mock("./race-time-store", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./race-time-store")>()),
  applyRaceTimeRows: ports.applyRaceTimeRows,
}));

const PROFILE: JraCardRaceTimeProfile = {
  runnerStart: "<row>",
  pastRunStart: "<run>",
  horseNumberPattern: "^\\s*(\\d+)",
  datePattern: "(\\d{4})/(\\d{1,2})/(\\d{1,2})",
  finishPattern: "pos (\\S+)",
  timePattern: "time ((?:\\d+:)?\\d{2}\\.\\d)",
  lastThreeFurlongsPattern: "last (\\d{2}\\.\\d)",
};
const CARD: string = [
  "<head>ignored</head>",
  "<row><b>1</b> name<run>2026/9/12 pos 2 time 2:32.7</run><run>2026/7/14 pos 1 time 3:02.8 last 35.1</run><run>no date</run>",
  "<row>x<run>2026/1/1 pos 1 time 2:00.0</run>",
  "<row>2<run>2026/6/1 pos 取消 time -</run>",
].join("");
const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "u",
  POSTGRES_PASSWORD: "p",
  POSTGRES_DB: "d",
  OVERSEA_JRA_CARD_RACE_TIME_PROFILE_PATH: "/private/profile.json",
};

beforeEach(() => {
  vi.resetAllMocks();
  ports.createTargetPool.mockReturnValue({ pool: { query: ports.query, end: ports.end } });
  ports.end.mockResolvedValue(undefined);
  ports.applyRaceTimeRows.mockResolvedValue({ fills: [{}, {}], conflicts: [], unmatched: 0 });
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/profile.json" ? JSON.stringify(PROFILE) : CARD,
  );
});

it("parseJraCardPastRuns reads every dated past run of each numbered runner", () => {
  expect(parseJraCardPastRuns(CARD, PROFILE)).toStrictEqual([
    {
      horseNumber: "01",
      raceDate: "2026-09-12",
      finishPosition: 2,
      raceTimeText: "2:32.7",
      lastThreeFurlongsSeconds: null,
    },
    {
      horseNumber: "01",
      raceDate: "2026-07-14",
      finishPosition: 1,
      raceTimeText: "3:02.8",
      lastThreeFurlongsSeconds: "35.1",
    },
    {
      horseNumber: "02",
      raceDate: "2026-06-01",
      finishPosition: null,
      raceTimeText: null,
      lastThreeFurlongsSeconds: null,
    },
  ]);
});

it("matchJraCardPastRuns keeps a winner measured, a beaten runner estimated and skips unmatched runs", () => {
  expect(
    matchJraCardPastRuns(
      parseJraCardPastRuns(CARD, PROFILE),
      new Map([
        ["01", "h1"],
        ["02", "h2"],
      ]),
      [
        { sourceHorseId: "h1", sourceRaceId: "r1", raceDate: "2026-09-12" },
        { sourceHorseId: "h1", sourceRaceId: "r2", raceDate: "2026-07-14" },
        { sourceHorseId: "h2", sourceRaceId: "r3", raceDate: "2026-06-01" },
        { sourceHorseId: "h3", sourceRaceId: "r4", raceDate: "2026-09-12" },
      ],
    ),
  ).toStrictEqual([
    {
      sourceHorseId: "h1",
      sourceRaceId: "r1",
      raceTimeText: "2:32.7",
      raceTimeSeconds: "152.70",
      lastThreeFurlongsSeconds: null,
      raceTimeSource: "jra",
      raceTimeKind: "estimated",
    },
    {
      sourceHorseId: "h1",
      sourceRaceId: "r2",
      raceTimeText: "3:02.8",
      raceTimeSeconds: "182.80",
      lastThreeFurlongsSeconds: "35.1",
      raceTimeSource: "jra",
      raceTimeKind: "measured",
    },
  ]);
});

it("matchJraCardPastRuns refuses a date that matches two stored races", () => {
  expect(
    matchJraCardPastRuns(
      [
        {
          horseNumber: "01",
          raceDate: "2026-09-12",
          finishPosition: 1,
          raceTimeText: "2:32.7",
          lastThreeFurlongsSeconds: null,
        },
      ],
      new Map([["01", "h1"]]),
      [
        { sourceHorseId: "h1", sourceRaceId: "r1", raceDate: "2026-09-12" },
        { sourceHorseId: "h1", sourceRaceId: "r2", raceDate: "2026-09-12" },
      ],
    ),
  ).toStrictEqual([]);
});

it("runJraCardRaceTimes maps runners through local source ids and applies to the target", async () => {
  ports.query
    .mockResolvedValueOnce({ rows: [{ umaban: "01", source_horse_id: "h1" }] })
    .mockResolvedValueOnce({
      rows: [
        { source_horse_id: "h1", source_race_id: "r1", race_date: "2026-09-12" },
        { source_horse_id: "h1", source_race_id: "r2", race_date: "2026-07-14" },
      ],
    });
  const report = await runJraCardRaceTimes({
    argv: [
      "jra-card-race-times-apply",
      "/card.html",
      "2026",
      "1103",
      "B6",
      "07",
      "production",
      "--confirm-write",
    ],
    env: ENV,
  });
  expect(report).toStrictEqual({
    status: "complete",
    operation: "applied",
    target: "production",
    runners: 2,
    pastRuns: 3,
    matched: 2,
    unmatched: 1,
    filled: 2,
    conflicts: [],
  });
  expect(ports.createTargetPool.mock.calls.map((call) => call[1])).toStrictEqual([
    "local",
    "production",
  ]);
  expect(ports.query.mock.calls[0]?.[1]).toStrictEqual(["2026", "1103", "B6", "07"]);
  expect(ports.query.mock.calls[1]?.[1]).toStrictEqual([["h1"]]);
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].apply).toBe(true);
  expect(ports.end).toHaveBeenCalledTimes(2);
});

it("runJraCardRaceTimes plans against the local target without writing", async () => {
  ports.query.mockResolvedValue({ rows: [] });
  const report = await runJraCardRaceTimes({
    argv: ["jra-card-race-times-plan", "/card.html", "2026", "1103", "B6", "07", "local"],
    env: ENV,
  });
  expect(report.operation).toBe("planned");
  expect(ports.createTargetPool.mock.calls.map((call) => call[1])).toStrictEqual([
    "local",
    "local",
  ]);
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].apply).toBe(false);
});

it("runJraCardRaceTimes rejects bad arguments, a missing profile and an incomplete profile", async () => {
  await expect(
    runJraCardRaceTimes({
      argv: ["jra-card-race-times-apply", "/card.html", "2026", "1103", "B6", "07", "local"],
      env: ENV,
    }),
  ).rejects.toThrow("Use jra-card-race-times-plan");
  await expect(
    runJraCardRaceTimes({
      argv: ["jra-card-race-times-plan", "/card.html", "2026", "1103", "B6", "07", "local"],
      env: { ...ENV, OVERSEA_JRA_CARD_RACE_TIME_PROFILE_PATH: "" },
    }),
  ).rejects.toThrow("OVERSEA_JRA_CARD_RACE_TIME_PROFILE_PATH is required.");
  ports.readFile.mockResolvedValue(JSON.stringify({ runnerStart: "<row>" }));
  await expect(
    runJraCardRaceTimes({
      argv: ["jra-card-race-times-plan", "/card.html", "2026", "1103", "B6", "07", "local"],
      env: ENV,
    }),
  ).rejects.toThrow("The JRA card race time profile is incomplete.");
});
