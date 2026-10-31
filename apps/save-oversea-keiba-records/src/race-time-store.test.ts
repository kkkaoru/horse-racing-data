// This file runs with Bun and Vitest. Database pools and the filesystem are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import {
  planRaceTimeStore,
  raceTimeRow,
  raceTimeSeconds,
  runRaceTimeStore,
  type RaceTimeRow,
} from "./race-time-store";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  poolQuery: vi.fn(),
  clientQuery: vi.fn(),
  release: vi.fn(),
  end: vi.fn(),
  readFile: vi.fn(),
  readdir: vi.fn(),
}));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));
vi.mock("node:fs/promises", () => ({ readFile: ports.readFile, readdir: ports.readdir }));

const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "user",
  POSTGRES_PASSWORD: "secret",
  POSTGRES_DB: "db",
  OVERSEA_RACE_TIME_PROFILE_PATH: "/private/race-time.json",
};
const PLAN = JSON.stringify({
  profile: {
    markup: {
      tableMarker: 'class="results"',
      racePathPrefix: "/race/",
      horseFields: { raceName: 0 },
    },
  },
});
const PAGE = `<table class="results">
<tr><td><a href="/race/r1/">Race 1</a></td><td>1:48.03</td><td>35.1</td></tr>
<tr><td><a href="/race/r2/">Race 2</a></td><td></td><td></td></tr>
</table>`;

beforeEach(() => {
  vi.resetAllMocks();
  ports.createTargetPool.mockImplementation((_env: unknown, target: string) => ({
    targetFingerprint: target,
    pool: {
      query: ports.poolQuery,
      connect: async () => ({ query: ports.clientQuery, release: ports.release }),
      end: ports.end,
    },
  }));
  ports.end.mockResolvedValue(undefined);
  ports.readdir.mockImplementation(async (path: string) =>
    path.endsWith("pages") ? ["a.raw", "a.raw.meta.json"] : ["horse-h1", "jockey-j1"],
  );
  ports.readFile.mockImplementation(async (path: string) => {
    if (path === "/private/race-time.json") return '{"time":1,"lastThreeFurlongs":2}';
    if (path.endsWith("plan.json")) return PLAN;
    return PAGE;
  });
});

it("raceTimeSeconds converts minute and second-only times", () => {
  expect(raceTimeSeconds("1:48.03")).toBe("108.03");
  expect(raceTimeSeconds("58.1")).toBe("58.10");
  expect(raceTimeSeconds("")).toBeNull();
  expect(raceTimeSeconds("計不")).toBeNull();
});

it("raceTimeRow keeps only well-formed values", () => {
  expect(
    raceTimeRow("h1", { sourceRaceId: "r1", raceTimeText: "x", lastThreeFurlongsText: "35" }),
  ).toStrictEqual({
    sourceHorseId: "h1",
    sourceRaceId: "r1",
    raceTimeText: null,
    raceTimeSeconds: null,
    lastThreeFurlongsSeconds: null,
    raceTimeSource: "netkeiba",
    raceTimeKind: "measured",
  });
});

it("planRaceTimeStore fills empty columns, reports differing ones and counts unmatched races", () => {
  const parsed: RaceTimeRow[] = [
    {
      sourceHorseId: "h1",
      sourceRaceId: "r1",
      raceTimeText: "1:48.03",
      raceTimeSeconds: "108.03",
      lastThreeFurlongsSeconds: "35.1",
      raceTimeSource: "netkeiba",
      raceTimeKind: "measured",
    },
    {
      sourceHorseId: "h1",
      sourceRaceId: "r2",
      raceTimeText: "1:50.00",
      raceTimeSeconds: "110.00",
      lastThreeFurlongsSeconds: "36.0",
      raceTimeSource: "netkeiba",
      raceTimeKind: "measured",
    },
    {
      sourceHorseId: "h1",
      sourceRaceId: "r9",
      raceTimeText: null,
      raceTimeSeconds: null,
      lastThreeFurlongsSeconds: null,
      raceTimeSource: "netkeiba",
      raceTimeKind: "measured",
    },
  ];
  const plan = planRaceTimeStore(parsed, [
    { sourceHorseId: "h1", sourceRaceId: "r1", raceTimeText: null, lastThreeFurlongsSeconds: null },
    {
      sourceHorseId: "h1",
      sourceRaceId: "r2",
      raceTimeText: "1:49.00",
      lastThreeFurlongsSeconds: "36.0",
    },
  ]);
  expect(plan.fills.map((row) => row.sourceRaceId)).toStrictEqual(["r1"]);
  expect(plan.conflicts).toStrictEqual(["h1:r2:race_time_text"]);
  expect(plan.unmatched).toBe(1);
});

it("planRaceTimeStore reports a differing closing time", () => {
  const plan = planRaceTimeStore(
    [
      {
        sourceHorseId: "h1",
        sourceRaceId: "r1",
        raceTimeText: null,
        raceTimeSeconds: null,
        lastThreeFurlongsSeconds: "35.1",
        raceTimeSource: "netkeiba",
        raceTimeKind: "measured",
      },
    ],
    [
      {
        sourceHorseId: "h1",
        sourceRaceId: "r1",
        raceTimeText: null,
        lastThreeFurlongsSeconds: "35.5",
      },
    ],
  );
  expect(plan.conflicts).toStrictEqual(["h1:r1:last_three_furlongs_seconds"]);
  expect(plan.fills).toStrictEqual([]);
});

it("runRaceTimeStore plans against stored rows without writing", async () => {
  ports.poolQuery.mockResolvedValue({
    rows: [
      {
        source_horse_id: "h1",
        source_race_id: "r1",
        race_time_text: null,
        last_three_furlongs_seconds: null,
      },
    ],
  });
  const report = await runRaceTimeStore({ argv: ["race-times-plan", "/h", "local"], env: ENV });
  expect(report).toStrictEqual({
    status: "complete",
    operation: "planned",
    target: "local",
    archivedHorses: 1,
    parsedRaces: 2,
    withTime: 1,
    withLastThreeFurlongs: 1,
    filled: 1,
    unmatched: 1,
    conflicts: [],
  });
  expect(ports.clientQuery).not.toHaveBeenCalled();
});

it("runRaceTimeStore applies fills in one locked transaction with a readback", async () => {
  const stored = [
    {
      source_horse_id: "h1",
      source_race_id: "r1",
      race_time_text: null,
      last_three_furlongs_seconds: null,
    },
  ];
  const filled = [
    {
      source_horse_id: "h1",
      source_race_id: "r1",
      race_time_text: "1:48.03",
      last_three_furlongs_seconds: "35.1",
    },
  ];
  ports.clientQuery.mockImplementation(async (sql: string) => {
    if (sql.startsWith("SELECT")) {
      const calls = ports.clientQuery.mock.calls.filter(([text]) =>
        String(text).startsWith("SELECT"),
      );
      return { rows: calls.length === 1 ? stored : filled };
    }
    return { rows: [] };
  });
  const report = await runRaceTimeStore({
    argv: ["race-times-apply", "/h", "production", "--confirm-write"],
    env: ENV,
  });
  expect(report.filled).toBe(1);
  expect(ports.clientQuery.mock.calls.map(([sql]) => String(sql).split(/\s/u)[0])).toStrictEqual([
    "BEGIN",
    "LOCK",
    "SELECT",
    "UPDATE",
    "SELECT",
    "COMMIT",
  ]);
  expect(ports.clientQuery.mock.calls[3]?.[1]).toStrictEqual([
    "netkeiba",
    "h1",
    "r1",
    "1:48.03",
    "108.03",
    "35.1",
    "netkeiba",
    "measured",
  ]);
  expect(ports.release).toHaveBeenCalledTimes(1);
});

it("runRaceTimeStore rolls back when the readback still misses a fill", async () => {
  ports.clientQuery.mockImplementation(async (sql: string) =>
    sql.startsWith("SELECT")
      ? {
          rows: [
            {
              source_horse_id: "h1",
              source_race_id: "r1",
              race_time_text: null,
              last_three_furlongs_seconds: null,
            },
          ],
        }
      : { rows: [] },
  );
  await expect(
    runRaceTimeStore({ argv: ["race-times-apply", "/h", "local", "--confirm-write"], env: ENV }),
  ).rejects.toThrow("Race time readback is missing filled values.");
  expect(ports.clientQuery).toHaveBeenLastCalledWith("ROLLBACK");
});

it("runRaceTimeStore skips unreadable pages and needs no database rows when nothing parsed", async () => {
  ports.readFile.mockImplementation(async (path: string) => {
    if (path === "/private/race-time.json") return '{"time":1,"lastThreeFurlongs":2}';
    if (path.endsWith("plan.json")) return PLAN;
    return "<html>no table</html>";
  });
  const report = await runRaceTimeStore({
    argv: ["race-times-apply", "/h", "local", "--confirm-write"],
    env: ENV,
  });
  expect(report.parsedRaces).toBe(0);
  expect(ports.poolQuery).not.toHaveBeenCalled();
});

it("runRaceTimeStore rejects bad arguments, a missing profile and a plan without markup", async () => {
  await expect(
    runRaceTimeStore({ argv: ["race-times-apply", "/h", "local"], env: ENV }),
  ).rejects.toThrow("Use race-times-plan");
  await expect(
    runRaceTimeStore({
      argv: ["race-times-plan", "/h", "local"],
      env: { ...ENV, OVERSEA_RACE_TIME_PROFILE_PATH: "" },
    }),
  ).rejects.toThrow("OVERSEA_RACE_TIME_PROFILE_PATH is required.");
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/race-time.json" ? '{"time":1}' : PLAN,
  );
  await expect(
    runRaceTimeStore({ argv: ["race-times-plan", "/h", "local"], env: ENV }),
  ).rejects.toThrow("The race time profile needs time and lastThreeFurlongs cell indexes.");
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/race-time.json" ? '{"time":1,"lastThreeFurlongs":2}' : "{}",
  );
  await expect(
    runRaceTimeStore({ argv: ["race-times-plan", "/h", "local"], env: ENV }),
  ).rejects.toThrow("History plan of horse-h1 has no markup profile.");
});
