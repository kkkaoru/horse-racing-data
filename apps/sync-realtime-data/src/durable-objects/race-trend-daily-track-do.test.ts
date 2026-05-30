// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import type {
  RaceTrendDailyTrackRow,
  RaceTrendDailyTrackState,
} from "horse-racing-realtime/race-trend-daily-track-types";
import type { Env } from "../types";
import {
  RACE_TREND_DAILY_TRACK_PUSH_URL,
  RACE_TREND_DAILY_TRACK_RACES_URL,
  RACE_TREND_DAILY_TRACK_STORAGE_KEY,
  RaceTrendDailyTrackDO,
  __testables,
  buildRaceTrendDailyTrackDoIdName,
  computeNextAlarmDelayMs,
  fetchRaceTrendDailyTrackRacesFromStub,
  parseDoContextFromRaceKey,
  pushRaceTrendDailyTrackRowToStub,
} from "./race-trend-daily-track-do";

interface FakeStorage {
  get: (key: string) => Promise<RaceTrendDailyTrackState | undefined>;
  put: (key: string, value: RaceTrendDailyTrackState) => Promise<void>;
  setAlarm: (at: number) => Promise<void>;
}

interface FakeState {
  blockConcurrencyWhile: (callback: () => Promise<void>) => Promise<void>;
  storage: FakeStorage;
}

interface FakeStateHandle {
  blockConcurrencyWhile: ReturnType<typeof vi.fn>;
  state: FakeState;
  storage: {
    get: ReturnType<typeof vi.fn>;
    put: ReturnType<typeof vi.fn>;
    setAlarm: ReturnType<typeof vi.fn>;
  };
}

const buildFakeState = (initial: Map<string, RaceTrendDailyTrackState>): FakeStateHandle => {
  const get = vi.fn(
    async (key: string): Promise<RaceTrendDailyTrackState | undefined> => initial.get(key),
  );
  const put = vi.fn(async (key: string, value: RaceTrendDailyTrackState): Promise<void> => {
    initial.set(key, value);
  });
  const setAlarm = vi.fn(async (_at: number): Promise<void> => {});
  const blockConcurrencyWhile = vi.fn((callback: () => Promise<void>): Promise<void> => callback());
  const storage: FakeStorage = { get, put, setAlarm };
  return {
    blockConcurrencyWhile,
    state: { blockConcurrencyWhile, storage },
    storage: { get, put, setAlarm },
  };
};

interface FakeD1ResultRecord extends Record<string, unknown> {}

const buildD1All = (results: ReadonlyArray<FakeD1ResultRecord>): ReturnType<typeof vi.fn> =>
  vi.fn(async () => ({ results }));

interface BuildEnvParams {
  runningStyleResults?: ReadonlyArray<FakeD1ResultRecord>;
  snapshotResults?: ReadonlyArray<FakeD1ResultRecord>;
}

const buildEnv = (params: BuildEnvParams = {}): Env => {
  const allSnapshots = buildD1All(params.snapshotResults ?? []);
  const allRunningStyles = buildD1All(params.runningStyleResults ?? []);
  const prepare = vi.fn((sql: string) => ({
    bind: vi.fn(() => ({
      all: sql.includes("from race_running_styles") ? allRunningStyles : allSnapshots,
    })),
  }));
  return {
    REALTIME_DB: { prepare } as unknown as D1Database,
  } as unknown as Env;
};

const JRA_ROW: RaceTrendDailyTrackRow = {
  fetchedAt: "2026-05-31T11:00:00+09:00",
  finishedAt: "2026-05-31T11:00:00+09:00",
  isComplete: true,
  raceBango: "03",
  raceKey: "jra:2026:0531:06:03",
  runningStyles: [],
  starterRows: [
    {
      bamei: "TestHorse",
      bataiju: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      finishPosition: 1,
      hassoJikoku: "1100",
      jockeyName: "Jockey",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0531",
      keibajoCode: "06",
      raceBango: "03",
      raceName: "TestRace",
      runnerCount: null,
      sohaTime: "1:34.2",
      source: "jra",
      tanshoOdds: null,
      tanshoPopularity: null,
      umaban: "1",
      wakuban: null,
      zogenFugo: null,
      zogenSa: null,
    },
  ],
};

const NAR_ROW: RaceTrendDailyTrackRow = {
  fetchedAt: "2026-05-31T19:30:00+09:00",
  finishedAt: null,
  isComplete: false,
  raceBango: "05",
  raceKey: "nar:2026:0531:48:05",
  runningStyles: [],
  starterRows: [],
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("parseDoContextFromRaceKey returns null for malformed raceKey segments", () => {
  expect(parseDoContextFromRaceKey("only:three:parts")).toBeNull();
});

it("parseDoContextFromRaceKey returns null when source is not jra / nar", () => {
  expect(parseDoContextFromRaceKey("ban:2026:0531:83:03")).toBeNull();
});

it("parseDoContextFromRaceKey returns null when any later segment is empty", () => {
  expect(parseDoContextFromRaceKey("jra::0531:06:03")).toBeNull();
});

it("parseDoContextFromRaceKey returns parsed tuple for a well-formed raceKey", () => {
  expect(parseDoContextFromRaceKey("jra:2026:0531:06:03")).toStrictEqual({
    keibajoCode: "06",
    source: "jra",
    targetYmd: "20260531",
  });
});

it("buildRaceTrendDailyTrackDoIdName joins source / targetYmd / keibajoCode", () => {
  expect(
    buildRaceTrendDailyTrackDoIdName({ keibajoCode: "06", source: "jra", targetYmd: "20260531" }),
  ).toBe("jra:20260531:06");
});

it("computeNextAlarmDelayMs returns 60s inside the JST polling window", () => {
  expect(computeNextAlarmDelayMs(new Date("2026-05-31T05:00:00Z"))).toBe(60_000);
});

it("computeNextAlarmDelayMs returns 30min outside the JST polling window", () => {
  expect(computeNextAlarmDelayMs(new Date("2026-05-31T20:00:00Z"))).toBe(30 * 60_000);
});

it("POST /push stores a row, merges into state, and persists snapshot", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(JRA_ROW),
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
  expect(handle.storage.put).toHaveBeenCalledTimes(1);
  expect(handle.storage.put.mock.calls[0]![0]).toBe("snapshot");
  const stored = handle.storage.put.mock.calls[0]![1] as RaceTrendDailyTrackState;
  expect(stored.source).toBe("jra");
  expect(stored.targetYmd).toBe("20260531");
  expect(stored.keibajoCode).toBe("06");
  expect(stored.races["03"]).toStrictEqual(JRA_ROW);
});

it("POST /push rejects a non-object body with 400", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(null),
      method: "POST",
    }),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "invalid body" });
});

it("POST /push rejects when raceKey cannot be parsed into a context", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify({ ...JRA_ROW, raceKey: "broken" }),
      method: "POST",
    }),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "invalid raceKey" });
});

it("POST /push is monotonic: a newer fetchedAt overwrites the older row", async () => {
  const olderRow: RaceTrendDailyTrackRow = { ...JRA_ROW, fetchedAt: "2026-05-31T10:00:00+09:00" };
  const newerRow: RaceTrendDailyTrackRow = { ...JRA_ROW, fetchedAt: "2026-05-31T11:00:00+09:00" };
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(olderRow),
      method: "POST",
    }),
  );
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(newerRow),
      method: "POST",
    }),
  );
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/races?beforeRaceBango=99"),
  );
  const payload = (await response.json()) as { races: RaceTrendDailyTrackRow[] };
  expect(payload.races[0]!.fetchedAt).toBe("2026-05-31T11:00:00+09:00");
});

it("POST /push is monotonic: a stale fetchedAt does not overwrite the newer row", async () => {
  const olderRow: RaceTrendDailyTrackRow = { ...JRA_ROW, fetchedAt: "2026-05-31T10:00:00+09:00" };
  const newerRow: RaceTrendDailyTrackRow = { ...JRA_ROW, fetchedAt: "2026-05-31T11:00:00+09:00" };
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(newerRow),
      method: "POST",
    }),
  );
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(olderRow),
      method: "POST",
    }),
  );
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/races?beforeRaceBango=99"),
  );
  const payload = (await response.json()) as { races: RaceTrendDailyTrackRow[] };
  expect(payload.races[0]!.fetchedAt).toBe("2026-05-31T11:00:00+09:00");
});

it("POST /push accepts a NAR partial (isComplete=false) row and stores it", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(NAR_ROW),
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  const stored = handle.storage.put.mock.calls[0]![1] as RaceTrendDailyTrackState;
  expect(stored.source).toBe("nar");
  expect(stored.keibajoCode).toBe("48");
  expect(stored.races["05"]!.isComplete).toBe(false);
});

it("GET /races returns sorted rows strictly less than beforeRaceBango", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify({ ...JRA_ROW, raceBango: "05", raceKey: "jra:2026:0531:06:05" }),
      method: "POST",
    }),
  );
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify({ ...JRA_ROW, raceBango: "02", raceKey: "jra:2026:0531:06:02" }),
      method: "POST",
    }),
  );
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify({ ...JRA_ROW, raceBango: "04", raceKey: "jra:2026:0531:06:04" }),
      method: "POST",
    }),
  );
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/races?beforeRaceBango=05"),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Race-Trend-DO")).toBe("hit");
  const payload = (await response.json()) as { races: RaceTrendDailyTrackRow[] };
  expect(payload.races.map((row) => row.raceBango)).toStrictEqual(["02", "04"]);
});

it("GET /races returns an empty result with the miss header when state is empty", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/races?beforeRaceBango=05"),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Race-Trend-DO")).toBe("miss");
  expect(await response.json()).toStrictEqual({ races: [] });
});

it("GET /races without beforeRaceBango returns every row in raceBango order", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify({ ...JRA_ROW, raceBango: "12", raceKey: "jra:2026:0531:06:12" }),
      method: "POST",
    }),
  );
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify({ ...JRA_ROW, raceBango: "01", raceKey: "jra:2026:0531:06:01" }),
      method: "POST",
    }),
  );
  const response = await cache.fetch(new Request("https://race-trend-daily-track-do/races"));
  const payload = (await response.json()) as { races: RaceTrendDailyTrackRow[] };
  expect(payload.races.map((row) => row.raceBango)).toStrictEqual(["01", "12"]);
});

it("returns 404 for an unknown path", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(new Request("https://race-trend-daily-track-do/unknown"));
  expect(response.status).toBe(404);
});

it("returns 404 for an unsupported method on /push", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", { method: "DELETE" }),
  );
  expect(response.status).toBe(404);
});

it("POST /sync returns 400 when the DO has not yet learned its context", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/sync", { method: "POST" }),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "DO id not yet learned" });
});

it("POST /sync triggers a D1 refresh once the context has been learned via push", async () => {
  const snapshotRow: FakeD1ResultRecord = {
    changeAmount: null,
    changeSign: null,
    expectedHorseCount: 1,
    fetchedAt: "2026-05-31T11:05:00+09:00",
    finishPosition: "1",
    hassoJikoku: "2026-05-31T11:00:00+09:00",
    horseName: "TestHorse",
    jockeyName: "Jockey",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "06",
    raceBango: "03",
    raceKey: "jra:2026:0531:06:03",
    raceName: "TestRace",
    resultCompleteAt: "2026-05-31T11:05:00+09:00",
    savedHorseCount: 1,
    sohaTime: "1:34.2",
    source: "jra",
    umaban: "1",
    weight: null,
  };
  const handle = buildFakeState(new Map());
  const env = buildEnv({ snapshotResults: [snapshotRow] });
  const cache = await RaceTrendDailyTrackDO.createForTest({ env, state: handle.state });
  await cache.fetch(
    new Request("https://race-trend-daily-track-do/push", {
      body: JSON.stringify(JRA_ROW),
      method: "POST",
    }),
  );
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/sync", { method: "POST" }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
  const lastPut = handle.storage.put.mock.calls.at(-1)![1] as RaceTrendDailyTrackState;
  expect(lastPut.races["03"]!.isComplete).toBe(true);
  expect(lastPut.races["03"]!.starterRows[0]!.bamei).toBe("TestHorse");
});

it("createForTest hydrates state and parsed context from previously stored snapshot", async () => {
  const persisted: RaceTrendDailyTrackState = {
    keibajoCode: "06",
    races: { "03": JRA_ROW },
    source: "jra",
    targetYmd: "20260531",
    updatedAt: "2026-05-31T11:05:00.000Z",
  };
  const handle = buildFakeState(new Map([["snapshot", persisted]]));
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/races?beforeRaceBango=99"),
  );
  expect(handle.blockConcurrencyWhile).toHaveBeenCalledTimes(1);
  expect(handle.storage.get).toHaveBeenCalledWith("snapshot");
  const payload = (await response.json()) as { races: RaceTrendDailyTrackRow[] };
  expect(payload.races[0]!).toStrictEqual(JRA_ROW);
});

it("runAlarmTick schedules a 60s alarm inside the polling window", async () => {
  const persisted: RaceTrendDailyTrackState = {
    keibajoCode: "06",
    races: {},
    source: "jra",
    targetYmd: "20260531",
    updatedAt: "2026-05-31T05:00:00.000Z",
  };
  const handle = buildFakeState(new Map([["snapshot", persisted]]));
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const now = new Date("2026-05-31T05:00:00Z");
  await cache.runAlarmTick({ env: buildEnv(), now });
  expect(handle.storage.setAlarm).toHaveBeenCalledTimes(1);
  expect(handle.storage.setAlarm.mock.calls[0]![0]).toBe(now.getTime() + 60_000);
});

it("runAlarmTick schedules a 30min alarm outside the polling window", async () => {
  const persisted: RaceTrendDailyTrackState = {
    keibajoCode: "06",
    races: {},
    source: "jra",
    targetYmd: "20260531",
    updatedAt: "2026-05-31T20:00:00.000Z",
  };
  const handle = buildFakeState(new Map([["snapshot", persisted]]));
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const now = new Date("2026-05-31T20:00:00Z");
  await cache.runAlarmTick({ env: buildEnv(), now });
  expect(handle.storage.setAlarm.mock.calls[0]![0]).toBe(now.getTime() + 30 * 60_000);
});

it("runAlarmTick still schedules the next alarm when the DO has no parsed context", async () => {
  const handle = buildFakeState(new Map());
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  const now = new Date("2026-05-31T05:00:00Z");
  await cache.runAlarmTick({ env: buildEnv(), now });
  expect(handle.storage.setAlarm).toHaveBeenCalledTimes(1);
});

it("alarm() delegates to runAlarmTick with the current clock", async () => {
  const persisted: RaceTrendDailyTrackState = {
    keibajoCode: "06",
    races: {},
    source: "jra",
    targetYmd: "20260531",
    updatedAt: "2026-05-31T05:00:00.000Z",
  };
  const handle = buildFakeState(new Map([["snapshot", persisted]]));
  const cache = await RaceTrendDailyTrackDO.createForTest({ env: buildEnv(), state: handle.state });
  await cache.alarm();
  expect(handle.storage.setAlarm).toHaveBeenCalledTimes(1);
});

it("pushRaceTrendDailyTrackRowToStub posts the row to the DO push URL", async () => {
  const upstream = new Response(JSON.stringify({ ok: true }), { status: 200 });
  const stubFetch = vi.fn(async (_url: string, _init?: RequestInit): Promise<Response> => upstream);
  const response = await pushRaceTrendDailyTrackRowToStub({
    row: JRA_ROW,
    stub: { fetch: stubFetch },
  });
  expect(stubFetch).toHaveBeenCalledTimes(1);
  expect(stubFetch.mock.calls[0]![0]).toBe("https://race-trend-daily-track-do/push");
  expect(stubFetch.mock.calls[0]![1]!.method).toBe("POST");
  expect(stubFetch.mock.calls[0]![1]!.body).toBe(JSON.stringify(JRA_ROW));
  expect(response).toBe(upstream);
});

it("fetchRaceTrendDailyTrackRacesFromStub GETs /races with the encoded query", async () => {
  const upstream = new Response(JSON.stringify({ races: [] }), { status: 200 });
  const stubFetch = vi.fn(async (_url: string, _init?: RequestInit): Promise<Response> => upstream);
  const response = await fetchRaceTrendDailyTrackRacesFromStub({
    beforeRaceBango: "05",
    stub: { fetch: stubFetch },
  });
  expect(stubFetch.mock.calls[0]![0]).toBe(
    "https://race-trend-daily-track-do/races?beforeRaceBango=05",
  );
  expect(stubFetch.mock.calls[0]![1]!.method).toBe("GET");
  expect(response).toBe(upstream);
});

it("__testables.mergeIncomingRow updates existing state without dropping other races", () => {
  const existing: RaceTrendDailyTrackState = {
    keibajoCode: "06",
    races: { "01": { ...JRA_ROW, raceBango: "01", raceKey: "jra:2026:0531:06:01" } },
    source: "jra",
    targetYmd: "20260531",
    updatedAt: "2026-05-31T10:00:00.000Z",
  };
  const next = __testables.mergeIncomingRow({
    existing,
    incoming: { ...JRA_ROW, raceBango: "03", raceKey: "jra:2026:0531:06:03" },
    parsed: { keibajoCode: "06", source: "jra", targetYmd: "20260531" },
    updatedAt: "2026-05-31T11:00:00.000Z",
  });
  expect(Object.keys(next.races).toSorted()).toStrictEqual(["01", "03"]);
});

it("__testables.selectRaces returns [] when state is null", () => {
  expect(__testables.selectRaces({ beforeRaceBango: "05", state: null })).toStrictEqual([]);
});

it("__testables.buildRowsFromSnapshotResults groups rows by raceBango and counts ranked horses", () => {
  const snapshotRows: ReadonlyArray<Record<string, unknown>> = [
    {
      changeAmount: null,
      changeSign: null,
      expectedHorseCount: 2,
      fetchedAt: "2026-05-31T11:05:00+09:00",
      finishPosition: "1",
      hassoJikoku: "2026-05-31T11:00:00+09:00",
      horseName: "TestHorseA",
      jockeyName: "JockeyA",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0531",
      keibajoCode: "06",
      raceBango: "03",
      raceKey: "jra:2026:0531:06:03",
      raceName: "TestRace",
      resultCompleteAt: "2026-05-31T11:05:00+09:00",
      savedHorseCount: 2,
      sohaTime: "1:34.2",
      source: "jra",
      umaban: "1",
      weight: null,
    },
    {
      changeAmount: null,
      changeSign: null,
      expectedHorseCount: 2,
      fetchedAt: "2026-05-31T11:05:00+09:00",
      finishPosition: "2",
      hassoJikoku: "2026-05-31T11:00:00+09:00",
      horseName: "TestHorseB",
      jockeyName: "JockeyB",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0531",
      keibajoCode: "06",
      raceBango: "03",
      raceKey: "jra:2026:0531:06:03",
      raceName: "TestRace",
      resultCompleteAt: "2026-05-31T11:05:00+09:00",
      savedHorseCount: 2,
      sohaTime: "1:35.0",
      source: "jra",
      umaban: "2",
      weight: null,
    },
  ];
  const runningStyles: ReadonlyArray<Record<string, unknown>> = [
    { horseNumber: 1, predictedLabel: "nige", raceKey: "jra:2026:0531:06:03" },
    { horseNumber: 2, predictedLabel: "unknown", raceKey: "jra:2026:0531:06:03" },
  ];
  const filteredSnapshots = snapshotRows.filter(__testables.isRawSnapshotRow);
  const filteredStyles = runningStyles.filter(
    (row): row is { horseNumber: number; predictedLabel: string; raceKey: string } =>
      typeof row.raceKey === "string" &&
      typeof row.horseNumber === "number" &&
      typeof row.predictedLabel === "string",
  );
  const rows = __testables.buildRowsFromSnapshotResults(filteredSnapshots, filteredStyles);
  expect(rows).toHaveLength(1);
  expect(rows[0]!.isComplete).toBe(true);
  expect(rows[0]!.starterRows.map((row) => row.umaban)).toStrictEqual(["1", "2"]);
  expect(rows[0]!.runningStyles).toStrictEqual([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0531:06:03" },
  ]);
});

it("exposes the snapshot key, push URL, and races URL constants", () => {
  expect(RACE_TREND_DAILY_TRACK_STORAGE_KEY).toBe("snapshot");
  expect(RACE_TREND_DAILY_TRACK_PUSH_URL).toBe("https://race-trend-daily-track-do/push");
  expect(RACE_TREND_DAILY_TRACK_RACES_URL).toBe("https://race-trend-daily-track-do/races");
});

it("__testables.isRawSnapshotRow returns false for non-object values", () => {
  expect(__testables.isRawSnapshotRow(null)).toBe(false);
  expect(__testables.isRawSnapshotRow("string")).toBe(false);
});

it("__testables.buildRowsFromSnapshotResults flags a partial row as not complete when fewer horses are ranked", () => {
  const snapshotRows: ReadonlyArray<Record<string, unknown>> = [
    {
      changeAmount: null,
      changeSign: null,
      expectedHorseCount: 2,
      fetchedAt: "2026-05-31T11:05:00+09:00",
      finishPosition: "1",
      hassoJikoku: null,
      horseName: null,
      jockeyName: null,
      kaisaiNen: "2026",
      kaisaiTsukihi: "0531",
      keibajoCode: "06",
      raceBango: "03",
      raceKey: "jra:2026:0531:06:03",
      raceName: null,
      resultCompleteAt: null,
      savedHorseCount: 1,
      sohaTime: null,
      source: "jra",
      umaban: "1",
      weight: null,
    },
  ];
  const filtered = snapshotRows.filter(__testables.isRawSnapshotRow);
  const rows = __testables.buildRowsFromSnapshotResults(filtered, []);
  expect(rows[0]!.isComplete).toBe(false);
});

it("constructor hydrates state via blockConcurrencyWhile when storage has a persisted snapshot", async () => {
  const persisted: RaceTrendDailyTrackState = {
    keibajoCode: "06",
    races: { "03": JRA_ROW },
    source: "jra",
    targetYmd: "20260531",
    updatedAt: "2026-05-31T11:05:00.000Z",
  };
  const initial = new Map<string, RaceTrendDailyTrackState>([["snapshot", persisted]]);
  const get = vi.fn(
    async (key: string): Promise<RaceTrendDailyTrackState | undefined> => initial.get(key),
  );
  const put = vi.fn(async (_key: string, _value: RaceTrendDailyTrackState): Promise<void> => {});
  const setAlarm = vi.fn(async (_at: number): Promise<void> => {});
  const blockConcurrencyWhile = vi.fn((callback: () => Promise<void>): Promise<void> => callback());
  const doState = {
    blockConcurrencyWhile,
    storage: { delete: vi.fn(), get, list: vi.fn(), put, setAlarm },
  } as unknown as DurableObjectState;
  const cache = new RaceTrendDailyTrackDO(doState, buildEnv());
  await blockConcurrencyWhile.mock.results[0]!.value;
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/races?beforeRaceBango=99"),
  );
  expect(response.headers.get("X-Race-Trend-DO")).toBe("hit");
});

it("constructor leaves state null when storage has nothing persisted", async () => {
  const get = vi.fn(
    async (_key: string): Promise<RaceTrendDailyTrackState | undefined> => undefined,
  );
  const put = vi.fn(async (_key: string, _value: RaceTrendDailyTrackState): Promise<void> => {});
  const setAlarm = vi.fn(async (_at: number): Promise<void> => {});
  const blockConcurrencyWhile = vi.fn((callback: () => Promise<void>): Promise<void> => callback());
  const doState = {
    blockConcurrencyWhile,
    storage: { delete: vi.fn(), get, list: vi.fn(), put, setAlarm },
  } as unknown as DurableObjectState;
  const cache = new RaceTrendDailyTrackDO(doState, buildEnv());
  await blockConcurrencyWhile.mock.results[0]!.value;
  const response = await cache.fetch(
    new Request("https://race-trend-daily-track-do/races?beforeRaceBango=99"),
  );
  expect(response.headers.get("X-Race-Trend-DO")).toBe("miss");
});
