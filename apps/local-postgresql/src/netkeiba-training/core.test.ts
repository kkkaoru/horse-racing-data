import { expect, it, vi } from "vitest";
import {
  buildCandidateQuery,
  buildRaceUpsertSql,
  groupCandidateRaces,
  joinWorkoutsToRunners,
  parseCandidateOutput,
  parseWorkoutResponse,
  runNetkeibaTrainingImport,
  runTrainingScrape,
  type CandidateRace,
  type NetkeibaWorkout,
} from "./core";

const workout: NetkeibaWorkout = {
  commentText: "Good condition",
  course: "CW",
  courseDirection: "right",
  evaluationGrade: "A",
  evaluationText: "Strong",
  horseName: "Horse One",
  horseNumber: "1",
  lapTime10f: null,
  lapTime1f: "119",
  lapTime2f: "121",
  lapTime3f: "123",
  lapTime4f: "125",
  lapTime5f: "127",
  lapTime6f: null,
  lapTime7f: null,
  lapTime8f: null,
  lapTime9f: null,
  riderName: "Rider",
  timeGokei10f: null,
  timeGokei2f: "240",
  timeGokei3f: "363",
  timeGokei4f: "488",
  timeGokei5f: "615",
  timeGokei6f: null,
  timeGokei7f: null,
  timeGokei8f: null,
  timeGokei9f: null,
  tracenKubun: "1",
  trainingDate: "20260821",
  trainingTime: "0615",
  trainingType: "woodchip",
  workoutIndex: 1,
};

const race: CandidateRace = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0822",
  keibajoCode: "05",
  raceBango: "11",
  raceDate: "20260822",
  runners: [
    {
      bamei: "Horse One",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0822",
      keibajoCode: "05",
      kettoTorokuBango: "2023100001",
      raceBango: "11",
      raceDate: "20260822",
      sourceRaceId: "202605040811",
      umaban: "01",
    },
  ],
  sourceRaceId: "202605040811",
};

it("builds a two-day JRA candidate query guarded by both official workout tables", () => {
  const sql = buildCandidateQuery({ base: "20260822", next: "20260823" });
  expect(sql).toContain("in ('20260822', '20260823')");
  expect(sql).toContain("from jvd_hc hc");
  expect(sql).toContain("from jvd_wc wc");
  expect(sql).toContain("interval '14 days'");
  expect(sql).toContain("hc.chokyo_nengappi");
  expect(sql).toContain("wc.chokyo_nengappi");
  expect(sql).toContain("lpad(ra.kaisai_kai, 2, '0')");
});

it("parses candidate JSON lines and groups runners by source race", () => {
  const rows = parseCandidateOutput(
    '{"raceDate":"20260822","sourceRaceId":"202605040811","kaisaiNen":"2026","kaisaiTsukihi":"0822","keibajoCode":"05","raceBango":"11","umaban":"01","kettoTorokuBango":"2023100001","bamei":"Horse One"}\n' +
      '{"raceDate":"20260822","sourceRaceId":"202605040811","kaisaiNen":"2026","kaisaiTsukihi":"0822","keibajoCode":"05","raceBango":"11","umaban":"02","kettoTorokuBango":"2023100002","bamei":"Horse Two"}\n\n',
  );
  expect(groupCandidateRaces(rows)).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0822",
      keibajoCode: "05",
      raceBango: "11",
      raceDate: "20260822",
      runners: rows,
      sourceRaceId: "202605040811",
    },
  ]);
});

it("rejects malformed candidate rows", () => {
  expect(() => parseCandidateOutput("null\n")).toThrow("Invalid local candidate row");
  expect(() => parseCandidateOutput('{"raceDate":1}\n')).toThrow(
    "Invalid netkeiba workout field: bamei",
  );
});

it("validates the complete internal workout response contract", () => {
  expect(parseWorkoutResponse({ workouts: [workout] })).toStrictEqual([workout]);
  expect(() => parseWorkoutResponse(null)).toThrow("Invalid netkeiba training response");
  expect(() => parseWorkoutResponse({ workouts: {} })).toThrow(
    "Invalid netkeiba training response",
  );
  expect(() => parseWorkoutResponse({ workouts: [null] })).toThrow("Invalid netkeiba workout row");
  expect(() => parseWorkoutResponse({ workouts: [{ ...workout, horseNumber: 1 }] })).toThrow(
    "Invalid netkeiba workout field: horseNumber",
  );
  expect(() => parseWorkoutResponse({ workouts: [{ ...workout, course: 1 }] })).toThrow(
    "Invalid netkeiba workout field: course",
  );
  expect(() => parseWorkoutResponse({ workouts: [{ ...workout, workoutIndex: 0 }] })).toThrow(
    "Invalid netkeiba workout field: workoutIndex",
  );
});

it("joins horse numbers to local ketto identities and ignores non-target horses", () => {
  const rows = joinWorkoutsToRunners(
    race,
    [workout, { ...workout, horseNumber: "9" }],
    new Date("2026-08-22T01:00:00.000Z"),
  );
  expect(rows).toHaveLength(1);
  expect(rows[0]?.kettoTorokuBango).toBe("2023100001");
  expect(rows[0]?.umaban).toBe("01");
  expect(rows[0]?.dataSakuseiNengappi).toBe("20260822");
  expect(rows[0]?.workoutKey).toMatch(/^[0-9a-f]{64}$/u);
  expect(rows[0]?.sourceUrl).toBe(
    "https://race.netkeiba.com/race/oikiri.html?race_id=202605040811",
  );
  expect(
    joinWorkoutsToRunners(
      race,
      [{ ...workout, horseName: null }],
      new Date("2026-08-22T01:00:00.000Z"),
    )[0]?.bamei,
  ).toBe("Horse One");
});

it("builds one transactional conflict-safe race upsert without deletes", () => {
  const rows = joinWorkoutsToRunners(race, [workout], new Date("2026-08-22T01:00:00.000Z"));
  const sql = buildRaceUpsertSql([{ ...rows[0]!, commentText: "Trainer's note" }]);
  expect(sql).toMatch(/^begin;/u);
  expect(sql).toContain("insert into netkeiba_training_workouts");
  expect(sql).toContain("on conflict (kaisai_nen, kaisai_tsukihi");
  expect(sql).toContain("Trainer''s note");
  expect(sql).toContain("updated_at = now()");
  expect(sql).toMatch(/commit;\s*$/u);
  expect(sql).not.toMatch(/\bdelete\b/iu);
  expect(() => buildRaceUpsertSql([])).toThrow("Refusing to upsert an empty netkeiba workout set");
});

it("scrapes and upserts races sequentially", async () => {
  const upsertRace = vi.fn<(rows: ReturnType<typeof joinWorkoutsToRunners>) => Promise<void>>();
  upsertRace.mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();
  const count = await runTrainingScrape({
    fetchWorkouts: vi.fn().mockResolvedValue([workout]),
    loadCandidates: vi.fn().mockResolvedValue([race]),
    log,
    now: () => new Date("2026-08-22T01:00:00.000Z"),
    upsertRace,
  });
  expect(count).toBe(1);
  expect(upsertRace).toHaveBeenCalledOnce();
  expect(log.mock.calls).toStrictEqual([["Stored 1 netkeiba workouts for race 202605040811."]]);
});

it("does nothing when official workouts cover all runners", async () => {
  const fetchWorkouts = vi.fn();
  const upsertRace = vi.fn();
  const log = vi.fn<(message: string) => void>();
  expect(
    await runTrainingScrape({
      fetchWorkouts,
      loadCandidates: vi.fn().mockResolvedValue([]),
      log,
      now: () => new Date("2026-08-22T01:00:00.000Z"),
      upsertRace,
    }),
  ).toBe(0);
  expect(fetchWorkouts).not.toHaveBeenCalled();
  expect(upsertRace).not.toHaveBeenCalled();
  expect(log.mock.calls).toStrictEqual([
    ["No JRA runners without official 14-day workouts were found."],
  ]);
});

it("safely skips an unpublished race with an empty workout set", async () => {
  const upsertRace = vi.fn();
  const log = vi.fn<(message: string) => void>();
  expect(
    await runTrainingScrape({
      fetchWorkouts: vi.fn().mockResolvedValue([]),
      loadCandidates: vi.fn().mockResolvedValue([race]),
      log,
      now: () => new Date("2026-08-22T01:00:00.000Z"),
      upsertRace,
    }),
  ).toBe(0);
  expect(upsertRace).not.toHaveBeenCalled();
  expect(log.mock.calls).toStrictEqual([
    ["Skipped race 202605040811: netkeiba returned no workouts."],
  ]);
});

it("continues to the next race after an unpublished race returns no workouts", async () => {
  const unpublishedRace: CandidateRace = {
    ...race,
    kaisaiTsukihi: "0823",
    raceBango: "11",
    raceDate: "20260823",
    runners: [
      {
        ...race.runners[0]!,
        kaisaiTsukihi: "0823",
        raceDate: "20260823",
        sourceRaceId: "202601020211",
      },
    ],
    sourceRaceId: "202601020211",
  };
  const fetchWorkouts = vi
    .fn<(candidate: CandidateRace) => Promise<NetkeibaWorkout[]>>()
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([workout]);
  const upsertRace = vi
    .fn<(rows: ReturnType<typeof joinWorkoutsToRunners>) => Promise<void>>()
    .mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();

  expect(
    await runTrainingScrape({
      fetchWorkouts,
      loadCandidates: vi.fn().mockResolvedValue([unpublishedRace, race]),
      log,
      now: () => new Date("2026-08-22T01:00:00.000Z"),
      upsertRace,
    }),
  ).toBe(1);
  expect(fetchWorkouts.mock.calls).toStrictEqual([[unpublishedRace], [race]]);
  expect(upsertRace).toHaveBeenCalledOnce();
  expect(upsertRace.mock.calls[0]?.[0][0]?.sourceRaceId).toBe("202605040811");
  expect(log.mock.calls).toStrictEqual([
    ["Skipped race 202601020211: netkeiba returned no workouts."],
    ["Stored 1 netkeiba workouts for race 202605040811."],
  ]);
});

it("safely skips unmatched candidate horses and continues with the next race", async () => {
  const secondRace: CandidateRace = {
    ...race,
    raceBango: "12",
    runners: [{ ...race.runners[0]!, raceBango: "12", sourceRaceId: "202605040812" }],
    sourceRaceId: "202605040812",
  };
  const fetchWorkouts = vi
    .fn<(candidate: CandidateRace) => Promise<NetkeibaWorkout[]>>()
    .mockResolvedValueOnce([{ ...workout, horseNumber: "9" }])
    .mockResolvedValueOnce([workout]);
  const upsertRace = vi
    .fn<(rows: ReturnType<typeof joinWorkoutsToRunners>) => Promise<void>>()
    .mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();

  expect(
    await runTrainingScrape({
      fetchWorkouts,
      loadCandidates: vi.fn().mockResolvedValue([race, secondRace]),
      log,
      now: () => new Date("2026-08-22T01:00:00.000Z"),
      upsertRace,
    }),
  ).toBe(1);
  expect(fetchWorkouts.mock.calls).toStrictEqual([[race], [secondRace]]);
  expect(upsertRace).toHaveBeenCalledOnce();
  expect(upsertRace.mock.calls[0]?.[0][0]?.sourceRaceId).toBe("202605040812");
  expect(log.mock.calls).toStrictEqual([
    [
      "Skipped race 202605040811: netkeiba workouts did not match runners missing official workouts.",
    ],
    ["Stored 1 netkeiba workouts for race 202605040812."],
  ]);
});

it("applies the migration, discovers candidates, fetches authenticated workouts, and upserts", async () => {
  const executeSql = vi
    .fn<(sql: string) => Promise<string>>()
    .mockResolvedValueOnce("")
    .mockResolvedValueOnce(
      '{"raceDate":"20260822","sourceRaceId":"202605040811","kaisaiNen":"2026","kaisaiTsukihi":"0822","keibajoCode":"05","raceBango":"11","umaban":"01","kettoTorokuBango":"2023100001","bamei":"Horse One"}\n',
    )
    .mockResolvedValueOnce("");
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
    new Response(JSON.stringify({ workouts: [workout] }), {
      headers: { "content-type": "application/json" },
      status: 200,
    }),
  );
  expect(
    await runNetkeibaTrainingImport({
      apiBaseUrl: "https://sync.example/",
      dates: { base: "20260822", next: "20260823" },
      executeSql,
      fetcher,
      log: vi.fn(),
      migrationSql: "create table if not exists netkeiba_training_workouts ();",
      now: () => new Date("2026-08-22T01:00:00.000Z"),
      token: "secret",
    }),
  ).toBe(1);
  expect(executeSql.mock.calls[0]).toStrictEqual([
    "create table if not exists netkeiba_training_workouts ();",
  ]);
  expect(executeSql.mock.calls[1]?.[0]).toContain("from jvd_se se");
  expect(executeSql.mock.calls[2]?.[0]).toContain("begin;");
  expect(fetcher.mock.calls).toStrictEqual([
    [
      "https://sync.example/api/internal/netkeiba-training-workouts",
      {
        body: '{"raceDate":"20260822","sourceRaceId":"202605040811"}',
        headers: { authorization: "Bearer secret", "content-type": "application/json" },
        method: "POST",
      },
    ],
  ]);
});

it("fails closed before local mutation on missing auth and remote HTTP errors", async () => {
  const executeWithoutAuth = vi.fn<(sql: string) => Promise<string>>();
  await expect(
    runNetkeibaTrainingImport({
      apiBaseUrl: "https://sync.example",
      dates: { base: "20260822", next: "20260823" },
      executeSql: executeWithoutAuth,
      fetcher: vi.fn(),
      log: vi.fn(),
      migrationSql: "migration",
      now: () => new Date("2026-08-22T01:00:00.000Z"),
      token: "",
    }),
  ).rejects.toThrow("REALTIME_ADMIN_TOKEN must not be empty");
  expect(executeWithoutAuth).not.toHaveBeenCalled();

  const executeSql = vi
    .fn<(sql: string) => Promise<string>>()
    .mockResolvedValueOnce("")
    .mockResolvedValueOnce(
      '{"raceDate":"20260822","sourceRaceId":"202605040811","kaisaiNen":"2026","kaisaiTsukihi":"0822","keibajoCode":"05","raceBango":"11","umaban":"01","kettoTorokuBango":"2023100001","bamei":"Horse One"}\n',
    );
  await expect(
    runNetkeibaTrainingImport({
      apiBaseUrl: "https://sync.example",
      dates: { base: "20260822", next: "20260823" },
      executeSql,
      fetcher: vi.fn().mockResolvedValue(new Response("upstream failed", { status: 502 })),
      log: vi.fn(),
      migrationSql: "migration",
      now: () => new Date("2026-08-22T01:00:00.000Z"),
      token: "secret",
    }),
  ).rejects.toThrow(
    "Netkeiba training API failed for race 202605040811 with HTTP 502: upstream failed",
  );
  expect(executeSql).toHaveBeenCalledTimes(2);
});
