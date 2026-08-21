// Run via Bun from scripts/scrape-netkeiba-training.ts.
import { createHash } from "node:crypto";
import type { RealtimeDiscoveryDates } from "../replica-push/realtime-discovery";

export interface CandidateRunnerRow {
  bamei: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  kettoTorokuBango: string;
  raceBango: string;
  raceDate: string;
  sourceRaceId: string;
  umaban: string;
}

export interface CandidateRace {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceDate: string;
  runners: CandidateRunnerRow[];
  sourceRaceId: string;
}

export interface NetkeibaWorkout {
  commentText: string | null;
  course: string | null;
  courseDirection: string | null;
  evaluationGrade: string | null;
  evaluationText: string | null;
  horseName: string | null;
  horseNumber: string;
  lapTime10f: string | null;
  lapTime1f: string | null;
  lapTime2f: string | null;
  lapTime3f: string | null;
  lapTime4f: string | null;
  lapTime5f: string | null;
  lapTime6f: string | null;
  lapTime7f: string | null;
  lapTime8f: string | null;
  lapTime9f: string | null;
  riderName: string | null;
  timeGokei10f: string | null;
  timeGokei2f: string | null;
  timeGokei3f: string | null;
  timeGokei4f: string | null;
  timeGokei5f: string | null;
  timeGokei6f: string | null;
  timeGokei7f: string | null;
  timeGokei8f: string | null;
  timeGokei9f: string | null;
  tracenKubun: string | null;
  trainingDate: string;
  trainingTime: string;
  trainingType: string;
  workoutIndex: number;
}

export interface StoredWorkout {
  bamei: string | null;
  babamawari: string | null;
  chokyoJikoku: string;
  chokyoNengappi: string;
  commentText: string | null;
  course: string | null;
  dataSakuseiNengappi: string;
  evaluationGrade: string | null;
  evaluationText: string | null;
  fetchedAt: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  kettoTorokuBango: string;
  lapTime10f: string | null;
  lapTime1f: string | null;
  lapTime2f: string | null;
  lapTime3f: string | null;
  lapTime4f: string | null;
  lapTime5f: string | null;
  lapTime6f: string | null;
  lapTime7f: string | null;
  lapTime8f: string | null;
  lapTime9f: string | null;
  raceBango: string;
  riderName: string | null;
  sourceRaceId: string;
  sourceUrl: string;
  timeGokei10f: string | null;
  timeGokei2f: string | null;
  timeGokei3f: string | null;
  timeGokei4f: string | null;
  timeGokei5f: string | null;
  timeGokei6f: string | null;
  timeGokei7f: string | null;
  timeGokei8f: string | null;
  timeGokei9f: string | null;
  tracenKubun: string | null;
  trainingType: string;
  umaban: string;
  workoutIndex: number;
  workoutKey: string;
}

export interface TrainingScrapeDependencies {
  fetchWorkouts: (race: CandidateRace) => Promise<NetkeibaWorkout[]>;
  loadCandidates: () => Promise<CandidateRace[]>;
  log: (message: string) => void;
  now: () => Date;
  upsertRace: (rows: StoredWorkout[]) => Promise<void>;
}

export interface TrainingImportOptions {
  apiBaseUrl: string;
  dates: RealtimeDiscoveryDates;
  executeSql: (sql: string) => Promise<string>;
  fetcher: TrainingHttpFetcher;
  log: (message: string) => void;
  migrationSql: string;
  now: () => Date;
  token: string;
}

export type TrainingHttpFetcher = (
  input: Request | string | URL,
  init?: RequestInit,
) => Promise<Response>;

const REQUIRED_WORKOUT_STRING_FIELDS = [
  "horseNumber",
  "trainingDate",
  "trainingTime",
  "trainingType",
] satisfies string[];
const NULLABLE_WORKOUT_STRING_FIELDS = [
  "commentText",
  "course",
  "courseDirection",
  "evaluationGrade",
  "evaluationText",
  "horseName",
  "lapTime10f",
  "lapTime1f",
  "lapTime2f",
  "lapTime3f",
  "lapTime4f",
  "lapTime5f",
  "lapTime6f",
  "lapTime7f",
  "lapTime8f",
  "lapTime9f",
  "riderName",
  "timeGokei10f",
  "timeGokei2f",
  "timeGokei3f",
  "timeGokei4f",
  "timeGokei5f",
  "timeGokei6f",
  "timeGokei7f",
  "timeGokei8f",
  "timeGokei9f",
  "tracenKubun",
] satisfies string[];

const isObjectRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const requireString = (value: unknown, field: string): string => {
  if (typeof value === "string") return value;
  throw new Error(`Invalid netkeiba workout field: ${field}`);
};

const requireNullableString = (value: unknown, field: string): string | null => {
  if (value === null || typeof value === "string") return value;
  throw new Error(`Invalid netkeiba workout field: ${field}`);
};

const requireWorkoutObject = (value: unknown): Record<string, unknown> => {
  if (!isObjectRecord(value)) throw new Error("Invalid netkeiba workout row");
  REQUIRED_WORKOUT_STRING_FIELDS.map((field) => requireString(value[field], field));
  NULLABLE_WORKOUT_STRING_FIELDS.map((field) => requireNullableString(value[field], field));
  if (!Number.isInteger(value.workoutIndex) || Number(value.workoutIndex) < 1) {
    throw new Error("Invalid netkeiba workout field: workoutIndex");
  }
  return value;
};

const parseWorkout = (value: unknown): NetkeibaWorkout => {
  const row = requireWorkoutObject(value);
  return {
    commentText: requireNullableString(row.commentText, "commentText"),
    course: requireNullableString(row.course, "course"),
    courseDirection: requireNullableString(row.courseDirection, "courseDirection"),
    evaluationGrade: requireNullableString(row.evaluationGrade, "evaluationGrade"),
    evaluationText: requireNullableString(row.evaluationText, "evaluationText"),
    horseName: requireNullableString(row.horseName, "horseName"),
    horseNumber: requireString(row.horseNumber, "horseNumber"),
    lapTime10f: requireNullableString(row.lapTime10f, "lapTime10f"),
    lapTime1f: requireNullableString(row.lapTime1f, "lapTime1f"),
    lapTime2f: requireNullableString(row.lapTime2f, "lapTime2f"),
    lapTime3f: requireNullableString(row.lapTime3f, "lapTime3f"),
    lapTime4f: requireNullableString(row.lapTime4f, "lapTime4f"),
    lapTime5f: requireNullableString(row.lapTime5f, "lapTime5f"),
    lapTime6f: requireNullableString(row.lapTime6f, "lapTime6f"),
    lapTime7f: requireNullableString(row.lapTime7f, "lapTime7f"),
    lapTime8f: requireNullableString(row.lapTime8f, "lapTime8f"),
    lapTime9f: requireNullableString(row.lapTime9f, "lapTime9f"),
    riderName: requireNullableString(row.riderName, "riderName"),
    timeGokei10f: requireNullableString(row.timeGokei10f, "timeGokei10f"),
    timeGokei2f: requireNullableString(row.timeGokei2f, "timeGokei2f"),
    timeGokei3f: requireNullableString(row.timeGokei3f, "timeGokei3f"),
    timeGokei4f: requireNullableString(row.timeGokei4f, "timeGokei4f"),
    timeGokei5f: requireNullableString(row.timeGokei5f, "timeGokei5f"),
    timeGokei6f: requireNullableString(row.timeGokei6f, "timeGokei6f"),
    timeGokei7f: requireNullableString(row.timeGokei7f, "timeGokei7f"),
    timeGokei8f: requireNullableString(row.timeGokei8f, "timeGokei8f"),
    timeGokei9f: requireNullableString(row.timeGokei9f, "timeGokei9f"),
    tracenKubun: requireNullableString(row.tracenKubun, "tracenKubun"),
    trainingDate: requireString(row.trainingDate, "trainingDate"),
    trainingTime: requireString(row.trainingTime, "trainingTime"),
    trainingType: requireString(row.trainingType, "trainingType"),
    workoutIndex: Number(row.workoutIndex),
  };
};

export const parseWorkoutResponse = (value: unknown): NetkeibaWorkout[] => {
  if (!isObjectRecord(value) || !Array.isArray(value.workouts)) {
    throw new Error("Invalid netkeiba training response");
  }
  return value.workouts.map(parseWorkout);
};

export const buildCandidateQuery = (dates: RealtimeDiscoveryDates): string => `
select json_build_object(
  'raceDate', se.kaisai_nen || se.kaisai_tsukihi,
  'sourceRaceId', se.kaisai_nen || lpad(se.keibajo_code, 2, '0') ||
    lpad(ra.kaisai_kai, 2, '0') || lpad(ra.kaisai_nichime, 2, '0') ||
    lpad(se.race_bango, 2, '0'),
  'kaisaiNen', se.kaisai_nen,
  'kaisaiTsukihi', se.kaisai_tsukihi,
  'keibajoCode', se.keibajo_code,
  'raceBango', se.race_bango,
  'umaban', se.umaban,
  'kettoTorokuBango', se.ketto_toroku_bango,
  'bamei', coalesce(se.bamei, '')
)::text
from jvd_se se
join jvd_ra ra using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
where se.kaisai_nen || se.kaisai_tsukihi in ('${dates.base}', '${dates.next}')
  and se.ketto_toroku_bango !~ '^0+$'
  and not exists (
    select 1 from jvd_hc hc
    where hc.ketto_toroku_bango = se.ketto_toroku_bango
      and to_date(hc.chokyo_nengappi, 'YYYYMMDD') >=
        to_date(se.kaisai_nen || se.kaisai_tsukihi, 'YYYYMMDD') - interval '14 days'
      and to_date(hc.chokyo_nengappi, 'YYYYMMDD') <
        to_date(se.kaisai_nen || se.kaisai_tsukihi, 'YYYYMMDD')
  )
  and not exists (
    select 1 from jvd_wc wc
    where wc.ketto_toroku_bango = se.ketto_toroku_bango
      and to_date(wc.chokyo_nengappi, 'YYYYMMDD') >=
        to_date(se.kaisai_nen || se.kaisai_tsukihi, 'YYYYMMDD') - interval '14 days'
      and to_date(wc.chokyo_nengappi, 'YYYYMMDD') <
        to_date(se.kaisai_nen || se.kaisai_tsukihi, 'YYYYMMDD')
  )
order by se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango, se.umaban;
`;

const parseCandidateRow = (value: unknown): CandidateRunnerRow => {
  if (!isObjectRecord(value)) throw new Error("Invalid local candidate row");
  return {
    bamei: requireString(value.bamei, "bamei"),
    kaisaiNen: requireString(value.kaisaiNen, "kaisaiNen"),
    kaisaiTsukihi: requireString(value.kaisaiTsukihi, "kaisaiTsukihi"),
    keibajoCode: requireString(value.keibajoCode, "keibajoCode"),
    kettoTorokuBango: requireString(value.kettoTorokuBango, "kettoTorokuBango"),
    raceBango: requireString(value.raceBango, "raceBango"),
    raceDate: requireString(value.raceDate, "raceDate"),
    sourceRaceId: requireString(value.sourceRaceId, "sourceRaceId"),
    umaban: requireString(value.umaban, "umaban"),
  };
};

export const parseCandidateOutput = (output: string): CandidateRunnerRow[] =>
  output
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line !== "")
    .map((line) => parseCandidateRow(JSON.parse(line)));

export const groupCandidateRaces = (rows: CandidateRunnerRow[]): CandidateRace[] => {
  const grouped = new Map<string, CandidateRace>();
  rows.map((row) => {
    const existing = grouped.get(row.sourceRaceId);
    if (existing !== undefined) {
      existing.runners.push(row);
      return;
    }
    grouped.set(row.sourceRaceId, {
      kaisaiNen: row.kaisaiNen,
      kaisaiTsukihi: row.kaisaiTsukihi,
      keibajoCode: row.keibajoCode,
      raceBango: row.raceBango,
      raceDate: row.raceDate,
      runners: [row],
      sourceRaceId: row.sourceRaceId,
    });
  });
  return [...grouped.values()];
};

const normalizeHorseNumber = (value: string): string => String(Number.parseInt(value, 10));

const buildWorkoutKey = (workout: NetkeibaWorkout): string =>
  createHash("sha256")
    .update(
      JSON.stringify({
        course: workout.course,
        courseDirection: workout.courseDirection,
        trainingDate: workout.trainingDate,
        trainingTime: workout.trainingTime,
        trainingType: workout.trainingType,
        workoutIndex: workout.workoutIndex,
      }),
    )
    .digest("hex");

export const joinWorkoutsToRunners = (
  race: CandidateRace,
  workouts: NetkeibaWorkout[],
  now: Date,
): StoredWorkout[] => {
  const runners = new Map(
    race.runners.map((runner) => [normalizeHorseNumber(runner.umaban), runner]),
  );
  const fetchedAt = now.toISOString();
  const dataSakuseiNengappi = now
    .toLocaleDateString("sv-SE", { timeZone: "Asia/Tokyo" })
    .replaceAll("-", "");
  return workouts.flatMap((workout) => {
    const runner = runners.get(normalizeHorseNumber(workout.horseNumber));
    if (runner === undefined) return [];
    return [
      {
        bamei: workout.horseName === null ? runner.bamei : workout.horseName,
        babamawari: workout.courseDirection,
        chokyoJikoku: workout.trainingTime,
        chokyoNengappi: workout.trainingDate,
        commentText: workout.commentText,
        course: workout.course,
        dataSakuseiNengappi,
        evaluationGrade: workout.evaluationGrade,
        evaluationText: workout.evaluationText,
        fetchedAt,
        kaisaiNen: race.kaisaiNen,
        kaisaiTsukihi: race.kaisaiTsukihi,
        keibajoCode: race.keibajoCode,
        kettoTorokuBango: runner.kettoTorokuBango,
        lapTime10f: workout.lapTime10f,
        lapTime1f: workout.lapTime1f,
        lapTime2f: workout.lapTime2f,
        lapTime3f: workout.lapTime3f,
        lapTime4f: workout.lapTime4f,
        lapTime5f: workout.lapTime5f,
        lapTime6f: workout.lapTime6f,
        lapTime7f: workout.lapTime7f,
        lapTime8f: workout.lapTime8f,
        lapTime9f: workout.lapTime9f,
        raceBango: race.raceBango,
        riderName: workout.riderName,
        sourceRaceId: race.sourceRaceId,
        sourceUrl: `https://race.netkeiba.com/race/oikiri.html?race_id=${race.sourceRaceId}`,
        timeGokei10f: workout.timeGokei10f,
        timeGokei2f: workout.timeGokei2f,
        timeGokei3f: workout.timeGokei3f,
        timeGokei4f: workout.timeGokei4f,
        timeGokei5f: workout.timeGokei5f,
        timeGokei6f: workout.timeGokei6f,
        timeGokei7f: workout.timeGokei7f,
        timeGokei8f: workout.timeGokei8f,
        timeGokei9f: workout.timeGokei9f,
        tracenKubun: workout.tracenKubun,
        trainingType: workout.trainingType,
        umaban: runner.umaban,
        workoutIndex: workout.workoutIndex,
        workoutKey: buildWorkoutKey(workout),
      },
    ];
  });
};

const quoteSql = (value: string | null): string =>
  value === null ? "null" : `'${value.replaceAll("'", "''")}'`;

const workoutValues = (row: StoredWorkout): string =>
  [
    row.kaisaiNen,
    row.kaisaiTsukihi,
    row.keibajoCode,
    row.raceBango,
    row.sourceRaceId,
    row.umaban,
    row.kettoTorokuBango,
    row.bamei,
    row.workoutKey,
    String(row.workoutIndex),
    row.dataSakuseiNengappi,
    row.chokyoNengappi,
    row.chokyoJikoku,
    row.trainingType,
    row.tracenKubun,
    row.course,
    row.babamawari,
    row.timeGokei10f,
    row.lapTime10f,
    row.timeGokei9f,
    row.lapTime9f,
    row.timeGokei8f,
    row.lapTime8f,
    row.timeGokei7f,
    row.lapTime7f,
    row.timeGokei6f,
    row.lapTime6f,
    row.timeGokei5f,
    row.lapTime5f,
    row.timeGokei4f,
    row.lapTime4f,
    row.timeGokei3f,
    row.lapTime3f,
    row.timeGokei2f,
    row.lapTime2f,
    row.lapTime1f,
    row.riderName,
    row.evaluationText,
    row.evaluationGrade,
    row.commentText,
    row.sourceUrl,
    row.fetchedAt,
  ]
    .map(quoteSql)
    .join(", ");

const UPSERT_COLUMNS = `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
source_race_id, umaban, ketto_toroku_bango, bamei, workout_key, workout_index,
data_sakusei_nengappi, chokyo_nengappi, chokyo_jikoku, training_type, tracen_kubun,
course, babamawari, time_gokei_10f, lap_time_10f, time_gokei_9f, lap_time_9f,
time_gokei_8f, lap_time_8f, time_gokei_7f, lap_time_7f, time_gokei_6f, lap_time_6f,
time_gokei_5f, lap_time_5f, time_gokei_4f, lap_time_4f, time_gokei_3f, lap_time_3f,
time_gokei_2f, lap_time_2f, lap_time_1f, rider_name, evaluation_text,
evaluation_grade, comment_text, source_url, fetched_at`;

export const buildRaceUpsertSql = (rows: StoredWorkout[]): string => {
  if (rows.length === 0) throw new Error("Refusing to upsert an empty netkeiba workout set");
  return `begin;
insert into netkeiba_training_workouts (${UPSERT_COLUMNS}) values
${rows.map((row) => `(${workoutValues(row)})`).join(",\n")}
on conflict (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, workout_key)
do update set
  source_race_id = excluded.source_race_id,
  umaban = excluded.umaban,
  bamei = excluded.bamei,
  workout_index = excluded.workout_index,
  data_sakusei_nengappi = excluded.data_sakusei_nengappi,
  chokyo_nengappi = excluded.chokyo_nengappi,
  chokyo_jikoku = excluded.chokyo_jikoku,
  training_type = excluded.training_type,
  tracen_kubun = excluded.tracen_kubun,
  course = excluded.course,
  babamawari = excluded.babamawari,
  time_gokei_10f = excluded.time_gokei_10f,
  lap_time_10f = excluded.lap_time_10f,
  time_gokei_9f = excluded.time_gokei_9f,
  lap_time_9f = excluded.lap_time_9f,
  time_gokei_8f = excluded.time_gokei_8f,
  lap_time_8f = excluded.lap_time_8f,
  time_gokei_7f = excluded.time_gokei_7f,
  lap_time_7f = excluded.lap_time_7f,
  time_gokei_6f = excluded.time_gokei_6f,
  lap_time_6f = excluded.lap_time_6f,
  time_gokei_5f = excluded.time_gokei_5f,
  lap_time_5f = excluded.lap_time_5f,
  time_gokei_4f = excluded.time_gokei_4f,
  lap_time_4f = excluded.lap_time_4f,
  time_gokei_3f = excluded.time_gokei_3f,
  lap_time_3f = excluded.lap_time_3f,
  time_gokei_2f = excluded.time_gokei_2f,
  lap_time_2f = excluded.lap_time_2f,
  lap_time_1f = excluded.lap_time_1f,
  rider_name = excluded.rider_name,
  evaluation_text = excluded.evaluation_text,
  evaluation_grade = excluded.evaluation_grade,
  comment_text = excluded.comment_text,
  source_url = excluded.source_url,
  fetched_at = excluded.fetched_at,
  updated_at = now();
commit;
`;
};

export const runTrainingScrape = async (
  dependencies: TrainingScrapeDependencies,
): Promise<number> => {
  const races = await dependencies.loadCandidates();
  if (races.length === 0) {
    dependencies.log("No JRA runners without official 14-day workouts were found.");
    return 0;
  }
  const counts = await races.reduce<Promise<number[]>>(async (pending, race) => {
    const completed = await pending;
    const workouts = await dependencies.fetchWorkouts(race);
    if (workouts.length === 0) {
      dependencies.log(`Skipped race ${race.sourceRaceId}: netkeiba returned no workouts.`);
      return [...completed, 0];
    }
    const rows = joinWorkoutsToRunners(race, workouts, dependencies.now());
    if (rows.length === 0) {
      dependencies.log(
        `Skipped race ${race.sourceRaceId}: netkeiba workouts did not match runners missing official workouts.`,
      );
      return [...completed, 0];
    }
    await dependencies.upsertRace(rows);
    dependencies.log(`Stored ${rows.length} netkeiba workouts for race ${race.sourceRaceId}.`);
    return [...completed, rows.length];
  }, Promise.resolve([]));
  return counts.reduce((total, count) => total + count, 0);
};

const fetchRaceWorkouts = async (
  options: TrainingImportOptions,
  race: CandidateRace,
): Promise<NetkeibaWorkout[]> => {
  const response = await options.fetcher(
    `${options.apiBaseUrl.replace(/\/+$/u, "")}/api/internal/netkeiba-training-workouts`,
    {
      body: JSON.stringify({ raceDate: race.raceDate, sourceRaceId: race.sourceRaceId }),
      headers: {
        authorization: `Bearer ${options.token}`,
        "content-type": "application/json",
      },
      method: "POST",
    },
  );
  if (!response.ok) {
    const detail = (await response.text()).trim();
    throw new Error(
      `Netkeiba training API failed for race ${race.sourceRaceId} with HTTP ${response.status}: ${detail === "" ? "no response body" : detail}`,
    );
  }
  const body: unknown = await response.json();
  return parseWorkoutResponse(body);
};

export const runNetkeibaTrainingImport = async (
  options: TrainingImportOptions,
): Promise<number> => {
  if (options.token === "") throw new Error("REALTIME_ADMIN_TOKEN must not be empty");
  await options.executeSql(options.migrationSql);
  const candidateOutput = await options.executeSql(buildCandidateQuery(options.dates));
  return runTrainingScrape({
    fetchWorkouts: (race) => fetchRaceWorkouts(options, race),
    loadCandidates: () =>
      Promise.resolve(groupCandidateRaces(parseCandidateOutput(candidateOutput))),
    log: options.log,
    now: options.now,
    upsertRace: async (rows) => {
      await options.executeSql(buildRaceUpsertSql(rows));
    },
  });
};
