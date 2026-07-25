// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import {
  buildExactPkExistsStatement,
  buildExistingRunnerKeysStatement,
  buildKeyMigrationStatement,
  buildRealKettoExistsStatement,
  writeJvdSeRunnersIdempotently,
  type JvdRaceKey,
  type QueryOutcome,
  type SqlExecutor,
  type WriteSummary,
} from "./idempotent-write";
import type { JvdSeRow, SqlStatement } from "../types";

interface RunnerOverrides {
  readonly umaban: string;
  readonly ketto_toroku_bango: string;
  readonly bamei: string;
}

interface RecordingExecutor {
  readonly executor: SqlExecutor;
  readonly statements: SqlStatement[];
}

const RACE_KEY: JvdRaceKey = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "0725",
  keibajo_code: "A6",
  race_bango: "05",
};
const REAL_KETTO: string = "2019101234";
const DIFFERENT_REAL_KETTO: string = "2020109876";
const PLACEHOLDER_KETTO: string = "0000000000";

const baseRunner = (overrides: RunnerOverrides): JvdSeRow => ({
  record_id: "SE",
  data_kubun: "7",
  data_sakusei_nengappi: "20260725",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0725",
  keibajo_code: "A6",
  kaisai_kai: "0",
  kaisai_nichime: "0",
  race_bango: "05",
  wakuban: "1",
  umaban: overrides.umaban,
  ketto_toroku_bango: overrides.ketto_toroku_bango,
  bamei: overrides.bamei,
  umakigo_code: "00",
  seibetsu_code: "1",
  hinshu_code: "1",
  moshoku_code: "01",
  barei: "04",
  tozai_shozoku_code: "0",
  chokyoshi_code: "00000",
  chokyoshimei_ryakusho: "TRAINER",
  banushi_code: "000000",
  banushimei: "OWNER",
  fukushoku_hyoji: "",
  yobi_1: "",
  futan_juryo: "610",
  futan_juryo_henkomae: "",
  blinker_shiyo_kubun: "0",
  yobi_2: "",
  kishu_code: "00000",
  kishu_code_henkomae: "",
  kishumei_ryakusho: "JOCKEY",
  kishumei_ryakusho_henkomae: "",
  kishu_minarai_code: "0",
  kishu_minarai_code_henkomae: "",
  bataiju: "",
  zogen_fugo: "",
  zogen_sa: "",
  ijo_kubun_code: "0",
  nyusen_juni: "",
  kakutei_chakujun: "",
  dochaku_kubun: "0",
  dochaku_tosu: "0",
  soha_time: "",
  chakusa_code_1: "",
  chakusa_code_2: "",
  chakusa_code_3: "",
  corner_1: "",
  corner_2: "",
  corner_3: "",
  corner_4: "",
  tansho_odds: "",
  tansho_ninkijun: "",
  kakutoku_honshokin: "",
  kakutoku_fukashokin: "",
  yobi_3: "",
  yobi_4: "",
  kohan_4f: "",
  kohan_3f: "",
  aiteuma_joho_1: "",
  aiteuma_joho_2: "",
  aiteuma_joho_3: "",
  time_sa: "",
  record_koshin_kubun: "0",
  mining_kubun: "0",
  yoso_soha_time: "",
  yoso_gosa_plus: "",
  yoso_gosa_minus: "",
  yoso_juni: "",
  kyakushitsu_hantei: "0",
});

const createRecordingExecutor = (outcomes: readonly QueryOutcome[]): RecordingExecutor => {
  const statements: SqlStatement[] = [];
  const cursor: { nextIndex: number } = { nextIndex: 0 };
  const executor: SqlExecutor = {
    execute: (statement: SqlStatement): Promise<QueryOutcome> => {
      statements.push(statement);
      const outcome: QueryOutcome | undefined = outcomes[cursor.nextIndex];
      cursor.nextIndex += 1;
      if (outcome === undefined) {
        return Promise.reject(new Error("Unexpected extra SQL execution in test."));
      }
      return Promise.resolve(outcome);
    },
  };
  return { executor, statements };
};

test("SQL builders emit fixed parameterized identity statements", () => {
  const migration: SqlStatement = buildKeyMigrationStatement({
    raceKey: RACE_KEY,
    umaban: "03",
    realKettoTorokuBango: "2019101234",
  });
  const exact: SqlStatement = buildExactPkExistsStatement({
    raceKey: RACE_KEY,
    umaban: "03",
    kettoTorokuBango: "2019101234",
  });
  const real: SqlStatement = buildRealKettoExistsStatement({
    raceKey: RACE_KEY,
    umaban: "03",
  });
  const list: SqlStatement = buildExistingRunnerKeysStatement({
    raceKey: RACE_KEY,
    umaban: "03",
  });

  expect(migration.text).toBe(
    "UPDATE jvd_se SET ketto_toroku_bango = $6 WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 AND ketto_toroku_bango = $7",
  );
  expect(migration.values).toStrictEqual([
    "2026",
    "0725",
    "A6",
    "05",
    "03",
    "2019101234",
    "0000000000",
  ]);
  expect(exact.text).toBe(
    "SELECT 1 FROM jvd_se WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 AND ketto_toroku_bango = $6 LIMIT 1",
  );
  expect(exact.values).toStrictEqual(["2026", "0725", "A6", "05", "03", "2019101234"]);
  expect(real.text).toBe(
    "SELECT 1 FROM jvd_se WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 AND ketto_toroku_bango <> $6 LIMIT 1",
  );
  expect(real.values).toStrictEqual(["2026", "0725", "A6", "05", "03", "0000000000"]);
  expect(list.text).toBe(
    "SELECT ketto_toroku_bango FROM jvd_se WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 ORDER BY ketto_toroku_bango FOR UPDATE",
  );
  expect(list.values).toStrictEqual(["2026", "0725", "A6", "05", "03"]);
});

test("zero existing rows performs a plain insert", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "01",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "NEW REAL HORSE",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 0, rows: [] },
    { rowCount: 1 },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [runner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 1,
    updated: 0,
    skipped: 0,
    conflicts: [],
  });
  expect(statements).toHaveLength(2);
  expect(statements[0]?.text).toBe(
    "SELECT ketto_toroku_bango FROM jvd_se WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 ORDER BY ketto_toroku_bango FOR UPDATE",
  );
  expect(statements[1]?.text.startsWith("INSERT INTO jvd_se (")).toBe(true);
});

test("single placeholder row migrates to an incoming real key then updates", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "03",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "MIGRATED HORSE",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 1, rows: [{ ketto_toroku_bango: "0000000000" }] },
    { rowCount: 1 },
    { rowCount: 1 },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [runner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 1,
    inserted: 0,
    updated: 1,
    skipped: 0,
    conflicts: [],
  });
  expect(statements).toHaveLength(3);
  expect(statements[1]?.text).toBe(
    "UPDATE jvd_se SET ketto_toroku_bango = $6 WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 AND umaban = $5 AND ketto_toroku_bango = $7",
  );
  expect(statements[2]?.text.startsWith("INSERT INTO jvd_se (")).toBe(true);
});

test("single row matching the incoming real key performs an update", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "02",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "EXISTING REAL HORSE",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 1, rows: [{ ketto_toroku_bango: "2019101234" }] },
    { rowCount: 1 },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [runner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 0,
    updated: 1,
    skipped: 0,
    conflicts: [],
  });
  expect(statements).toHaveLength(2);
});

test("single different real key fails closed and surfaces both identities", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "04",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "CONFLICTING HORSE",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 1, rows: [{ ketto_toroku_bango: "2020109876" }] },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [runner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 0,
    updated: 0,
    skipped: 0,
    conflicts: [
      {
        raceKey: {
          kaisai_nen: "2026",
          kaisai_tsukihi: "0725",
          keibajo_code: "A6",
          race_bango: "05",
        },
        umaban: "04",
        storedKettoTorokuBango: "2020109876",
        incomingKettoTorokuBango: "2019101234",
      },
    ],
  });
  expect(statements).toHaveLength(1);
});

test("placeholder and target real rows fail closed before a colliding migration", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "05",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "PREEXISTING DUPLICATE",
  });
  const { executor, statements } = createRecordingExecutor([
    {
      rowCount: 2,
      rows: [{ ketto_toroku_bango: "0000000000" }, { ketto_toroku_bango: "2019101234" }],
    },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [runner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 0,
    updated: 0,
    skipped: 0,
    conflicts: [
      {
        raceKey: {
          kaisai_nen: "2026",
          kaisai_tsukihi: "0725",
          keibajo_code: "A6",
          race_bango: "05",
        },
        umaban: "05",
        storedKettoTorokuBango: "0000000000",
        incomingKettoTorokuBango: "2019101234",
      },
      {
        raceKey: {
          kaisai_nen: "2026",
          kaisai_tsukihi: "0725",
          keibajo_code: "A6",
          race_bango: "05",
        },
        umaban: "05",
        storedKettoTorokuBango: "2019101234",
        incomingKettoTorokuBango: "2019101234",
      },
    ],
  });
  expect(statements).toHaveLength(1);
});

test("incoming placeholder skips when a real-keyed row exists", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "06",
    ketto_toroku_bango: PLACEHOLDER_KETTO,
    bamei: "STALE PLACEHOLDER",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 1, rows: [{ ketto_toroku_bango: "2019101234" }] },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [runner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 0,
    updated: 0,
    skipped: 1,
    conflicts: [],
  });
  expect(statements).toHaveLength(1);
});

test("incoming placeholder inserts with no row and updates its matching row", async () => {
  const insertRunner: JvdSeRow = baseRunner({
    umaban: "07",
    ketto_toroku_bango: PLACEHOLDER_KETTO,
    bamei: "NEW PLACEHOLDER",
  });
  const updateRunner: JvdSeRow = baseRunner({
    umaban: "08",
    ketto_toroku_bango: PLACEHOLDER_KETTO,
    bamei: "EXISTING PLACEHOLDER",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 0, rows: [] },
    { rowCount: 1 },
    { rowCount: 1, rows: [{ ketto_toroku_bango: "0000000000" }] },
    { rowCount: 1 },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [insertRunner, updateRunner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 1,
    updated: 1,
    skipped: 0,
    conflicts: [],
  });
  expect(statements).toHaveLength(4);
});

test("one conflicting runner is skipped while independent runners continue", async () => {
  const insertRunner: JvdSeRow = baseRunner({
    umaban: "09",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "INSERT ME",
  });
  const conflictRunner: JvdSeRow = baseRunner({
    umaban: "10",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "REPORT ME",
  });
  const updateRunner: JvdSeRow = baseRunner({
    umaban: "11",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "UPDATE ME",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 0, rows: [] },
    { rowCount: 1 },
    { rowCount: 1, rows: [{ ketto_toroku_bango: "2020109876" }] },
    { rowCount: 1, rows: [{ ketto_toroku_bango: "2019101234" }] },
    { rowCount: 1 },
  ]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [insertRunner, conflictRunner, updateRunner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 1,
    updated: 1,
    skipped: 0,
    conflicts: [
      {
        raceKey: {
          kaisai_nen: "2026",
          kaisai_tsukihi: "0725",
          keibajo_code: "A6",
          race_bango: "05",
        },
        umaban: "10",
        storedKettoTorokuBango: "2020109876",
        incomingKettoTorokuBango: "2019101234",
      },
    ],
  });
  expect(statements).toHaveLength(5);
});

test("empty runners return a zero summary without SQL", async () => {
  const { executor, statements } = createRecordingExecutor([]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 0,
    updated: 0,
    skipped: 0,
    conflicts: [],
  });
  expect(statements).toHaveLength(0);
});

test("positive identity row count without rows fails closed", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "12",
    ketto_toroku_bango: DIFFERENT_REAL_KETTO,
    bamei: "MISSING ROW DATA",
  });
  const { executor, statements } = createRecordingExecutor([{ rowCount: 1 }]);

  await expect(
    writeJvdSeRunnersIdempotently({
      raceKey: RACE_KEY,
      runners: [runner],
      executor,
    }),
  ).rejects.toThrow("Identity lookup returned a positive row count without identity rows.");
  expect(statements).toHaveLength(1);
});

test("zero identity row count remains compatible with executors that omit rows", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "13",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "COMPATIBLE INSERT",
  });
  const { executor, statements } = createRecordingExecutor([{ rowCount: 0 }, { rowCount: 1 }]);

  const summary: WriteSummary = await writeJvdSeRunnersIdempotently({
    raceKey: RACE_KEY,
    runners: [runner],
    executor,
  });

  expect(summary).toStrictEqual({
    migrated: 0,
    inserted: 1,
    updated: 0,
    skipped: 0,
    conflicts: [],
  });
  expect(statements).toHaveLength(2);
});

test("identity row without a ketto column fails closed", async () => {
  const runner: JvdSeRow = baseRunner({
    umaban: "14",
    ketto_toroku_bango: REAL_KETTO,
    bamei: "INVALID ROW DATA",
  });
  const { executor, statements } = createRecordingExecutor([
    { rowCount: 1, rows: [{ unrelated_column: "value" }] },
  ]);

  await expect(
    writeJvdSeRunnersIdempotently({
      raceKey: RACE_KEY,
      runners: [runner],
      executor,
    }),
  ).rejects.toThrow("Identity lookup returned a row without ketto_toroku_bango.");
  expect(statements).toHaveLength(1);
});
