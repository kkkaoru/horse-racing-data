// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  buildRaceEntityHistoryQuery,
  buildRaceEntityInitialQuery,
  buildRaceEntityPage,
  buildRaceEntityTargetQuery,
  createRaceEntityCursor,
  normaliseRaceEntityHistoryRow,
  normaliseRaceEntityInitialTarget,
  normaliseRaceEntityTarget,
  parseRaceEntityCursor,
} from "./race-entity-recent-results";
import type { RaceEntityRecentResultsFilters, RaceEntityType } from "./types";

const CURSOR_SECRET: string = "cursor-signing-secret-at-least-32-characters";

const env = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "catalog",
  R2_SQL_TOKEN: "token",
};

const filters = (entityType: RaceEntityType): RaceEntityRecentResultsFilters => ({
  cursor: null,
  date: "20260827",
  entityType,
  horseNumber: "07",
  keibajoCode: "50",
  limit: entityType === "horse" ? 5 : 10,
  raceBango: "05",
  source: "nar",
});

const target = {
  entityId: "21379",
  entityName: "小谷哲平",
  horseId: "2022103916",
  horseName: "ロングロングロング",
  raceName: "御幣島7ハロン",
  raceStartTime: "1240",
  runnerFound: true,
};

const rawHistory = (resultId: string, startKey: string): Record<string, unknown> => ({
  abnormality_code: null,
  carried_weight: 57,
  class_name: "C3",
  corner_1: "02",
  corner_2: null,
  corner_3: "03",
  corner_4: "01",
  dirt_condition_code: "1",
  distance: 1400,
  field_size: 10,
  final_3f_seconds: 36.1,
  finish_position: 1,
  frame_number: "7",
  grade_code: "G",
  horse_id: "2022103916",
  horse_name: "Horse",
  horse_number: "07",
  horse_weight: 480,
  horse_weight_diff: -2,
  jockey_id: "21379",
  jockey_name: "Jockey",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0827",
  keibajo_code: "50",
  margin: "000",
  owner_id: "768006",
  owner_name: "Owner",
  popularity: 2,
  race_bango: "04",
  race_id: "nar:20260827:50:04",
  race_name: "Earlier",
  race_start_sort_key: startKey,
  race_start_time: "1210",
  race_time_seconds: 88.1,
  result_id: resultId,
  source: "nar",
  track_code: "24",
  trainer_id: "20692",
  trainer_name: "Trainer",
  turf_condition_code: "2",
  weather_code: "1",
  win_odds: 3.2,
});

it("builds target queries with canonical horse, jockey, trainer, and owner IDs", () => {
  expect(buildRaceEntityTargetQuery(env, filters("horse"))).toMatch(/se\.ketto_toroku_bango/u);
  expect(buildRaceEntityTargetQuery(env, filters("jockey"))).toMatch(/se\.kishu_code/u);
  expect(buildRaceEntityTargetQuery(env, filters("trainer"))).toMatch(/se\.chokyoshi_code/u);
  expect(buildRaceEntityTargetQuery(env, filters("owner"))).toMatch(/se\.banushi_code/u);
  expect(buildRaceEntityTargetQuery(env, filters("horse"))).toMatch(/catalog\.nvd_ra/u);
});

it("builds JRA target query and rejects an unsafe namespace", () => {
  expect(buildRaceEntityTargetQuery(env, { ...filters("horse"), source: "jra" })).toMatch(
    /catalog\.jvd_ra/u,
  );
  expect(() =>
    buildRaceEntityTargetQuery({ ...env, R2_SQL_NAMESPACE: "bad-name" }, filters("horse")),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
});

it("builds a single initial target and history query", () => {
  const query = buildRaceEntityInitialQuery(env, filters("trainer"));
  expect(query).toMatch(/WITH target AS/u);
  expect(query).toMatch(/CROSS JOIN target/u);
  expect(query).toMatch(/LEFT JOIN bounded_history ON true/u);
  expect(query).toMatch(/se\.chokyoshi_code = target\.entity_id/u);
  expect(query).not.toMatch(/catalog\.jvd_se/u);
  expect(
    normaliseRaceEntityInitialTarget({
      target_entity_id: "20692",
      target_entity_name: "Trainer",
      target_horse_id: "2022103916",
      target_horse_name: "Horse",
      target_race_name: "Race",
      target_race_start_time: "1240",
      target_runner_found: true,
    }),
  ).toMatchObject({ entityId: "20692", runnerFound: true });
});

it("normalises target values from R2 SQL scalar shapes", () => {
  expect(
    normaliseRaceEntityTarget({
      entity_id: 21379,
      entity_name: "Jockey",
      horse_id: 2022103916n,
      horse_name: "Horse",
      race_name: "Race",
      race_start_time: 1240,
      runner_found: "true",
    }),
  ).toStrictEqual({
    entityId: "21379",
    entityName: "Jockey",
    horseId: "2022103916",
    horseName: "Horse",
    raceName: "Race",
    raceStartTime: "1240",
    runnerFound: true,
  });
  expect(normaliseRaceEntityTarget({ runner_found: false })).toStrictEqual({
    entityId: null,
    entityName: null,
    horseId: null,
    horseName: null,
    raceName: null,
    raceStartTime: null,
    runnerFound: false,
  });
});

it("builds bounded N+1 history queries for all canonical entity columns", () => {
  expect(buildRaceEntityHistoryQuery(env, filters("horse"), target, null)).toMatch(
    /se\.ketto_toroku_bango/u,
  );
  expect(buildRaceEntityHistoryQuery(env, filters("jockey"), target, null)).toMatch(
    /se\.kishu_code/u,
  );
  expect(buildRaceEntityHistoryQuery(env, filters("trainer"), target, null)).toMatch(
    /se\.chokyoshi_code/u,
  );
  expect(buildRaceEntityHistoryQuery(env, filters("owner"), target, null)).toMatch(
    /se\.banushi_code/u,
  );
  expect(buildRaceEntityHistoryQuery(env, filters("horse"), target, null)).toMatch(/LIMIT 6/u);
  expect(buildRaceEntityHistoryQuery(env, filters("horse"), target, null)).toMatch(
    /race_start_sort_key DESC, result_id DESC/u,
  );
  expect(buildRaceEntityHistoryQuery(env, filters("horse"), target, null)).toMatch(/regexp_match/u);
});

it("prunes recent year partitions and scopes non-horse IDs to the target source", () => {
  const jockeyQuery = buildRaceEntityHistoryQuery(env, filters("jockey"), target, null);
  expect(jockeyQuery).toMatch(/se\.kaisai_nen >= '2025'/u);
  expect(jockeyQuery).toMatch(/se\.kishu_code = '21379'/u);
  expect(jockeyQuery).toMatch(/catalog\.nvd_se/u);
  expect(jockeyQuery).not.toMatch(/catalog\.jvd_se/u);

  const jraOwnerQuery = buildRaceEntityHistoryQuery(
    env,
    { ...filters("owner"), source: "jra" },
    target,
    null,
  );
  expect(jraOwnerQuery).toMatch(/catalog\.jvd_se/u);
  expect(jraOwnerQuery).not.toMatch(/catalog\.nvd_se/u);

  const horseQuery = buildRaceEntityHistoryQuery(
    env,
    filters("horse"),
    { ...target, entityId: "2022103916" },
    null,
  );
  expect(horseQuery).toMatch(/se\.kaisai_nen >= '2024'/u);
  expect(horseQuery).toMatch(/catalog\.jvd_se/u);
  expect(horseQuery).toMatch(/catalog\.nvd_se/u);
});

it("uses keyset cursor predicates and rejects malformed canonical IDs", () => {
  expect(
    buildRaceEntityHistoryQuery(env, filters("jockey"), target, {
      raceStartSortKey: "202608271210",
      resultId: "nar:20260827:50:04:07:2022103916",
    }),
  ).toMatch(/AND concat\('nar'/u);
  expect(
    buildRaceEntityHistoryQuery(env, filters("jockey"), target, {
      raceStartSortKey: "202512311500",
      resultId: "nar:20251231:50:12:07:2022103916",
    }),
  ).toMatch(/se\.kaisai_nen <= '2025'/u);
  expect(() =>
    buildRaceEntityHistoryQuery(env, filters("jockey"), { ...target, entityId: "bad'id" }, null),
  ).toThrow("entityId is malformed");
});

it("normalises complete and nullable history fields", () => {
  expect(
    normaliseRaceEntityHistoryRow(rawHistory("nar:20260827:50:04:07:2022103916", "202608271210")),
  ).toMatchObject({
    carriedWeight: 57,
    cornerPositions: ["02", "03", "01"],
    finishPosition: 1,
    horseId: "2022103916",
    jockeyId: "21379",
    ownerId: "768006",
    raceId: "nar:20260827:50:04",
    raceStartSortKey: "202608271210",
    source: "nar",
    trainerId: "20692",
  });
  expect(
    normaliseRaceEntityHistoryRow({
      kaisai_nen: "2025",
      kaisai_tsukihi: "0101",
      keibajo_code: "05",
      race_bango: "01",
      race_id: "jra:20250101:05:01",
      race_start_sort_key: "202501011000",
      result_id: "jra:20250101:05:01:01:2020100001",
      source: "jra",
    }),
  ).toMatchObject({ cornerPositions: [], finishPosition: null, source: "jra" });
  expect(() => normaliseRaceEntityHistoryRow({})).toThrow("R2 SQL row is missing kaisai_nen");
});

it("creates signed opaque cursors and rejects tampering or scope reuse", async () => {
  const first = filters("jockey");
  const cursor = await createRaceEntityCursor(
    first,
    "21379",
    {
      raceStartSortKey: "202608271210",
      resultId: "nar:20260827:50:04:07:2022103916",
    },
    CURSOR_SECRET,
  );
  expect(await parseRaceEntityCursor({ ...first, cursor }, "21379", CURSOR_SECRET)).toStrictEqual({
    raceStartSortKey: "202608271210",
    resultId: "nar:20260827:50:04:07:2022103916",
  });
  expect(await parseRaceEntityCursor(first, "21379", CURSOR_SECRET)).toBe(null);
  expect(
    await parseRaceEntityCursor({ ...first, cursor: "not-base64" }, "21379", CURSOR_SECRET),
  ).toBe("invalid");
  expect(
    await parseRaceEntityCursor(
      { ...first, cursor: `${cursor.slice(0, -1)}A` },
      "21379",
      CURSOR_SECRET,
    ),
  ).toBe("invalid");
  expect(
    await parseRaceEntityCursor(
      { ...first, cursor, entityType: "trainer" },
      "21379",
      CURSOR_SECRET,
    ),
  ).toBe("invalid");
  expect(
    await parseRaceEntityCursor({ ...first, cursor, raceBango: "06" }, "21379", CURSOR_SECRET),
  ).toBe("invalid");
  expect(await parseRaceEntityCursor({ ...first, cursor }, "99999", CURSOR_SECRET)).toBe("invalid");
  expect(await parseRaceEntityCursor({ ...first, cursor }, "21379", `${CURSOR_SECRET}x`)).toBe(
    "invalid",
  );
  await expect(
    createRaceEntityCursor(
      first,
      "21379",
      {
        raceStartSortKey: "202608271210",
        resultId: "nar:20260827:50:04:07:2022103916",
      },
      "short",
    ),
  ).rejects.toThrow("RACE_ENTITY_CURSOR_SECRET must contain at least 32 characters");
});

it("rejects malformed signed cursor envelopes", async () => {
  const first = filters("horse");
  expect(await parseRaceEntityCursor({ ...first, cursor: btoa("null") }, "1", CURSOR_SECRET)).toBe(
    "invalid",
  );
  expect(await parseRaceEntityCursor({ ...first, cursor: btoa("[]") }, "1", CURSOR_SECRET)).toBe(
    "invalid",
  );
  expect(await parseRaceEntityCursor({ ...first, cursor: btoa("{}") }, "1", CURSOR_SECRET)).toBe(
    "invalid",
  );
  expect(
    await parseRaceEntityCursor(
      { ...first, cursor: btoa(JSON.stringify({ payload: "bad", signature: "bad" })) },
      "1",
      CURSOR_SECRET,
    ),
  ).toBe("invalid");
});

it("normalises unsupported scalar shapes and numeric values safely", () => {
  expect(
    normaliseRaceEntityTarget({ entity_id: {}, runner_found: 1, race_start_time: true }),
  ).toMatchObject({ entityId: null, raceStartTime: "true", runnerFound: true });
  expect(
    normaliseRaceEntityHistoryRow({
      carried_weight: "not-number",
      kaisai_nen: "2025",
      kaisai_tsukihi: "0101",
      keibajo_code: "05",
      race_bango: "01",
      race_id: "jra:20250101:05:01",
      race_start_sort_key: "202501011000",
      result_id: "jra:20250101:05:01:01:2020100001",
      source: "other",
    }),
  ).toMatchObject({ carriedWeight: null, source: "nar" });
});

it("builds a deterministic public page and N+1 hasMore cursor", async () => {
  const rows = [
    normaliseRaceEntityHistoryRow(rawHistory("nar:20260827:50:04:07:2022103916", "202608271210")),
    normaliseRaceEntityHistoryRow(rawHistory("nar:20260820:50:09:03:2021100001", "202608201500")),
  ];
  const page = await buildRaceEntityPage(
    { ...filters("horse"), limit: 1 },
    target,
    rows,
    CURSOR_SECRET,
  );
  expect(page.pagination).toMatchObject({
    effectiveLimit: 1,
    hasMore: true,
    requestedLimit: 1,
    returned: 1,
  });
  expect(page.results[0]).toMatchObject({
    raceDate: "2026-08-27",
    raceStartAt: "2026-08-27T12:10:00+09:00",
    resultStatus: "finished",
    surface: "ダート",
    trackCondition: "良",
    trackConditionCode: "1",
    venue: "園田",
    venueCode: "50",
    weather: "晴",
  });
  expect(page.pagination.nextCursor).not.toBe(null);
});

it("maps abnormal, unknown, turf, sand, obstacle, and missing start-time results", async () => {
  const abnormal = normaliseRaceEntityHistoryRow({
    ...rawHistory("nar:20260827:50:04:07:2022103916", "202608271210"),
    abnormality_code: "2",
    finish_position: null,
    race_start_time: null,
    track_code: "17",
  });
  const unknown = normaliseRaceEntityHistoryRow({
    ...rawHistory("jra:20260820:05:01:01:2020100001", "202608201000"),
    finish_position: null,
    source: "jra",
    track_code: "27",
  });
  const obstacle = normaliseRaceEntityHistoryRow({
    ...rawHistory("jra:20260819:05:01:01:2020100001", "202608191000"),
    source: "jra",
    track_code: "51",
  });
  const page = await buildRaceEntityPage(
    { ...filters("horse"), limit: 3 },
    { ...target, raceStartTime: null },
    [abnormal, unknown, obstacle],
    CURSOR_SECRET,
  );
  expect(page.results.map((row) => row.resultStatus)).toStrictEqual([
    "abnormal:2",
    "unknown",
    "finished",
  ]);
  expect(page.results.map((row) => row.surface)).toStrictEqual(["芝", "サンド", "障害"]);
  expect(page.results[0]?.raceStartAt).toBe(null);
  expect(page.targetRace.raceStartAt).toBe(null);
  expect(page.pagination.nextCursor).toBe(null);
});

it("reduces returned rows when the serialized hard limit would be exceeded", async () => {
  const first = normaliseRaceEntityHistoryRow({
    ...rawHistory("nar:20260827:50:04:07:2022103916", "202608271210"),
    horse_name: "x".repeat(35_000),
  });
  const second = normaliseRaceEntityHistoryRow({
    ...rawHistory("nar:20260820:50:09:03:2021100001", "202608201500"),
    horse_name: "y".repeat(35_000),
  });
  const page = await buildRaceEntityPage(
    { ...filters("horse"), limit: 2 },
    target,
    [first, second],
    CURSOR_SECRET,
  );
  expect(page.pagination.effectiveLimit).toBe(1);
  expect(page.pagination.returned).toBe(1);
  expect(page.pagination.hasMore).toBe(true);
  expect(new TextEncoder().encode(JSON.stringify(page)).byteLength).toBeLessThan(65_536);
});

it("maps null, unknown, and invalid track codes to a null surface", async () => {
  const missing = normaliseRaceEntityHistoryRow({
    ...rawHistory("nar:20260827:50:04:07:2022103916", "202608271210"),
    race_start_time: "9999",
    track_code: null,
  });
  const unknown = normaliseRaceEntityHistoryRow({
    ...rawHistory("nar:20260820:50:09:03:2021100001", "202608201500"),
    track_code: "30",
  });
  const page = await buildRaceEntityPage(
    { ...filters("horse"), limit: 2 },
    target,
    [missing, unknown],
    CURSOR_SECRET,
  );
  expect(page.results.map((row) => row.surface)).toStrictEqual([null, null]);
  expect(page.results[0]?.raceStartAt).toBe(null);
});

it("rejects a single malformed oversized history row", async () => {
  const oversized = normaliseRaceEntityHistoryRow({
    ...rawHistory("nar:20260827:50:04:07:2022103916", "202608271210"),
    horse_name: "x".repeat(70_000),
  });
  await expect(
    buildRaceEntityPage(filters("horse"), target, [oversized], CURSOR_SECRET),
  ).rejects.toThrow("A single race entity history row exceeds the response hard limit");
});
