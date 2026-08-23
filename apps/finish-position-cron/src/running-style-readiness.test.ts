// Run with bun. Tests for per-race running-style readiness.

import { expect, test, vi } from "vitest";
import type { RaceEntry } from "./cron-decision";
import { getRunningStyleRaceReadiness } from "./running-style-readiness";

interface ReadinessTestRow {
  entrant_count: number | null;
  expected_horse_count: number | null;
  features_r2_key: string | null;
  prediction_count: number | null;
  running_key: string;
  status: string | null;
  written_horse_count: number | null;
}

const JRA_RACE: RaceEntry = { category: "jra", keibajoCode: "01", raceBango: "01" };
const SECOND_JRA_RACE: RaceEntry = { category: "jra", keibajoCode: "01", raceBango: "02" };
const BAN_EI_RACE: RaceEntry = { category: "ban-ei", keibajoCode: "83", raceBango: "01" };

test("returns no rows without querying D1 for an empty race list", async () => {
  const prepare = vi.fn();
  await expect(
    getRunningStyleRaceReadiness({
      category: "jra",
      db: { prepare } as unknown as D1Database,
      races: [],
      runYmd: "20260823",
    }),
  ).resolves.toStrictEqual([]);
  expect(prepare).not.toHaveBeenCalled();
});

test("marks only a completed feature and prediction set covering every active entrant ready", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        entrant_count: 12,
        expected_horse_count: 12,
        features_r2_key: "running-style/jra/20260823/01/01/features.parquet",
        prediction_count: 12,
        running_key: "jra:20260823:01:01",
        status: "completed",
        written_horse_count: 12,
      },
      {
        entrant_count: 14,
        expected_horse_count: 14,
        features_r2_key: "running-style/jra/20260823/01/02/features.parquet",
        prediction_count: 7,
        running_key: "jra:20260823:01:02",
        status: "completed",
        written_horse_count: 14,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn((_sql: string) => ({ bind }));

  await expect(
    getRunningStyleRaceReadiness({
      category: "jra",
      db: { prepare } as unknown as D1Database,
      races: [JRA_RACE, SECOND_JRA_RACE],
      runYmd: "20260823",
    }),
  ).resolves.toStrictEqual([
    { race: JRA_RACE, reason: null },
    { race: SECOND_JRA_RACE, reason: "prediction-count-7-of-14" },
  ]);
  expect(bind).toHaveBeenCalledWith(
    "jra:20260823:01:01",
    "jra:2026:0823:01:01",
    "jra:20260823:01:02",
    "jra:2026:0823:01:02",
  );
  const sql = String(prepare.mock.calls[0]?.[0]);
  expect(sql).toMatch("latest_entries");
  expect(sql).toMatch("join daily_race_entries daily");
  expect(sql).toMatch(
    "active.horse_number is null\n         or styles.horse_number = active.horse_number",
  );
});

test("maps ban-ei to the NAR running-style and realtime source keys", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        entrant_count: 10,
        expected_horse_count: 10,
        features_r2_key: "running-style/ban-ei/20260823/83/01/features.parquet",
        prediction_count: 10,
        running_key: "nar:20260823:83:01",
        status: "completed",
        written_horse_count: 10,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));

  await expect(
    getRunningStyleRaceReadiness({
      category: "ban-ei",
      db: { prepare } as unknown as D1Database,
      races: [BAN_EI_RACE],
      runYmd: "20260823",
    }),
  ).resolves.toStrictEqual([{ race: BAN_EI_RACE, reason: null }]);
  expect(bind).toHaveBeenCalledWith("nar:20260823:83:01", "nar:2026:0823:83:01");
});

test("uses the completed Catalog inference expected count when realtime entrant mirrors are empty", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        entrant_count: 0,
        expected_horse_count: 12,
        features_r2_key: "running-style/jra/20260823/01/01/features.parquet",
        prediction_count: 12,
        running_key: "jra:20260823:01:01",
        status: "completed",
        written_horse_count: 12,
      },
      {
        entrant_count: 0,
        expected_horse_count: 14,
        features_r2_key: "running-style/jra/20260823/01/02/features.parquet",
        prediction_count: 7,
        running_key: "jra:20260823:01:02",
        status: "completed",
        written_horse_count: 14,
      },
    ],
  }));
  const db = {
    prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all })) })),
  } as unknown as D1Database;

  await expect(
    getRunningStyleRaceReadiness({
      category: "jra",
      db,
      races: [JRA_RACE, SECOND_JRA_RACE],
      runYmd: "20260823",
    }),
  ).resolves.toStrictEqual([
    { race: JRA_RACE, reason: null },
    { race: SECOND_JRA_RACE, reason: "prediction-count-7-of-14" },
  ]);
});

test("fails closed with explicit reasons for every incomplete prerequisite", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        entrant_count: 0,
        expected_horse_count: null,
        features_r2_key: null,
        prediction_count: 0,
        running_key: "jra:20260823:01:01",
        status: null,
        written_horse_count: null,
      },
    ],
  }));
  const db = {
    prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all })) })),
  } as unknown as D1Database;

  await expect(
    getRunningStyleRaceReadiness({
      category: "jra",
      db,
      races: [JRA_RACE, SECOND_JRA_RACE],
      runYmd: "20260823",
    }),
  ).resolves.toStrictEqual([
    { race: JRA_RACE, reason: "entrants-missing" },
    { race: SECOND_JRA_RACE, reason: "state-missing" },
  ]);
});

test("fails closed for state, feature, expected feature count, and written count gaps", async () => {
  const base: ReadinessTestRow = {
    entrant_count: 12,
    expected_horse_count: 12,
    features_r2_key: "features.parquet",
    prediction_count: 12,
    running_key: "jra:20260823:01:01",
    status: "processing",
    written_horse_count: 12,
  };
  const statuses: ReadinessTestRow[] = [base];
  const all = vi.fn(async () => ({ results: statuses }));
  const db = {
    prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all })) })),
  } as unknown as D1Database;
  const run = () =>
    getRunningStyleRaceReadiness({ category: "jra", db, races: [JRA_RACE], runYmd: "20260823" });

  await expect(run()).resolves.toStrictEqual([{ race: JRA_RACE, reason: "status-processing" }]);
  statuses[0] = { ...base, entrant_count: null, expected_horse_count: 0 };
  await expect(run()).resolves.toStrictEqual([{ race: JRA_RACE, reason: "entrants-missing" }]);
  statuses[0] = { ...base, status: null };
  await expect(run()).resolves.toStrictEqual([{ race: JRA_RACE, reason: "status-missing" }]);
  statuses[0] = { ...base, features_r2_key: null, status: "completed" };
  await expect(run()).resolves.toStrictEqual([
    { race: JRA_RACE, reason: "feature-artifact-missing" },
  ]);
  statuses[0] = { ...base, expected_horse_count: 6, status: "completed" };
  await expect(run()).resolves.toStrictEqual([{ race: JRA_RACE, reason: "feature-count-6-of-12" }]);
  statuses[0] = { ...base, status: "completed", written_horse_count: 5 };
  await expect(run()).resolves.toStrictEqual([{ race: JRA_RACE, reason: "written-count-5-of-12" }]);
  statuses[0] = { ...base, prediction_count: null, status: "completed" };
  await expect(run()).resolves.toStrictEqual([
    { race: JRA_RACE, reason: "prediction-count-0-of-12" },
  ]);
});
