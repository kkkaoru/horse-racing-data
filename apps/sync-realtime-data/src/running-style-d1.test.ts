// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type {
  RaceRunningStyleRow,
  RunningStyleInferenceRace,
  RunningStylePendingRace,
} from "./running-style-d1";

vi.mock("./d1-query-cache", () => ({
  withD1QueryCache: vi.fn(),
}));

const ROW: RaceRunningStyleRow = {
  bamei: "サンプル",
  category: "jra",
  horseNumber: 1,
  kaisaiNen: "2026",
  kettoTorokuBango: "2024100001",
  modelVersion: "v7-lineage",
  pNige: 0.1,
  pOikomi: 0.2,
  pSashi: 0.3,
  pSenkou: 0.4,
  predictedAt: "2026-05-12T11:30:00+09:00",
  predictedLabel: "senkou",
  raceKey: "jra:20260512:08:01",
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("upsertRaceRunningStyles returns 0 and skips batch when rows empty", async () => {
  const { upsertRaceRunningStyles } = await import("./running-style-d1");
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  expect(await upsertRaceRunningStyles(db, [])).toBe(0);
  expect(batch).not.toHaveBeenCalled();
});

it("upsertRaceRunningStyles batches statements and returns row count", async () => {
  const { upsertRaceRunningStyles } = await import("./running-style-d1");
  const bind = vi.fn(() => ({ bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  const count = await upsertRaceRunningStyles(db, [ROW, ROW]);
  expect(count).toBe(2);
  expect(prepare).toHaveBeenCalledTimes(2);
});

it("listRaceRunningStyleCounts returns empty map when raceKeys is empty", async () => {
  const { listRaceRunningStyleCounts } = await import("./running-style-d1");
  const db = {} as unknown as D1Database;
  const result = await listRaceRunningStyleCounts(db, []);
  expect(result.size).toBe(0);
});

it("listRaceRunningStyleCounts dedupes raceKeys and skips empty strings", async () => {
  const { listRaceRunningStyleCounts } = await import("./running-style-d1");
  const { withD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(withD1QueryCache).mockResolvedValue({});
  const db = {} as unknown as D1Database;
  await listRaceRunningStyleCounts(db, ["k", "k", "", "k2"]);
  expect(withD1QueryCache).toHaveBeenCalledTimes(1);
  expect(vi.mocked(withD1QueryCache).mock.calls[0]![1]).toStrictEqual([
    "listRaceRunningStyleCounts",
    ["k", "k2"],
  ]);
});

it("listRaceRunningStyleCounts bypasses cache when bypassCache=true", async () => {
  const { listRaceRunningStyleCounts } = await import("./running-style-d1");
  const { withD1QueryCache } = await import("./d1-query-cache");
  const all = vi.fn(async () => ({
    results: [{ count: 3, race_key: "k1" }],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const counts = await listRaceRunningStyleCounts(db, ["k1"], { bypassCache: true });
  expect(counts.get("k1")).toBe(3);
  expect(withD1QueryCache).not.toHaveBeenCalled();
});

it("listRaceRunningStyleCounts accepts an ExecutionContext as the third arg", async () => {
  const { listRaceRunningStyleCounts } = await import("./running-style-d1");
  const { withD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(withD1QueryCache).mockResolvedValue({ k1: 5 });
  const db = {} as unknown as D1Database;
  const ctx = { waitUntil: vi.fn() } as unknown as ExecutionContext;
  const counts = await listRaceRunningStyleCounts(db, ["k1"], ctx);
  expect(counts.get("k1")).toBe(5);
  expect(vi.mocked(withD1QueryCache).mock.calls[0]![3]).toStrictEqual({ ctx, raceDay: undefined });
});

it("listRaceRunningStylesForRace bypasses cache when bypassCache=true", async () => {
  const { listRaceRunningStylesForRace } = await import("./running-style-d1");
  const { withD1QueryCache } = await import("./d1-query-cache");
  const all = vi.fn(async () => ({
    results: [
      {
        bamei: "サンプル",
        category: "jra",
        horse_number: 1,
        kaisai_nen: "2026",
        ketto_toroku_bango: "2024100001",
        model_version: "v7-lineage",
        p_nige: 0.1,
        p_oikomi: 0.2,
        p_sashi: 0.3,
        p_senkou: 0.4,
        predicted_at: "2026-05-12T11:30:00+09:00",
        predicted_label: "senkou",
        race_key: "jra:20260512:08:01",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const rows = await listRaceRunningStylesForRace(db, "jra:20260512:08:01", { bypassCache: true });
  expect(rows).toStrictEqual([ROW]);
  expect(withD1QueryCache).not.toHaveBeenCalled();
});

it("listRaceRunningStylesForRace returns cached value via withD1QueryCache", async () => {
  const { listRaceRunningStylesForRace } = await import("./running-style-d1");
  const { withD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(withD1QueryCache).mockResolvedValue([ROW]);
  const db = {} as unknown as D1Database;
  const rows = await listRaceRunningStylesForRace(db, "jra:20260512:08:01");
  expect(rows).toStrictEqual([ROW]);
});

it("listRunningStyleInferenceStates returns mapped rows by race_key", async () => {
  const { listRunningStyleInferenceStates } = await import("./running-style-d1");
  const all = vi.fn(async () => ({
    results: [
      {
        attempted_at: "2026-05-12T11:00:00+09:00",
        completed_at: null,
        expected_horse_count: 5,
        features_r2_key: "k.parquet",
        model_version: "v7-lineage",
        race_key: "jra:20260512:08:01",
        status: "processing",
        written_horse_count: null,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const states = await listRunningStyleInferenceStates(db, ["jra:20260512:08:01"]);
  expect(states.get("jra:20260512:08:01")).toStrictEqual({
    attemptedAt: "2026-05-12T11:00:00+09:00",
    completedAt: null,
    expectedHorseCount: 5,
    featuresR2Key: "k.parquet",
    modelVersion: "v7-lineage",
    raceKey: "jra:20260512:08:01",
    status: "processing",
    writtenHorseCount: null,
  });
});

it("getRunningStyleInferenceState returns null when no row", async () => {
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const first = vi.fn(async () => null);
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  expect(await getRunningStyleInferenceState(db, "jra:20260512:08:01")).toBeNull();
});

it("getRunningStyleInferenceState maps row columns to camelCase fields", async () => {
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const first = vi.fn(async () => ({
    attempted_at: "2026-05-12T11:00:00+09:00",
    completed_at: "2026-05-12T11:31:00+09:00",
    expected_horse_count: 5,
    features_r2_key: "k.parquet",
    model_version: "v7-lineage",
    race_key: "jra:20260512:08:01",
    status: "completed",
    written_horse_count: 5,
  }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const state = await getRunningStyleInferenceState(db, "jra:20260512:08:01");
  expect(state).toStrictEqual({
    attemptedAt: "2026-05-12T11:00:00+09:00",
    completedAt: "2026-05-12T11:31:00+09:00",
    expectedHorseCount: 5,
    featuresR2Key: "k.parquet",
    modelVersion: "v7-lineage",
    raceKey: "jra:20260512:08:01",
    status: "completed",
    writtenHorseCount: 5,
  });
});

it("getRunningStyleInferenceState maps null expected/written horse counts to null", async () => {
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const first = vi.fn(async () => ({
    attempted_at: null,
    completed_at: null,
    expected_horse_count: null,
    features_r2_key: null,
    model_version: null,
    race_key: "jra:20260512:08:02",
    status: "pending",
    written_horse_count: null,
  }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const state = await getRunningStyleInferenceState(db, "jra:20260512:08:02");
  expect(state?.expectedHorseCount).toBe(null);
  expect(state?.writtenHorseCount).toBe(null);
});

it("listRunningStyleInferenceStates maps non-null written_horse_count to a number", async () => {
  const { listRunningStyleInferenceStates } = await import("./running-style-d1");
  const all = vi.fn(async () => ({
    results: [
      {
        attempted_at: "2026-05-12T11:00:00+09:00",
        completed_at: "2026-05-12T11:31:00+09:00",
        expected_horse_count: null,
        features_r2_key: "k.parquet",
        model_version: "v7-lineage",
        race_key: "jra:20260512:08:03",
        status: "completed",
        written_horse_count: 7,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const states = await listRunningStyleInferenceStates(db, ["jra:20260512:08:03"]);
  expect(states.get("jra:20260512:08:03")?.expectedHorseCount).toBe(null);
  expect(states.get("jra:20260512:08:03")?.writtenHorseCount).toBe(7);
});

it("upsertRunningStylePendingStates short-circuits when rows empty", async () => {
  const { upsertRunningStylePendingStates } = await import("./running-style-d1");
  const batch = vi.fn(async () => []);
  const db = { batch, prepare: vi.fn() } as unknown as D1Database;
  await upsertRunningStylePendingStates(db, [], "now");
  expect(batch).not.toHaveBeenCalled();
});

it("upsertRunningStylePendingStates batches by D1_BATCH_SIZE chunks", async () => {
  const { upsertRunningStylePendingStates } = await import("./running-style-d1");
  const bind = vi.fn(() => ({ bind: vi.fn() }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  const pendingRow: RunningStylePendingRace = {
    expectedHorseCount: 8,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    raceKey: "jra:20260512:08:01",
    source: "jra",
  };
  await upsertRunningStylePendingStates(db, [pendingRow], "2026-05-12T11:00:00+09:00");
  expect(prepare).toHaveBeenCalledTimes(1);
});

it("markRunningStyleInferenceProcessing binds inputs and runs the insert", async () => {
  const { markRunningStyleInferenceProcessing } = await import("./running-style-d1");
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const race: RunningStyleInferenceRace = {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    raceKey: "jra:20260512:08:01",
    source: "jra",
  };
  await markRunningStyleInferenceProcessing(db, race, "2026-05-12T11:00:00+09:00");
  expect(bind.mock.calls[0]).toStrictEqual([
    "jra:20260512:08:01",
    "jra",
    "2026",
    "0512",
    "08",
    "01",
    "2026-05-12T11:00:00+09:00",
  ]);
});

it("markRunningStyleInferenceCompleted binds completion params and runs the update", async () => {
  const { markRunningStyleInferenceCompleted } = await import("./running-style-d1");
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await markRunningStyleInferenceCompleted(db, {
    completedAt: "2026-05-12T11:31:00+09:00",
    expectedHorseCount: 5,
    featuresR2Key: "k.parquet",
    modelVersion: "v7-lineage",
    raceKey: "jra:20260512:08:01",
    writtenHorseCount: 5,
  });
  expect(bind.mock.calls[0]).toStrictEqual([
    "k.parquet",
    "v7-lineage",
    5,
    5,
    "2026-05-12T11:31:00+09:00",
    "jra:20260512:08:01",
  ]);
});

it("markRunningStyleInferenceFailed binds error message from an Error instance", async () => {
  const { markRunningStyleInferenceFailed } = await import("./running-style-d1");
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await markRunningStyleInferenceFailed(db, "jra:20260512:08:01", new Error("boom"));
  expect(bind.mock.calls[0]).toStrictEqual(["boom", "jra:20260512:08:01"]);
});

it("markRunningStyleInferenceFailed stringifies non-Error values", async () => {
  const { markRunningStyleInferenceFailed } = await import("./running-style-d1");
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await markRunningStyleInferenceFailed(db, "jra:20260512:08:01", "x");
  expect(bind.mock.calls[0]).toStrictEqual(["x", "jra:20260512:08:01"]);
});
