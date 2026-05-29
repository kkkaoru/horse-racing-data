// Run with: bun run --filter pc-keiba-viewer test
import { describe, expect, test } from "vitest";

import {
  buildPredictionMapFromRows,
  buildRaceKey,
  buildSelectSql,
  FinishPositionLocalLoader,
  loadFinishPositionLocalLoader,
} from "./finish-position-local-loader";

const SAMPLE_RAW_ROW = {
  source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0530",
  keibajo_code: "05",
  race_bango: "02",
  ketto_toroku_bango: "ABC123",
  predicted_score: 0.85,
  predicted_rank: 1,
  predicted_top1_prob: 0.34,
  predicted_top3_prob: 0.72,
  predicted_finish_position: 1,
  model_version: "jra-finish-position-lambdarank-v7-baseline",
  running_style_feature_version: "v1",
  finish_position_version: "v1",
};

const SAMPLE_RACE_KEY = {
  source: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0530",
  keibajoCode: "05",
  raceBango: "02",
  kettoTorokuBango: "ABC123",
};

const SAMPLE_KEY_JOINED = "jra|2026|0530|05|02|ABC123";

const SAMPLE_SELECT_SQL =
  "SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, predicted_score, predicted_rank, predicted_top1_prob, predicted_top3_prob, predicted_finish_position, model_version, running_style_feature_version, finish_position_version FROM read_parquet('/tmp/p.parquet', hive_partitioning = true)";

test("buildRaceKey joins six fields with pipe", () => {
  expect(buildRaceKey(SAMPLE_RACE_KEY)).toBe(SAMPLE_KEY_JOINED);
});

test("buildSelectSql emits the full Hive-partitioned read_parquet projection", () => {
  expect(buildSelectSql("/tmp/p.parquet")).toBe(SAMPLE_SELECT_SQL);
});

test("buildPredictionMapFromRows constructs Map of one entry", () => {
  const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
  expect(map.size).toBe(1);
});

test("buildPredictionMapFromRows preserves predicted_rank 1", () => {
  const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
  expect(map.get(SAMPLE_KEY_JOINED)?.predictedRank).toBe(1);
});

test("buildPredictionMapFromRows preserves predicted_score 0.85", () => {
  const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
  expect(map.get(SAMPLE_KEY_JOINED)?.predictedScore).toBe(0.85);
});

test("buildPredictionMapFromRows preserves finishPositionVersion v1", () => {
  const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
  expect(map.get(SAMPLE_KEY_JOINED)?.finishPositionVersion).toBe("v1");
});

test("buildPredictionMapFromRows preserves runningStyleFeatureVersion v1", () => {
  const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
  expect(map.get(SAMPLE_KEY_JOINED)?.runningStyleFeatureVersion).toBe("v1");
});

test("buildPredictionMapFromRows preserves model_version", () => {
  const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
  expect(map.get(SAMPLE_KEY_JOINED)?.modelVersion).toBe(
    "jra-finish-position-lambdarank-v7-baseline",
  );
});

test("buildPredictionMapFromRows raises when string column is missing string type", () => {
  const bad = { ...SAMPLE_RAW_ROW, model_version: 42 };
  expect(() => buildPredictionMapFromRows([bad])).toThrowError(
    "Column model_version is not a string.",
  );
});

test("buildPredictionMapFromRows raises when numeric column is non-finite", () => {
  const bad = { ...SAMPLE_RAW_ROW, predicted_score: "not-a-number" };
  expect(() => buildPredictionMapFromRows([bad])).toThrowError(
    "Column predicted_score is not numeric.",
  );
});

test("buildPredictionMapFromRows tolerates null numeric column as 0", () => {
  const tolerant = { ...SAMPLE_RAW_ROW, predicted_top1_prob: null };
  const map = buildPredictionMapFromRows([tolerant]);
  expect(map.get(SAMPLE_KEY_JOINED)?.predictedTop1Prob).toBe(0);
});

test("buildPredictionMapFromRows raises when finish_position_version missing", () => {
  const bad = { ...SAMPLE_RAW_ROW, finish_position_version: 0 };
  expect(() => buildPredictionMapFromRows([bad])).toThrowError(
    "Column finish_position_version is not a string.",
  );
});

test("FinishPositionLocalLoader.get returns prediction when key present", () => {
  const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
  const loader = new FinishPositionLocalLoader(map);
  expect(loader.get(SAMPLE_RACE_KEY)?.predictedRank).toBe(1);
});

test("FinishPositionLocalLoader.get returns null when key absent", () => {
  const loader = new FinishPositionLocalLoader(new Map());
  expect(loader.get(SAMPLE_RACE_KEY)).toBe(null);
});

describe("loadFinishPositionLocalLoader", () => {
  test("populates loader from DuckDB rows", () => {
    const fakeConnection = {
      query: () => ({ toArray: () => [SAMPLE_RAW_ROW] }),
      close: () => undefined,
    };
    const fakeDatabase = {
      connect: () => fakeConnection,
      close: () => undefined,
    };
    class FakeDatabase {
      connect() {
        return fakeDatabase.connect();
      }
      close() {
        return fakeDatabase.close();
      }
    }
    const loader = loadFinishPositionLocalLoader({
      parquetGlob: "/tmp/p.parquet",
      duckdbModule: { Database: FakeDatabase },
    });
    expect(loader.get(SAMPLE_RACE_KEY)?.finishPositionVersion).toBe("v1");
  });
});

const PRODUCTION_RANKED_ROWS = [
  { ...SAMPLE_RAW_ROW, ketto_toroku_bango: "H1", predicted_rank: 1, predicted_score: 0.95 },
  { ...SAMPLE_RAW_ROW, ketto_toroku_bango: "H2", predicted_rank: 2, predicted_score: 0.72 },
  { ...SAMPLE_RAW_ROW, ketto_toroku_bango: "H3", predicted_rank: 3, predicted_score: 0.58 },
];

const LOCAL_RANKED_ROWS = [
  { ...SAMPLE_RAW_ROW, ketto_toroku_bango: "H1", predicted_rank: 1, predicted_score: 0.93 },
  { ...SAMPLE_RAW_ROW, ketto_toroku_bango: "H2", predicted_rank: 2, predicted_score: 0.71 },
  { ...SAMPLE_RAW_ROW, ketto_toroku_bango: "H3", predicted_rank: 3, predicted_score: 0.55 },
];

const buildRaceKeyForHorse = (horseId: string) => ({
  ...SAMPLE_RACE_KEY,
  kettoTorokuBango: horseId,
});

test("production-equivalence: local loader top1 matches production top1 for identical features", () => {
  const productionMap = buildPredictionMapFromRows(PRODUCTION_RANKED_ROWS);
  const localMap = buildPredictionMapFromRows(LOCAL_RANKED_ROWS);
  const productionLoader = new FinishPositionLocalLoader(productionMap);
  const localLoader = new FinishPositionLocalLoader(localMap);
  expect(productionLoader.get(buildRaceKeyForHorse("H1"))?.predictedRank).toBe(1);
  expect(localLoader.get(buildRaceKeyForHorse("H1"))?.predictedRank).toBe(1);
});

test("production-equivalence: local loader preserves full top3 ordering for identical features", () => {
  const localMap = buildPredictionMapFromRows(LOCAL_RANKED_ROWS);
  const localLoader = new FinishPositionLocalLoader(localMap);
  expect(localLoader.get(buildRaceKeyForHorse("H1"))?.predictedRank).toBe(1);
  expect(localLoader.get(buildRaceKeyForHorse("H2"))?.predictedRank).toBe(2);
  expect(localLoader.get(buildRaceKeyForHorse("H3"))?.predictedRank).toBe(3);
});
