// Run with: bun run --filter pc-keiba-viewer test
import { describe, expect, test } from "vitest";

import {
  buildPredictionMapFromRows,
  buildRaceKey,
  buildSelectSql,
  loadRunningStyleLocalLoader,
  RunningStyleLocalLoader,
} from "./running-style-local-loader";

const SAMPLE_RAW_ROW = {
  source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0530",
  keibajo_code: "05",
  race_bango: "02",
  ketto_toroku_bango: "ABC123",
  predicted_label: "senkou",
  p_nige: 0.1,
  p_senkou: 0.6,
  p_sashi: 0.2,
  p_oikomi: 0.1,
  running_style_feature_version: "v1",
};

const SAMPLE_RACE_KEY = {
  source: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0530",
  keibajoCode: "05",
  raceBango: "02",
  kettoTorokuBango: "ABC123",
};

describe("running-style-local-loader", () => {
  test("buildRaceKey joins six fields with pipe", () => {
    expect(buildRaceKey(SAMPLE_RACE_KEY)).toBe("jra|2026|0530|05|02|ABC123");
  });

  test("buildSelectSql includes predicted_label column", () => {
    const sql = buildSelectSql("/tmp/p.parquet");
    expect(sql.includes("predicted_label") satisfies boolean).toBe(true);
  });

  test("buildSelectSql includes hive_partitioning true", () => {
    const sql = buildSelectSql("/tmp/p.parquet");
    expect(sql.includes("hive_partitioning = true") satisfies boolean).toBe(true);
  });

  test("buildSelectSql interpolates parquet glob path", () => {
    const sql = buildSelectSql("/abc/def.parquet");
    expect(sql.includes("'/abc/def.parquet'") satisfies boolean).toBe(true);
  });

  test("buildPredictionMapFromRows constructs Map of one entry", () => {
    const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
    expect(map.size).toBe(1);
  });

  test("buildPredictionMapFromRows maps key to prediction with predicted_label senkou", () => {
    const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
    expect(map.get("jra|2026|0530|05|02|ABC123")?.predictedLabel).toBe("senkou");
  });

  test("buildPredictionMapFromRows raises when column is missing string type", () => {
    const bad = { ...SAMPLE_RAW_ROW, predicted_label: 42 };
    expect(() => buildPredictionMapFromRows([bad])).toThrowError(
      "Column predicted_label is not a string.",
    );
  });

  test("buildPredictionMapFromRows raises when probability is non-numeric", () => {
    const bad = { ...SAMPLE_RAW_ROW, p_nige: "not-a-number" };
    expect(() => buildPredictionMapFromRows([bad])).toThrowError("Column p_nige is not numeric.");
  });

  test("RunningStyleLocalLoader.get returns prediction when key present", () => {
    const map = buildPredictionMapFromRows([SAMPLE_RAW_ROW]);
    const loader = new RunningStyleLocalLoader(map);
    expect(loader.get(SAMPLE_RACE_KEY)?.predictedLabel).toBe("senkou");
  });

  test("RunningStyleLocalLoader.get returns null when key absent", () => {
    const loader = new RunningStyleLocalLoader(new Map());
    expect(loader.get(SAMPLE_RACE_KEY)).toBe(null);
  });

  test("loadRunningStyleLocalLoader returns loader populated from DuckDB rows", () => {
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
    const loader = loadRunningStyleLocalLoader({
      parquetGlob: "/tmp/p.parquet",
      duckdbModule: { Database: FakeDatabase },
    });
    expect(loader.get(SAMPLE_RACE_KEY)?.featureVersion).toBe("v1");
  });
});
