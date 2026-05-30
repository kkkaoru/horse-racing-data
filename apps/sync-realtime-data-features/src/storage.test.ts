// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import {
  getFinishPositionInferenceState,
  getFinishPositionPredictions,
  getRunningStyleInferenceState,
  listRaceRunningStyles,
  upsertFinishPositionInferenceState,
  upsertFinishPositionPredictions,
  upsertRunningStyle,
  upsertRunningStyleInferenceState,
} from "./storage";
import type { RunningStyleRow } from "./types";

const buildPrepared = (response: unknown) => {
  const run = vi.fn().mockResolvedValue({});
  const all = vi.fn().mockResolvedValue(response);
  const first = vi.fn().mockResolvedValue(response);
  // `bind` accepts variadic args at runtime; typing the param list as
  // `unknown[]` lets the tests inspect `mock.calls[i][j]` by index without
  // hitting the `Tuple of length 0` error from a zero-arg `vi.fn`.
  const bind = vi.fn((..._args: unknown[]) => ({ run, all, first }));
  const prepare = vi.fn(() => ({ bind }));
  return { all, bind, first, prepare, run };
};

it("upserts running-style row with bound params", async () => {
  const stub = buildPrepared({ results: [] });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await upsertRunningStyle(db, {
    raceKey: "nar:20260529:30:08",
    horseNumber: 1,
    kettoTorokuBango: "kt-1",
    bamei: "horse",
    category: "nar",
    kaisaiNen: "2026",
    modelVersion: "v1",
    pNige: 0.1,
    pSenkou: 0.2,
    pSashi: 0.3,
    pOikomi: 0.4,
    predictedLabel: "senkou",
    predictedAt: "2026-05-29T00:00:00Z",
  });
  expect(stub.run).toHaveBeenCalled();
});

it("upsertRunningStyle coerces undefined bamei to null at bind site", async () => {
  const stub = buildPrepared({ results: [] });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  const row: RunningStyleRow = {
    raceKey: "nar:20260529:30:08",
    horseNumber: 1,
    kettoTorokuBango: "kt-1",
    bamei: "horse",
    category: "nar",
    kaisaiNen: "2026",
    modelVersion: "v1",
    pNige: 0.1,
    pSenkou: 0.2,
    pSashi: 0.3,
    pOikomi: 0.4,
    predictedLabel: "senkou",
    predictedAt: "2026-05-29T00:00:00Z",
  };
  Reflect.set(row, "bamei", undefined);
  await upsertRunningStyle(db, row);
  expect(stub.bind).toHaveBeenCalledTimes(1);
  expect(stub.bind.mock.calls[0]![3]).toBeNull();
});

it("upsertRunningStyle binds null when ketto_toroku_bango is undefined", async () => {
  const stub = buildPrepared({ results: [] });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  const row: RunningStyleRow = {
    raceKey: "nar:20260529:30:08",
    horseNumber: 1,
    kettoTorokuBango: "kt-1",
    bamei: "horse",
    category: "nar",
    kaisaiNen: "2026",
    modelVersion: "v1",
    pNige: 0.1,
    pSenkou: 0.2,
    pSashi: 0.3,
    pOikomi: 0.4,
    predictedLabel: "senkou",
    predictedAt: "2026-05-29T00:00:00Z",
  };
  Reflect.set(row, "kettoTorokuBango", undefined);
  await upsertRunningStyle(db, row);
  expect(stub.bind.mock.calls[0]![2]).toBeNull();
});

it("lists running-style rows mapping snake_case to camelCase", async () => {
  const stub = buildPrepared({
    results: [
      {
        race_key: "r",
        horse_number: 2,
        ketto_toroku_bango: "kt",
        bamei: null,
        category: "jra",
        kaisai_nen: "2026",
        model_version: "v",
        p_nige: 0.1,
        p_senkou: 0.2,
        p_sashi: 0.3,
        p_oikomi: 0.4,
        predicted_label: "nige",
        predicted_at: "t",
      },
    ],
  });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  const rows = await listRaceRunningStyles(db, "r");
  expect(rows).toStrictEqual([
    {
      raceKey: "r",
      horseNumber: 2,
      kettoTorokuBango: "kt",
      bamei: null,
      category: "jra",
      kaisaiNen: "2026",
      modelVersion: "v",
      pNige: 0.1,
      pSenkou: 0.2,
      pSashi: 0.3,
      pOikomi: 0.4,
      predictedLabel: "nige",
      predictedAt: "t",
    },
  ]);
});

it("upserts finish-position predictions", async () => {
  const stub = buildPrepared({ results: [] });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await upsertFinishPositionPredictions(db, {
    raceKey: "r",
    source: "nar",
    predictionsJson: "[]",
    predictedAt: "t",
    predictorVersion: "v1",
  });
  expect(stub.run).toHaveBeenCalled();
});

it("returns null when finish-position predictions miss", async () => {
  const stub = buildPrepared(null);
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await expect(getFinishPositionPredictions(db, "r")).resolves.toBeNull();
});

it("returns mapped predictions when present", async () => {
  const stub = buildPrepared({
    race_key: "r",
    source: "nar",
    predictions_json: "[]",
    predicted_at: "t",
    predictor_version: "v1",
  });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await expect(getFinishPositionPredictions(db, "r")).resolves.toStrictEqual({
    raceKey: "r",
    source: "nar",
    predictionsJson: "[]",
    predictedAt: "t",
    predictorVersion: "v1",
  });
});

it("upserts running-style inference state", async () => {
  const stub = buildPrepared({});
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await upsertRunningStyleInferenceState(db, {
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    status: "completed",
    featuresR2Key: "k",
    modelVersion: "v",
    expectedHorseCount: 10,
    writtenHorseCount: 10,
    attemptedAt: "t",
    completedAt: "t",
    errorMessage: null,
  });
  expect(stub.run).toHaveBeenCalled();
});

it("returns null when running-style state miss", async () => {
  const stub = buildPrepared(null);
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await expect(getRunningStyleInferenceState(db, "r")).resolves.toBeNull();
});

it("returns mapped running-style state when present", async () => {
  const stub = buildPrepared({
    race_key: "r",
    source: "nar",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0529",
    keibajo_code: "30",
    race_bango: "08",
    status: "completed",
    features_r2_key: "k",
    model_version: "v",
    expected_horse_count: 10,
    written_horse_count: 10,
    attempted_at: "t",
    completed_at: "t",
    error_message: null,
  });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await expect(getRunningStyleInferenceState(db, "r")).resolves.toStrictEqual({
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    status: "completed",
    featuresR2Key: "k",
    modelVersion: "v",
    expectedHorseCount: 10,
    writtenHorseCount: 10,
    attemptedAt: "t",
    completedAt: "t",
    errorMessage: null,
  });
});

it("upserts finish-position inference state", async () => {
  const stub = buildPrepared({});
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await upsertFinishPositionInferenceState(db, {
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    status: "completed",
    predictionsR2Key: null,
    modelVersion: "v",
    attemptedAt: "t",
    completedAt: "t",
    errorMessage: null,
  });
  expect(stub.run).toHaveBeenCalled();
});

it("returns null when finish-position state miss", async () => {
  const stub = buildPrepared(null);
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await expect(getFinishPositionInferenceState(db, "r")).resolves.toBeNull();
});

it("returns mapped finish-position state when present", async () => {
  const stub = buildPrepared({
    race_key: "r",
    source: "nar",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0529",
    keibajo_code: "30",
    race_bango: "08",
    status: "completed",
    predictions_r2_key: "k",
    model_version: "v",
    attempted_at: "t",
    completed_at: "t",
    error_message: null,
  });
  const db = { prepare: stub.prepare } as unknown as D1Database;
  await expect(getFinishPositionInferenceState(db, "r")).resolves.toStrictEqual({
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    status: "completed",
    predictionsR2Key: "k",
    modelVersion: "v",
    attemptedAt: "t",
    completedAt: "t",
    errorMessage: null,
  });
});
