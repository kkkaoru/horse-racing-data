// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";

import * as runningStyleNeon from "../src/running-style-neon";
import {
  ensureRunningStylePredictionNeonSchema,
  mapD1RowsToNeonRows,
  upsertNeonRows,
  upsertRunningStylePredictionsToNeon,
} from "./sync-running-style-d1-to-neon";

afterEach(() => {
  vi.restoreAllMocks();
});

it("ensureRunningStylePredictionNeonSchema is the src writable-client helper", () => {
  expect(ensureRunningStylePredictionNeonSchema).toBe(
    runningStyleNeon.ensureRunningStylePredictionNeonSchema,
  );
});

it("upsertRunningStylePredictionsToNeon is the src writable-client helper", () => {
  expect(upsertRunningStylePredictionsToNeon).toBe(
    runningStyleNeon.upsertRunningStylePredictionsToNeon,
  );
});

it("upsertNeonRows calls src upsert without local pool.query DML", async () => {
  const upsert = vi
    .spyOn(runningStyleNeon, "upsertRunningStylePredictionsToNeon")
    .mockResolvedValue(1);
  const query = vi.fn();
  const pool = { query };
  const rows = mapD1RowsToNeonRows([
    {
      cell_model_key: "running-style/models/jra/cells/tokyo-turf.flatbin",
      cell_variant_id: "tokyo-turf",
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101234",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.2,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: 0.9,
      predicted_corner_rank: 3,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
  ]);
  const count = await upsertNeonRows(pool as never, rows);
  expect(count).toBe(1);
  expect(upsert).toHaveBeenCalledTimes(1);
  expect(upsert.mock.calls[0]?.[0]).toBe(pool);
  expect(upsert.mock.calls[0]?.[1]).toStrictEqual([
    {
      bamei: null,
      category: "jra",
      cellModelKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
      cellVariantId: "tokyo-turf",
      horseNumber: 1,
      kaisaiNen: "2026",
      kettoTorokuBango: "2022101234",
      modelVersion: "v7",
      pNige: 0.5,
      pOikomi: 0.1,
      pSashi: 0.2,
      pSenkou: 0.2,
      predictedAt: "2026-06-19T00:00:00.000Z",
      predictedCornerFrontScore: 0.9,
      predictedCornerRank: 3,
      predictedLabel: "nige",
      raceKey: "jra:20260619:08:01",
    },
  ]);
  expect(query).not.toHaveBeenCalled();
});

it("upsertNeonRows drops invalid predicted labels before src upsert", async () => {
  const upsert = vi
    .spyOn(runningStyleNeon, "upsertRunningStylePredictionsToNeon")
    .mockResolvedValue(0);
  const query = vi.fn();
  const rows = mapD1RowsToNeonRows([
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101234",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.2,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: 0.9,
      predicted_corner_rank: 1,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
  ]);
  const count = await upsertNeonRows({ query } as never, [
    { ...rows[0]!, predicted_label: "unknown" },
  ]);
  expect(count).toBe(0);
  expect(upsert).toHaveBeenCalledTimes(1);
  expect(upsert.mock.calls[0]?.[1]).toStrictEqual([]);
  expect(query).not.toHaveBeenCalled();
});

it("mapD1RowsToNeonRows preserves stored predicted corner score and rank", () => {
  const rows = mapD1RowsToNeonRows([
    {
      cell_model_key: "running-style/models/jra/cells/tokyo-turf.flatbin",
      cell_variant_id: "tokyo-turf",
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101234",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.2,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: 0.9,
      predicted_corner_rank: 3,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
  ]);
  expect(rows[0]?.predicted_corner_front_score).toBe(0.9);
  expect(rows[0]?.predicted_corner_rank).toBe(3);
});

it("mapD1RowsToNeonRows derives predicted corner score and rank for legacy D1 rows", () => {
  const rows = mapD1RowsToNeonRows([
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101235",
      model_version: "v7",
      p_nige: 0.1,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.6,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: null,
      predicted_corner_rank: null,
      predicted_label: "senkou",
      race_key: "jra:20260619:08:01",
    },
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101234",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.2,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: null,
      predicted_corner_rank: null,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
  ]);
  expect(rows[0]?.predicted_corner_front_score).toBeCloseTo(1.3);
  expect(rows[0]?.predicted_corner_rank).toBe(2);
  expect(rows[1]?.predicted_corner_front_score).toBeCloseTo(0.9);
  expect(rows[1]?.predicted_corner_rank).toBe(1);
});

it("mapD1RowsToNeonRows breaks front-score ties by higher nige probability", () => {
  const rows = mapD1RowsToNeonRows([
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101234",
      model_version: "v7",
      p_nige: 0.2,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.4,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: null,
      predicted_corner_rank: null,
      predicted_label: "senkou",
      race_key: "jra:20260619:08:01",
    },
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101235",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.4,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: null,
      predicted_corner_rank: null,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
  ]);
  expect(rows[0]?.predicted_corner_front_score).toBeCloseTo(1.1);
  expect(rows[0]?.predicted_corner_rank).toBe(2);
  expect(rows[1]?.predicted_corner_front_score).toBeCloseTo(1.1);
  expect(rows[1]?.predicted_corner_rank).toBe(1);
});

it("mapD1RowsToNeonRows breaks remaining rank ties by ketto number", () => {
  const rows = mapD1RowsToNeonRows([
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101235",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.4,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: null,
      predicted_corner_rank: null,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101234",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.4,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: null,
      predicted_corner_rank: null,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
  ]);
  expect(rows[0]?.predicted_corner_rank).toBe(2);
  expect(rows[1]?.predicted_corner_rank).toBe(1);
});

it("mapD1RowsToNeonRows uses stored predicted corner score when deriving rank", () => {
  const rows = mapD1RowsToNeonRows([
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101234",
      model_version: "v7",
      p_nige: 0.5,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.6,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: 0.8,
      predicted_corner_rank: null,
      predicted_label: "nige",
      race_key: "jra:20260619:08:01",
    },
    {
      cell_model_key: null,
      cell_variant_id: null,
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "2022101235",
      model_version: "v7",
      p_nige: 0.1,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.2,
      predicted_at: "2026-06-19T00:00:00.000Z",
      predicted_corner_front_score: null,
      predicted_corner_rank: null,
      predicted_label: "senkou",
      race_key: "jra:20260619:08:01",
    },
  ]);
  expect(rows[0]?.predicted_corner_front_score).toBe(0.8);
  expect(rows[0]?.predicted_corner_rank).toBe(1);
  expect(rows[1]?.predicted_corner_front_score).toBeCloseTo(0.9);
  expect(rows[1]?.predicted_corner_rank).toBe(2);
});
