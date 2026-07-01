// run with: bun run test
import { expect, it, vi } from "vitest";
import type { Pool } from "pg";

import {
  listRaceRunningStylePredictionCountsByDate,
  upsertRunningStylePredictionsToNeon,
} from "./running-style-neon";
import type { RaceRunningStyleRow } from "./running-style-d1";

type QueryFn = (sql: string, values?: unknown[]) => Promise<unknown>;

const buildPool = (queryFn: QueryFn = vi.fn(async () => {})): Pool =>
  ({ query: queryFn }) as unknown as Pool;

const buildRow = (overrides?: Partial<RaceRunningStyleRow>): RaceRunningStyleRow => ({
  bamei: "テスト馬",
  category: "jra",
  cellModelKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
  cellVariantId: "tokyo-turf",
  horseNumber: 1,
  kaisaiNen: "2026",
  kettoTorokuBango: "2022101234",
  modelVersion: "nar-running-style-lgbm-prod-v3",
  pNige: 0.5,
  pOikomi: 0.1,
  pSashi: 0.2,
  pSenkou: 0.2,
  predictedAt: "2026-06-19T00:00:00.000Z",
  predictedLabel: "nige",
  raceKey: "jra:20260619:08:01",
  ...overrides,
});

it("returns 0 for empty rows", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const result = await upsertRunningStylePredictionsToNeon(pool, []);
  expect(result).toBe(0);
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
});

it("upserts a single valid row and returns 1", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const row = buildRow();
  const result = await upsertRunningStylePredictionsToNeon(pool, [row]);
  expect(result).toBe(1);
  expect(vi.mocked(queryFn)).toHaveBeenCalledTimes(1);
  const sql = vi.mocked(queryFn).mock.calls[0]?.[0] ?? "";
  expect(sql.startsWith("insert into race_running_style_model_predictions")).toBe(true);
  expect(sql.indexOf("on conflict") > -1).toBe(true);
  const values = vi.mocked(queryFn).mock.calls[0]?.[1] ?? [];
  expect(values[8]).toBe("running-style/models/jra/cells/tokyo-turf.flatbin");
  expect(values[9]).toBe("tokyo-turf");
});

it("filters rows with invalid race_key format", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const invalid = buildRow({ raceKey: "bad-key" });
  const result = await upsertRunningStylePredictionsToNeon(pool, [invalid]);
  expect(result).toBe(0);
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
});

it("filters rows with unknown predicted_label", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const invalid = buildRow({ predictedLabel: "unknown" as never });
  const result = await upsertRunningStylePredictionsToNeon(pool, [invalid]);
  expect(result).toBe(0);
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
});

it("filters rows with wrong date part length in race_key", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const invalid = buildRow({ raceKey: "jra:260619:08:01" });
  const result = await upsertRunningStylePredictionsToNeon(pool, [invalid]);
  expect(result).toBe(0);
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
});

it("correctly maps label to class index: nige=0 senkou=1 sashi=2 oikomi=3", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const rows = [
    buildRow({ horseNumber: 1, predictedLabel: "nige" }),
    buildRow({ horseNumber: 2, predictedLabel: "senkou", kettoTorokuBango: "2022101235" }),
    buildRow({ horseNumber: 3, predictedLabel: "sashi", kettoTorokuBango: "2022101236" }),
    buildRow({ horseNumber: 4, predictedLabel: "oikomi", kettoTorokuBango: "2022101237" }),
  ];
  await upsertRunningStylePredictionsToNeon(pool, rows);
  const values = vi.mocked(queryFn).mock.calls[0]?.[1] ?? [];
  const classIndexOffset = 15;
  const colCount = 16;
  expect(values[classIndexOffset]).toBe(0);
  expect(values[colCount + classIndexOffset]).toBe(1);
  expect(values[colCount * 2 + classIndexOffset]).toBe(2);
  expect(values[colCount * 3 + classIndexOffset]).toBe(3);
});

it("binds null cell provenance when a row omits it", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const row = buildRow({ cellModelKey: undefined, cellVariantId: undefined });
  await upsertRunningStylePredictionsToNeon(pool, [row]);
  const values = vi.mocked(queryFn).mock.calls[0]?.[1] ?? [];
  expect(values[8]).toBe(null);
  expect(values[9]).toBe(null);
});

it("batches large row sets into NEON_BATCH_SIZE chunks", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const rows = Array.from({ length: 110 }, (_, index) =>
    buildRow({ horseNumber: index + 1, kettoTorokuBango: String(2022100000 + index) }),
  );
  await upsertRunningStylePredictionsToNeon(pool, rows);
  expect(vi.mocked(queryFn)).toHaveBeenCalledTimes(3);
});

it("parses source from race_key correctly", async () => {
  const queryFn: QueryFn = vi.fn(async () => {});
  const pool = buildPool(queryFn);
  const row = buildRow({ raceKey: "nar:20260619:45:01" });
  await upsertRunningStylePredictionsToNeon(pool, [row]);
  const values = vi.mocked(queryFn).mock.calls[0]?.[1] ?? [];
  expect(values[1]).toBe("nar");
  expect(values[3]).toBe("0619");
  expect(values[4]).toBe("45");
  expect(values[5]).toBe("01");
});

it("lists Neon prediction counts by race key and model version", async () => {
  const queryFn: QueryFn = vi.fn(async () => ({
    rows: [
      {
        count: "16",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0619",
        keibajo_code: "08",
        model_version: "v7",
        race_bango: "01",
        source: "jra",
      },
      {
        count: "12",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0619",
        keibajo_code: "45",
        model_version: "v3",
        race_bango: "02",
        source: "nar",
      },
    ],
  }));
  const counts = await listRaceRunningStylePredictionCountsByDate(buildPool(queryFn), "20260619");
  expect(counts.get("jra:20260619:08:01")?.get("v7")).toBe(16);
  expect(counts.get("nar:20260619:45:02")?.get("v3")).toBe(12);
  expect(vi.mocked(queryFn).mock.calls[0]?.[1]).toStrictEqual(["2026", "0619"]);
});

it("returns an empty Neon prediction count map when no rows exist", async () => {
  const queryFn: QueryFn = vi.fn(async () => ({ rows: [] }));
  const counts = await listRaceRunningStylePredictionCountsByDate(buildPool(queryFn), "20260619");
  expect(counts.size).toBe(0);
});
