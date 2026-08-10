// run with: bun run test
import { expect, it, vi } from "vitest";
import type { Pool } from "pg";

import { NeonTransactionReadOnlyError } from "./neon-writable-client";
import {
  ensureRunningStylePredictionNeonSchema,
  listRaceRunningStylePredictionCountsByDate,
  upsertRunningStylePredictionsToNeon,
} from "./running-style-neon";
import type { RaceRunningStyleRow } from "./running-style-d1";

type QueryFn = (sql: string, values?: unknown[]) => Promise<unknown>;

interface WritablePoolHarness {
  connect: ReturnType<typeof vi.fn>;
  pool: Pool;
  poolQuery: ReturnType<typeof vi.fn>;
  queryFn: QueryFn;
  release: ReturnType<typeof vi.fn>;
}

const buildPool = (queryFn: QueryFn = vi.fn(async () => {})): Pool =>
  ({ query: queryFn }) as unknown as Pool;

const buildWritablePool = (queryFn: QueryFn): WritablePoolHarness => {
  const release = vi.fn();
  const connect = vi.fn(async () => ({ query: queryFn, release }));
  const poolQuery = vi.fn();
  return {
    connect,
    pool: { connect, query: poolQuery } as unknown as Pool,
    poolQuery,
    queryFn,
    release,
  };
};

const writableQuery = (dmlQuery: QueryFn = vi.fn(async () => {})): QueryFn =>
  vi.fn(async (sql: string, values?: unknown[]) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "off" }] };
    }
    return dmlQuery(sql, values);
  });

const lastInsertCall = (queryFn: QueryFn): [string, unknown[] | undefined] => {
  const insertCalls = vi
    .mocked(queryFn)
    .mock.calls.filter(
      ([sql]) =>
        typeof sql === "string" &&
        sql.startsWith("insert into race_running_style_model_predictions"),
    );
  return insertCalls.at(-1) as [string, unknown[] | undefined];
};

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
  predictedCornerFrontScore: 0.7,
  predictedCornerRank: 1,
  predictedLabel: "nige",
  raceKey: "jra:20260619:08:01",
  ...overrides,
});

it("returns 0 for empty rows", async () => {
  const queryFn: QueryFn = writableQuery();
  const { connect, pool, poolQuery } = buildWritablePool(queryFn);
  const result = await upsertRunningStylePredictionsToNeon(pool, []);
  expect(result).toBe(0);
  expect(connect).not.toHaveBeenCalled();
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
  expect(poolQuery).not.toHaveBeenCalled();
});

it("upserts a single valid row and returns 1", async () => {
  const queryFn: QueryFn = writableQuery();
  const { connect, pool, poolQuery, release } = buildWritablePool(queryFn);
  const row = buildRow();
  const result = await upsertRunningStylePredictionsToNeon(pool, [row]);
  expect(result).toBe(1);
  expect(connect).toHaveBeenCalledTimes(1);
  expect(vi.mocked(queryFn).mock.calls[0]?.[0]).toBe("BEGIN");
  expect(vi.mocked(queryFn).mock.calls[1]?.[0]).toBe("SET TRANSACTION READ WRITE");
  expect(vi.mocked(queryFn).mock.calls[2]?.[0]).toBe("SHOW transaction_read_only");
  expect(vi.mocked(queryFn).mock.calls[4]?.[0]).toBe("COMMIT");
  const [sql, values = []] = lastInsertCall(queryFn);
  expect(sql.startsWith("insert into race_running_style_model_predictions")).toBe(true);
  expect(sql.indexOf("on conflict") > -1).toBe(true);
  expect(values[8]).toBe("running-style/models/jra/cells/tokyo-turf.flatbin");
  expect(values[9]).toBe("tokyo-turf");
  expect(values[14]).toBe(0.7);
  expect(values[15]).toBe(1);
  expect(release).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("filters rows with invalid race_key format", async () => {
  const queryFn: QueryFn = writableQuery();
  const { connect, pool, poolQuery } = buildWritablePool(queryFn);
  const invalid = buildRow({ raceKey: "bad-key" });
  const result = await upsertRunningStylePredictionsToNeon(pool, [invalid]);
  expect(result).toBe(0);
  expect(connect).not.toHaveBeenCalled();
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
  expect(poolQuery).not.toHaveBeenCalled();
});

it("filters rows with unknown predicted_label", async () => {
  const queryFn: QueryFn = writableQuery();
  const { connect, pool, poolQuery } = buildWritablePool(queryFn);
  const invalid = buildRow({ predictedLabel: "unknown" as never });
  const result = await upsertRunningStylePredictionsToNeon(pool, [invalid]);
  expect(result).toBe(0);
  expect(connect).not.toHaveBeenCalled();
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
  expect(poolQuery).not.toHaveBeenCalled();
});

it("filters rows with wrong date part length in race_key", async () => {
  const queryFn: QueryFn = writableQuery();
  const { connect, pool, poolQuery } = buildWritablePool(queryFn);
  const invalid = buildRow({ raceKey: "jra:260619:08:01" });
  const result = await upsertRunningStylePredictionsToNeon(pool, [invalid]);
  expect(result).toBe(0);
  expect(connect).not.toHaveBeenCalled();
  expect(vi.mocked(queryFn)).not.toHaveBeenCalled();
  expect(poolQuery).not.toHaveBeenCalled();
});

it("correctly maps label to class index: nige=0 senkou=1 sashi=2 oikomi=3", async () => {
  const queryFn: QueryFn = writableQuery();
  const { pool } = buildWritablePool(queryFn);
  const rows = [
    buildRow({ horseNumber: 1, predictedLabel: "nige" }),
    buildRow({ horseNumber: 2, predictedLabel: "senkou", kettoTorokuBango: "2022101235" }),
    buildRow({ horseNumber: 3, predictedLabel: "sashi", kettoTorokuBango: "2022101236" }),
    buildRow({ horseNumber: 4, predictedLabel: "oikomi", kettoTorokuBango: "2022101237" }),
  ];
  await upsertRunningStylePredictionsToNeon(pool, rows);
  const [, values = []] = lastInsertCall(queryFn);
  const classIndexOffset = 17;
  const colCount = 18;
  expect(values[classIndexOffset]).toBe(0);
  expect(values[colCount + classIndexOffset]).toBe(1);
  expect(values[colCount * 2 + classIndexOffset]).toBe(2);
  expect(values[colCount * 3 + classIndexOffset]).toBe(3);
});

it("binds null cell provenance when a row omits it", async () => {
  const queryFn: QueryFn = writableQuery();
  const { pool } = buildWritablePool(queryFn);
  const row = buildRow({ cellModelKey: undefined, cellVariantId: undefined });
  await upsertRunningStylePredictionsToNeon(pool, [row]);
  const [, values = []] = lastInsertCall(queryFn);
  expect(values[8]).toBe(null);
  expect(values[9]).toBe(null);
});

it("batches large row sets into NEON_BATCH_SIZE chunks on one writable client", async () => {
  const queryFn: QueryFn = writableQuery();
  const { connect, pool, poolQuery, release } = buildWritablePool(queryFn);
  const rows = Array.from({ length: 110 }, (_, index) =>
    buildRow({ horseNumber: index + 1, kettoTorokuBango: String(2022100000 + index) }),
  );
  await upsertRunningStylePredictionsToNeon(pool, rows);
  expect(connect).toHaveBeenCalledTimes(1);
  const sqls = vi.mocked(queryFn).mock.calls.map(([sql]) => String(sql));
  expect(sqls[0]).toBe("BEGIN");
  expect(sqls[1]).toBe("SET TRANSACTION READ WRITE");
  expect(sqls[2]).toBe("SHOW transaction_read_only");
  expect(sqls[3]?.startsWith("insert into race_running_style_model_predictions")).toBe(true);
  expect(sqls[4]?.startsWith("insert into race_running_style_model_predictions")).toBe(true);
  expect(sqls[5]?.startsWith("insert into race_running_style_model_predictions")).toBe(true);
  expect(sqls[6]).toBe("COMMIT");
  expect(sqls.length).toBe(7);
  expect(sqls.filter((sql) => sql.indexOf("alter table") > -1).length).toBe(0);
  expect(release).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("ensureRunningStylePredictionNeonSchema still runs DDL when called explicitly", async () => {
  const queryFn: QueryFn = writableQuery();
  const { connect, pool, poolQuery, release } = buildWritablePool(queryFn);
  await ensureRunningStylePredictionNeonSchema(pool);
  expect(connect).toHaveBeenCalledTimes(1);
  expect(vi.mocked(queryFn).mock.calls[0]?.[0]).toBe("BEGIN");
  expect(vi.mocked(queryFn).mock.calls[1]?.[0]).toBe("SET TRANSACTION READ WRITE");
  expect(vi.mocked(queryFn).mock.calls[2]?.[0]).toBe("SHOW transaction_read_only");
  expect(vi.mocked(queryFn).mock.calls[3]?.[0]).toMatch(
    /add column if not exists predicted_corner_front_score/,
  );
  expect(vi.mocked(queryFn).mock.calls[4]?.[0]).toBe("COMMIT");
  expect(release).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("ensureRunningStylePredictionNeonSchema rolls back without DDL when read-only stays on", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "on" }] };
    }
    return undefined;
  });
  const { connect, pool, poolQuery, release } = buildWritablePool(queryFn);
  await expect(ensureRunningStylePredictionNeonSchema(pool)).rejects.toBeInstanceOf(
    NeonTransactionReadOnlyError,
  );
  expect(connect).toHaveBeenCalledTimes(1);
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(release).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("ensureRunningStylePredictionNeonSchema rolls back when DDL fails", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "off" }] };
    }
    if (typeof sql === "string" && sql.indexOf("alter table") > -1) {
      throw new Error("alter failed");
    }
    return undefined;
  });
  const { connect, pool, poolQuery, release } = buildWritablePool(queryFn);
  await expect(ensureRunningStylePredictionNeonSchema(pool)).rejects.toThrow("alter failed");
  expect(connect).toHaveBeenCalledTimes(1);
  expect(vi.mocked(queryFn).mock.calls[0]?.[0]).toBe("BEGIN");
  expect(vi.mocked(queryFn).mock.calls[1]?.[0]).toBe("SET TRANSACTION READ WRITE");
  expect(vi.mocked(queryFn).mock.calls[2]?.[0]).toBe("SHOW transaction_read_only");
  expect(String(vi.mocked(queryFn).mock.calls[3]?.[0]).indexOf("alter table") > -1).toBe(true);
  expect(vi.mocked(queryFn).mock.calls[4]?.[0]).toBe("ROLLBACK");
  expect(release).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("parses source from race_key correctly", async () => {
  const queryFn: QueryFn = writableQuery();
  const { pool } = buildWritablePool(queryFn);
  const row = buildRow({ raceKey: "nar:20260619:45:01" });
  await upsertRunningStylePredictionsToNeon(pool, [row]);
  const [, values = []] = lastInsertCall(queryFn);
  expect(values[1]).toBe("nar");
  expect(values[3]).toBe("0619");
  expect(values[4]).toBe("45");
  expect(values[5]).toBe("01");
});

it("rolls back without DML when transaction_read_only stays on", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "on" }] };
    }
    return undefined;
  });
  const { connect, pool, poolQuery, release } = buildWritablePool(queryFn);
  await expect(upsertRunningStylePredictionsToNeon(pool, [buildRow()])).rejects.toBeInstanceOf(
    NeonTransactionReadOnlyError,
  );
  expect(connect).toHaveBeenCalledTimes(1);
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(release).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("rolls back when an insert fails mid-transaction", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "off" }] };
    }
    if (
      typeof sql === "string" &&
      sql.startsWith("insert into race_running_style_model_predictions")
    ) {
      throw new Error("insert failed");
    }
    return undefined;
  });
  const { connect, pool, poolQuery, release } = buildWritablePool(queryFn);
  await expect(upsertRunningStylePredictionsToNeon(pool, [buildRow()])).rejects.toThrow(
    "insert failed",
  );
  expect(connect).toHaveBeenCalledTimes(1);
  expect(vi.mocked(queryFn).mock.calls[0]?.[0]).toBe("BEGIN");
  expect(vi.mocked(queryFn).mock.calls[1]?.[0]).toBe("SET TRANSACTION READ WRITE");
  expect(vi.mocked(queryFn).mock.calls[2]?.[0]).toBe("SHOW transaction_read_only");
  expect(
    String(vi.mocked(queryFn).mock.calls[3]?.[0]).startsWith(
      "insert into race_running_style_model_predictions",
    ),
  ).toBe(true);
  expect(vi.mocked(queryFn).mock.calls[4]?.[0]).toBe("ROLLBACK");
  expect(release).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
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
