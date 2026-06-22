// Run with: bunx vitest run src/scripts/transfer-odds-old-to-hot.test.ts
import { expect, test, vi } from "vitest";
import type { OddsType } from "../types";
import {
  applyInsertBatches,
  buildInsertBatches,
  buildKvKeysFilePath,
  buildKvKeysJson,
  buildSqlFilePath,
  listOldOddsRows,
  parseDateArg,
  purgeKvKeys,
  runTransfer,
  sqlNullableNumber,
  sqlString,
  type OldOddsRow,
} from "./transfer-odds-old-to-hot";

test("parseDateArg returns the YYYYMMDD argv element", () => {
  expect(parseDateArg(["bun", "script.ts", "20260622"])).toBe("20260622");
});

test("parseDateArg throws when argv lacks a date", () => {
  expect(() => parseDateArg(["bun", "script.ts"])).toThrow(
    "usage: bun src/scripts/transfer-odds-old-to-hot.ts YYYYMMDD",
  );
});

test("parseDateArg throws when argv contains only 7 digits", () => {
  expect(() => parseDateArg(["bun", "script.ts", "2026062"])).toThrow(
    "usage: bun src/scripts/transfer-odds-old-to-hot.ts YYYYMMDD",
  );
});

test("parseDateArg throws when argv contains non-digit characters", () => {
  expect(() => parseDateArg(["bun", "script.ts", "2026-06-22"])).toThrow(
    "usage: bun src/scripts/transfer-odds-old-to-hot.ts YYYYMMDD",
  );
});

test("sqlString single-quotes the value", () => {
  expect(sqlString("hello")).toBe("'hello'");
});

test("sqlString escapes embedded single quotes", () => {
  expect(sqlString("it's")).toBe("'it''s'");
});

test("sqlNullableNumber returns null literal for null", () => {
  expect(sqlNullableNumber(null)).toBe("null");
});

test("sqlNullableNumber returns null literal for undefined", () => {
  expect(sqlNullableNumber(undefined)).toBe("null");
});

test("sqlNullableNumber stringifies a finite number", () => {
  expect(sqlNullableNumber(2.5)).toBe("2.5");
});

test("sqlNullableNumber preserves zero", () => {
  expect(sqlNullableNumber(0)).toBe("0");
});

test("buildInsertBatches returns an empty array when rows is empty", () => {
  expect(buildInsertBatches({ chunkSize: 3, rows: [] })).toStrictEqual([]);
});

test("buildInsertBatches returns an empty array when chunkSize is zero", () => {
  const tansho: OddsType = "tansho";
  const row: OldOddsRow = {
    average_odds: null,
    combination: "5",
    fetched_at: "2026-06-22T14:30:00+09:00",
    max_odds: null,
    min_odds: null,
    odds: 2.4,
    odds_type: tansho,
    race_key: "nar:20260622:44:01",
    rank: 1,
  };
  expect(buildInsertBatches({ chunkSize: 0, rows: [row] })).toStrictEqual([]);
});

test("buildInsertBatches emits one batch for a single row", () => {
  const tansho: OddsType = "tansho";
  const row: OldOddsRow = {
    average_odds: null,
    combination: "5",
    fetched_at: "2026-06-22T14:30:00+09:00",
    max_odds: null,
    min_odds: null,
    odds: 2.4,
    odds_type: tansho,
    race_key: "nar:20260622:44:01",
    rank: 1,
  };
  expect(buildInsertBatches({ chunkSize: 50, rows: [row] })).toStrictEqual([
    "insert or ignore into odds_snapshots\n  (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank)\nvalues ('nar:20260622:44:01', '2026-06-22T14:30:00+09:00', 'tansho', '5', 2.4, null, null, null, 1);",
  ]);
});

test("buildInsertBatches chunks 7 rows into 3 batches of 3+3+1 with chunkSize 3", () => {
  const tansho: OddsType = "tansho";
  const makeRow = (index: number): OldOddsRow => ({
    average_odds: null,
    combination: String(index),
    fetched_at: "2026-06-22T14:30:00+09:00",
    max_odds: null,
    min_odds: null,
    odds: 2,
    odds_type: tansho,
    race_key: "nar:20260622:44:01",
    rank: index,
  });
  const rows = [makeRow(1), makeRow(2), makeRow(3), makeRow(4), makeRow(5), makeRow(6), makeRow(7)];
  const batches = buildInsertBatches({ chunkSize: 3, rows });
  expect(batches.length).toBe(3);
  expect(batches[0]!.split("\n").length).toBe(5);
  expect(batches[1]!.split("\n").length).toBe(5);
  expect(batches[2]!.split("\n").length).toBe(3);
});

test("buildKvKeysJson returns an empty JSON array for empty input", () => {
  expect(buildKvKeysJson([])).toBe("[]");
});

test("buildKvKeysJson deduplicates race keys and prefixes them with odds:latest", () => {
  const tansho: OddsType = "tansho";
  const rows: OldOddsRow[] = [
    {
      average_odds: null,
      combination: "5",
      fetched_at: "2026-06-22T14:30:00+09:00",
      max_odds: null,
      min_odds: null,
      odds: 2.4,
      odds_type: tansho,
      race_key: "nar:20260622:44:01",
      rank: 1,
    },
    {
      average_odds: null,
      combination: "6",
      fetched_at: "2026-06-22T14:30:00+09:00",
      max_odds: null,
      min_odds: null,
      odds: 5.0,
      odds_type: tansho,
      race_key: "nar:20260622:44:01",
      rank: 2,
    },
    {
      average_odds: null,
      combination: "1",
      fetched_at: "2026-06-22T15:00:00+09:00",
      max_odds: null,
      min_odds: null,
      odds: 1.5,
      odds_type: tansho,
      race_key: "nar:20260622:44:02",
      rank: 1,
    },
  ];
  expect(buildKvKeysJson(rows)).toBe(
    '["odds:latest:nar:20260622:44:01","odds:latest:nar:20260622:44:02"]',
  );
});

test("buildSqlFilePath builds a /tmp path with the date and batch index", () => {
  expect(buildSqlFilePath("20260622", 0)).toBe("/tmp/transfer-odds-old-to-hot-20260622-0.sql");
});

test("buildKvKeysFilePath builds a /tmp path with the date", () => {
  expect(buildKvKeysFilePath("20260622")).toBe(
    "/tmp/transfer-odds-old-to-hot-kv-keys-20260622.json",
  );
});

test("listOldOddsRows sends a SELECT against the legacy D1 via wrangler", async () => {
  const runWranglerImpl = vi.fn(
    async (_args: readonly string[]): Promise<string> =>
      JSON.stringify([
        {
          results: [
            {
              average_odds: null,
              combination: "5",
              fetched_at: "2026-06-22T14:30:00+09:00",
              max_odds: null,
              min_odds: null,
              odds: 2.4,
              odds_type: "tansho",
              race_key: "nar:20260622:44:01",
              rank: 1,
            },
          ],
          success: true,
        },
      ]),
  );
  const rows = await listOldOddsRows("20260622", { runWranglerImpl });
  expect(rows).toStrictEqual([
    {
      average_odds: null,
      combination: "5",
      fetched_at: "2026-06-22T14:30:00+09:00",
      max_odds: null,
      min_odds: null,
      odds: 2.4,
      odds_type: "tansho",
      race_key: "nar:20260622:44:01",
      rank: 1,
    },
  ]);
  expect(runWranglerImpl).toHaveBeenCalledOnce();
  const wranglerArgs = runWranglerImpl.mock.calls[0]![0];
  expect(wranglerArgs[0]).toBe("d1");
  expect(wranglerArgs[1]).toBe("execute");
  expect(wranglerArgs[2]).toBe("sync-realtime-data");
  expect(wranglerArgs[3]).toBe("--remote");
  expect(wranglerArgs[4]).toBe("--json");
  expect(wranglerArgs[5]).toBe("--command");
  expect(wranglerArgs[6]).toBe(
    "select race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank\nfrom odds_snapshots\nwhere race_key like 'nar:20260622:%'\norder by race_key, fetched_at, odds_type, combination",
  );
});

test("listOldOddsRows throws when wrangler reports failure", async () => {
  const runWranglerImpl = vi.fn(
    async (_args: readonly string[]): Promise<string> =>
      JSON.stringify([{ results: [], success: false }]),
  );
  await expect(listOldOddsRows("20260622", { runWranglerImpl })).rejects.toThrow(
    "failed to list old odds_snapshots for 20260622",
  );
});

test("listOldOddsRows returns an empty array when results is missing", async () => {
  const runWranglerImpl = vi.fn(
    async (_args: readonly string[]): Promise<string> => JSON.stringify([{ success: true }]),
  );
  expect(await listOldOddsRows("20260622", { runWranglerImpl })).toStrictEqual([]);
});

test("applyInsertBatches writes each batch SQL and runs wrangler against the hot D1", async () => {
  const writeFileImpl = vi.fn(async (_path: string, _contents: string): Promise<number> => 1);
  const runWranglerImpl = vi.fn(async (_args: readonly string[]): Promise<string> => "{}");
  await applyInsertBatches({
    batches: ["insert or ignore into odds_snapshots ...batch0;", "insert or ignore ...batch1;"],
    runWranglerImpl,
    targetDate: "20260622",
    writeFileImpl,
  });
  expect(writeFileImpl).toHaveBeenCalledTimes(2);
  expect(writeFileImpl.mock.calls[0]![0]).toBe("/tmp/transfer-odds-old-to-hot-20260622-0.sql");
  expect(writeFileImpl.mock.calls[1]![0]).toBe("/tmp/transfer-odds-old-to-hot-20260622-1.sql");
  expect(runWranglerImpl).toHaveBeenCalledTimes(2);
  expect(runWranglerImpl.mock.calls[0]![0]).toStrictEqual([
    "d1",
    "execute",
    "sync-realtime-data-hot",
    "--remote",
    "--json",
    "--file",
    "/tmp/transfer-odds-old-to-hot-20260622-0.sql",
  ]);
  expect(runWranglerImpl.mock.calls[1]![0]).toStrictEqual([
    "d1",
    "execute",
    "sync-realtime-data-hot",
    "--remote",
    "--json",
    "--file",
    "/tmp/transfer-odds-old-to-hot-20260622-1.sql",
  ]);
});

test("purgeKvKeys writes the KV keys JSON and invokes wrangler kv bulk delete once", async () => {
  const writeFileImpl = vi.fn(async (_path: string, _contents: string): Promise<number> => 1);
  const runWranglerImpl = vi.fn(async (_args: readonly string[]): Promise<string> => "{}");
  await purgeKvKeys({
    keysJson: '["odds:latest:nar:20260622:44:01"]',
    runWranglerImpl,
    targetDate: "20260622",
    writeFileImpl,
  });
  expect(writeFileImpl).toHaveBeenCalledOnce();
  expect(writeFileImpl.mock.calls[0]![0]).toBe(
    "/tmp/transfer-odds-old-to-hot-kv-keys-20260622.json",
  );
  expect(writeFileImpl.mock.calls[0]![1]).toBe('["odds:latest:nar:20260622:44:01"]');
  expect(runWranglerImpl).toHaveBeenCalledOnce();
  expect(runWranglerImpl.mock.calls[0]![0]).toStrictEqual([
    "kv",
    "bulk",
    "delete",
    "--namespace-id",
    "844a7bf58c514402b7d5ae3149734052",
    "--remote",
    "/tmp/transfer-odds-old-to-hot-kv-keys-20260622.json",
  ]);
});

test("runTransfer orchestrates list, insert batches, and KV purge for a populated date", async () => {
  const tansho: OddsType = "tansho";
  const sampleRow: OldOddsRow = {
    average_odds: null,
    combination: "5",
    fetched_at: "2026-06-22T14:30:00+09:00",
    max_odds: null,
    min_odds: null,
    odds: 2.4,
    odds_type: tansho,
    race_key: "nar:20260622:44:01",
    rank: 1,
  };
  const listOldRows = vi.fn(async (_targetDate: string): Promise<OldOddsRow[]> => [sampleRow]);
  const writeFileImpl = vi.fn(async (_path: string, _contents: string): Promise<number> => 1);
  const runWranglerImpl = vi.fn(async (_args: readonly string[]): Promise<string> => "{}");
  const logCalls: string[] = [];
  const log = (message: string): void => {
    logCalls.push(message);
  };
  const summary = await runTransfer({
    listOldRows,
    log,
    runWranglerImpl,
    targetDate: "20260622",
    writeFileImpl,
  });
  expect(summary).toStrictEqual({
    kvKeysPurged: 1,
    oddsRowsInserted: 1,
    raceKeys: 1,
  });
  expect(listOldRows).toHaveBeenCalledOnce();
  expect(writeFileImpl).toHaveBeenCalledTimes(2);
  expect(runWranglerImpl).toHaveBeenCalledTimes(2);
  expect(logCalls.length).toBe(4);
});

test("runTransfer skips inserts and KV purge when no legacy rows exist", async () => {
  const listOldRows = vi.fn(async (_targetDate: string): Promise<OldOddsRow[]> => []);
  const writeFileImpl = vi.fn(async (_path: string, _contents: string): Promise<number> => 1);
  const runWranglerImpl = vi.fn(async (_args: readonly string[]): Promise<string> => "{}");
  const logCalls: string[] = [];
  const log = (message: string): void => {
    logCalls.push(message);
  };
  const summary = await runTransfer({
    listOldRows,
    log,
    runWranglerImpl,
    targetDate: "20260622",
    writeFileImpl,
  });
  expect(summary).toStrictEqual({
    kvKeysPurged: 0,
    oddsRowsInserted: 0,
    raceKeys: 0,
  });
  expect(writeFileImpl).not.toHaveBeenCalled();
  expect(runWranglerImpl).not.toHaveBeenCalled();
  expect(logCalls.length).toBe(4);
});
