// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { safeGetCloudflareEnv } = vi.hoisted(() => ({
  safeGetCloudflareEnv: vi.fn<(...args: never[]) => unknown>(),
}));

vi.mock("../lib/cloudflare-context.server", () => ({
  safeGetCloudflareEnv,
}));

import { getRaceFinishPositionsFromD1 } from "./race-finish-d1.server";

type AnyMockFn = (...args: never[]) => unknown;

interface RawRaceFinishD1Row {
  finishPosition: string;
  horseNumber: string;
}

interface PreparedStub {
  all: ReturnType<typeof vi.fn<AnyMockFn>>;
  bind: ReturnType<typeof vi.fn<AnyMockFn>>;
  first: ReturnType<typeof vi.fn<AnyMockFn>>;
  run: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface D1Stub {
  prepare: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface BuildStubResult {
  db: PcKeibaD1Database;
  prepared: PreparedStub;
  raw: D1Stub;
}

const isPreparedStatement = (value: unknown): value is PcKeibaD1PreparedStatement =>
  typeof value === "object" &&
  value !== null &&
  "bind" in value &&
  typeof value.bind === "function";

const emptyBatch = <T = unknown>(): Promise<PcKeibaD1Result<T>[]> => Promise.resolve([]);
const noopExec = (): Promise<PcKeibaD1RunResult> => Promise.resolve({ success: true });

const buildPreparedStub = (rows: ReadonlyArray<unknown>): PreparedStub => {
  const all = vi.fn<AnyMockFn>().mockResolvedValue({ results: rows, success: true });
  const bind = vi.fn<AnyMockFn>();
  const first = vi.fn<AnyMockFn>().mockResolvedValue(null);
  const run = vi.fn<AnyMockFn>().mockResolvedValue({ success: true });
  const prepared: PreparedStub = { all, bind, first, run };
  bind.mockReturnValue(prepared);
  return prepared;
};

const buildD1Stub = (rows: ReadonlyArray<unknown>): BuildStubResult => {
  const prepared = buildPreparedStub(rows);
  const raw: D1Stub = {
    prepare: vi.fn<AnyMockFn>().mockReturnValue(prepared),
  };
  const typedPrepare = (query: string): PcKeibaD1PreparedStatement => {
    const result = Reflect.apply(raw.prepare, raw, [query]);
    if (!isPreparedStatement(result)) {
      throw new Error("Stub returned an invalid prepared statement");
    }
    return result;
  };
  const db: PcKeibaD1Database = {
    batch: emptyBatch,
    exec: noopExec,
    prepare: typedPrepare,
  };
  return { db, prepared, raw };
};

const placedRows: RawRaceFinishD1Row[] = [
  { finishPosition: "1", horseNumber: "02" },
  { finishPosition: "2", horseNumber: "05" },
];

beforeEach(() => {
  safeGetCloudflareEnv.mockReset();
});

it("returns an empty array when REALTIME_DB is not bound", async () => {
  safeGetCloudflareEnv.mockResolvedValue(null);
  const result = await getRaceFinishPositionsFromD1({
    day: "19",
    keibajoCode: "45",
    month: "06",
    raceNumber: "11",
    source: "nar",
    year: "2026",
  });
  expect(result).toStrictEqual([]);
});

it("maps the D1 rows to serializable finish entries", async () => {
  const { db } = buildD1Stub(placedRows);
  safeGetCloudflareEnv.mockResolvedValue({ REALTIME_DB: db });
  const result = await getRaceFinishPositionsFromD1({
    day: "19",
    keibajoCode: "45",
    month: "06",
    raceNumber: "11",
    source: "nar",
    year: "2026",
  });
  expect(result).toStrictEqual([
    { finishPosition: "1", horseNumber: "02" },
    { finishPosition: "2", horseNumber: "05" },
  ]);
});

it("drops rows that fail the type guard", async () => {
  const { db } = buildD1Stub([
    { finishPosition: "1", horseNumber: "02" },
    { finishPosition: 2, horseNumber: "05" },
    { horseNumber: "07" },
    null,
  ]);
  safeGetCloudflareEnv.mockResolvedValue({ REALTIME_DB: db });
  const result = await getRaceFinishPositionsFromD1({
    day: "19",
    keibajoCode: "45",
    month: "06",
    raceNumber: "11",
    source: "nar",
    year: "2026",
  });
  expect(result).toStrictEqual([{ finishPosition: "1", horseNumber: "02" }]);
});

it("binds the zero-padded kaisai_tsukihi, keibajo_code and race_bango", async () => {
  const { db, prepared } = buildD1Stub(placedRows);
  safeGetCloudflareEnv.mockResolvedValue({ REALTIME_DB: db });
  await getRaceFinishPositionsFromD1({
    day: "9",
    keibajoCode: "45",
    month: "6",
    raceNumber: "3",
    source: "nar",
    year: "2026",
  });
  expect(prepared.bind).toHaveBeenCalledWith("nar", "2026", "0609", "45", "03");
});

it("returns an empty array when the D1 query throws", async () => {
  const throwingPrepared = buildPreparedStub([]);
  throwingPrepared.all.mockRejectedValue(new Error("D1 saturated"));
  const raw: D1Stub = {
    prepare: vi.fn<AnyMockFn>().mockReturnValue(throwingPrepared),
  };
  const db: PcKeibaD1Database = {
    batch: emptyBatch,
    exec: noopExec,
    prepare: (query: string): PcKeibaD1PreparedStatement => {
      const result = Reflect.apply(raw.prepare, raw, [query]);
      if (!isPreparedStatement(result)) {
        throw new Error("Stub returned an invalid prepared statement");
      }
      return result;
    },
  };
  safeGetCloudflareEnv.mockResolvedValue({ REALTIME_DB: db });
  const result = await getRaceFinishPositionsFromD1({
    day: "19",
    keibajoCode: "45",
    month: "06",
    raceNumber: "11",
    source: "nar",
    year: "2026",
  });
  expect(result).toStrictEqual([]);
});
