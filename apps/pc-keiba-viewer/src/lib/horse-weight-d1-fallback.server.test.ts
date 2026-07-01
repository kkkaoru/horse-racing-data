// Run with bun. `bun run --filter pc-keiba-viewer test`
import { expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

import { fetchHorseWeightsFromD1 } from "./horse-weight-d1-fallback.server";

interface HorseWeightSnapshotRow {
  horse_number: string;
  horse_name: string | null;
  weight: number | null;
  change_sign: string | null;
  change_amount: number | null;
  fetched_at: string;
}

type AnyMockFn = (...args: never[]) => unknown;

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

const buildPreparedStub = (rows: HorseWeightSnapshotRow[]): PreparedStub => {
  const all = vi.fn<AnyMockFn>().mockResolvedValue({ results: rows, success: true });
  const bind = vi.fn<AnyMockFn>();
  const first = vi.fn<AnyMockFn>().mockResolvedValue(null);
  const run = vi.fn<AnyMockFn>().mockResolvedValue({ success: true });
  const prepared: PreparedStub = { all, bind, first, run };
  bind.mockReturnValue(prepared);
  return prepared;
};

const buildD1Stub = (rows: HorseWeightSnapshotRow[]): BuildStubResult => {
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

it("fetchHorseWeightsFromD1 returns sorted snapshot ordered by numeric horse number", async () => {
  const { db } = buildD1Stub([
    {
      change_amount: 4,
      change_sign: "+",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Beta",
      horse_number: "10",
      weight: 482,
    },
    {
      change_amount: -2,
      change_sign: "-",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Alpha",
      horse_number: "2",
      weight: 460,
    },
    {
      change_amount: 0,
      change_sign: " ",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Gamma",
      horse_number: "5",
      weight: 478,
    },
  ]);
  const result = await fetchHorseWeightsFromD1({ db, raceKey: "jra:2026:0529:05:01" });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T08:00:00.000Z",
    horses: [
      {
        changeAmount: -2,
        changeSign: "-",
        horseName: "Alpha",
        horseNumber: "2",
        weight: 460,
      },
      {
        changeAmount: 0,
        changeSign: " ",
        horseName: "Gamma",
        horseNumber: "5",
        weight: 478,
      },
      {
        changeAmount: 4,
        changeSign: "+",
        horseName: "Beta",
        horseNumber: "10",
        weight: 482,
      },
    ],
  });
});

it("fetchHorseWeightsFromD1 returns null when D1 returns an empty result set", async () => {
  const { db } = buildD1Stub([]);
  const result = await fetchHorseWeightsFromD1({ db, raceKey: "jra:2026:0529:05:01" });
  expect(result).toBeNull();
});

it("fetchHorseWeightsFromD1 maps snake_case row fields to camelCase entry fields including nulls", async () => {
  const { db } = buildD1Stub([
    {
      change_amount: null,
      change_sign: null,
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: null,
      horse_number: "1",
      weight: null,
    },
  ]);
  const result = await fetchHorseWeightsFromD1({ db, raceKey: "jra:2026:0529:05:01" });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T08:00:00.000Z",
    horses: [
      {
        changeAmount: null,
        changeSign: null,
        horseName: null,
        horseNumber: "1",
        weight: null,
      },
    ],
  });
});

it("fetchHorseWeightsFromD1 calls D1 with the right SQL and binds the raceKey argument", async () => {
  const { db, prepared, raw } = buildD1Stub([
    {
      change_amount: 0,
      change_sign: " ",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Alpha",
      horse_number: "1",
      weight: 480,
    },
  ]);
  await fetchHorseWeightsFromD1({ db, raceKey: "nar:2026:0529:47:01" });
  expect(raw.prepare).toHaveBeenCalledWith(
    "select horse_number, horse_name, weight, change_sign, change_amount, fetched_at from horse_weight_snapshots where race_key = ? and fetched_at = (select max(fetched_at) from horse_weight_snapshots where race_key = ?)",
  );
  expect(prepared.bind).toHaveBeenCalledWith("nar:2026:0529:47:01", "nar:2026:0529:47:01");
});

it("fetchHorseWeightsFromD1 uses the fetched_at of the first row when rows share the same timestamp", async () => {
  const { db } = buildD1Stub([
    {
      change_amount: 1,
      change_sign: "+",
      fetched_at: "2026-05-29T07:55:00.000Z",
      horse_name: "First",
      horse_number: "3",
      weight: 470,
    },
    {
      change_amount: -1,
      change_sign: "-",
      fetched_at: "2026-05-29T07:55:00.000Z",
      horse_name: "Second",
      horse_number: "1",
      weight: 472,
    },
  ]);
  const result = await fetchHorseWeightsFromD1({ db, raceKey: "jra:2026:0529:05:02" });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T07:55:00.000Z",
    horses: [
      {
        changeAmount: -1,
        changeSign: "-",
        horseName: "Second",
        horseNumber: "1",
        weight: 472,
      },
      {
        changeAmount: 1,
        changeSign: "+",
        horseName: "First",
        horseNumber: "3",
        weight: 470,
      },
    ],
  });
});
