// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import type { Env } from "../types";
import {
  __testables,
  buildTrainerKeyForLookup,
  fetchTrainerMapForVenueDay,
  type TrainerFetchContext,
  type TrainerPoolLike,
} from "./race-trend-trainer-fetch";

vi.mock("../finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(),
}));

interface BuildPoolArgs {
  rows: ReadonlyArray<unknown>;
  recordCall?: (sql: string, params: ReadonlyArray<unknown>) => void;
}

// Centralised cast: TrainerPoolLike.query is generic over Row but the test
// fixtures are typed as unknown so the harness can feed both fully-typed
// rows and malformed records that the type guard must reject. Existing DO
// test file (race-trend-daily-track-do.test.ts) uses the same `satisfies X
// as unknown as Y` pattern for the D1Database / Env factories.
const buildFakePool = (args: BuildPoolArgs): TrainerPoolLike => {
  const queryImpl = async (
    sql: string,
    params: ReadonlyArray<unknown>,
  ): Promise<{ rows: ReadonlyArray<unknown> }> => {
    if (args.recordCall) args.recordCall(sql, params);
    return { rows: args.rows };
  };
  return { query: queryImpl } as unknown as TrainerPoolLike;
};

const buildThrowingPool = (error: Error): TrainerPoolLike => {
  const queryImpl = async (): Promise<{ rows: ReadonlyArray<unknown> }> => {
    throw error;
  };
  return { query: queryImpl } as unknown as TrainerPoolLike;
};

const buildSlowPool = (delayMs: number): TrainerPoolLike => {
  const queryImpl = (): Promise<{ rows: ReadonlyArray<unknown> }> =>
    new Promise((resolve) => {
      setTimeout(() => resolve({ rows: [] }), delayMs);
    });
  return { query: queryImpl } as unknown as TrainerPoolLike;
};

const JRA_PARSED: TrainerFetchContext = {
  keibajoCode: "06",
  source: "jra",
  targetYmd: "20260606",
};

const NAR_PARSED: TrainerFetchContext = {
  keibajoCode: "48",
  source: "nar",
  targetYmd: "20260606",
};

const buildEnvWithHyperdrive = (): Env =>
  ({
    HYPERDRIVE: { connectionString: "postgres://test" },
  }) satisfies Partial<Env> as unknown as Env;

const buildEnvWithoutHyperdrive = (): Env => ({}) satisfies Partial<Env> as unknown as Env;

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it("buildTrainerKeyForLookup joins raceBango and umaban with a colon", () => {
  expect(buildTrainerKeyForLookup("03", "12")).toBe("03:12");
});

it("__testables.buildTrainerSelectSql uses jvd_se for jra source", () => {
  const sql = __testables.buildTrainerSelectSql("jra", 2);
  expect(sql.includes("from jvd_se")).toBe(true);
});

it("__testables.buildTrainerSelectSql uses nvd_se for nar source", () => {
  const sql = __testables.buildTrainerSelectSql("nar", 1);
  expect(sql.includes("from nvd_se")).toBe(true);
});

it("__testables.buildTrainerSelectSql expands placeholders for each raceBango", () => {
  const sql = __testables.buildTrainerSelectSql("jra", 3);
  expect(sql.includes("$4, $5, $6")).toBe(true);
});

it("__testables.isTrainerRawRow returns true for a fully populated row", () => {
  expect(
    __testables.isTrainerRawRow({
      chokyoshimei_ryakusho: "テスト調教師",
      race_bango: "03",
      umaban: "01",
    }),
  ).toBe(true);
});

it("__testables.isTrainerRawRow returns true when chokyoshimei is explicitly null", () => {
  expect(
    __testables.isTrainerRawRow({
      chokyoshimei_ryakusho: null,
      race_bango: "03",
      umaban: "01",
    }),
  ).toBe(true);
});

it("__testables.isTrainerRawRow returns false for null input", () => {
  expect(__testables.isTrainerRawRow(null)).toBe(false);
});

it("__testables.isTrainerRawRow returns false for missing race_bango", () => {
  expect(
    __testables.isTrainerRawRow({
      chokyoshimei_ryakusho: "x",
      umaban: "01",
    }),
  ).toBe(false);
});

it("__testables.isTrainerRawRow returns false when chokyoshimei_ryakusho is a number", () => {
  expect(
    __testables.isTrainerRawRow({
      chokyoshimei_ryakusho: 12,
      race_bango: "03",
      umaban: "01",
    }),
  ).toBe(false);
});

it("__testables.shouldSkipTrainerFetch returns true when raceBangoList is empty", () => {
  expect(
    __testables.shouldSkipTrainerFetch({
      env: buildEnvWithHyperdrive(),
      parsed: JRA_PARSED,
      raceBangoList: [],
    }),
  ).toBe(true);
});

it("__testables.shouldSkipTrainerFetch returns true when HYPERDRIVE binding is missing", () => {
  expect(
    __testables.shouldSkipTrainerFetch({
      env: buildEnvWithoutHyperdrive(),
      parsed: JRA_PARSED,
      raceBangoList: ["01"],
    }),
  ).toBe(true);
});

it("__testables.shouldSkipTrainerFetch returns false when HYPERDRIVE and list are both present", () => {
  expect(
    __testables.shouldSkipTrainerFetch({
      env: buildEnvWithHyperdrive(),
      parsed: JRA_PARSED,
      raceBangoList: ["01", "02"],
    }),
  ).toBe(false);
});

it("__testables.buildTrainerMap excludes entries with null chokyoshimei", () => {
  const map = __testables.buildTrainerMap([
    { chokyoshimeiRyakusho: "TrainerA", raceBango: "03", umaban: "01" },
    { chokyoshimeiRyakusho: null, raceBango: "03", umaban: "02" },
  ]);
  expect(map.size).toBe(1);
  expect(map.get("03:01")).toBe("TrainerA");
});

it("__testables.withTimeout resolves when work completes before the timer", async () => {
  const work = Promise.resolve("ok");
  const result = await __testables.withTimeout(work, 5000);
  expect(result).toBe("ok");
});

it("__testables.withTimeout rejects when work takes longer than the budget", async () => {
  const slowWork = new Promise<string>((resolve) => {
    setTimeout(() => resolve("late"), 50);
  });
  await expect(__testables.withTimeout(slowWork, 5)).rejects.toThrow(
    "trainer fetch hyperdrive timeout",
  );
});

it("fetchTrainerMapForVenueDay returns an empty Map when HYPERDRIVE is missing", async () => {
  const result = await fetchTrainerMapForVenueDay({
    env: buildEnvWithoutHyperdrive(),
    parsed: JRA_PARSED,
    raceBangoList: ["01"],
  });
  expect(result.size).toBe(0);
});

it("fetchTrainerMapForVenueDay returns an empty Map when raceBangoList is empty", async () => {
  const result = await fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: JRA_PARSED,
    raceBangoList: [],
  });
  expect(result.size).toBe(0);
});

it("fetchTrainerMapForVenueDay populates the map from pool rows for jra", async () => {
  const pool = buildFakePool({
    rows: [
      { chokyoshimei_ryakusho: "JraTrainerA", race_bango: "03", umaban: "01" },
      { chokyoshimei_ryakusho: "JraTrainerB", race_bango: "03", umaban: "02" },
    ],
  });
  const result = await fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: JRA_PARSED,
    poolFactory: () => pool,
    raceBangoList: ["03"],
  });
  expect(result.size).toBe(2);
  expect(result.get("03:01")).toBe("JraTrainerA");
  expect(result.get("03:02")).toBe("JraTrainerB");
});

it("fetchTrainerMapForVenueDay populates the map from pool rows for nar", async () => {
  const pool = buildFakePool({
    rows: [{ chokyoshimei_ryakusho: "NarTrainerA", race_bango: "05", umaban: "01" }],
  });
  const result = await fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: NAR_PARSED,
    poolFactory: () => pool,
    raceBangoList: ["05"],
  });
  expect(result.get("05:01")).toBe("NarTrainerA");
});

it("fetchTrainerMapForVenueDay binds (year, monthDay, keibajoCode, ...raceBangoList) in that order", async () => {
  const recordedParams: Array<ReadonlyArray<unknown>> = [];
  const pool = buildFakePool({
    recordCall: (_sql, params) => recordedParams.push(params),
    rows: [],
  });
  await fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: JRA_PARSED,
    poolFactory: () => pool,
    raceBangoList: ["02", "05"],
  });
  expect(recordedParams).toHaveLength(1);
  expect(recordedParams[0]).toStrictEqual(["2026", "0606", "06", "02", "05"]);
});

it("fetchTrainerMapForVenueDay drops malformed rows that fail the type guard", async () => {
  const pool = buildFakePool({
    rows: [
      { chokyoshimei_ryakusho: "Good", race_bango: "03", umaban: "01" },
      { chokyoshimei_ryakusho: 99, race_bango: "03", umaban: "02" },
    ],
  });
  const result = await fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: JRA_PARSED,
    poolFactory: () => pool,
    raceBangoList: ["03"],
  });
  expect(result.size).toBe(1);
  expect(result.get("03:01")).toBe("Good");
});

it("fetchTrainerMapForVenueDay returns an empty Map when the pool query throws", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const pool = buildThrowingPool(new Error("pg down"));
  const result = await fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: JRA_PARSED,
    poolFactory: () => pool,
    raceBangoList: ["03"],
  });
  expect(result.size).toBe(0);
  expect(consoleSpy).toHaveBeenCalled();
});

it("fetchTrainerMapForVenueDay returns an empty Map when the pool acquisition throws", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const result = await fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: JRA_PARSED,
    poolFactory: () => {
      throw new Error("pool acquisition failed");
    },
    raceBangoList: ["03"],
  });
  expect(result.size).toBe(0);
  expect(consoleSpy).toHaveBeenCalled();
});

it("fetchTrainerMapForVenueDay returns an empty Map when the pool query exceeds the timeout", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const pool = buildSlowPool(__testables.HYPERDRIVE_TIMEOUT_MS + 100);
  vi.useFakeTimers();
  const promise = fetchTrainerMapForVenueDay({
    env: buildEnvWithHyperdrive(),
    parsed: JRA_PARSED,
    poolFactory: () => pool,
    raceBangoList: ["03"],
  });
  await vi.advanceTimersByTimeAsync(__testables.HYPERDRIVE_TIMEOUT_MS + 10);
  const result = await promise;
  expect(result.size).toBe(0);
  expect(consoleSpy).toHaveBeenCalled();
});
