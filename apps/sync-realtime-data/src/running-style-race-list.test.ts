// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(),
}));

const RACE_ROW = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "0512",
  keibajo_code: "08",
  race_bango: "01",
  source: "jra" as const,
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("listRunningStyleRacesByDate returns D1 rows when present", async () => {
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const all = vi.fn(async () => ({ results: [RACE_ROW] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const env = { REALTIME_DB: { prepare } } as unknown as Env;
  const result = await listRunningStyleRacesByDate(env, "20260512");
  expect(result.source).toBe("d1");
  expect(result.races).toStrictEqual([RACE_ROW]);
  expect(bind).toHaveBeenCalledWith("2026", "0512");
});

it("listRunningStyleRacesByDate falls back to Postgres when D1 has no rows", async () => {
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const env = { REALTIME_DB: { prepare } } as unknown as Env;
  const query = vi.fn(async (_sql: string, _params: unknown[]) => ({ rows: [RACE_ROW] }));
  vi.mocked(getFinishPositionPool).mockReturnValue({ query } as never);

  const result = await listRunningStyleRacesByDate(env, "20260512");
  expect(result.source).toBe("features");
  expect(result.races).toStrictEqual([RACE_ROW]);
  expect(query).toHaveBeenCalledTimes(1);
  expect(query.mock.calls[0]![1]).toStrictEqual(["20260512"]);
});

it("listRunningStyleRacesByDate returns empty features array when Postgres has no rows", async () => {
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const env = { REALTIME_DB: { prepare } } as unknown as Env;
  const query = vi.fn(async () => ({ rows: [] }));
  vi.mocked(getFinishPositionPool).mockReturnValue({ query } as never);
  const result = await listRunningStyleRacesByDate(env, "20260512");
  expect(result.source).toBe("features");
  expect(result.races).toStrictEqual([]);
});
