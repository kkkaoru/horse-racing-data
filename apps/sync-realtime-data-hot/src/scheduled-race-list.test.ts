// Run with: bun run --filter sync-realtime-data-hot test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("./postgres-pool", () => ({
  getHotPool: vi.fn(),
}));

vi.mock("./keiba-go", () => ({
  buildRaceListUrl: vi.fn((targetDate: string, babaCode: string) => ({
    babaCode,
    url: `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=${targetDate}&k_babaCode=${babaCode}`,
  })),
  fetchRaceLinksFromRaceList: vi.fn(async () => []),
}));

import { buildRaceListUrl, fetchRaceLinksFromRaceList } from "./keiba-go";
import { getHotPool } from "./postgres-pool";
import { listTodayRacesFromHyperdrive, populateTodayOddsFetchState } from "./scheduled-race-list";
import type { Env } from "./types";

const buildKv = (): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildDb = (upsertRun?: ReturnType<typeof vi.fn>): D1Database => {
  const run = upsertRun ?? vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  return { prepare } as unknown as D1Database;
};

const buildEnv = (): Env =>
  ({
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: buildKv(),
    REALTIME_HOT_DB: buildDb(),
  }) as unknown as Env;

afterEach(() => {
  vi.restoreAllMocks();
  vi.mocked(fetchRaceLinksFromRaceList).mockReset();
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([]);
  vi.mocked(buildRaceListUrl).mockClear();
});

it("listTodayRacesFromHyperdrive resolves NAR per-race deba URL via fetchRaceLinksFromRaceList and keeps JRA placeholder", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1015",
        kaisai_kai: "3",
        kaisai_nen: "2026",
        kaisai_nichime: "8",
        kaisai_tsukihi: "0529",
        keibajo_code: "8",
        race_bango: "1",
        source: "jra",
      },
      {
        hasso_jikoku: "1430",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
    ],
  });
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([
    {
      babaCode: "36",
      raceNumber: "08",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=36",
    },
  ]);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([
    {
      debaUrl: "https://www.jra.go.jp/",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "08",
      oddsLinksJson: "{}",
      raceBango: "01",
      raceKey: "jra:2026:0529:08:01",
      raceStartAtJst: "2026-05-29T10:15:00+09:00",
      source: "jra",
    },
    {
      debaUrl:
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=36",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      oddsLinksJson: "{}",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      raceStartAtJst: "2026-05-29T14:30:00+09:00",
      source: "nar",
    },
  ]);
  expect(vi.mocked(buildRaceListUrl)).toHaveBeenCalledWith("20260529", "36");
  expect(vi.mocked(fetchRaceLinksFromRaceList)).toHaveBeenCalledWith(
    "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=20260529&k_babaCode=36",
  );
});

it("listTodayRacesFromHyperdrive fetches NAR venue race list once per venue using NAR babaCode mapped from keibajoCode", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1430",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
      {
        hasso_jikoku: "1500",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "09",
        source: "nar",
      },
    ],
  });
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([
    {
      babaCode: "36",
      raceNumber: "08",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=36",
    },
    {
      babaCode: "36",
      raceNumber: "09",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=9&k_babaCode=36",
    },
  ]);
  const env = buildEnv();
  await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(vi.mocked(fetchRaceLinksFromRaceList)).toHaveBeenCalledTimes(1);
  expect(vi.mocked(buildRaceListUrl)).toHaveBeenCalledWith("20260529", "36");
  expect(vi.mocked(fetchRaceLinksFromRaceList)).toHaveBeenCalledWith(
    "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=20260529&k_babaCode=36",
  );
});

it("listTodayRacesFromHyperdrive skips NAR rows whose per-race deba URL is missing from venue HTML", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1430",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
      {
        hasso_jikoku: "1500",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "12",
        source: "nar",
      },
    ],
  });
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([
    {
      babaCode: "36",
      raceNumber: "08",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=36",
    },
  ]);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([
    {
      debaUrl:
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=36",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      oddsLinksJson: "{}",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      raceStartAtJst: "2026-05-29T14:30:00+09:00",
      source: "nar",
    },
  ]);
  expect(warnSpy).toHaveBeenCalled();
});

it("listTodayRacesFromHyperdrive skips all NAR rows when venue HTML fetch throws", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1430",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
    ],
  });
  vi.mocked(fetchRaceLinksFromRaceList).mockRejectedValue(new Error("network down"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
  expect(warnSpy).toHaveBeenCalled();
});

it("listTodayRacesFromHyperdrive uses injected resolveNarDebaUrl when provided", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1430",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
    resolveNarDebaUrl: async () => "https://example.com/injected-deba",
  });
  expect(rows).toStrictEqual([
    {
      debaUrl: "https://example.com/injected-deba",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      oddsLinksJson: "{}",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      raceStartAtJst: "2026-05-29T14:30:00+09:00",
      source: "nar",
    },
  ]);
  expect(vi.mocked(fetchRaceLinksFromRaceList)).not.toHaveBeenCalled();
});

it("listTodayRacesFromHyperdrive binds kaisaiNen and kaisaiTsukihi extracted from yyyymmdd", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const env = buildEnv();
  await listTodayRacesFromHyperdrive(env, "20260529", { pool: { query } as never });
  expect(query).toHaveBeenCalledWith(expect.any(String), ["2026", "0529"]);
});

it("listTodayRacesFromHyperdrive returns empty array when no rows match", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
});

it("listTodayRacesFromHyperdrive skips rows with unknown source", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1015",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "unknown",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
});

it("listTodayRacesFromHyperdrive skips rows with missing string columns", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1015",
        kaisai_kai: null,
        kaisai_nen: null,
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
      {
        hasso_jikoku: "1015",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: undefined,
        source: "nar",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
});

it("listTodayRacesFromHyperdrive skips rows without a valid hasso_jikoku", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: null,
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
      {
        hasso_jikoku: "abcd",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "09",
        source: "nar",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
});

it("listTodayRacesFromHyperdrive falls back to getHotPool when context.pool absent", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  vi.mocked(getHotPool).mockReturnValueOnce({ query } as never);
  const env = buildEnv();
  await listTodayRacesFromHyperdrive(env, "20260529");
  expect(getHotPool).toHaveBeenCalledWith(env);
  expect(query).toHaveBeenCalledWith(expect.any(String), ["2026", "0529"]);
});

it("listTodayRacesFromHyperdrive resolves correctly when keibajoCode is 47 (NAR Kasamatsu) by mapping to babaCode 23", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1620",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "47",
        race_bango: "05",
        source: "nar",
      },
    ],
  });
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([
    {
      babaCode: "23",
      raceNumber: "05",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=5&k_babaCode=23",
    },
  ]);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(vi.mocked(buildRaceListUrl)).toHaveBeenCalledWith("20260529", "23");
  expect(vi.mocked(fetchRaceLinksFromRaceList)).toHaveBeenCalledWith(
    "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=20260529&k_babaCode=23",
  );
  expect(rows).toStrictEqual([
    {
      debaUrl:
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=5&k_babaCode=23",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "47",
      oddsLinksJson: "{}",
      raceBango: "05",
      raceKey: "nar:2026:0529:47:05",
      raceStartAtJst: "2026-05-29T16:20:00+09:00",
      source: "nar",
    },
  ]);
});

it("listTodayRacesFromHyperdrive skips NAR rows when keibajoCode is unknown (not in LOCAL_KEIBAJO_TO_NAR_BABA_CODE)", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1430",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "99",
        race_bango: "08",
        source: "nar",
      },
    ],
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
  expect(vi.mocked(fetchRaceLinksFromRaceList)).not.toHaveBeenCalled();
  expect(vi.mocked(buildRaceListUrl)).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalled();
});

it("populateTodayOddsFetchState upserts each row into D1 and invalidates race-list KV cache", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1015",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
      {
        hasso_jikoku: "1430",
        kaisai_kai: "3",
        kaisai_nen: "2026",
        kaisai_nichime: "8",
        kaisai_tsukihi: "0529",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
  });
  const env = buildEnv();
  const result = await populateTodayOddsFetchState(env, new Date("2026-05-28T20:55:00Z"), {
    pool: { query } as never,
    resolveNarDebaUrl: async () =>
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=30",
  });
  expect(result).toStrictEqual({ inserted: 2, total: 2 });
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.delete)).toHaveBeenCalledWith("odds:race-list:v1:nar:20260529");
  expect(vi.mocked(env.ODDS_HOT_KV.delete)).toHaveBeenCalledWith("odds:race-list:v1:jra:20260529");
});

it("populateTodayOddsFetchState returns zero when Hyperdrive yields no rows", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const env = buildEnv();
  const result = await populateTodayOddsFetchState(env, new Date("2026-05-28T20:55:00Z"), {
    pool: { query } as never,
  });
  expect(result).toStrictEqual({ inserted: 0, total: 0 });
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).not.toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.delete)).not.toHaveBeenCalled();
});

it("populateTodayOddsFetchState propagates D1 upsert errors", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1015",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
        source: "nar",
      },
    ],
  });
  const failingRun = vi.fn(async () => {
    throw new Error("D1 upsert failed");
  });
  const env = {
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: buildKv(),
    REALTIME_HOT_DB: buildDb(failingRun),
  } as unknown as Env;
  await expect(
    populateTodayOddsFetchState(env, new Date("2026-05-28T20:55:00Z"), {
      pool: { query } as never,
      resolveNarDebaUrl: async () =>
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=30",
    }),
  ).rejects.toThrowError("D1 upsert failed");
});
