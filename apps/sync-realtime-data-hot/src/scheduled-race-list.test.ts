// Run with: bun run --filter sync-realtime-data-hot test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("./postgres-pool", () => ({
  getHotPool: vi.fn(),
}));

vi.mock("./jra-overseas", () => ({
  createJraOverseasRaceResolver: vi.fn(() => async () => null),
  resolveKnownOverseasEntryUrl: vi.fn((raceKey: string) =>
    raceKey === "jra:2026:0816:A8:04"
      ? "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
      : null,
  ),
}));

vi.mock("./keiba-go", () => ({
  fetchRaceLinksFromRaceList: vi.fn(async () => []),
  fetchTodayRaceListUrls: vi.fn(async (targetDate: string) => [
    {
      babaCode: "23",
      url: `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=${targetDate}&k_babaCode=23`,
    },
    {
      babaCode: "36",
      url: `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=${targetDate}&k_babaCode=36`,
    },
  ]),
}));

import { fetchRaceLinksFromRaceList, fetchTodayRaceListUrls } from "./keiba-go";
import { getHotPool } from "./postgres-pool";
import {
  listTodayRacesFromHyperdrive,
  populateMultiDayOddsFetchState,
  populateTodayOddsFetchState,
} from "./scheduled-race-list";
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
  vi.mocked(fetchTodayRaceListUrls).mockReset();
  vi.mocked(fetchTodayRaceListUrls).mockImplementation(async (targetDate: string) => [
    {
      babaCode: "23",
      url: `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=${targetDate}&k_babaCode=23`,
    },
    {
      babaCode: "36",
      url: `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=${targetDate}&k_babaCode=36`,
    },
  ]);
});

it("listTodayRacesFromHyperdrive resolves NAR per-race deba URL via fetchRaceLinksFromRaceList and builds JRA entry URL via netkeiba checksum", async () => {
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
  vi.mocked(fetchTodayRaceListUrls).mockResolvedValue([
    {
      babaCode: "36",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F29&k_babaCode=36",
    },
  ]);
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
      debaUrl: "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0108202603080120260529/86",
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
  expect(vi.mocked(fetchRaceLinksFromRaceList)).toHaveBeenCalledWith(
    "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F29&k_babaCode=36",
  );
});

it("listTodayRacesFromHyperdrive skips JRA rows when kaisai_kai is null because URL builder returns null", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1015",
        kaisai_kai: null,
        kaisai_nen: "2026",
        kaisai_nichime: "8",
        kaisai_tsukihi: "0529",
        keibajo_code: "8",
        race_bango: "1",
        source: "jra",
      },
    ],
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
  expect(warnSpy).toHaveBeenCalled();
});

it("listTodayRacesFromHyperdrive skips JRA rows when kaisai_nichime is null because URL builder returns null", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1015",
        kaisai_kai: "3",
        kaisai_nen: "2026",
        kaisai_nichime: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "8",
        race_bango: "1",
        source: "jra",
      },
    ],
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
  expect(warnSpy).toHaveBeenCalled();
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
  expect(vi.mocked(fetchRaceLinksFromRaceList)).toHaveBeenCalledWith(
    "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=20260529&k_babaCode=36",
  );
});

it("listTodayRacesFromHyperdrive falls back to Hyperdrive-synthesized deba URL for NAR rows missing from venue HTML", async () => {
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
    {
      debaUrl:
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=12&k_babaCode=36",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      oddsLinksJson: "{}",
      raceBango: "12",
      raceKey: "nar:2026:0529:30:12",
      raceStartAtJst: "2026-05-29T15:00:00+09:00",
      source: "nar",
    },
  ]);
  expect(vi.mocked(fetchRaceLinksFromRaceList)).toHaveBeenCalledWith(
    "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=20260529&k_babaCode=36",
  );
});

it("listTodayRacesFromHyperdrive falls back to Hyperdrive deba URL for all NAR rows when venue HTML fetch throws", async () => {
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
});

it("listTodayRacesFromHyperdrive does not fetch RaceList when the venue is absent from TodayRaceInfoTop", async () => {
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
  vi.mocked(fetchTodayRaceListUrls).mockResolvedValue([]);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(vi.mocked(fetchRaceLinksFromRaceList)).not.toHaveBeenCalled();
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
});

it("listTodayRacesFromHyperdrive writes a populate-nar-venue empty fetch_log when venue HTML returns zero race links", async () => {
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
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([]);
  const logBind = vi.fn((..._args: unknown[]) => ({
    run: vi.fn(async () => ({ meta: { changes: 1 } })),
  }));
  const logPrepare = vi.fn(() => ({ bind: logBind }));
  const env = {
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: buildKv(),
    REALTIME_HOT_DB: { prepare: logPrepare } as unknown as D1Database,
  } as unknown as Env;
  await listTodayRacesFromHyperdrive(env, "20260529", { pool: { query } as never });
  expect(logPrepare).toHaveBeenCalledWith(
    "insert into fetch_logs (race_key, job_type, status, message, created_at) values (?, ?, ?, ?, ?)",
  );
  const args = logBind.mock.calls[0];
  expect(args?.[0]).toBe("nar:20260529:30");
  expect(args?.[1]).toBe("populate-nar-venue");
  expect(args?.[2]).toBe("empty");
});

it("listTodayRacesFromHyperdrive writes a populate-nar-venue error fetch_log when venue HTML fetch throws", async () => {
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
  const logBind = vi.fn((..._args: unknown[]) => ({
    run: vi.fn(async () => ({ meta: { changes: 1 } })),
  }));
  const logPrepare = vi.fn(() => ({ bind: logBind }));
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = {
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: buildKv(),
    REALTIME_HOT_DB: { prepare: logPrepare } as unknown as D1Database,
  } as unknown as Env;
  await listTodayRacesFromHyperdrive(env, "20260529", { pool: { query } as never });
  expect(logPrepare).toHaveBeenCalledWith(
    "insert into fetch_logs (race_key, job_type, status, message, created_at) values (?, ?, ?, ?, ?)",
  );
  const args = logBind.mock.calls[0];
  expect(args?.[0]).toBe("nar:20260529:30");
  expect(args?.[1]).toBe("populate-nar-venue");
  expect(args?.[2]).toBe("error");
});

it("listTodayRacesFromHyperdrive swallows logFetch failure during populate-nar-venue empty path and still produces a fallback row", async () => {
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
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([]);
  const failingRun = vi.fn(async () => {
    throw new Error("D1 log insert failed");
  });
  const logBind = vi.fn(() => ({ run: failingRun }));
  const logPrepare = vi.fn(() => ({ bind: logBind }));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = {
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: buildKv(),
    REALTIME_HOT_DB: { prepare: logPrepare } as unknown as D1Database,
  } as unknown as Env;
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

it("listTodayRacesFromHyperdrive keeps a domestic JRA row on the checksum URL path", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "1535",
        kaisai_kai: "02",
        kaisai_nen: "2026",
        kaisai_nichime: "05",
        kaisai_tsukihi: "0509",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260509", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([
    {
      debaUrl: "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0105202602050120260509/6A",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0509",
      keibajoCode: "05",
      oddsLinksJson: "{}",
      raceBango: "01",
      raceKey: "jra:2026:0509:05:01",
      raceStartAtJst: "2026-05-09T15:35:00+09:00",
      source: "jra",
    },
  ]);
});

it("populateTodayOddsFetchState skips an unresolved overseas-shaped JRA row instead of upserting it", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "0000",
        kaisai_kai: "00",
        kaisai_nen: "2026",
        kaisai_nichime: "00",
        kaisai_tsukihi: "0725",
        keibajo_code: "A6",
        kyosomei_hondai: "キングジョージ６世＆クイーンエリザベスステークス",
        race_bango: "05",
        source: "jra",
      },
    ],
  });
  const env = buildEnv();
  const result = await populateTodayOddsFetchState(env, new Date("2026-07-24T15:05:00Z"), {
    pool: { query } as never,
    resolveJraOverseasRace: async () => null,
  });
  expect(result).toStrictEqual({ inserted: 0, total: 0 });
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).not.toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.delete)).not.toHaveBeenCalled();
});

it("listTodayRacesFromHyperdrive resolves an overseas JRA URL and real post time through the official resolver", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "0000",
        kaisai_kai: "00",
        kaisai_nen: "2026",
        kaisai_nichime: "00",
        kaisai_tsukihi: "0725",
        keibajo_code: "A6",
        kyosomei_hondai: "キングジョージ６世＆クイーンエリザベスステークス　　　　　　",
        race_bango: "05",
        source: "jra",
      },
    ],
  });
  const resolveJraOverseasRace = vi.fn(async () => ({
    debaUrl: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32",
    raceStartAtJst: "2026-07-25T23:35:00+09:00",
  }));
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260725", {
    pool: { query } as never,
    resolveJraOverseasRace,
  });
  expect(rows).toStrictEqual([
    {
      debaUrl: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0725",
      keibajoCode: "A6",
      oddsLinksJson: "{}",
      raceBango: "05",
      raceKey: "jra:2026:0725:A6:05",
      raceStartAtJst: "2026-07-25T23:35:00+09:00",
      source: "jra",
    },
  ]);
  expect(resolveJraOverseasRace).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0725",
    kyosomeiHondai: "キングジョージ６世＆クイーンエリザベスステークス",
  });
});

it("listTodayRacesFromHyperdrive skips an overseas JRA row when official resolution throws", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "0000",
        kaisai_kai: "00",
        kaisai_nen: "2026",
        kaisai_nichime: "00",
        kaisai_tsukihi: "0725",
        keibajo_code: "A6",
        kyosomei_hondai: "キングジョージ６世＆クイーンエリザベスステークス",
        race_bango: "05",
        source: "jra",
      },
    ],
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260725", {
    pool: { query } as never,
    resolveJraOverseasRace: async () => {
      throw new Error("official page unavailable");
    },
  });
  expect(rows).toStrictEqual([]);
  expect(warnSpy).toHaveBeenCalledWith(
    "[scheduled-race-list] JRA overseas race resolution failed, trying known entry URL raceKey=jra:2026:0725:A6:05: official page unavailable",
  );
});

it("listTodayRacesFromHyperdrive uses the known accessSD URL and JV post time when official resolve misses A8/04", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "2250",
        kaisai_kai: "00",
        kaisai_nen: "2026",
        kaisai_nichime: "00",
        kaisai_tsukihi: "0816",
        keibajo_code: "A8",
        kyosomei_hondai: "ジャックルマロワ賞　　　　　　　　　　　　　　　　　　　　　",
        race_bango: "04",
        source: "jra",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260816", {
    pool: { query } as never,
    resolveJraOverseasRace: async () => null,
  });
  expect(rows).toStrictEqual([
    {
      debaUrl: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0816",
      keibajoCode: "A8",
      oddsLinksJson: "{}",
      raceBango: "04",
      raceKey: "jra:2026:0816:A8:04",
      raceStartAtJst: "2026-08-16T22:50:00+09:00",
      source: "jra",
    },
  ]);
});

it("listTodayRacesFromHyperdrive skips overseas JRA rows without a race name", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        hasso_jikoku: "0000",
        kaisai_kai: "00",
        kaisai_nen: "2026",
        kaisai_nichime: "00",
        kaisai_tsukihi: "0725",
        keibajo_code: "A6",
        kyosomei_hondai: "   ",
        race_bango: "05",
        source: "jra",
      },
    ],
  });
  const resolveJraOverseasRace = vi.fn(async () => null);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260725", {
    pool: { query } as never,
    resolveJraOverseasRace,
  });
  expect(rows).toStrictEqual([]);
  expect(resolveJraOverseasRace).not.toHaveBeenCalled();
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

it("populateMultiDayOddsFetchState populates today plus the next two days by default and aggregates totals", async () => {
  const query = vi.fn().mockImplementation(async (_sql: string, params: string[]) => {
    if (params[0] === "2026" && params[1] === "0529") {
      return {
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
      };
    }
    if (params[0] === "2026" && params[1] === "0530") {
      return {
        rows: [
          {
            hasso_jikoku: "1430",
            kaisai_kai: null,
            kaisai_nen: "2026",
            kaisai_nichime: null,
            kaisai_tsukihi: "0530",
            keibajo_code: "30",
            race_bango: "09",
            source: "nar",
          },
          {
            hasso_jikoku: "1500",
            kaisai_kai: null,
            kaisai_nen: "2026",
            kaisai_nichime: null,
            kaisai_tsukihi: "0530",
            keibajo_code: "30",
            race_bango: "10",
            source: "nar",
          },
        ],
      };
    }
    return { rows: [] };
  });
  const env = buildEnv();
  const result = await populateMultiDayOddsFetchState(env, new Date("2026-05-28T20:55:00Z"), 2, {
    pool: { query } as never,
    resolveNarDebaUrl: async () => "https://example.com/multi-day-deba",
  });
  expect(result).toStrictEqual({
    inserted: 3,
    perDay: [
      { inserted: 1, total: 1, yyyymmdd: "20260529" },
      { inserted: 2, total: 2, yyyymmdd: "20260530" },
      { inserted: 0, total: 0, yyyymmdd: "20260531" },
    ],
    total: 3,
  });
});

it("populateMultiDayOddsFetchState with daysAhead=0 populates only today", async () => {
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
  const result = await populateMultiDayOddsFetchState(env, new Date("2026-05-28T20:55:00Z"), 0, {
    pool: { query } as never,
    resolveNarDebaUrl: async () => "https://example.com/multi-day-deba",
  });
  expect(result).toStrictEqual({
    inserted: 1,
    perDay: [{ inserted: 1, total: 1, yyyymmdd: "20260529" }],
    total: 1,
  });
});

it("populateMultiDayOddsFetchState defaults daysAhead to 2 yielding three days when omitted", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const env = buildEnv();
  const result = await populateMultiDayOddsFetchState(
    env,
    new Date("2026-05-28T20:55:00Z"),
    undefined,
    {
      pool: { query } as never,
    },
  );
  expect(result).toStrictEqual({
    inserted: 0,
    perDay: [
      { inserted: 0, total: 0, yyyymmdd: "20260529" },
      { inserted: 0, total: 0, yyyymmdd: "20260530" },
      { inserted: 0, total: 0, yyyymmdd: "20260531" },
    ],
    total: 0,
  });
});

it("listTodayRacesFromHyperdrive logs populate-nar-venue-hyperdrive-fallback fetch_log when fallback synthesizes a deba URL", async () => {
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
  vi.mocked(fetchRaceLinksFromRaceList).mockRejectedValue(new Error("CloudFront 404"));
  const logBind = vi.fn((..._args: unknown[]) => ({
    run: vi.fn(async () => ({ meta: { changes: 1 } })),
  }));
  const logPrepare = vi.fn(() => ({ bind: logBind }));
  const env = {
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: buildKv(),
    REALTIME_HOT_DB: { prepare: logPrepare } as unknown as D1Database,
  } as unknown as Env;
  await listTodayRacesFromHyperdrive(env, "20260529", { pool: { query } as never });
  const fallbackCall = logBind.mock.calls.find(
    (args) => args[1] === "populate-nar-venue-hyperdrive-fallback",
  );
  expect(fallbackCall?.[2]).toBe("fallback");
  expect(fallbackCall?.[0]).toBe("nar:20260529:30");
});

it("listTodayRacesFromHyperdrive primary keiba.go links win over Hyperdrive fallback when both produce URLs", async () => {
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
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValue([
    {
      babaCode: "36",
      raceNumber: "08",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=36&primary=1",
    },
  ]);
  const env = buildEnv();
  const rows = await listTodayRacesFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows[0]?.debaUrl).toBe(
    "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=8&k_babaCode=36&primary=1",
  );
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
