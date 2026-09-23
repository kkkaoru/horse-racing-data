// Run with bun. The time-score Catalog branch must not touch PostgreSQL.
import { beforeEach, expect, it, vi } from "vitest";

import type { RaceDetail, SimilarRaceStatsSettings } from "../lib/race-types";

const mocks = vi.hoisted(() => ({
  runners: vi.fn<(binding: unknown, query: unknown) => Promise<unknown[]>>(),
  history: vi.fn<(binding: unknown, query: unknown) => Promise<unknown[]>>(),
  overseas: vi.fn<(binding: unknown, query: unknown) => Promise<unknown[]>>(),
  profile: vi.fn<(binding: unknown, query: unknown) => Promise<unknown>>(),
  target: vi.fn<() => "cloudflare" | "local" | "neon">(),
  db: vi.fn<() => unknown>(() => {
    throw new Error("PostgreSQL must not be used");
  }),
  env: vi.fn<() => Promise<CloudflareEnv | undefined>>(),
  keys: vi.fn<(key: readonly unknown[]) => void>(),
}));
vi.mock("./client", () => ({ getDatabaseTarget: mocks.target, getDb: mocks.db }));
vi.mock("../lib/cloudflare-context.server", () => ({ safeGetCloudflareEnv: mocks.env }));
vi.mock("../lib/race-runners-catalog", () => ({ readCatalogRaceRunners: mocks.runners }));
vi.mock("../lib/race-history-catalog", () => ({ readCatalogRaceHistory: mocks.history }));
vi.mock("../lib/race-history-overseas-catalog", () => ({
  readCatalogOverseasRaceHistory: mocks.overseas,
}));
vi.mock("../lib/race-matched-profile-catalog", () => ({
  readCatalogRaceMatchedProfile: mocks.profile,
}));
vi.mock("./query-cache", () => ({
  withDbQueryCache: async <T>(key: readonly unknown[], load: () => Promise<T>): Promise<T> => {
    mocks.keys(key);
    return await load();
  },
}));
import { getTimeScoreRows } from "./queries";

const race: RaceDetail = {
  source: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0920",
  keibajoCode: "A8",
  raceBango: "08",
  kyosomeiHondai: "テストステークス",
  kyosomeiFukudai: null,
  gradeCode: "A",
  kyosoShubetsuCode: "01",
  kyosoKigoCode: null,
  juryoShubetsuCode: null,
  kyosoJokenCode: "999",
  kyosoJokenMeisho: "オープン",
  kyori: "1600",
  trackCode: "10",
  hassoJikoku: null,
  shussoTosu: null,
  kaisaiKai: null,
  kaisaiNichime: null,
  kyosomeiKakkonai: null,
  torokuTosu: null,
  tenkoCode: null,
  babajotaiCodeShiba: null,
  babajotaiCodeDirt: null,
};

const settings: SimilarRaceStatsSettings = {
  cellMatching: true,
  classConditionName: null,
  includeAge: true,
  includeBloodlineAncestors: false,
  includeClass: true,
  includeConditionKey: true,
  includeDistance: true,
  includeFrame: false,
  includeGrade: true,
  includeMonthWindow: true,
  includeNarOnly: false,
  includeRaceNumber: false,
  includeRaceSubtitle: false,
  includeRaceTitle: true,
  includeRunnerCount: true,
  includeSex: false,
  includeSurface: false,
  includeTrackCode: true,
  includeTurn: false,
  includeVenue: true,
  includeWeight: false,
  runnerCount: 16,
  sourceScope: "jra",
  years: 3,
};

const historyRow = {
  kettoTorokuBango: "2021106753",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0601",
  keibajoCode: "A8",
  raceBango: "08",
  umaban: "08",
  kyori: "1600",
  sohaTime: "1110",
  kohan3f: "0340",
  bataiju: "480",
  futanJuryo: "0550",
  timeSa: "0005",
  kakuteiChakujun: "01",
};

beforeEach(() => {
  vi.clearAllMocks();
  mocks.target.mockReturnValue("cloudflare");
  mocks.db.mockImplementation(() => {
    throw new Error("PostgreSQL must not be used");
  });
  mocks.env.mockResolvedValue({ R2_RACE_DETAIL: { fetch: vi.fn<typeof fetch>() } });
  mocks.runners.mockResolvedValue([
    {
      umaban: "08",
      kettoTorokuBango: "2021106753",
      bamei: "テストホース",
      barei: "4",
      sourceHorseId: null,
      jockeyNameFull: "ルメール",
    },
  ]);
  mocks.history.mockResolvedValue([historyRow]);
  mocks.overseas.mockResolvedValue([]);
  mocks.profile.mockResolvedValue({
    targetRaceTime: 710,
    targetLast3f: 340,
    targetBodyWeight: 480,
    targetCarriedWeight: 550,
    targetMargin: 5,
  });
});

it("scores from the Catalog without touching PostgreSQL", async () => {
  const rows = await getTimeScoreRows(race, settings);
  expect(rows).toHaveLength(1);
  expect(rows[0]?.horseNumber).toBe("8");
  expect(rows[0]?.horseName).toBe("テストホース");
  // The incumbent SQL emits no jockey, so the Catalog branch mirrors that.
  expect(rows[0]?.jockeyName).toBe("");
  expect(rows[0]?.score).toBe(1);
  expect(rows[0]?.details).toHaveLength(7);
  expect(rows[0]?.details.map((detail) => detail.label)).toStrictEqual([
    "レースタイム",
    "上がり3F",
    "距離適性",
    "競馬場",
    "馬体重",
    "負担重量",
    "着差",
  ]);
  expect(mocks.runners).toHaveBeenCalledWith(expect.anything(), {
    source: "jra",
    date: "20260920",
    keibajoCode: "A8",
    raceBango: "08",
  });
  expect(mocks.history).toHaveBeenCalledWith(expect.anything(), {
    horseIds: ["2021106753"],
    beforeDate: "20260920",
    minDate: null,
    limit: 4000,
  });
  expect(mocks.profile).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({
      date: "20260920",
      kyori: "1600",
      kyosoJokenCode: "999",
      years: "3",
      runnerCount: 16,
      flags: [
        "includeVenue",
        "includeDistance",
        "includeAge",
        "includeClass",
        "includeConditionKey",
        "includeTrackCode",
        "includeGrade",
        "includeRaceTitle",
        "includeMonthWindow",
        "includeRunnerCount",
      ],
    }),
  );
  expect(mocks.db).not.toHaveBeenCalled();
  expect(mocks.keys).toHaveBeenCalledWith(["getTimeScoreRows", expect.anything(), race, settings]);
});

it("keeps the cache key parts unchanged", async () => {
  await getTimeScoreRows(race, settings);
  const key: readonly unknown[] | undefined = mocks.keys.mock.calls[0]?.[0];
  expect(key?.[0]).toBe("getTimeScoreRows");
  expect(key?.[2]).toBe(race);
  expect(key?.[3]).toBe(settings);
});

it("drops a horse whose all-zero registration number never resolved", async () => {
  mocks.runners.mockResolvedValue([
    {
      umaban: "01",
      kettoTorokuBango: "0000000000",
      bamei: "未解決",
      barei: "4",
      sourceHorseId: null,
      jockeyNameFull: null,
    },
  ]);
  await expect(getTimeScoreRows({ ...race, raceBango: "09" }, settings)).resolves.toStrictEqual([]);
  // With no usable horse id there is nothing to look up.
  expect(mocks.history).not.toHaveBeenCalled();
  expect(mocks.overseas).not.toHaveBeenCalled();
});

it("merges overseas history under the mapped registration number", async () => {
  mocks.runners.mockResolvedValue([
    {
      umaban: "01",
      kettoTorokuBango: "0000000000",
      bamei: "海外馬",
      barei: "4",
      sourceHorseId: "2021999999",
      jockeyNameFull: null,
    },
  ]);
  mocks.history.mockResolvedValue([]);
  mocks.overseas.mockResolvedValue([
    { sourceHorseId: "2021999999", raceDate: "2026-04-26", distanceMetres: 1600 },
  ]);
  const rows = await getTimeScoreRows({ ...race, raceBango: "10" }, settings);
  expect(rows).toHaveLength(1);
  expect(mocks.overseas).toHaveBeenCalledWith(expect.anything(), {
    horseIds: ["2021999999"],
    beforeDate: "20260920",
    minDate: null,
    limit: 4000,
  });
  // The overseas row carries only a distance, so the metric sub-scores fall
  // back to 0.5 while distance and venue still score.
  expect(rows[0]?.details[0]?.score).toBe(0.5);
});

it("starts the matched profile before runners resolve and reads histories together", async () => {
  const order: string[] = [];
  mocks.runners.mockImplementation(async () => {
    order.push("runners");
    return [
      {
        umaban: "08",
        kettoTorokuBango: "2021106753",
        bamei: "テストホース",
        barei: "4",
        sourceHorseId: null,
        jockeyNameFull: "ルメール",
      },
    ];
  });
  mocks.profile.mockImplementation(async () => {
    order.push("profile");
    return {
      targetRaceTime: 710,
      targetLast3f: 340,
      targetBodyWeight: 480,
      targetCarriedWeight: 550,
      targetMargin: 5,
    };
  });
  mocks.history.mockImplementation(async () => {
    order.push("history");
    return [historyRow];
  });
  mocks.overseas.mockImplementation(async () => {
    order.push("overseas");
    return [];
  });
  await getTimeScoreRows(race, settings);
  expect(order).toStrictEqual(["profile", "runners", "history", "overseas"]);
});

it("rejects with the matched profile failure after runners resolve", async () => {
  mocks.profile.mockRejectedValue(new Error("Catalog matched profile unavailable"));
  await expect(getTimeScoreRows(race, settings)).rejects.toThrow(
    "Catalog matched profile unavailable",
  );
});

it("skips history reads when no runner has a usable horse id", async () => {
  mocks.runners.mockResolvedValue([
    {
      umaban: "08",
      kettoTorokuBango: "",
      bamei: "テストホース",
      barei: "4",
      sourceHorseId: null,
      jockeyNameFull: "ルメール",
    },
  ]);
  const rows = await getTimeScoreRows(race, settings);
  expect(rows).toStrictEqual([]);
  expect(mocks.history).not.toHaveBeenCalled();
  expect(mocks.overseas).not.toHaveBeenCalled();
});
