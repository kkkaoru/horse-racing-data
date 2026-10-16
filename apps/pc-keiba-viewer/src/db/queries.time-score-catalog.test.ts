// Run with bun. The time-score Catalog branch must not touch PostgreSQL.
import { beforeEach, expect, it, vi } from "vitest";

import published from "../fixtures/overseas-published.json";
import type { CatalogRaceHistoryQuery, RaceHistoryRow } from "../lib/race-history-catalog";
import type { RaceDetail, SimilarRaceStatsSettings } from "../lib/race-types";

interface SourceCase {
  source: RaceDetail["source"];
  scope: SimilarRaceStatsSettings["sourceScope"];
  targetSource: RaceDetail["source"];
}

const mocks = vi.hoisted(() => ({
  runners: vi.fn<(binding: unknown, query: unknown) => Promise<unknown[]>>(),
  history: vi.fn<(binding: unknown, query: CatalogRaceHistoryQuery) => Promise<RaceHistoryRow[]>>(),
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
      sourceHorseId: "2021106753",
      jockeyNameFull: "ルメール",
    },
  ]);
  mocks.history.mockImplementation(async (_binding, query) =>
    query.source === "jra" ? [historyRow] : [],
  );
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
    source: "jra",
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

it("versions the repaired query while keeping race and settings cache key parts", async () => {
  await getTimeScoreRows(race, settings);
  const key: readonly unknown[] | undefined = mocks.keys.mock.calls[0]?.[0];
  expect(key?.[0]).toBe("getTimeScoreRows");
  expect(key?.[1]).toBe("v6-overseas-source-mapping");
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
        sourceHorseId: "2021106753",
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
  mocks.history.mockImplementation(async (_binding, query) => {
    order.push(query.source);
    return query.source === "jra" ? [historyRow] : [];
  });
  mocks.overseas.mockImplementation(async () => {
    order.push("overseas");
    return [];
  });
  await getTimeScoreRows(race, settings);
  expect(order).toStrictEqual(["profile", "runners", "jra", "nar", "overseas"]);
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

it.each<SourceCase>([
  { source: "jra", scope: "all", targetSource: "jra" },
  { source: "jra", scope: "jra", targetSource: "jra" },
  { source: "jra", scope: "nar", targetSource: "nar" },
  { source: "nar", scope: "all", targetSource: "nar" },
  { source: "nar", scope: "jra", targetSource: "jra" },
  { source: "nar", scope: "nar", targetSource: "nar" },
])(
  "retains both domestic histories for $source runners with $scope scope",
  async ({ source, scope, targetSource }) => {
    mocks.history.mockImplementation(async (_binding, query) =>
      query.source === "jra"
        ? [{ ...historyRow, bataiju: "400" }]
        : [{ ...historyRow, keibajoCode: "35", bataiju: "500" }],
    );
    const rows = await getTimeScoreRows({ ...race, source }, { ...settings, sourceScope: scope });
    expect(mocks.runners).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ source }),
    );
    expect(mocks.profile).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ source: targetSource }),
    );
    expect(mocks.history.mock.calls.map((call) => call[1])).toStrictEqual([
      {
        source: "jra",
        horseIds: ["2021106753"],
        beforeDate: "20260920",
        minDate: null,
        limit: 4000,
      },
      {
        source: "nar",
        horseIds: ["2021106753"],
        beforeDate: "20260920",
        minDate: null,
        limit: 4000,
      },
    ]);
    expect(rows[0]?.details[4]?.value).toBe(450);
    expect(mocks.overseas).toHaveBeenCalledTimes(1);
    expect(mocks.db).not.toHaveBeenCalled();
  },
);

it("reads ban-ei prior time from NAR without introducing another source", async () => {
  mocks.history.mockImplementation(async (_binding, query) =>
    query.source === "nar"
      ? [
          {
            ...historyRow,
            keibajoCode: "83",
            kyori: "200",
            sohaTime: "2100",
          },
        ]
      : [],
  );
  const rows = await getTimeScoreRows(
    { ...race, source: "nar", keibajoCode: "83", trackCode: "90", kyori: "200" },
    { ...settings, sourceScope: "all" },
  );
  expect(rows[0]?.details[0]?.value).toBe(1300);
  expect(mocks.history.mock.calls.map((call) => call[1].source)).toStrictEqual(["jra", "nar"]);
  expect(mocks.profile).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ source: "nar" }),
  );
});

it("keeps a genuine no-history entrant with null metrics and neutral scores", async () => {
  mocks.history.mockResolvedValue([]);
  const rows = await getTimeScoreRows(race, settings);
  expect(rows).toHaveLength(1);
  expect(rows[0]?.score).toBe(0.5);
  expect(rows[0]?.details.map((detail) => detail.value)).toStrictEqual([
    null,
    null,
    null,
    null,
    null,
    null,
    null,
  ]);
  expect(rows[0]?.details.map((detail) => detail.score)).toStrictEqual([
    0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5,
  ]);
  expect(mocks.history).toHaveBeenCalledTimes(2);
});

it("keeps horse 06's recorded NAR weight without inventing a zero-time result", async () => {
  mocks.history.mockImplementation(async (_binding, query) =>
    query.source === "nar"
      ? [
          {
            ...historyRow,
            umaban: "06",
            keibajoCode: "35",
            kaisaiTsukihi: "0809",
            sohaTime: "0000",
            bataiju: "427",
            kakuteiChakujun: "00",
          },
        ]
      : [],
  );
  const rows = await getTimeScoreRows({ ...race, source: "nar", keibajoCode: "35" }, settings);
  expect(rows[0]?.details[0]?.value).toBeNull();
  expect(rows[0]?.details[0]?.score).toBe(0.5);
  expect(rows[0]?.details[4]?.value).toBe(427);
});

it.each(["jra", "nar"])(
  "rejects a failed %s history instead of scoring partial history",
  async (source) => {
    mocks.history.mockImplementation(async (_binding, query) => {
      if (query.source === source) throw new Error("Catalog race history unavailable");
      return [historyRow];
    });
    await expect(getTimeScoreRows(race, settings)).rejects.toThrow(
      "Catalog race history unavailable",
    );
    expect(mocks.history).toHaveBeenCalledTimes(2);
    expect(mocks.db).not.toHaveBeenCalled();
  },
);

it("propagates an overseas history failure", async () => {
  mocks.overseas.mockRejectedValue(new Error("Catalog overseas race history unavailable"));
  await expect(getTimeScoreRows(race, settings)).rejects.toThrow(
    "Catalog overseas race history unavailable",
  );
  expect(mocks.db).not.toHaveBeenCalled();
});

it("selects all sixteen real saved histories and prefers domestic evidence for overlapping horses", async () => {
  mocks.runners.mockResolvedValue(
    published.mappings.map((mapping) => ({
      umaban: mapping.umaban,
      kettoTorokuBango: published.runners.find((runner) => runner.umaban === mapping.umaban)
        ?.ketto_toroku_bango,
      bamei: "Saved runner",
      barei: "4",
      sourceHorseId: mapping.source_horse_id,
    })),
  );
  mocks.overseas.mockResolvedValue(published.histories);
  mocks.history.mockImplementation(async (_binding, query) =>
    query.source === "jra"
      ? [
          { ...historyRow, kettoTorokuBango: "2021105369" },
          { ...historyRow, kettoTorokuBango: "2021103272" },
        ]
      : [],
  );
  const rows = await getTimeScoreRows(
    { ...race, kaisaiTsukihi: "1004", raceBango: "05", kyori: "2400" },
    settings,
  );
  expect(mocks.overseas).toHaveBeenCalledWith(expect.anything(), {
    horseIds: [
      "000a029d42",
      "000a02cbc8",
      "000a02d860",
      "2021105369",
      "2021103272",
      "000a02bb45",
      "000a024a34",
      "000a029d4f",
      "000a024a0d",
      "000a024841",
      "000a02d8c0",
      "000a02caba",
      "000a02daf6",
      "000a02d68d",
      "000a02d40c",
      "000a02ca5a",
    ],
    beforeDate: "20261004",
    minDate: null,
    limit: 4000,
  });
  expect(mocks.history.mock.calls.map((call) => call[1].horseIds)).toStrictEqual([
    ["2021105369", "2021103272"],
    ["2021105369", "2021103272"],
  ]);
  expect(rows).toHaveLength(16);
  expect(rows.find((row) => row.horseNumber === "1")?.historyEvidence).toStrictEqual({
    generation: "overseas-readers-v1",
    historyHorseId: "000a029d42",
    overseasHistoryCount: 11,
    selectedHistoryCount: 11,
    selection: "overseas",
  });
  expect(rows.find((row) => row.horseNumber === "4")?.historyEvidence).toStrictEqual({
    generation: "overseas-readers-v1",
    historyHorseId: "2021105369",
    overseasHistoryCount: 15,
    selectedHistoryCount: 1,
    selection: "domestic-preferred",
  });
  expect(rows.find((row) => row.horseNumber === "5")?.historyEvidence).toStrictEqual({
    generation: "overseas-readers-v1",
    historyHorseId: "2021103272",
    overseasHistoryCount: 17,
    selectedHistoryCount: 1,
    selection: "domestic-preferred",
  });
  expect(
    rows
      .filter((row) => !["4", "5"].includes(row.horseNumber))
      .every(
        (row) =>
          row.details[0]?.value === null &&
          row.details[1]?.value === null &&
          row.details[3]?.value === null &&
          row.details[4]?.value === null,
      ),
  ).toBe(true);
  expect(published.histories).toHaveLength(173);
  expect(
    rows
      .map((row) => row.historyEvidence)
      .every(
        (evidence) =>
          typeof evidence === "object" &&
          evidence !== null &&
          "selectedHistoryCount" in evidence &&
          typeof evidence.selectedHistoryCount === "number" &&
          evidence.selectedHistoryCount > 0,
      ),
  ).toBe(true);
});
it("aliases different provider IDs onto existing domestic evidence without double weighting", async () => {
  mocks.runners.mockResolvedValue([
    {
      umaban: "01",
      kettoTorokuBango: "2021106753",
      bamei: "Mapped",
      barei: "4",
      sourceHorseId: "000a029d42",
    },
  ]);
  mocks.overseas.mockResolvedValue(
    published.histories.filter((row) => row.sourceHorseId === "000a029d42"),
  );
  const rows = await getTimeScoreRows(race, settings);
  expect(rows[0]?.historyEvidence).toMatchObject({
    historyHorseId: "000a029d42",
    selectedHistoryCount: 1,
    overseasHistoryCount: 11,
    selection: "domestic-preferred",
  });
});
it("rejects a malformed provider ID instead of sending it to either history reader", async () => {
  mocks.runners.mockResolvedValue([
    { umaban: "01", kettoTorokuBango: "0000000000", sourceHorseId: "000a'bad42" },
  ]);
  await expect(getTimeScoreRows(race, settings)).rejects.toThrow(
    "Invalid overseas provider horse identity",
  );
  expect(mocks.history).not.toHaveBeenCalled();
  expect(mocks.overseas).not.toHaveBeenCalled();
});
it("records a successful empty overseas history as unknown evidence", async () => {
  mocks.runners.mockResolvedValue([
    {
      umaban: "01",
      kettoTorokuBango: "0000000000",
      bamei: "No history",
      barei: "4",
      sourceHorseId: "000a029d42",
    },
  ]);
  const rows = await getTimeScoreRows(race, settings);
  expect(rows[0]?.historyEvidence).toStrictEqual({
    generation: "overseas-readers-v1",
    historyHorseId: "000a029d42",
    selectedHistoryCount: 0,
    overseasHistoryCount: 0,
    selection: "overseas",
  });
  expect(rows[0]?.details.map((detail) => detail.value)).toStrictEqual([
    null,
    null,
    null,
    null,
    null,
    null,
    null,
  ]);
  expect(mocks.history).not.toHaveBeenCalled();
});

it("retains the domestic cache version and incumbent domestic history selection", async () => {
  mocks.runners.mockResolvedValue([
    {
      umaban: "01",
      kettoTorokuBango: "2021106753",
      bamei: "Domestic",
      barei: "4",
      sourceHorseId: null,
    },
  ]);
  mocks.overseas.mockResolvedValue([
    { sourceHorseId: "2021106753", raceDate: "2026-04-26", distanceMetres: 1600 },
  ]);
  const rows = await getTimeScoreRows({ ...race, keibajoCode: "05" }, settings);
  expect(mocks.keys.mock.calls[0]?.[0]?.[1]).toBe("v5-catalog-history-fields");
  expect(mocks.overseas).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ horseIds: ["2021106753"] }),
  );
  expect(rows[0]?.historyEvidence).toBeUndefined();
});
