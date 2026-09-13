// Run with bun (bunx vitest).
import { expect, it, vi } from "vitest";
import { loadPartnershipStats, type PartnershipStatsRequest } from "./heatmap-partnership-stats";
import { buildPartnershipEntriesQuery } from "./heatmap-partnership-sql";

const request = (): PartnershipStatsRequest => ({
  raceBango: "01",
  cache: { load: vi.fn(async () => ({ targetRaces: [], values: [] })) },
  cohort: {
    warm: true,
    cache: {
      match: vi.fn(async () => undefined),
      put: vi.fn(async () => undefined),
      delete: vi.fn(async () => true),
    },
    kv: {
      get: vi.fn(async () => null),
      put: vi.fn(async () => undefined),
      delete: vi.fn(async () => undefined),
    },
    execute: vi.fn(async () => [
      {
        umaban: "01",
        horse_id: "2023100001",
        jockey_id: "j1",
        trainer_id: "t1",
        horse_name: "Horse",
        jockey_name: "Jockey",
        trainer_name: "Trainer",
      },
    ]),
    query: {
      config: {
        R2_SQL_ACCOUNT_ID: "account",
        R2_SQL_BUCKET_NAME: "bucket",
        R2_SQL_NAMESPACE: "ns",
        R2_SQL_TOKEN: "test",
      },
      horseIds: [],
      scope: {
        date: "20260913",
        keibajoCode: "06",
        kind: "jockeyVenue",
        source: "jra",
        revision: "snapshot",
      },
    },
  },
});

it("maps all three cohorts by stable identities and preserves genuine zero counts", async () => {
  const input = request();
  vi.mocked(input.cache.load)
    .mockResolvedValueOnce({
      targetRaces: [],
      values: [
        { partner_id: "2023100001", jockey_id: "j1", starts: "10", wins: 2, places: 3, shows: 4 },
      ],
    })
    .mockResolvedValueOnce({
      targetRaces: [],
      values: [{ partner_id: "j1", jockey_id: "j1", starts: 100, wins: 10, places: 20, shows: 30 }],
    })
    .mockResolvedValueOnce({ targetRaces: [], values: [] });
  expect(await loadPartnershipStats(input)).toStrictEqual([
    {
      kind: "horseJockey",
      umaban: 1,
      name: "Horse × Jockey",
      starts: 10,
      wins: 2,
      places: 3,
      shows: 4,
    },
    {
      kind: "jockeyVenue",
      umaban: 1,
      name: "Jockey",
      starts: 100,
      wins: 10,
      places: 20,
      shows: 30,
    },
    {
      kind: "jockeyTrainerVenue",
      umaban: 1,
      name: "Jockey × Trainer",
      starts: 0,
      wins: 0,
      places: 0,
      shows: 0,
    },
  ]);
  expect(input.cache.load).toHaveBeenCalledTimes(3);
});

it("returns unavailable when a shared cohort is not warmed", async () => {
  const input = request();
  vi.mocked(input.cache.load).mockResolvedValueOnce(null);
  expect(await loadPartnershipStats(input)).toBeNull();
});

it("omits unavailable identities instead of reporting fictitious zero rates", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([
    { umaban: "01" },
    { umaban: "00", jockey_id: "j1" },
  ]);
  expect(await loadPartnershipStats(input)).toStrictEqual([]);
  expect(input.cache.load).toHaveBeenCalledTimes(2);
});

it("uses explicit unknown labels while retaining known entity counts", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([
    { umaban: "01", horse_id: "2023100001", jockey_id: "j1", trainer_id: "t1" },
  ]);
  expect((await loadPartnershipStats(input))?.map((row) => row.name)).toStrictEqual([
    "不明 × 不明",
    "不明",
    "不明 × 不明",
  ]);
});

it.each([null, true, "", -1, "not-number", 1.5])(
  "rejects malformed count values: %j",
  async (starts) => {
    const input = request();
    vi.mocked(input.cache.load).mockResolvedValue({
      targetRaces: [],
      values: [{ partner_id: "j1", jockey_id: "j1", starts, wins: 0, places: 0, shows: 0 }],
    });
    await expect(loadPartnershipStats(input)).rejects.toThrow("count is malformed");
  },
);

it.each([
  { starts: 2, wins: 2, places: 1, shows: 2 },
  { starts: 2, wins: 0, places: 2, shows: 1 },
  { starts: 2, wins: 0, places: 1, shows: 3 },
])("rejects inconsistent counts: %j", async (counts) => {
  const input = request();
  vi.mocked(input.cache.load).mockResolvedValue({
    targetRaces: [],
    values: [{ partner_id: "j1", jockey_id: "j1", ...counts }],
  });
  await expect(loadPartnershipStats(input)).rejects.toThrow("counts are inconsistent");
});

it("validates the target race identity used for partnership lookups", () => {
  const input = request();
  expect(
    buildPartnershipEntriesQuery({
      config: input.cohort.query.config,
      scope: input.cohort.query.scope,
      raceBango: "01",
    }),
  ).toMatch("se.race_bango = '01'");
  expect(() =>
    buildPartnershipEntriesQuery({
      config: input.cohort.query.config,
      scope: input.cohort.query.scope,
      raceBango: "bad",
    }),
  ).toThrow("two digits");
});
