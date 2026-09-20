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
        surface: "芝",
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

it("maps owner venue and exact triples without merging different owners", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([
    {
      umaban: 1,
      surface: "芝",
      jockey_id: "j1",
      trainer_id: "t1",
      owner_id: "o1",
      owner_name: "Owner",
    },
    { umaban: 2, surface: "芝", jockey_id: "j1", trainer_id: "t1", owner_id: "o2" },
    { umaban: 3, surface: "芝", owner_id: "o1", owner_name: "Owner" },
  ]);
  vi.mocked(input.cache.load).mockImplementation(async ({ query }) => ({
    targetRaces: [],
    values:
      query.scope.kind === "ownerVenue"
        ? [{ partner_id: "o1", jockey_id: "owner", starts: 10, wins: 1, places: 2, shows: 3 }]
        : [
            {
              partner_id: "t1",
              jockey_id: "j1",
              owner_id: "o1",
              starts: 4,
              wins: 1,
              places: 2,
              shows: 3,
            },
          ],
  }));
  expect(
    (await loadPartnershipStats(input))?.filter(
      (row) => row.kind === "ownerVenue" || row.kind === "jockeyTrainerOwner",
    ),
  ).toStrictEqual([
    { kind: "ownerVenue", umaban: 1, name: "Owner", starts: 10, wins: 1, places: 2, shows: 3 },
    { kind: "ownerVenue", umaban: 2, name: "不明", starts: 0, wins: 0, places: 0, shows: 0 },
    { kind: "ownerVenue", umaban: 3, name: "Owner", starts: 10, wins: 1, places: 2, shows: 3 },
    {
      kind: "jockeyTrainerOwner",
      umaban: 1,
      name: "不明 × 不明 × Owner",
      starts: 4,
      wins: 1,
      places: 2,
      shows: 3,
    },
    {
      kind: "jockeyTrainerOwner",
      umaban: 2,
      name: "不明 × 不明 × 不明",
      starts: 0,
      wins: 0,
      places: 0,
      shows: 0,
    },
  ]);
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
  expect(input.cache.load).toHaveBeenCalledTimes(5);
});

it("returns unavailable when a shared cohort is not warmed", async () => {
  const input = request();
  vi.mocked(input.cache.load).mockResolvedValueOnce(null);
  expect(await loadPartnershipStats(input)).toBeNull();
});

it("omits unavailable identities instead of reporting fictitious zero rates", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([
    { umaban: "01", surface: "芝" },
    { umaban: "00", jockey_id: "j1", surface: "芝" },
  ]);
  expect(await loadPartnershipStats(input)).toStrictEqual([]);
  expect(input.cache.load).toHaveBeenCalledTimes(4);
});

it("uses explicit unknown labels while retaining known entity counts", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([
    { umaban: "01", horse_id: "2023100001", jockey_id: "j1", trainer_id: "t1", surface: "芝" },
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

it.each(["芝", "ダート", "障害", "ばんえい"])(
  "derives %s from target race metadata for both venue cohorts",
  async (surface) => {
    const input = request();
    vi.mocked(input.cohort.execute).mockResolvedValue([{ umaban: "01", surface }]);
    await loadPartnershipStats(input);
    expect(input.cache.load).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        query: expect.objectContaining({
          scope: expect.objectContaining({ kind: "jockeyVenue", surface }),
        }),
      }),
    );
    expect(input.cache.load).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        query: expect.objectContaining({
          scope: expect.objectContaining({ kind: "jockeyTrainerVenue", surface }),
        }),
      }),
    );
  },
);

it("does not publish zero counts when target metadata is absent", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([]);
  expect(await loadPartnershipStats(input)).toBeNull();
  expect(input.cache.load).not.toHaveBeenCalled();
});

it("rejects unknown target surface instead of mixing all history", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([{ umaban: "01" }]);
  await expect(loadPartnershipStats(input)).rejects.toThrow("target surface is unavailable");
  expect(input.cache.load).not.toHaveBeenCalled();
});

it("rejects conflicting target race metadata", async () => {
  const input = request();
  vi.mocked(input.cohort.execute).mockResolvedValue([
    { umaban: "01", surface: "芝" },
    { umaban: "02", surface: "障害" },
  ]);
  await expect(loadPartnershipStats(input)).rejects.toThrow("surfaces are inconsistent");
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
