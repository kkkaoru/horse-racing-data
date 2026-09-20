// Runs with bun via Vitest; all Vectorize I/O is mocked.
import { expect, test, vi } from "vitest";
import {
  queryHistoricalVectors,
  upsertHistoricalVectors,
  type HistoricalVectorQuery,
} from "./vector-search";

const query: HistoricalVectorQuery = {
  dimensions: 8,
  earliestDate: "20230101",
  filters: { venue: "06" },
  namespace: "corner-v1",
  raceDate: "20260916",
  source: "jra",
  topK: 80,
  values: [0, 0, 0, 0, 0, 0, 0, 0],
};
const vector: VectorizeVector = {
  id: "entry-1",
  namespace: "corner-v1",
  values: [1, 0, 0, 0, 0, 0, 0, 0],
  metadata: { source: "jra", raceDate: "20260915", corner1: 0.2 },
};

const hydratedVector: VectorizeVector = {
  ...vector,
  values: [1, ...Array.from({ length: 31 }, () => 0)],
};

test("retrieves 80 candidates without metadata, hydrates and computes exact L2 weights", async () => {
  const index = {
    query: vi.fn().mockResolvedValue({ count: 1, matches: [{ id: "entry-1", score: 999 }] }),
    getByIds: vi.fn().mockResolvedValue([hydratedVector]),
  };
  expect(await queryHistoricalVectors(index, query)).toStrictEqual([
    {
      id: "entry-1",
      distance: 1,
      weight: 0.5,
      metadata: { source: "jra", raceDate: "20260915", corner1: 0.2 },
    },
  ]);
  expect(index.query).toHaveBeenCalledWith(
    [
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,
    ],
    {
      filter: { venue: "06", source: "jra", raceDate: { $gte: "20230101", $lt: "20260916" } },
      namespace: "corner-v1",
      returnMetadata: "none",
      returnValues: false,
      topK: 80,
    },
  );
  expect(index.getByIds).toHaveBeenCalledWith(["entry-1"]);
});

test("sorts hydrated neighbors by exact distance and stable identifier on ties", async () => {
  const index = {
    query: vi
      .fn()
      .mockResolvedValue({ count: 3, matches: [{ id: "z" }, { id: "b" }, { id: "a" }] }),
    getByIds: vi.fn().mockResolvedValue([
      { ...hydratedVector, id: "z", values: [2, ...Array.from({ length: 31 }, () => 0)] },
      { ...hydratedVector, id: "b" },
      { ...hydratedVector, id: "a" },
    ]),
  };
  expect((await queryHistoricalVectors(index, query)).map((neighbor) => neighbor.id)).toStrictEqual(
    ["a", "b", "z"],
  );
});

test("hydrates 80 finish neighbors in bounded 20-ID requests", async () => {
  const ids: string[] = Array.from({ length: 80 }, (_, index) => `v${index}`);
  const index = {
    query: vi.fn().mockResolvedValue({ count: 80, matches: ids.map((id) => ({ id, score: 1 })) }),
    getByIds: vi.fn().mockImplementation(async (batch: string[]) => {
      if (batch.length > 20) throw new Error("Hydration batch exceeds provider limit");
      return batch.map((id) => ({ ...hydratedVector, id }));
    }),
  };
  expect(await queryHistoricalVectors(index, query)).toHaveLength(80);
  expect(index.getByIds).toHaveBeenCalledTimes(4);
  expect(index.getByIds.mock.calls.map(([batch]) => batch.length)).toStrictEqual([20, 20, 20, 20]);
});

test("does not hydrate an empty match list", async () => {
  const index = { query: vi.fn().mockResolvedValue({ count: 0, matches: [] }), getByIds: vi.fn() };
  expect(await queryHistoricalVectors(index, query)).toStrictEqual([]);
  expect(index.getByIds).not.toHaveBeenCalled();
});

test.each<Partial<HistoricalVectorQuery>>([
  { namespace: "" },
  { namespace: "bad namespace" },
  { values: [1] },
  { values: [NaN, 0, 0, 0, 0, 0, 0, 0] },
  { raceDate: "not-date" },
  { raceDate: "20260230" },
  { raceDate: "20261301" },
  { earliestDate: "bad" },
  { earliestDate: "20260916" },
  { topK: 0 },
  { topK: 101 },
  { topK: 1.5 },
  { filters: { source: "nar" } },
  { filters: { raceDate: "20300101" } },
  { filters: { large: "x".repeat(2048) } },
])("rejects unsafe search inputs before I/O: %j", async (overrides) => {
  const index = { query: vi.fn(), getByIds: vi.fn() };
  await expect(queryHistoricalVectors(index, { ...query, ...overrides })).rejects.toThrow();
  expect(index.query).not.toHaveBeenCalled();
});

test.each([
  { namespace: "other" },
  { metadata: undefined },
  { metadata: { source: "nar", raceDate: "20260915" } },
  { metadata: { source: "jra", raceDate: 20260915 } },
  { metadata: { source: "jra", raceDate: "20260230" } },
  { metadata: { source: "jra", raceDate: "20220101" } },
  { metadata: { source: "jra", raceDate: "20260916" } },
  { values: [1] },
  { values: Array.from({ length: 32 }, () => 1) },
] satisfies Partial<VectorizeVector>[])(
  "rejects unsafe hydration rather than caching incomplete history: %j",
  async (overrides) => {
    const index = {
      query: vi.fn().mockResolvedValue({ matches: [{ id: "entry-1" }] }),
      getByIds: vi.fn().mockResolvedValue([{ ...hydratedVector, ...overrides }]),
    };
    await expect(queryHistoricalVectors(index, query)).rejects.toThrow();
  },
);

test.each([
  { ids: ["entry-1", "entry-1"], vectors: [vector] },
  { ids: ["entry-1"], vectors: [] },
  { ids: ["entry-1", "entry-2"], vectors: [vector, vector] },
  { ids: ["missing"], vectors: [vector] },
  { ids: Array.from({ length: 81 }, (_, i) => String(i)), vectors: [] },
])("rejects duplicate, excessive or not-yet-visible matches: %j", async ({ ids, vectors }) => {
  const index = {
    query: vi.fn().mockResolvedValue({ matches: ids.map((id) => ({ id })) }),
    getByIds: vi.fn().mockResolvedValue(vectors),
  };
  await expect(queryHistoricalVectors(index, query)).rejects.toThrow();
});

test("propagates Vectorize errors without a Neon fallback", async () => {
  const index = { query: vi.fn().mockRejectedValue(new Error("Unavailable")), getByIds: vi.fn() };
  await expect(queryHistoricalVectors(index, query)).rejects.toThrow("Unavailable");
});

test("returns an async mutation receipt, not a claim of query visibility", async () => {
  const index = { upsert: vi.fn().mockResolvedValue({ mutationId: "mutation-1" }) };
  expect(
    await upsertHistoricalVectors(index, {
      dimensions: 8,
      namespace: "corner-v1",
      vectors: [vector],
    }),
  ).toStrictEqual({ mutationId: "mutation-1" });
  expect(index.upsert).toHaveBeenCalledWith([
    {
      id: "entry-1",
      namespace: "corner-v1",
      metadata: { source: "jra", raceDate: "20260915", corner1: 0.2 },
      values: [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0,
      ],
    },
  ]);
});

test.each([{ ids: ["entry-1"], count: 1 }, { mutationId: 1 }, { mutationId: "" }])(
  "rejects legacy or malformed mutation receipts",
  async (receipt) => {
    const index = { upsert: vi.fn().mockResolvedValue(receipt) };
    await expect(
      upsertHistoricalVectors(index, {
        dimensions: 8,
        namespace: "corner-v1",
        vectors: [vector],
      }),
    ).rejects.toThrow("Vectorize V2 mutation receipt is required");
  },
);

test("accepts a separate 256-dimensional versioned finish namespace", async () => {
  const index = { upsert: vi.fn().mockResolvedValue({ mutationId: "finish-1" }) };
  expect(
    await upsertHistoricalVectors(index, {
      dimensions: 256,
      namespace: "finish-v1",
      vectors: [
        { ...vector, namespace: "finish-v1", values: Array.from({ length: 256 }, () => 0) },
      ],
    }),
  ).toStrictEqual({ mutationId: "finish-1" });
});

test.each<Partial<VectorizeVector>>([
  { id: "" },
  { id: "あ".repeat(22) },
  { namespace: "other" },
  { values: [Infinity] },
  { metadata: undefined },
  { metadata: { source: "other", raceDate: "20260915" } },
  { metadata: { source: "jra", raceDate: 20260915 } },
  { metadata: { source: "nar", raceDate: "bad" } },
  { metadata: { source: "jra", raceDate: "20260915", body: "x".repeat(10240) } },
])("validates every vector before accepting any writes: %j", async (overrides) => {
  const index = { upsert: vi.fn() };
  await expect(
    upsertHistoricalVectors(index, {
      dimensions: 8,
      namespace: "corner-v1",
      vectors: [{ ...vector, ...overrides }],
    }),
  ).rejects.toThrow();
  expect(index.upsert).not.toHaveBeenCalled();
});

test.each([
  { vectors: [] },
  { vectors: [vector, vector] },
  { vectors: Array.from({ length: 501 }, () => vector) },
])("rejects empty, duplicate or excessive write batches", async ({ vectors }) => {
  const index = { upsert: vi.fn() };
  await expect(
    upsertHistoricalVectors(index, { dimensions: 8, namespace: "corner-v1", vectors }),
  ).rejects.toThrow();
  expect(index.upsert).not.toHaveBeenCalled();
});
