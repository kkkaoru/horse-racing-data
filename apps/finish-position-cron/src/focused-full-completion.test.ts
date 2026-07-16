// Run with bun. Tests for the raw-Catalog-backed focused full completion guard.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const buildCatalogRows = (count = 12): Record<string, unknown>[] =>
  Array.from({ length: count }, (_, index) => ({
    grade_code: null,
    ketto_toroku_bango: `horse-${String(index + 1).padStart(2, "0")}`,
    kyoso_joken_code: null,
    shusso_tosu: count,
    track_code: null,
  }));

const { catalogFetchMock, neonMock, queryMock } = vi.hoisted(() => {
  const query = vi.fn(
    async (_query: string, _params: readonly unknown[]): Promise<unknown> => [{ actual_rows: 12 }],
  );
  const catalogFetch = vi.fn(
    async (_request: Request): Promise<Response> =>
      Response.json({
        rows: Array.from({ length: 12 }, (_, index) => ({
          grade_code: null,
          ketto_toroku_bango: `horse-${String(index + 1).padStart(2, "0")}`,
          kyoso_joken_code: null,
          shusso_tosu: 12,
          track_code: null,
        })),
      }),
  );
  return { catalogFetchMock: catalogFetch, neonMock: vi.fn(() => ({ query })), queryMock: query };
});

vi.mock("@neondatabase/serverless", () => ({ neon: neonMock }));

import { isFocusedFullPredictionComplete } from "./focused-full-completion";

const makeEnv = (): Env =>
  Object.assign(Object.create(null), {
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_R2_CATALOG: { fetch: catalogFetchMock },
  });

const setCatalogRows = (rows: readonly Record<string, unknown>[]): void => {
  catalogFetchMock.mockImplementation(async () => Response.json({ rows }));
};

beforeEach(() => {
  catalogFetchMock.mockReset();
  queryMock.mockReset();
  setCatalogRows(buildCatalogRows());
  queryMock.mockResolvedValue([{ actual_rows: 12 }]);
  neonMock.mockClear();
});

test("uses raw Catalog entries and only queries Neon prediction output", async () => {
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  ).resolves.toBe(true);

  const request = catalogFetchMock.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-features?date=20260701&source=nar&keibajoCode=50&raceBango=12",
  );
  expect(neonMock).toHaveBeenCalledWith("postgres://example");
  expect(queryMock).toHaveBeenCalledWith(
    expect.not.stringContaining("race_entry_corner_features"),
    [
      "nar",
      "2026",
      "0701",
      "50",
      "12",
      "iter40-nar-settransformer-blend-v1",
      buildCatalogRows().map((row) => row.ketto_toroku_bango),
    ],
  );
});

test("uses the NAR base model when transformer blending is disabled", async () => {
  await isFocusedFullPredictionComplete({
    category: "nar",
    env: { ...makeEnv(), NAR_TRANSFORMER_BLEND_ENABLED: "off" },
    keibajoCode: "45",
    raceBango: "03",
    runYmd: "20260710",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("iter12-nar-xgb-hpo-v8-clean188");
});

test("routes JRA 703 and prior-corner races from raw Catalog fields", async () => {
  setCatalogRows([
    {
      ...buildCatalogRows(1)[0],
      kyoso_joken_code: "703",
    },
  ]);
  queryMock.mockResolvedValue([{ actual_rows: 1 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "02",
    raceBango: "02",
    runYmd: "20260621",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("jra-cb-v9-sim-2013-clean-jockey-pedigree269");

  setCatalogRows([
    {
      ...buildCatalogRows(1)[0],
      kyoso_joken_code: "005",
      shusso_tosu: 10,
      track_code: "2A",
    },
  ]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "10",
    raceBango: "06",
    runYmd: "20260705",
  });
  expect(queryMock.mock.calls[1]?.[1]?.[5]).toBe("jra-cb-v10-prior-corner274-2013");
});

test("routes JRA venue 02 (Hakodate) to jockey-pedigree269 with no other special conditions", async () => {
  setCatalogRows(buildCatalogRows(11));
  queryMock.mockResolvedValue([{ actual_rows: 11 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260613",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("jra-cb-v9-sim-2013-clean-jockey-pedigree269");
});

test("routes an un-padded venue 2 the same as venue 02", async () => {
  setCatalogRows(buildCatalogRows(11));
  queryMock.mockResolvedValue([{ actual_rows: 11 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "2",
    raceBango: "01",
    runYmd: "20260613",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("jra-cb-v9-sim-2013-clean-jockey-pedigree269");
});

test("prior-corner-005 outranks the venue 02 fallback when a race matches both", async () => {
  setCatalogRows([
    {
      ...buildCatalogRows(1)[0],
      kyoso_joken_code: "005",
      shusso_tosu: 8,
      track_code: "2A",
    },
  ]);
  queryMock.mockResolvedValue([{ actual_rows: 1 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "02",
    raceBango: "03",
    runYmd: "20260613",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("jra-cb-v10-prior-corner274-2013");
});

test("a non-venue-02 JRA race with no special conditions keeps the category default", async () => {
  setCatalogRows(buildCatalogRows(14));
  queryMock.mockResolvedValue([{ actual_rows: 14 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "07",
    runYmd: "20260613",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("jra-cb-v9-sim-2013-clean");
});

test("routes ban-ei grade E through the ban-ei Catalog filter", async () => {
  setCatalogRows([{ ...buildCatalogRows(1)[0], grade_code: "E" }]);
  queryMock.mockResolvedValue([{ actual_rows: 1 }]);
  await isFocusedFullPredictionComplete({
    category: "ban-ei",
    env: makeEnv(),
    keibajoCode: "83",
    raceBango: "01",
    runYmd: "20260701",
  });
  expect(catalogFetchMock.mock.calls[0]?.[0]?.url).toContain("source=ban-ei");
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("banei-cb-v8-window2011-wf-15y");
});

test("returns false without querying Neon when raw Catalog has no entries", async () => {
  setCatalogRows([]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260628",
    }),
  ).resolves.toBe(false);
  expect(queryMock).not.toHaveBeenCalled();
});

test("deduplicates raw Catalog horses before comparing prediction count", async () => {
  const row = {
    grade_code: null,
    ketto_toroku_bango: "horse-01",
    kyoso_joken_code: null,
    shusso_tosu: 1,
    track_code: null,
  };
  setCatalogRows([row, row]);
  queryMock.mockResolvedValue([{ actual_rows: "1" }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260628",
    }),
  ).resolves.toBe(true);
});

test("returns false when prediction output is incomplete or malformed", async () => {
  queryMock.mockResolvedValue([{ actual_rows: 11 }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  ).resolves.toBe(false);

  queryMock.mockResolvedValue([]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  ).resolves.toBe(false);
});

test("fails closed when Catalog returns an HTTP or schema error", async () => {
  catalogFetchMock.mockResolvedValue(new Response("unavailable", { status: 503 }));
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  ).rejects.toThrow("HTTP 503");

  catalogFetchMock.mockResolvedValue(Response.json({ rows: "invalid" }));
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  ).rejects.toThrow("invalid rows");
});

test("rejects malformed raw Catalog entries", async () => {
  setCatalogRows([{ ketto_toroku_bango: null }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260628",
    }),
  ).rejects.toThrow("invalid entry");
});
