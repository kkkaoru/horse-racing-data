// Run with bun. Tests for the raw-Catalog-backed focused full completion guard.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { beforeEach, expect, test, vi } from "vitest";
import type { Env, PredictCategory } from "./types";

interface CellRoutingRuleCondition {
  dimension: string;
  values: string[];
}

interface CellRoutingRule {
  conditions: CellRoutingRuleCondition[];
  variant: string;
}

interface CellRoutingVariant {
  model_version: string;
}

interface CellRoutingCategoryConfig {
  default_variant: string;
  variants: Record<string, CellRoutingVariant>;
  rules: CellRoutingRule[];
}

interface CellRoutingConfig {
  jra: CellRoutingCategoryConfig;
}

const buildCatalogRows = (count = 12): Record<string, unknown>[] =>
  Array.from({ length: count }, (_, index) => ({
    grade_code: null,
    ketto_toroku_bango: `horse-${String(index + 1).padStart(2, "0")}`,
    kyoso_joken_code: null,
    shusso_tosu: count,
    track_code: null,
  }));

const { cacheHeadMock, catalogFetchMock, neonMock, queryMock } = vi.hoisted(() => {
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
  return {
    cacheHeadMock: vi.fn(),
    catalogFetchMock: catalogFetch,
    neonMock: vi.fn(() => ({ query })),
    queryMock: query,
  };
});

vi.mock("@neondatabase/serverless", () => ({ neon: neonMock }));

import { isFocusedFullPredictionComplete, isPerRaceRescoreReady } from "./focused-full-completion";

const makeEnv = (): Env =>
  Object.assign(Object.create(null), {
    NEON_DATABASE_URL: "postgres://example",
    FEATURES_CACHE: { head: cacheHeadMock } as unknown as R2Bucket,
    PC_KEIBA_R2_CATALOG: { fetch: catalogFetchMock },
  });

const setCatalogRows = (rows: readonly Record<string, unknown>[]): void => {
  catalogFetchMock.mockImplementation(async () => Response.json({ rows }));
};

beforeEach(() => {
  cacheHeadMock.mockReset();
  catalogFetchMock.mockReset();
  queryMock.mockReset();
  setCatalogRows(buildCatalogRows());
  queryMock.mockResolvedValue([{ actual_rows: 12 }]);
  neonMock.mockClear();
});

test("rescore is ready only after Neon completion and the per-race R2 cache both exist", async () => {
  cacheHeadMock.mockResolvedValue({ key: "cached" });

  await expect(
    isPerRaceRescoreReady({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "1",
      raceBango: "4",
      runYmd: "20260822",
    }),
  ).resolves.toBe(true);

  expect(cacheHeadMock).toHaveBeenCalledWith(
    "feat-cache/catalog-v1/jra/20260822/01/04/features.parquet",
  );
});

test("rescore stays deferred when Neon is complete but the per-race R2 cache is missing", async () => {
  cacheHeadMock.mockResolvedValue(null);

  await expect(
    isPerRaceRescoreReady({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "01",
      raceBango: "04",
      runYmd: "20260822",
    }),
  ).resolves.toBe(false);
});

test("rescore does not query R2 while the initial prediction is incomplete", async () => {
  queryMock.mockResolvedValue([{ actual_rows: 0 }]);

  await expect(
    isPerRaceRescoreReady({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "01",
      raceBango: "04",
      runYmd: "20260822",
    }),
  ).resolves.toBe(false);

  expect(cacheHeadMock).not.toHaveBeenCalled();
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
      "2026-06-30T15:00:00",
    ],
  );
});

// Characterization test (2026-07-17 bug-regression-test audit, item H): documents
// a known, ACCEPTED limitation, not a bug being fixed here -- see
// docs/probes/bug-regression-test-audit-2026-07-17.md. The completion check
// this guard performs is `count(distinct ketto_toroku_bango) === entries.length`
// -- it counts rows, it never inspects predicted_score or any other quality
// signal, so a race with the RIGHT row count but a degenerate/near-random score
// distribution (the actual 2026-07-12 Cluster-B signature -- see
// feature_guard.py's own docstring in the predict-container package, which
// fixed the WRITE side of this incident) is reported complete here just the
// same as a genuinely healthy race, because this guard structurally has no way
// to tell them apart. Pins the exact query text (no predicted_score/stddev
// term anywhere in it) so a future change to what this guard checks shows up
// here as a deliberate, reviewed diff rather than a silent behavior change.
test("isFocusedFullPredictionComplete only counts rows -- it cannot detect a degenerate-score race with the right row count (documented limitation)", async () => {
  setCatalogRows(buildCatalogRows(12));
  queryMock.mockResolvedValue([{ actual_rows: 12 }]);

  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "01",
      runYmd: "20260613",
    }),
  ).resolves.toBe(true);

  expect(queryMock).toHaveBeenCalledWith(
    expect.not.stringContaining("predicted_score"),
    expect.anything(),
  );
  expect(queryMock).toHaveBeenCalledWith(
    expect.stringContaining("count(distinct ketto_toroku_bango)"),
    expect.anything(),
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

test("returns true when prediction rows are fresh (generated on the target race day)", async () => {
  setCatalogRows(buildCatalogRows(12));
  queryMock.mockResolvedValue([{ actual_rows: 12 }]);

  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260801",
    }),
  ).resolves.toBe(true);

  const queryCall = queryMock.mock.calls[0];
  expect(queryCall?.[0]).toContain("prediction_generated_at >= $8::timestamp");
  expect(queryCall?.[1]?.[7]).toBe("2026-07-31T15:00:00");
});

test("returns false when prediction rows are stale (generated on a prior day)", async () => {
  setCatalogRows(buildCatalogRows(12));
  queryMock.mockResolvedValue([{ actual_rows: 0 }]);

  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260811",
    }),
  ).resolves.toBe(false);

  const queryCall = queryMock.mock.calls[0];
  expect(queryCall?.[1]?.[7]).toBe("2026-08-10T15:00:00");
});

test("rejects invalid runYmd and returns false without querying", async () => {
  setCatalogRows(buildCatalogRows(12));

  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "01",
      runYmd: "2026080",
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

test("recognizes a NAR race the stage1 market-free fallback gate routed away from the primary model_version as complete", async () => {
  queryMock
    .mockResolvedValueOnce([{ actual_rows: 0 }])
    .mockResolvedValueOnce([{ actual_rows: 12 }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "01",
      runYmd: "20260724",
    }),
  ).resolves.toBe(true);

  expect(queryMock).toHaveBeenCalledTimes(2);
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("iter40-nar-settransformer-blend-v1");
  expect(queryMock.mock.calls[1]?.[1]?.[5]).toBe("iter12-nar-xgb-hpo-v8-stage1-marketfree-184");
});

test("recognizes a JRA race the stage1 market-free fallback gate routed away from the primary model_version as complete", async () => {
  queryMock
    .mockResolvedValueOnce([{ actual_rows: 0 }])
    .mockResolvedValueOnce([{ actual_rows: 12 }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "07",
      runYmd: "20260613",
    }),
  ).resolves.toBe(true);

  expect(queryMock).toHaveBeenCalledTimes(2);
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe("jra-cb-v9-sim-2013-clean");
  expect(queryMock.mock.calls[1]?.[1]?.[5]).toBe("jra-cb-stage1-marketfree235-2013");
});

test("does not retry under a stage1 fallback model_version for ban-ei -- stage1_routing.json has no ban-ei entry", async () => {
  setCatalogRows([{ ...buildCatalogRows(1)[0], grade_code: "E" }]);
  queryMock.mockResolvedValue([{ actual_rows: 0 }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "ban-ei",
      env: makeEnv(),
      keibajoCode: "83",
      raceBango: "01",
      runYmd: "20260701",
    }),
  ).resolves.toBe(false);

  expect(queryMock).toHaveBeenCalledTimes(1);
});

test("still reports incomplete when neither the primary nor the stage1 fallback model_version has the full row count", async () => {
  queryMock.mockResolvedValueOnce([{ actual_rows: 5 }]).mockResolvedValueOnce([{ actual_rows: 7 }]);
  await expect(
    isFocusedFullPredictionComplete({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "50",
      raceBango: "01",
      runYmd: "20260724",
    }),
  ).resolves.toBe(false);

  expect(queryMock).toHaveBeenCalledTimes(2);
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

// --- parity against the container's real cell_routing.json ----------------

const readContainerCellRoutingConfig = (): CellRoutingConfig => {
  const containerConfigPath = resolve(
    process.cwd(),
    "../finish-position-predict-container/src/predict_lib/cell_routing.json",
  );
  return JSON.parse(readFileSync(containerConfigPath, "utf-8")) as CellRoutingConfig;
};

// Intentional exception to "mock all file I/O in tests": this test's entire
// purpose is to catch drift between expectedModelVersion()'s hand-written JRA
// rule branches (703, prior-corner-005, venue==02) and the container's real
// cell_routing.json, so it must read the real file. This is exactly the class
// of bug this suite's venue==02 tests above were added to fix (the rule
// survived a full rewrite of this file unnoticed) -- mirrors the same
// pattern already established in
// apps/pc-keiba-viewer/src/lib/finish-position-cell-routing.test.ts.
//
// This specific test only pins cell_routing.json's OWN shape (rule count +
// declared model_version list) -- it never calls expectedModelVersion(), so
// it cannot by itself detect a CODE-side regression (e.g. a branch silently
// dropped during a refactor, which is exactly how the venue==02 gap this
// suite fixes originally happened -- the JSON never changed, only the code
// did). Found during the 2026-07-17 bug-regression-test audit: confirmed by
// mutation (temporarily removing the venue==02 branch from
// expectedModelVersion() left this test green). The 3 tests below close that
// gap by deriving each rule's expected model_version from this same parsed
// config and asserting expectedModelVersion()'s ACTUAL behaviour (observed
// via isFocusedFullPredictionComplete's resolved query parameter) matches
// it -- genuine two-sided parity, not just a JSON shape sentinel.
test("expectedModelVersion covers every JRA rule in the real cell_routing.json (parity guard)", () => {
  const containerConfig = readContainerCellRoutingConfig();
  const jraRules = containerConfig.jra.rules;
  // The rule COUNT is the parity contract: a new rule added to
  // cell_routing.json without a matching branch in expectedModelVersion()
  // fails silently exactly like the venue==02 gap this test guards against.
  // A failure here means: add the matching branch above FIRST, then update
  // this expectation to the new count (AND the 3 behavioural parity tests
  // below, which index into this same rules array by position).
  expect(jraRules).toHaveLength(3);
  const ruleModelVersions = jraRules.map(
    (rule) => containerConfig.jra.variants[rule.variant]?.model_version,
  );
  expect(ruleModelVersions).toStrictEqual([
    "jra-cb-v9-sim-2013-clean-jockey-pedigree269", // rule 1: kyoso_joken_code=703
    "jra-cb-v10-prior-corner274-2013", // rule 2: dirt + f_le10 + kyoso_joken_code=005
    "jra-cb-v9-sim-2013-clean-jockey-pedigree269", // rule 3: venue=02
  ]);
});

test("expectedModelVersion resolves rule 1 (kyoso_joken_code=703) to the model_version cell_routing.json declares for it", async () => {
  const containerConfig = readContainerCellRoutingConfig();
  const rule = containerConfig.jra.rules[0];
  const expectedFromConfig = containerConfig.jra.variants[rule?.variant ?? ""]?.model_version;

  setCatalogRows([{ ...buildCatalogRows(1)[0], kyoso_joken_code: "703" }]);
  queryMock.mockResolvedValue([{ actual_rows: 1 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "01",
    runYmd: "20260613",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe(expectedFromConfig);
});

test("expectedModelVersion resolves rule 2 (dirt + f_le10 + kyoso_joken_code=005) to the model_version cell_routing.json declares for it", async () => {
  const containerConfig = readContainerCellRoutingConfig();
  const rule = containerConfig.jra.rules[1];
  const expectedFromConfig = containerConfig.jra.variants[rule?.variant ?? ""]?.model_version;

  setCatalogRows([
    { ...buildCatalogRows(1)[0], kyoso_joken_code: "005", shusso_tosu: 8, track_code: "2A" },
  ]);
  queryMock.mockResolvedValue([{ actual_rows: 1 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "01",
    runYmd: "20260613",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe(expectedFromConfig);
});

test("expectedModelVersion resolves rule 3 (venue=02) to the model_version cell_routing.json declares for it", async () => {
  const containerConfig = readContainerCellRoutingConfig();
  const rule = containerConfig.jra.rules[2];
  const expectedFromConfig = containerConfig.jra.variants[rule?.variant ?? ""]?.model_version;

  setCatalogRows(buildCatalogRows(11));
  queryMock.mockResolvedValue([{ actual_rows: 11 }]);
  await isFocusedFullPredictionComplete({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260613",
  });
  expect(queryMock.mock.calls[0]?.[1]?.[5]).toBe(expectedFromConfig);
});

// --- parity against the container's real stage1_routing.json --------------

interface Stage1CategoryConfig {
  enabled: boolean;
  model_version: string;
}

const readContainerStage1RoutingConfig = (): Partial<Record<string, Stage1CategoryConfig>> => {
  const containerConfigPath = resolve(
    process.cwd(),
    "../finish-position-predict-container/src/predict_lib/stage1_routing.json",
  );
  return JSON.parse(readFileSync(containerConfigPath, "utf-8")) as Partial<
    Record<string, Stage1CategoryConfig>
  >;
};

// Same drift-guard shape as the cell_routing.json parity tests above, for
// STAGE1_MARKET_FREE_MODEL_VERSIONS instead of expectedModelVersion()'s JRA
// rules: a category added to (or removed from, or re-versioned in)
// stage1_routing.json without a matching update here silently reintroduces
// the "stage1-gated race never observed complete" bug this file's fallback
// check exists to fix. Observed indirectly via isFocusedFullPredictionComplete's
// second query call, same technique as the cell-routing parity tests.
test("STAGE1_MARKET_FREE_MODEL_VERSIONS covers every enabled category in the real stage1_routing.json (parity guard)", async () => {
  const containerConfig = readContainerStage1RoutingConfig();
  const enabledCategories = Object.entries(containerConfig).filter(
    ([, config]) => config?.enabled === true,
  );
  expect(enabledCategories.map(([category]) => category).toSorted()).toStrictEqual(["jra", "nar"]);

  for (const [category, config] of enabledCategories) {
    queryMock
      .mockResolvedValueOnce([{ actual_rows: 0 }])
      .mockResolvedValueOnce([{ actual_rows: 12 }]);
    await isFocusedFullPredictionComplete({
      category: category as PredictCategory,
      env: makeEnv(),
      keibajoCode: category === "jra" ? "05" : "50",
      raceBango: "01",
      runYmd: "20260724",
    });
    expect(queryMock.mock.calls.at(-1)?.[1]?.[5]).toBe(config?.model_version);
  }
});
