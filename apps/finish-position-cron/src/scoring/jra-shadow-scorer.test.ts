import { expect, test, vi } from "vitest";
import { parseCatBoostJsonModel } from "catboost-json-tree";

import cbSmall from "./__fixtures__/cb-small.json";
import golden from "./__fixtures__/jra-parity-golden.json";
import type { FeatureEntry } from "./feature-projection";
import {
  applyTop1ScoreSwap,
  JRA_SHADOW_MODEL_SPECS,
  loadSelectedJraShadowModel,
  scoreJraRaceShadow,
  selectJraShadowModel,
  type JraShadowModelSpec,
} from "./jra-shadow-scorer";

const RAW_SCORE_TOLERANCE = 1e-6;

const raceEntry = (overrides: FeatureEntry = {}): FeatureEntry => ({
  grade_code: "C",
  keibajo_code: "04",
  ketto_toroku_bango: "horse-1",
  kyoso_joken_code: "701",
  race_id: "jra:2026:0823:04:03",
  tansho_ninkijun: 1,
  track_code: "10",
  umaban: 1,
  ...overrides,
});

test("selects the current JRA cell routes in declared rule order", () => {
  expect(selectJraShadowModel([raceEntry({ kyoso_joken_code: "703" })]).variant).toBe(
    "jockey_pedigree_703",
  );
  expect(
    selectJraShadowModel(
      Array.from({ length: 10 }, (_, index) =>
        raceEntry({
          ketto_toroku_bango: `horse-${index}`,
          kyoso_joken_code: "005",
          track_code: "23",
          umaban: index + 1,
        }),
      ),
    ).variant,
  ).toBe("prior_corner_dirt_smallfield_005");
  expect(selectJraShadowModel([raceEntry({ keibajo_code: "02" })]).variant).toBe(
    "jockey_pedigree_703",
  );
  expect(selectJraShadowModel([raceEntry()]).variant).toBe("sim");
  expect(
    selectJraShadowModel([raceEntry({ kyoso_joken_code: "005", track_code: "20" })]).variant,
  ).toBe("sim");
});

test("routes odds-missing races directly to Stage-1 before loading a model", () => {
  expect(selectJraShadowModel([raceEntry({ tansho_ninkijun: null })]).variant).toBe(
    "stage1_marketfree",
  );
});

test("supports the current preserved full-board Stage-1 freshness gate", () => {
  const fullBoard = [
    raceEntry({ tansho_ninkijun: 1, tansho_odds: 2.1, umaban: 1 }),
    raceEntry({ ketto_toroku_bango: "horse-2", tansho_ninkijun: 2, tansho_odds: 4.2, umaban: 2 }),
  ];
  expect(selectJraShadowModel(fullBoard, { preservedOddsGateEnabled: true }).variant).toBe("sim");
  expect(
    selectJraShadowModel(
      fullBoard.map((entry, index) => ({
        ...entry,
        popularity_score: index,
        tansho_ninkijun: null,
      })),
      { preservedOddsGateEnabled: true },
    ).variant,
  ).toBe("sim");
  expect(
    selectJraShadowModel(
      fullBoard.map((entry) => ({ ...entry, tansho_odds: null })),
      { preservedOddsGateEnabled: true },
    ).variant,
  ).toBe("stage1_marketfree");
  expect(selectJraShadowModel([fullBoard[0]!], { preservedOddsGateEnabled: true }).variant).toBe(
    "stage1_marketfree",
  );
  expect(
    selectJraShadowModel(
      fullBoard.map((entry) => ({ ...entry, tansho_ninkijun: 1 })),
      { preservedOddsGateEnabled: true },
    ).variant,
  ).toBe("stage1_marketfree");
  expect(
    selectJraShadowModel(
      fullBoard.map((entry, index) => ({ ...entry, tansho_odds: index === 0 ? 5 : 2 })),
      { preservedOddsGateEnabled: true },
    ).variant,
  ).toBe("stage1_marketfree");
  expect(
    selectJraShadowModel(
      fullBoard.map((entry, index) => ({
        ...entry,
        popularity_score: index === 0 ? 0.2 : 1,
        tansho_ninkijun: null,
      })),
      { preservedOddsGateEnabled: true },
    ).variant,
  ).toBe("stage1_marketfree");
});

test("cell routing fails closed on empty, missing, or inconsistent dimensions", () => {
  expect(() => selectJraShadowModel([])).toThrow("race has no entries");
  const { grade_code: _gradeCode, ...missingGrade } = raceEntry();
  expect(() => selectJraShadowModel([missingGrade])).toThrow("grade_code");
  expect(selectJraShadowModel([raceEntry({ grade_code: null })]).variant).toBe("sim");
  expect(() =>
    selectJraShadowModel([raceEntry(), raceEntry({ grade_code: "G", umaban: 2 })]),
  ).toThrow("grade_code");
  expect(() => selectJraShadowModel([raceEntry({ keibajo_code: {} })])).toThrow("keibajo_code");
  expect(() =>
    selectJraShadowModel([raceEntry(), raceEntry({ keibajo_code: "05", umaban: 2 })]),
  ).toThrow("keibajo_code");
});

const jsonObject = (value: unknown): R2ObjectBody =>
  ({ json: async () => value }) as unknown as R2ObjectBody;

const featureNamesFor = (spec: JraShadowModelSpec): string[] => {
  const fatherFather =
    spec.variant === "jockey_pedigree_703"
      ? [
          "gsire_dist_surface_win_eb",
          "gsire_dist_surface_top3_eb",
          "gsire_venue_win_eb",
          "gsire_dist_surface_edge",
          "gsire_dist_surface_logn",
          "gsire_dist_surface_win_rank_in_race",
          "gsire_dist_surface_edge_rank_in_race",
        ]
      : [];
  return [
    ...fatherFather,
    ...Array.from(
      { length: spec.featureCount - fatherFather.length },
      (_, index) => `feature_${index}`,
    ),
  ];
};

const metadataFor = (spec: JraShadowModelSpec): Record<string, unknown> => ({
  architecture: "catboost-yetirank",
  feature_count: spec.featureCount,
  feature_names: featureNamesFor(spec),
  model_version: spec.modelVersion,
});

const bucketFor = (
  model: unknown,
  metadata: unknown,
): { bucket: R2Bucket; get: ReturnType<typeof vi.fn> } => {
  const get = vi.fn(async (key: string) =>
    key.endsWith("model.json") ? jsonObject(model) : jsonObject(metadata),
  );
  return { bucket: { get } as unknown as R2Bucket, get };
};

test("loads only the selected production model and its metadata from R2", async () => {
  const spec = JRA_SHADOW_MODEL_SPECS.jockey_pedigree_703;
  const { bucket, get } = bucketFor(cbSmall, metadataFor(spec));
  const loaded = await loadSelectedJraShadowModel(bucket, spec);
  expect(loaded.featureNames).toHaveLength(269);
  expect(loaded.spec).toBe(spec);
  expect(get).toHaveBeenCalledTimes(2);
  expect(get).toHaveBeenCalledWith(
    "finish-position/jra/jra-cb-v9-sim-2013-clean-jockey-pedigree269/model.json",
  );
});

test("loads the Stage-1 top1 companion and its base contract together", async () => {
  const spec = JRA_SHADOW_MODEL_SPECS.stage1_marketfree;
  const featureNames = featureNamesFor(spec);
  const get = vi.fn(async (key: string) => {
    if (key.endsWith("model.json")) return jsonObject(cbSmall);
    const modelVersion = key.includes("iter500-top1swap")
      ? spec.modelVersion
      : "jra-cb-stage1-marketfree235-2013";
    return jsonObject({
      architecture: "catboost-yetirank",
      feature_count: 235,
      feature_names: featureNames,
      model_version: modelVersion,
    });
  });

  const loaded = await loadSelectedJraShadowModel({ get } as unknown as R2Bucket, spec);

  expect(loaded.baseFeatureNames).toStrictEqual(featureNames);
  expect(loaded.baseModel).toBeDefined();
  expect(get).toHaveBeenCalledTimes(4);
  expect(get).toHaveBeenCalledWith(
    "finish-position/jra/jra-cb-stage1-marketfree235-2013/model.json",
  );
});

test("selected model load fails closed when its R2 object is absent", async () => {
  const bucket = { get: vi.fn(async () => null) } as unknown as R2Bucket;
  await expect(loadSelectedJraShadowModel(bucket, JRA_SHADOW_MODEL_SPECS.sim)).rejects.toThrow(
    "R2 object not found",
  );
});

test.each([
  ["non-object", "bad", "must be an object"],
  ["non-array names", { feature_names: "bad" }, "must be an array"],
  [
    "invalid name",
    {
      ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim),
      feature_names: Array.from({ length: 250 }, () => ""),
    },
    "non-empty strings",
  ],
  [
    "non-string name",
    {
      ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim),
      feature_names: [1, ...featureNamesFor(JRA_SHADOW_MODEL_SPECS.sim).slice(1)],
    },
    "non-empty strings",
  ],
  [
    "duplicate names",
    { ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim), feature_names: Array(250).fill("same") },
    "must be unique",
  ],
  [
    "wrong count",
    { ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim), feature_count: 249 },
    "feature count mismatch",
  ],
  [
    "wrong names length",
    {
      ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim),
      feature_names: featureNamesFor(JRA_SHADOW_MODEL_SPECS.sim).slice(1),
    },
    "feature count mismatch",
  ],
  [
    "wrong version",
    { ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim), model_version: "historical-model" },
    "version mismatch",
  ],
  [
    "wrong architecture",
    { ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim), architecture: "xgboost" },
    "architecture mismatch",
  ],
  [
    "leak feature",
    {
      ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim),
      feature_names: [
        "target_corner_2_norm",
        ...featureNamesFor(JRA_SHADOW_MODEL_SPECS.sim).slice(1),
      ],
    },
    "within-race leak",
  ],
  [
    "sorted leak features",
    {
      ...metadataFor(JRA_SHADOW_MODEL_SPECS.sim),
      feature_names: [
        "target_corner_4_norm",
        "target_corner_2_norm",
        ...featureNamesFor(JRA_SHADOW_MODEL_SPECS.sim).slice(2),
      ],
    },
    "within-race leak columns: target_corner_2_norm, target_corner_4_norm",
  ],
])("rejects %s metadata", async (_label, metadata, message) => {
  await expect(
    loadSelectedJraShadowModel(bucketFor(cbSmall, metadata).bucket, JRA_SHADOW_MODEL_SPECS.sim),
  ).rejects.toThrow(message);
});

test("rejects a jockey-pedigree artifact without the father-father contract", async () => {
  const spec = JRA_SHADOW_MODEL_SPECS.jockey_pedigree_703;
  const metadata = {
    ...metadataFor(spec),
    feature_names: Array.from({ length: spec.featureCount }, (_, index) => `feature_${index}`),
  };
  await expect(
    loadSelectedJraShadowModel(bucketFor(cbSmall, metadata).bucket, spec),
  ).rejects.toThrow("father-father");
});

const goldenRaceEntries: FeatureEntry[] = golden.entries.map((entry) =>
  raceEntry({
    ...Object.fromEntries(golden.featureNames.map((name) => [name, null])),
    ...entry,
    grade_code: "C",
    keibajo_code: "04",
    race_id: "jra:2026:0823:04:03",
    track_code: "10",
  }),
);

const goldenLoadedModel = {
  featureNames: golden.featureNames,
  model: parseCatBoostJsonModel(cbSmall),
  spec: JRA_SHADOW_MODEL_SPECS.sim,
};

test("matches Python-native CatBoost golden scores and ranking", () => {
  const result = scoreJraRaceShadow(goldenRaceEntries, goldenLoadedModel);
  const scoreByKetto = new Map(
    golden.entries.map((entry, index) => [
      String(entry.ketto_toroku_bango),
      golden.cbScores[index]!,
    ]),
  );
  expect(
    Math.max(
      ...result.predictions.map((prediction) =>
        Math.abs(prediction.predictedScore - scoreByKetto.get(prediction.kettoTorokuBango)!),
      ),
    ),
  ).toBeLessThan(RAW_SCORE_TOLERANCE);
  expect(result.predictions.map((row) => row.umaban)).toStrictEqual(
    golden.endToEnd.excluded.map((row) => row.umaban),
  );
  expect(result.shadowOnly).toBe(true);
  expect(result.raceId).toBe("jra:2026:0823:04:03");
  expect(result.gradeCode).toBe("C");
});

test("reports the Stage-1 safety-net rescore without loading or writing it", () => {
  const zeroModel = parseCatBoostJsonModel({
    features_info: { float_features: [] },
    oblivious_trees: [],
    scale_and_bias: [1, [0]],
  });
  const result = scoreJraRaceShadow(goldenRaceEntries, {
    ...goldenLoadedModel,
    model: zeroModel,
  });
  expect(result.scoreStddev).toBe(0);
  expect(result.stage1RescoreRequired).toBe(true);
  expect(
    scoreJraRaceShadow(goldenRaceEntries, {
      ...goldenLoadedModel,
      baseFeatureNames: golden.featureNames,
      baseModel: zeroModel,
      model: zeroModel,
      spec: JRA_SHADOW_MODEL_SPECS.stage1_marketfree,
    }).stage1RescoreRequired,
  ).toBe(false);
});

test("applyTop1ScoreSwap changes only the two top selections", () => {
  expect(applyTop1ScoreSwap(["H1", "H2", "H3", "H4"], [4, 3, 2, 1], [0, 1, 5, 2])).toStrictEqual([
    2, 3, 4, 1,
  ]);
  expect(applyTop1ScoreSwap(["H1", "H2"], [2, 1], [3, 1])).toStrictEqual([2, 1]);
  expect(applyTop1ScoreSwap([], [], [])).toStrictEqual([]);
});

test("applyTop1ScoreSwap uses horse-id ties and rejects misaligned inputs", () => {
  expect(applyTop1ScoreSwap(["H2", "H1"], [2, 1], [3, 3])).toStrictEqual([1, 2]);
  expect(() => applyTop1ScoreSwap(["H1"], [1], [])).toThrow("equal lengths");
});

test("Stage-1 scoring fails closed when its base model is missing", () => {
  expect(() =>
    scoreJraRaceShadow(goldenRaceEntries, {
      ...goldenLoadedModel,
      spec: JRA_SHADOW_MODEL_SPECS.stage1_marketfree,
    }),
  ).toThrow("base model is not loaded");
});

test("final feature contract rejects missing, non-numeric, and identity gaps", () => {
  expect(() =>
    scoreJraRaceShadow(
      goldenRaceEntries.map(({ kyori: _removed, ...entry }) => entry),
      goldenLoadedModel,
    ),
  ).toThrow("missing model features");
  expect(() =>
    scoreJraRaceShadow(
      goldenRaceEntries.map((entry) => ({ ...entry, kyori: "not-a-number" })),
      goldenLoadedModel,
    ),
  ).toThrow("non-numeric model features");
  expect(() =>
    scoreJraRaceShadow(
      goldenRaceEntries.map((entry) => ({ ...entry, ketto_toroku_bango: null })),
      goldenLoadedModel,
    ),
  ).toThrow("missing horse identity");
  expect(() => scoreJraRaceShadow([], goldenLoadedModel)).toThrow("race has no entries");
});
