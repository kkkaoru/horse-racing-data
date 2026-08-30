// Run with bun (bunx vitest)

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, it } from "vitest";

import {
  NAMED_RACE_CELLS_BY_CATEGORY,
  buildNamedRaceCellIndex,
  inheritNamedRaceVariant,
  isNamedRacePreraceLockRoute,
  lookupNamedRaceCell,
  namedRaceLookupKey,
  namedRaceVariantsForCategory,
} from "./finish-position-named-race-cells";
import type { NamedRaceCell } from "./finish-position-named-race-cells";

it("namedRaceLookupKey joins category venue and token", () => {
  expect(
    namedRaceLookupKey({ category: "jra", raceNameToken: "BSN賞", venue: "04" }),
  ).toStrictEqual("jra\u000004\u0000BSN賞");
});

it("NAMED_RACE_CELLS_BY_CATEGORY mirrors the container named_race_cells.json", () => {
  const containerCatalogPath = resolve(
    process.cwd(),
    "../finish-position-predict-container/src/predict_lib/named_race_cells.json",
  );
  const containerCatalog: unknown = JSON.parse(readFileSync(containerCatalogPath, "utf-8"));
  expect(NAMED_RACE_CELLS_BY_CATEGORY).toStrictEqual(containerCatalog);
});

it("lookupNamedRaceCell finds BSN, Suzuran, Chukyo 2yo Stakes, and Niigata Kinen", () => {
  expect(
    lookupNamedRaceCell({ category: "jra", raceNameToken: "BSN賞", venue: "04" }),
  ).toStrictEqual({
    architecture: "catboost",
    base_variant: "joken_999",
    feature_count: 113,
    model_version: "jra-named-niigata-bsn-v1",
    race_name_token: "BSN賞",
    variant: "niigata_bsn",
    venue: "04",
  });
  expect(
    lookupNamedRaceCell({ category: "jra", raceNameToken: "すずらん賞", venue: "01" }),
  ).toStrictEqual({
    architecture: "catboost",
    base_variant: "joken_999",
    feature_count: 113,
    model_version: "jra-named-sapporo-suzuran-v2",
    race_name_token: "すずらん賞",
    variant: "sapporo_suzuran",
    venue: "01",
  });
  expect(
    lookupNamedRaceCell({ category: "jra", raceNameToken: "中京2歳ステークス", venue: "07" }),
  ).toStrictEqual({
    architecture: "catboost",
    base_variant: "joken_999",
    feature_count: 113,
    model_version: "jra-named-chukyo-nisai-stakes-focal-v3",
    prerace_router: {
      routes: [
        {
          model_version: "jra-named-chukyo-nisai-stakes-jul1600-ninki2-v4",
          variant: "chukyo_nisai_stakes_jul1600_ninki2",
          when: {
            kyori: ["1600"],
            month: ["07"],
          },
        },
        {
          model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
          variant: "chukyo_nisai_stakes_largefield",
          when: {
            kyori: ["1200"],
            month: ["12"],
          },
        },
        {
          when: {
            kyori: ["1400"],
            month: ["07", "08"],
          },
        },
      ],
    },
    race_name_token: "中京2歳ステークス",
    rerank_feature_count: 113,
    rerank_model_version: "jra-named-chukyo-nisai-stakes-v2",
    routing_mode: "jra_lock1_rerank_rest",
    variant: "chukyo_nisai_stakes",
    venue: "07",
  });
  expect(
    lookupNamedRaceCell({ category: "jra", raceNameToken: "新潟記念", venue: "04" }),
  ).toStrictEqual({
    architecture: "catboost",
    base_variant: "joken_999",
    feature_count: 113,
    model_version: "jra-named-niigata-kinen-draw-v2",
    race_name_token: "新潟記念",
    rerank_feature_count: 114,
    rerank_model_version: "jra-named-niigata-kinen-going-v2",
    routing_mode: "jra_lock1_rerank_rest",
    variant: "niigata_kinen",
    venue: "04",
  });
  expect(
    lookupNamedRaceCell({ category: "jra", raceNameToken: "長篠ステークス", venue: "07" }),
  ).toBeUndefined();
});

it("lookupNamedRaceCell misses an uncatalogued venue and token pair", () => {
  expect(
    lookupNamedRaceCell({ category: "jra", raceNameToken: "有馬記念", venue: "06" }),
  ).toBeUndefined();
});

it("inheritNamedRaceVariant copies the base spec when the cell has no overrides", () => {
  const cell: NamedRaceCell = {
    base_variant: "joken_999",
    race_name_token: "BSN賞",
    variant: "niigata_bsn",
    venue: "04",
  };
  expect(
    inheritNamedRaceVariant(cell, {
      architecture: "catboost",
      feature_count: 113,
      model_version: "jra-joken-999-pooled-yetirank-v2",
    }),
  ).toStrictEqual({
    architecture: "catboost",
    base_variant: "joken_999",
    feature_count: 113,
    model_version: "jra-joken-999-pooled-yetirank-v2",
  });
});

it("inheritNamedRaceVariant copies lock1 rerank catalog fields", () => {
  const cell: NamedRaceCell = {
    architecture: "catboost",
    base_variant: "joken_999",
    feature_count: 113,
    model_version: "jra-named-niigata-kinen-draw-v2",
    race_name_token: "新潟記念",
    rerank_feature_count: 114,
    rerank_model_version: "jra-named-niigata-kinen-going-v2",
    routing_mode: "jra_lock1_rerank_rest",
    variant: "niigata_kinen",
    venue: "04",
  };
  expect(
    inheritNamedRaceVariant(cell, {
      architecture: "catboost",
      feature_count: 113,
      model_version: "jra-joken-999-pooled-yetirank-v2",
    }),
  ).toStrictEqual({
    architecture: "catboost",
    base_variant: "joken_999",
    feature_count: 113,
    model_version: "jra-named-niigata-kinen-draw-v2",
    rerank_model_version: "jra-named-niigata-kinen-going-v2",
    routing_mode: "jra_lock1_rerank_rest",
  });
});

it("inheritNamedRaceVariant uses dedicated model fields when present", () => {
  const cell: NamedRaceCell = {
    architecture: "lightgbm",
    base_variant: "joken_999",
    feature_count: 200,
    model_version: "jra-named-niigata-bsn-v1",
    race_name_token: "BSN賞",
    variant: "niigata_bsn",
    venue: "04",
  };
  expect(
    inheritNamedRaceVariant(cell, {
      architecture: "catboost",
      feature_count: 113,
      model_version: "jra-joken-999-pooled-yetirank-v2",
    }),
  ).toStrictEqual({
    architecture: "lightgbm",
    base_variant: "joken_999",
    feature_count: 200,
    model_version: "jra-named-niigata-bsn-v1",
  });
});

it("namedRaceVariantsForCategory injects prerace lock variants", () => {
  expect(
    namedRaceVariantsForCategory({
      cells: [
        {
          architecture: "catboost",
          base_variant: "joken_999",
          feature_count: 113,
          model_version: "jra-named-chukyo-nisai-stakes-focal-v3",
          prerace_router: {
            routes: [
              {
                model_version: "jra-named-chukyo-nisai-stakes-jul1600-ninki2-v4",
                variant: "chukyo_nisai_stakes_jul1600_ninki2",
                when: {
                  kyori: ["1600"],
                  month: ["07"],
                },
              },
              {
                model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
                variant: "chukyo_nisai_stakes_largefield",
                when: {
                  kyori: ["1200"],
                  month: ["12"],
                },
              },
            ],
          },
          race_name_token: "中京2歳ステークス",
          rerank_model_version: "jra-named-chukyo-nisai-stakes-v2",
          routing_mode: "jra_lock1_rerank_rest",
          variant: "chukyo_nisai_stakes",
          venue: "07",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toStrictEqual({
    chukyo_nisai_stakes: {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-chukyo-nisai-stakes-focal-v3",
      rerank_model_version: "jra-named-chukyo-nisai-stakes-v2",
      routing_mode: "jra_lock1_rerank_rest",
    },
    chukyo_nisai_stakes_jul1600_ninki2: {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-chukyo-nisai-stakes-jul1600-ninki2-v4",
      rerank_model_version: "jra-named-chukyo-nisai-stakes-v2",
      routing_mode: "jra_lock1_rerank_rest",
    },
    chukyo_nisai_stakes_largefield: {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
      rerank_model_version: "jra-named-chukyo-nisai-stakes-v2",
      routing_mode: "jra_lock1_rerank_rest",
    },
  });
});

it("isNamedRacePreraceLockRoute requires both variant and model_version", () => {
  expect(
    isNamedRacePreraceLockRoute({
      when: {
        kyori: ["1200"],
      },
    }),
  ).toBe(false);
  expect(
    isNamedRacePreraceLockRoute({
      model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
      variant: "",
      when: {
        kyori: ["1200"],
      },
    }),
  ).toBe(false);
  expect(
    isNamedRacePreraceLockRoute({
      model_version: "",
      variant: "chukyo_nisai_stakes_largefield",
      when: {
        kyori: ["1200"],
      },
    }),
  ).toBe(false);
  expect(
    isNamedRacePreraceLockRoute({
      model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
      variant: "chukyo_nisai_stakes_largefield",
      when: {
        kyori: ["1200"],
      },
    }),
  ).toBe(true);
});

it("namedRaceVariantsForCategory skips a prerace route that repeats the cell variant", () => {
  expect(
    namedRaceVariantsForCategory({
      cells: [
        {
          architecture: "catboost",
          base_variant: "joken_999",
          feature_count: 113,
          model_version: "jra-named-chukyo-nisai-stakes-focal-v3",
          prerace_router: {
            routes: [
              {
                model_version: "jra-named-chukyo-nisai-stakes-focal-v3",
                variant: "chukyo_nisai_stakes",
                when: {
                  kyori: ["1400"],
                  month: ["08"],
                },
              },
            ],
          },
          race_name_token: "中京2歳ステークス",
          variant: "chukyo_nisai_stakes",
          venue: "07",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toStrictEqual({
    chukyo_nisai_stakes: {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-chukyo-nisai-stakes-focal-v3",
    },
  });
});

it("namedRaceVariantsForCategory copies prerace lock variants without rerank fields", () => {
  expect(
    namedRaceVariantsForCategory({
      cells: [
        {
          architecture: "catboost",
          base_variant: "joken_999",
          feature_count: 113,
          model_version: "jra-named-niigata-bsn-v1",
          prerace_router: {
            routes: [
              {
                model_version: "jra-named-niigata-bsn-v1",
                variant: "niigata_bsn_alt",
                when: {
                  kyori: ["1800"],
                },
              },
            ],
          },
          race_name_token: "BSN賞",
          variant: "niigata_bsn",
          venue: "04",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toStrictEqual({
    niigata_bsn: {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-niigata-bsn-v1",
    },
    niigata_bsn_alt: {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-niigata-bsn-v1",
    },
  });
});

it("namedRaceVariantsForCategory throws when two cells share a prerace lock variant", () => {
  expect(() =>
    namedRaceVariantsForCategory({
      cells: [
        {
          base_variant: "joken_999",
          prerace_router: {
            routes: [
              {
                model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
                variant: "shared_lock",
                when: {
                  kyori: ["1200"],
                },
              },
            ],
          },
          race_name_token: "中京2歳ステークス",
          variant: "chukyo_nisai_stakes",
          venue: "07",
        },
        {
          base_variant: "joken_999",
          prerace_router: {
            routes: [
              {
                model_version: "jra-named-niigata-bsn-v1",
                variant: "shared_lock",
                when: {
                  kyori: ["1800"],
                },
              },
            ],
          },
          race_name_token: "BSN賞",
          variant: "niigata_bsn",
          venue: "04",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toThrow("Named-race variant 'shared_lock' already exists in the category");
});

it("namedRaceVariantsForCategory throws when a prerace lock variant already exists", () => {
  expect(() =>
    namedRaceVariantsForCategory({
      cells: [
        {
          base_variant: "joken_999",
          prerace_router: {
            routes: [
              {
                model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
                variant: "joken_999",
                when: {
                  kyori: ["1200"],
                  month: ["12"],
                },
              },
            ],
          },
          race_name_token: "中京2歳ステークス",
          variant: "chukyo_nisai_stakes",
          venue: "07",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toThrow("Named-race variant 'joken_999' already exists in the category");
});

it("namedRaceVariantsForCategory inherits the open-class base variant", () => {
  expect(
    namedRaceVariantsForCategory({
      cells: [
        {
          base_variant: "joken_999",
          race_name_token: "BSN賞",
          variant: "niigata_bsn",
          venue: "04",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toStrictEqual({
    niigata_bsn: {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-joken-999-pooled-yetirank-v2",
    },
  });
});

it("namedRaceVariantsForCategory throws when the variant name already exists", () => {
  expect(() =>
    namedRaceVariantsForCategory({
      cells: [
        {
          base_variant: "joken_999",
          race_name_token: "BSN賞",
          variant: "joken_999",
          venue: "04",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toThrow("Named-race variant 'joken_999' already exists in the category");
});

it("namedRaceVariantsForCategory throws when the base variant is missing", () => {
  expect(() =>
    namedRaceVariantsForCategory({
      cells: [
        {
          base_variant: "missing_base",
          race_name_token: "BSN賞",
          variant: "niigata_bsn",
          venue: "04",
        },
      ],
      variants: {
        joken_999: {
          architecture: "catboost",
          feature_count: 113,
          model_version: "jra-joken-999-pooled-yetirank-v2",
        },
      },
    }),
  ).toThrow("Named-race cell 'niigata_bsn' references missing base_variant 'missing_base'");
});

it("buildNamedRaceCellIndex throws on a duplicate venue and token", () => {
  expect(() =>
    buildNamedRaceCellIndex({
      jra: [
        {
          base_variant: "joken_999",
          race_name_token: "BSN賞",
          variant: "niigata_bsn",
          venue: "04",
        },
        {
          base_variant: "joken_999",
          race_name_token: "BSN賞",
          variant: "niigata_bsn_dup",
          venue: "04",
        },
      ],
    }),
  ).toThrow("Duplicate named-race cell for category=jra venue=04 token=BSN賞");
});

it("buildNamedRaceCellIndex throws on a duplicate variant name", () => {
  expect(() =>
    buildNamedRaceCellIndex({
      jra: [
        {
          base_variant: "joken_999",
          race_name_token: "BSN賞",
          variant: "niigata_bsn",
          venue: "04",
        },
        {
          base_variant: "joken_999",
          race_name_token: "新潟記念",
          variant: "niigata_bsn",
          venue: "04",
        },
      ],
    }),
  ).toThrow("Duplicate named-race variant 'niigata_bsn' in category=jra");
});
