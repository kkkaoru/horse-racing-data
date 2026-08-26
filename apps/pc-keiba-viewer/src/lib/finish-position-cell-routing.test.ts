// Run with bun (bunx vitest)

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, it } from "vitest";

import {
  FINISH_POSITION_CELL_ROUTING_CONFIG_FOR_TESTS,
  getAllFinishPositionCellRoutingModelVersions,
  getAllFinishPositionCellRoutingOffLabelVariantModelVersions,
  resolveCellRoutingModelVersionForConfig,
  resolveDimension,
  resolveFinishPositionCellRoutingModelVersion,
  resolveFinishPositionDisplayPriorityModelVersion,
} from "./finish-position-cell-routing";
import type { CellRoutingConfig } from "./finish-position-cell-routing";
import type { RaceDetail } from "./race-types";

const BASE_RACE: RaceDetail = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: null,
  hassoJikoku: "1000",
  jockeyNames: [],
  kaisaiKai: null,
  kaisaiNen: "2026",
  kaisaiNichime: null,
  kaisaiTsukihi: "0711",
  keibajoCode: "02",
  kyori: "1800",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: null,
  juryoShubetsuCode: null,
  raceBango: "01",
  shussoTosu: "14",
  source: "jra",
  tenkoCode: null,
  torokuTosu: null,
  trackCode: "20",
};

// --- resolveDimension: special dimensions ---------------------------------

it("resolveDimension resolves venue from keibajoCode", () => {
  const value = resolveDimension({ category: "jra", dimension: "venue", race: BASE_RACE });
  expect(value).toBe("02");
});

it("resolveDimension resolves non-jra surface to dirt regardless of trackCode", () => {
  const race: RaceDetail = { ...BASE_RACE, source: "nar", trackCode: null };
  const value = resolveDimension({ category: "nar", dimension: "surface", race });
  expect(value).toBe("dirt");
});

it("resolveDimension resolves jra surface to null when trackCode is null", () => {
  const race: RaceDetail = { ...BASE_RACE, trackCode: null };
  const value = resolveDimension({ category: "jra", dimension: "surface", race });
  expect(value).toBe(null);
});

it("resolveDimension resolves jra surface to turf for trackCode starting with 1", () => {
  const race: RaceDetail = { ...BASE_RACE, trackCode: "10" };
  const value = resolveDimension({ category: "jra", dimension: "surface", race });
  expect(value).toBe("turf");
});

it("resolveDimension resolves jra surface to dirt for trackCode starting with 2", () => {
  const race: RaceDetail = { ...BASE_RACE, trackCode: "20" };
  const value = resolveDimension({ category: "jra", dimension: "surface", race });
  expect(value).toBe("dirt");
});

it("resolveDimension resolves jra surface to other for an unrecognized trackCode", () => {
  const race: RaceDetail = { ...BASE_RACE, trackCode: "90" };
  const value = resolveDimension({ category: "jra", dimension: "surface", race });
  expect(value).toBe("other");
});

it("resolveDimension resolves distance_band to null when kyori is null", () => {
  const race: RaceDetail = { ...BASE_RACE, kyori: null };
  const value = resolveDimension({ category: "jra", dimension: "distance_band", race });
  expect(value).toBe(null);
});

it("resolveDimension resolves distance_band to null when kyori is not numeric", () => {
  const race: RaceDetail = { ...BASE_RACE, kyori: "abc" };
  const value = resolveDimension({ category: "jra", dimension: "distance_band", race });
  expect(value).toBe(null);
});

it("resolveDimension resolves distance_band to sprint under 1200", () => {
  const race: RaceDetail = { ...BASE_RACE, kyori: "1000" };
  const value = resolveDimension({ category: "jra", dimension: "distance_band", race });
  expect(value).toBe("sprint");
});

it("resolveDimension resolves distance_band to mile under 1600", () => {
  const race: RaceDetail = { ...BASE_RACE, kyori: "1400" };
  const value = resolveDimension({ category: "jra", dimension: "distance_band", race });
  expect(value).toBe("mile");
});

it("resolveDimension resolves distance_band to intermediate under 2000", () => {
  const race: RaceDetail = { ...BASE_RACE, kyori: "1800" };
  const value = resolveDimension({ category: "jra", dimension: "distance_band", race });
  expect(value).toBe("intermediate");
});

it("resolveDimension resolves distance_band to long under 2400", () => {
  const race: RaceDetail = { ...BASE_RACE, kyori: "2200" };
  const value = resolveDimension({ category: "jra", dimension: "distance_band", race });
  expect(value).toBe("long");
});

it("resolveDimension resolves distance_band to extended at or above 2400", () => {
  const race: RaceDetail = { ...BASE_RACE, kyori: "3000" };
  const value = resolveDimension({ category: "jra", dimension: "distance_band", race });
  expect(value).toBe("extended");
});

it("resolveDimension resolves field_band to null when shussoTosu is null", () => {
  const race: RaceDetail = { ...BASE_RACE, shussoTosu: null };
  const value = resolveDimension({ category: "jra", dimension: "field_band", race });
  expect(value).toBe(null);
});

it("resolveDimension resolves field_band to f_le10 at or under 10", () => {
  const race: RaceDetail = { ...BASE_RACE, shussoTosu: "8" };
  const value = resolveDimension({ category: "jra", dimension: "field_band", race });
  expect(value).toBe("f_le10");
});

it("resolveDimension resolves field_band to f11_13 at or under 13", () => {
  const race: RaceDetail = { ...BASE_RACE, shussoTosu: "12" };
  const value = resolveDimension({ category: "jra", dimension: "field_band", race });
  expect(value).toBe("f11_13");
});

it("resolveDimension resolves field_band to f14_15 at or under 15", () => {
  const race: RaceDetail = { ...BASE_RACE, shussoTosu: "15" };
  const value = resolveDimension({ category: "jra", dimension: "field_band", race });
  expect(value).toBe("f14_15");
});

it("resolveDimension resolves field_band to f16p above 15", () => {
  const race: RaceDetail = { ...BASE_RACE, shussoTosu: "18" };
  const value = resolveDimension({ category: "jra", dimension: "field_band", race });
  expect(value).toBe("f16p");
});

it("resolveDimension resolves season to null for a non-numeric month prefix", () => {
  const race: RaceDetail = { ...BASE_RACE, kaisaiTsukihi: "XX11" };
  const value = resolveDimension({ category: "jra", dimension: "season", race });
  expect(value).toBe(null);
});

it("resolveDimension resolves season to spring for March-May", () => {
  const race: RaceDetail = { ...BASE_RACE, kaisaiTsukihi: "0405" };
  const value = resolveDimension({ category: "jra", dimension: "season", race });
  expect(value).toBe("spring");
});

it("resolveDimension resolves season to summer for June-August", () => {
  const race: RaceDetail = { ...BASE_RACE, kaisaiTsukihi: "0711" };
  const value = resolveDimension({ category: "jra", dimension: "season", race });
  expect(value).toBe("summer");
});

it("resolveDimension resolves season to autumn for September-November", () => {
  const race: RaceDetail = { ...BASE_RACE, kaisaiTsukihi: "1003" };
  const value = resolveDimension({ category: "jra", dimension: "season", race });
  expect(value).toBe("autumn");
});

it("resolveDimension resolves season to winter for December-February", () => {
  const race: RaceDetail = { ...BASE_RACE, kaisaiTsukihi: "0115" };
  const value = resolveDimension({ category: "jra", dimension: "season", race });
  expect(value).toBe("winter");
});

it("resolveDimension resolves class to unknown when gradeCode and condition are null", () => {
  const race: RaceDetail = { ...BASE_RACE, gradeCode: null, kyosoJokenCode: null };
  const value = resolveDimension({ category: "jra", dimension: "class", race });
  expect(value).toBe("unknown");
});

it("resolveDimension resolves class from condition when gradeCode is blank", () => {
  const race: RaceDetail = { ...BASE_RACE, gradeCode: "  ", kyosoJokenCode: "703" };
  const value = resolveDimension({ category: "jra", dimension: "class", race });
  expect(value).toBe("joken-703");
});

it("resolveDimension resolves class to the trimmed gradeCode when present", () => {
  const race: RaceDetail = { ...BASE_RACE, gradeCode: " E " };
  const value = resolveDimension({ category: "jra", dimension: "class", race });
  expect(value).toBe("E");
});

it("resolveDimension resolves is_final_race to true when raceBango matches cardMaxRaceBango", () => {
  const race: RaceDetail = { ...BASE_RACE, raceBango: "10" };
  const value = resolveDimension({
    cardMaxRaceBango: 10,
    category: "nar",
    dimension: "is_final_race",
    race,
  });
  expect(value).toBe("true");
});

it("resolveDimension resolves is_final_race to false when raceBango does not match cardMaxRaceBango", () => {
  const race: RaceDetail = { ...BASE_RACE, raceBango: "05" };
  const value = resolveDimension({
    cardMaxRaceBango: 10,
    category: "nar",
    dimension: "is_final_race",
    race,
  });
  expect(value).toBe("false");
});

it("resolveDimension resolves is_final_race to null when cardMaxRaceBango is undefined", () => {
  const race: RaceDetail = { ...BASE_RACE, raceBango: "10" };
  const value = resolveDimension({ category: "nar", dimension: "is_final_race", race });
  expect(value).toBe(null);
});

it("resolveDimension resolves is_final_race to null when cardMaxRaceBango is null", () => {
  const race: RaceDetail = { ...BASE_RACE, raceBango: "10" };
  const value = resolveDimension({
    cardMaxRaceBango: null,
    category: "nar",
    dimension: "is_final_race",
    race,
  });
  expect(value).toBe(null);
});

it("resolveDimension resolves is_final_race to null when raceBango is not numeric", () => {
  const race: RaceDetail = { ...BASE_RACE, raceBango: "xx" };
  const value = resolveDimension({
    cardMaxRaceBango: 10,
    category: "nar",
    dimension: "is_final_race",
    race,
  });
  expect(value).toBe(null);
});

// --- resolveDimension: raw fallback dimensions ----------------------------

it("resolveDimension resolves kyoso_joken_code from the raw column", () => {
  const race: RaceDetail = { ...BASE_RACE, kyosoJokenCode: "703" };
  const value = resolveDimension({ category: "jra", dimension: "kyoso_joken_code", race });
  expect(value).toBe("703");
});

it("resolveDimension resolves kyoso_joken_code to null when absent", () => {
  const value = resolveDimension({
    category: "jra",
    dimension: "kyoso_joken_code",
    race: { ...BASE_RACE, kyosoJokenCode: null },
  });
  expect(value).toBe(null);
});

it("resolveDimension resolves grade_code from the raw column", () => {
  const race: RaceDetail = { ...BASE_RACE, gradeCode: "E" };
  const value = resolveDimension({ category: "ban-ei", dimension: "grade_code", race });
  expect(value).toBe("E");
});

it("resolveDimension resolves keibajo_code from the raw column", () => {
  const value = resolveDimension({ category: "jra", dimension: "keibajo_code", race: BASE_RACE });
  expect(value).toBe("02");
});

it("resolveDimension resolves track_code from the raw column", () => {
  const value = resolveDimension({ category: "jra", dimension: "track_code", race: BASE_RACE });
  expect(value).toBe("20");
});

it("resolveDimension resolves kyori from the raw column", () => {
  const value = resolveDimension({ category: "jra", dimension: "kyori", race: BASE_RACE });
  expect(value).toBe("1800");
});

it("resolveDimension resolves shusso_tosu from the raw column", () => {
  const value = resolveDimension({ category: "jra", dimension: "shusso_tosu", race: BASE_RACE });
  expect(value).toBe("14");
});

it("resolveDimension resolves kaisai_tsukihi from the raw column", () => {
  const value = resolveDimension({ category: "jra", dimension: "kaisai_tsukihi", race: BASE_RACE });
  expect(value).toBe("0711");
});

it("resolveDimension resolves an unknown dimension name to null", () => {
  const value = resolveDimension({
    category: "jra",
    dimension: "not_a_real_dimension",
    race: BASE_RACE,
  });
  expect(value).toBe(null);
});

it("resolveDimension resolves canonical distance boundaries", () => {
  const sprint = resolveDimension({
    category: "nar",
    dimension: "canonical_distance_band",
    race: { ...BASE_RACE, kyori: "1400" },
  });
  const mile = resolveDimension({
    category: "nar",
    dimension: "canonical_distance_band",
    race: { ...BASE_RACE, kyori: "1800" },
  });
  const intermediate = resolveDimension({
    category: "nar",
    dimension: "canonical_distance_band",
    race: { ...BASE_RACE, kyori: "2200" },
  });
  const long = resolveDimension({
    category: "nar",
    dimension: "canonical_distance_band",
    race: { ...BASE_RACE, kyori: "2800" },
  });
  const extended = resolveDimension({
    category: "nar",
    dimension: "canonical_distance_band",
    race: { ...BASE_RACE, kyori: "2801" },
  });
  expect([sprint, mile, intermediate, long, extended]).toStrictEqual([
    "sprint",
    "mile",
    "intermediate",
    "long",
    "extended",
  ]);
});

it("resolveDimension resolves canonical field-size boundaries", () => {
  const small = resolveDimension({
    category: "nar",
    dimension: "canonical_field_size_band",
    race: { ...BASE_RACE, shussoTosu: "8" },
  });
  const medium = resolveDimension({
    category: "nar",
    dimension: "canonical_field_size_band",
    race: { ...BASE_RACE, shussoTosu: "14" },
  });
  const large = resolveDimension({
    category: "nar",
    dimension: "canonical_field_size_band",
    race: { ...BASE_RACE, shussoTosu: "15" },
  });
  const missing = resolveDimension({
    category: "nar",
    dimension: "canonical_field_size_band",
    race: { ...BASE_RACE, shussoTosu: null },
  });
  expect([small, medium, large, missing]).toStrictEqual(["small", "medium", "large", null]);
});

it("resolveDimension derives NAR subclasses and current condition", () => {
  const mukatsu = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "未勝利", source: "nar" },
  });
  const condition = resolveDimension({
    category: "nar",
    dimension: "current_baba_condition",
    race: { ...BASE_RACE, babajotaiCodeDirt: "2", source: "nar" },
  });
  expect([mukatsu, condition]).toStrictEqual(["MUKATSU", "2"]);
});

it("resolveDimension derives every remaining NAR subclass branch", () => {
  const op = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "ＯＰ", source: "nar" },
  });
  const newcomer = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "新馬", source: "nar" },
  });
  const twoYear = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "2歳", source: "nar" },
  });
  const threeYear = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "３歳", source: "nar" },
  });
  const a = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "Ａ級", source: "nar" },
  });
  const b = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "Ｂ級", source: "nar" },
  });
  const c = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "Ｃ級", source: "nar" },
  });
  const other = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: null, source: "nar" },
  });
  const jra = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, kyosoJokenMeisho: "Ｃ級", source: "jra" },
  });
  const banei = resolveDimension({
    category: "nar",
    dimension: "nar_subclass",
    race: { ...BASE_RACE, keibajoCode: "83", kyosoJokenMeisho: "Ｃ級", source: "nar" },
  });
  expect([op, newcomer, twoYear, threeYear, a, b, c, other, jra, banei]).toStrictEqual([
    "OP",
    "NEW",
    "2YO",
    "3YO",
    "A",
    "B",
    "C",
    "other",
    null,
    null,
  ]);
});

// --- resolveFinishPositionCellRoutingModelVersion -------------------------

it("resolveFinishPositionCellRoutingModelVersion returns null for a category with no routing config", () => {
  const race: RaceDetail = { ...BASE_RACE, source: "nar", keibajoCode: "54" };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "nar", race });
  expect(value).toBe(null);
});

it("resolveFinishPositionCellRoutingModelVersion routes ungraded JRA class-703 races to the matching gated model", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "05", kyosoJokenCode: "703" };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "jra", race });
  expect(value).toBe("jra-joken-703-dirt-intermediate-qsm-gated-v1");
});

it("resolveFinishPositionCellRoutingModelVersion routes ungraded class-005 races to the matching gated model", () => {
  const race: RaceDetail = {
    ...BASE_RACE,
    keibajoCode: "05",
    kyosoJokenCode: "005",
    shussoTosu: "9",
    trackCode: "20",
  };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "jra", race });
  expect(value).toBe("jra-joken-005-dirt-1800-nonautumn-qsm-gated-v1");
});

it("resolveFinishPositionCellRoutingModelVersion prioritizes the gated class model over Hakodate venue", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "02", kyosoJokenCode: "010" };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "jra", race });
  expect(value).toBe("jra-joken-010-dirt-intermediate-yeti-gated-v1");
});

it("resolveFinishPositionCellRoutingModelVersion returns null for a JRA race matching no rule", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "05", kyosoJokenCode: "000" };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "jra", race });
  expect(value).toBe(null);
});

it("resolveFinishPositionCellRoutingModelVersion routes Ban-ei grade E races to the base variant", () => {
  const race: RaceDetail = { ...BASE_RACE, gradeCode: "E", keibajoCode: "83", source: "nar" };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "ban-ei", race });
  expect(value).toBe("banei-cb-v8-window2011-wf-15y");
});

it("resolveFinishPositionCellRoutingModelVersion returns null for a non-E Ban-ei race", () => {
  const race: RaceDetail = { ...BASE_RACE, gradeCode: "A", keibajoCode: "83", source: "nar" };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "ban-ei", race });
  expect(value).toBe(null);
});

it("resolveFinishPositionCellRoutingModelVersion routes promoted NAR canonical cells", () => {
  const race: RaceDetail = {
    ...BASE_RACE,
    babajotaiCodeDirt: "1",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0711",
    keibajoCode: "30",
    kyori: "1200",
    kyosoJokenMeisho: "未勝利",
    shussoTosu: "10",
    source: "nar",
    trackCode: "23",
  };
  const value = resolveFinishPositionCellRoutingModelVersion({ category: "nar", race });
  expect(value).toBe("nar-cell-top1-30-mukatsu-sprint-summer-tc1-v1");
});

it("resolveFinishPositionCellRoutingModelVersion enforces effective date and medium field", () => {
  const common: RaceDetail = {
    ...BASE_RACE,
    babajotaiCodeDirt: "2",
    kaisaiNen: "2026",
    keibajoCode: "30",
    kyori: "1200",
    kyosoJokenMeisho: "Ｃ級",
    shussoTosu: "9",
    source: "nar",
    trackCode: "23",
  };
  const before = resolveFinishPositionCellRoutingModelVersion({
    category: "nar",
    race: { ...common, kaisaiTsukihi: "0630" },
  });
  const after = resolveFinishPositionCellRoutingModelVersion({
    category: "nar",
    race: { ...common, kaisaiTsukihi: "0701" },
  });
  const small = resolveFinishPositionCellRoutingModelVersion({
    category: "nar",
    race: { ...common, kaisaiTsukihi: "0701", shussoTosu: "8" },
  });
  expect([before, after, small]).toStrictEqual([
    null,
    "nar-cell-top1-30-c-sprint-summer-tc2-adaptive-v1",
    null,
  ]);
});

it("resolveFinishPositionDisplayPriorityModelVersion prefers NAR cell then transformer", () => {
  const matched: RaceDetail = {
    ...BASE_RACE,
    babajotaiCodeDirt: "1",
    kaisaiTsukihi: "0711",
    keibajoCode: "42",
    kyori: "1400",
    kyosoJokenMeisho: "Ｃ級",
    shussoTosu: "14",
    source: "nar",
    trackCode: "23",
  };
  const unmatched: RaceDetail = { ...matched, shussoTosu: "8" };
  expect(resolveFinishPositionDisplayPriorityModelVersion({ category: "nar", race: matched })).toBe(
    "nar-cell-top1-42-c-sprint-summer-tc1-v1",
  );
  expect(
    resolveFinishPositionDisplayPriorityModelVersion({ category: "nar", race: unmatched }),
  ).toBe("iter40-nar-settransformer-blend-v1");
});

// --- resolveCellRoutingModelVersionForConfig: malformed-config guard -------

it("resolveCellRoutingModelVersionForConfig returns null when a matched rule references a missing variant", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "02" };
  const brokenConfig: CellRoutingConfig = {
    jra: {
      default_variant: "sim",
      rules: [
        {
          conditions: [{ dimension: "venue", values: ["02"] }],
          variant: "variant_not_in_variants_map",
        },
      ],
      variants: {
        sim: {
          architecture: "catboost",
          feature_count: 1,
          model_version: "jra-cb-v9-sim-2013-clean",
        },
      },
    },
  };
  const value = resolveCellRoutingModelVersionForConfig({
    category: "jra",
    config: brokenConfig,
    race,
  });
  expect(value).toBe(null);
});

// --- is_final_race threaded through resolveCellRoutingModelVersionForConfig
// (mirrors tmp/kochi-final/cell_design.md section 4's not-yet-live rule
// shape, proving cardMaxRaceBango threads end-to-end through a real
// multi-condition AND rule without that shape existing in the real
// cell_routing.json yet) --------------------------------------------------

const KOCHI_FINAL_SHAPED_CONFIG: CellRoutingConfig = {
  nar: {
    default_variant: "sim",
    rules: [
      {
        conditions: [
          { dimension: "venue", values: ["54"] },
          { dimension: "is_final_race", values: ["true"] },
        ],
        variant: "kochi_final",
      },
    ],
    variants: {
      kochi_final: {
        architecture: "catboost",
        feature_count: 50,
        model_version: "nar-cb-kochi-final-v1",
      },
      sim: {
        architecture: "xgboost",
        feature_count: 188,
        model_version: "iter12-nar-xgb-hpo-v8-clean188",
      },
    },
  },
};

it("resolveCellRoutingModelVersionForConfig routes a Kochi final race to the kochi_final variant", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "54", raceBango: "10", source: "nar" };
  const value = resolveCellRoutingModelVersionForConfig({
    cardMaxRaceBango: 10,
    category: "nar",
    config: KOCHI_FINAL_SHAPED_CONFIG,
    race,
  });
  expect(value).toBe("nar-cb-kochi-final-v1");
});

it("resolveCellRoutingModelVersionForConfig does not route a non-final Kochi race", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "54", raceBango: "05", source: "nar" };
  const value = resolveCellRoutingModelVersionForConfig({
    cardMaxRaceBango: 10,
    category: "nar",
    config: KOCHI_FINAL_SHAPED_CONFIG,
    race,
  });
  expect(value).toBe(null);
});

it("resolveCellRoutingModelVersionForConfig fails closed without cardMaxRaceBango", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "54", raceBango: "10", source: "nar" };
  const value = resolveCellRoutingModelVersionForConfig({
    category: "nar",
    config: KOCHI_FINAL_SHAPED_CONFIG,
    race,
  });
  expect(value).toBe(null);
});

it("resolveFinishPositionCellRoutingModelVersion threads cardMaxRaceBango through to resolveDimension", () => {
  const race: RaceDetail = { ...BASE_RACE, keibajoCode: "02", kyosoJokenCode: "703" };
  const value = resolveFinishPositionCellRoutingModelVersion({
    cardMaxRaceBango: 12,
    category: "jra",
    race,
  });
  expect(value).toBe("jra-joken-703-dirt-intermediate-qsm-gated-v1");
});

// --- getAllFinishPositionCellRoutingModelVersions --------------------------

it("getAllFinishPositionCellRoutingModelVersions returns every distinct variant model_version", () => {
  const versions = getAllFinishPositionCellRoutingModelVersions();
  expect(versions).toStrictEqual([
    "jra-cb-v9-sim-2013-clean",
    "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
    "jra-cb-v10-prior-corner274-2013",
    "jra-joken-005-pooled-yetirank-v2",
    "jra-joken-005-dirt-mile-autumn-yeti-gated-v1",
    "jra-joken-005-turf-mile-yeti-gated-v1",
    "jra-joken-005-turf-long-hierarchical-qsm-gated-v2",
    "jra-joken-010-pooled-yetirank-v2",
    "jra-joken-016-pooled-yetirank-v2",
    "jra-joken-701-pooled-yetirank-v2",
    "jra-joken-010-dirt-intermediate-yeti-gated-v1",
    "jra-joken-701-turf-mile-qsm-gated-v1",
    "jra-joken-701-turf-intermediate-qsm-gated-v1",
    "jra-joken-701-turf-long-qsm-gated-v1",
    "jra-joken-703-pooled-yetirank-v2",
    "jra-joken-703-turf-long-spring-qsm-gated-v1",
    "jra-joken-703-turf-intermediate-qsm-gated-v1",
    "jra-joken-703-other-extended-qsm-gated-v1",
    "jra-joken-703-dirt-sprint-yeti-gated-v1",
    "jra-joken-703-dirt-intermediate-qsm-gated-v1",
    "jra-joken-703-querysoftmax-maxrange-v1",
    "jra-joken-999-pooled-yetirank-v2",
    "jra-joken-005-turf-intermediate-spring-qsm-gated-v1",
    "jra-joken-005-dirt-mile-spring-qsm-gated-v1",
    "jra-joken-005-dirt-intermediate-autumn-yeti-gated-v1",
    "jra-joken-703-turf-long-summer-yeti-gated-v1",
    "jra-joken-005-dirt-1700-summer-qsm-gated-v1",
    "jra-joken-005-dirt-1200-winter-summer-qsm-gated-v1",
    "jra-joken-703-turf-1400-qsm-gated-v1",
    "jra-joken-005-dirt-1800-nonautumn-qsm-gated-v1",
    "jra-joken-703-turf-1200-largefield-yeti-gated-v1",
    "banei-cb-v9-sim-2011",
    "banei-cb-v8-window2011-wf-15y",
    "iter12-nar-xgb-hpo-v8-clean188",
    "nar-cell-top1-30-mukatsu-sprint-summer-tc1-v1",
    "nar-cell-top2-30-mukatsu-sprint-summer-tc2-v1",
    "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-market-rider",
    "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-pedigree-surface",
    "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-physiology-form",
    "nar-cell-top1-42-c-sprint-summer-tc1-v1",
    "nar-cell-top1-30-c-sprint-summer-tc1-v1",
    "nar-cell-top1-30-c-sprint-summer-tc2-adaptive-v1",
    "nar-cell-top1-50-c-sprint-summer-tc1-rolling-v1",
    "nar-cell-top1-43-c-sprint-winter-tc1-rolling-v1",
  ]);
});

// --- getAllFinishPositionCellRoutingOffLabelVariantModelVersions -----------

it("getAllFinishPositionCellRoutingOffLabelVariantModelVersions excludes each category's default variant", () => {
  const versions = getAllFinishPositionCellRoutingOffLabelVariantModelVersions();
  expect(versions).toStrictEqual([
    "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
    "jra-cb-v10-prior-corner274-2013",
    "jra-joken-005-pooled-yetirank-v2",
    "jra-joken-005-dirt-mile-autumn-yeti-gated-v1",
    "jra-joken-005-turf-mile-yeti-gated-v1",
    "jra-joken-005-turf-long-hierarchical-qsm-gated-v2",
    "jra-joken-010-pooled-yetirank-v2",
    "jra-joken-016-pooled-yetirank-v2",
    "jra-joken-701-pooled-yetirank-v2",
    "jra-joken-010-dirt-intermediate-yeti-gated-v1",
    "jra-joken-701-turf-mile-qsm-gated-v1",
    "jra-joken-701-turf-intermediate-qsm-gated-v1",
    "jra-joken-701-turf-long-qsm-gated-v1",
    "jra-joken-703-pooled-yetirank-v2",
    "jra-joken-703-turf-long-spring-qsm-gated-v1",
    "jra-joken-703-turf-intermediate-qsm-gated-v1",
    "jra-joken-703-other-extended-qsm-gated-v1",
    "jra-joken-703-dirt-sprint-yeti-gated-v1",
    "jra-joken-703-dirt-intermediate-qsm-gated-v1",
    "jra-joken-703-querysoftmax-maxrange-v1",
    "jra-joken-999-pooled-yetirank-v2",
    "jra-joken-005-turf-intermediate-spring-qsm-gated-v1",
    "jra-joken-005-dirt-mile-spring-qsm-gated-v1",
    "jra-joken-005-dirt-intermediate-autumn-yeti-gated-v1",
    "jra-joken-703-turf-long-summer-yeti-gated-v1",
    "jra-joken-005-dirt-1700-summer-qsm-gated-v1",
    "jra-joken-005-dirt-1200-winter-summer-qsm-gated-v1",
    "jra-joken-703-turf-1400-qsm-gated-v1",
    "jra-joken-005-dirt-1800-nonautumn-qsm-gated-v1",
    "jra-joken-703-turf-1200-largefield-yeti-gated-v1",
    "banei-cb-v8-window2011-wf-15y",
    "nar-cell-top1-30-mukatsu-sprint-summer-tc1-v1",
    "nar-cell-top2-30-mukatsu-sprint-summer-tc2-v1",
    "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-market-rider",
    "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-pedigree-surface",
    "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-physiology-form",
    "nar-cell-top1-42-c-sprint-summer-tc1-v1",
    "nar-cell-top1-30-c-sprint-summer-tc1-v1",
    "nar-cell-top1-30-c-sprint-summer-tc2-adaptive-v1",
    "nar-cell-top1-50-c-sprint-summer-tc1-rolling-v1",
    "nar-cell-top1-43-c-sprint-winter-tc1-rolling-v1",
  ]);
});

// --- parity against the container's real cell_routing.json ----------------

// Intentional exception to "mock all file I/O in tests": this test's entire
// purpose is to catch drift between this viewer-side mirror and the
// container's real config, so it must read the real file. Mirrors the same
// pattern already established on the Python side
// (test_cell_router.py::test_load_cell_router_real_config_has_no_nar_routing).
it("FINISH_POSITION_CELL_ROUTING_CONFIG mirrors the container's cell_routing.json exactly", () => {
  const containerConfigPath = resolve(
    process.cwd(),
    "../finish-position-predict-container/src/predict_lib/cell_routing.json",
  );
  const containerConfig: unknown = JSON.parse(readFileSync(containerConfigPath, "utf-8"));
  expect(FINISH_POSITION_CELL_ROUTING_CONFIG_FOR_TESTS).toStrictEqual(containerConfig);
});
