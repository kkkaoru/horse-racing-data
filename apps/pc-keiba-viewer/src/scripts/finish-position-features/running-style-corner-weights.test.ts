// Run with: bun run --filter pc-keiba-viewer test
import { expect, test } from "vitest";

import {
  FIXED_CORNER_FRONT_SCORE_WEIGHTS,
  JRA_CORNER_FRONT_SCORE_WEIGHTS,
  NAR_CORNER_FRONT_SCORE_WEIGHTS,
  computeCornerFrontScore,
  resolveCornerFrontScoreWeightsForRace,
} from "./running-style-corner-weights";

test("resolveCornerFrontScoreWeightsForRace returns JRA weights for source jra", () => {
  expect(resolveCornerFrontScoreWeightsForRace({ source: "jra", keibajoCode: "05" })).toStrictEqual(
    {
      nige: 0,
      senkou: 0.49,
      sashi: 1.5,
      oikomi: 3,
    },
  );
});

test("resolveCornerFrontScoreWeightsForRace returns NAR weights for source nar off the ban-ei code", () => {
  expect(resolveCornerFrontScoreWeightsForRace({ source: "nar", keibajoCode: "30" })).toStrictEqual(
    {
      nige: 0,
      senkou: 0.68,
      sashi: 1.39,
      oikomi: 3,
    },
  );
});

test("resolveCornerFrontScoreWeightsForRace returns FIXED weights for the ban-ei keibajo code even when source is nar", () => {
  expect(resolveCornerFrontScoreWeightsForRace({ source: "nar", keibajoCode: "83" })).toStrictEqual(
    {
      nige: 0,
      senkou: 1,
      sashi: 2,
      oikomi: 3,
    },
  );
});

test("resolveCornerFrontScoreWeightsForRace returns FIXED weights for an unknown source", () => {
  expect(
    resolveCornerFrontScoreWeightsForRace({ source: "ban-ei", keibajoCode: "44" }),
  ).toStrictEqual({
    nige: 0,
    senkou: 1,
    sashi: 2,
    oikomi: 3,
  });
});

test("computeCornerFrontScore applies FIXED weights to a concrete probability array", () => {
  expect(computeCornerFrontScore([0.4, 0.3, 0.2, 0.1], FIXED_CORNER_FRONT_SCORE_WEIGHTS)).toBe(1);
});

test("computeCornerFrontScore applies JRA weights to a concrete probability array", () => {
  expect(computeCornerFrontScore([0.4, 0.3, 0.2, 0.1], JRA_CORNER_FRONT_SCORE_WEIGHTS)).toBe(
    0.7470000000000001,
  );
});

test("computeCornerFrontScore applies NAR weights to a concrete probability array", () => {
  expect(computeCornerFrontScore([0.4, 0.3, 0.2, 0.1], NAR_CORNER_FRONT_SCORE_WEIGHTS)).toBe(0.782);
});

test("computeCornerFrontScore falls back to zero for missing indices with FIXED weights", () => {
  expect(computeCornerFrontScore([0.5, 0.5], FIXED_CORNER_FRONT_SCORE_WEIGHTS)).toBe(0.5);
});

test("computeCornerFrontScore falls back to zero for every missing index on an empty array", () => {
  expect(computeCornerFrontScore([], JRA_CORNER_FRONT_SCORE_WEIGHTS)).toBe(0);
});
