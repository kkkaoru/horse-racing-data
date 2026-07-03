// Run with bun test apps/sync-realtime-data/src/running-style-corner-weights.test.ts
import { expect, test } from "vitest";

import {
  computeCornerFrontScore,
  resolveCornerFrontScoreWeights,
} from "./running-style-corner-weights";

test("resolveCornerFrontScoreWeights returns learned JRA weights for jra", () => {
  expect(resolveCornerFrontScoreWeights("jra")).toStrictEqual({
    nige: 0,
    oikomi: 3.0,
    sashi: 1.5,
    senkou: 0.49,
  });
});

test("resolveCornerFrontScoreWeights returns learned NAR weights for nar", () => {
  expect(resolveCornerFrontScoreWeights("nar")).toStrictEqual({
    nige: 0,
    oikomi: 3.0,
    sashi: 1.39,
    senkou: 0.68,
  });
});

test("resolveCornerFrontScoreWeights returns fixed weights for ban-ei", () => {
  expect(resolveCornerFrontScoreWeights("ban-ei")).toStrictEqual({
    nige: 0,
    oikomi: 3.0,
    sashi: 2,
    senkou: 1,
  });
});

test("computeCornerFrontScore under jra weights weights sashi and oikomi more than senkou", () => {
  expect(
    computeCornerFrontScore(
      { nige: 0.4, oikomi: 0.1, sashi: 0.2, senkou: 0.3 },
      { nige: 0, oikomi: 3.0, sashi: 1.5, senkou: 0.49 },
    ),
  ).toBe(0.7470000000000001);
});

test("computeCornerFrontScore under nar weights applies the NAR coefficients", () => {
  expect(
    computeCornerFrontScore(
      { nige: 0.4, oikomi: 0.1, sashi: 0.2, senkou: 0.3 },
      { nige: 0, oikomi: 3.0, sashi: 1.39, senkou: 0.68 },
    ),
  ).toBe(0.782);
});

test("computeCornerFrontScore under fixed weights reproduces the legacy 0-1-2-3 formula", () => {
  expect(
    computeCornerFrontScore(
      { nige: 0.4, oikomi: 0.1, sashi: 0.2, senkou: 0.3 },
      { nige: 0, oikomi: 3.0, sashi: 2, senkou: 1 },
    ),
  ).toBe(1);
});
