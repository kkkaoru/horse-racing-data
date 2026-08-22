// Run with: bunx vitest run src/app/races/detail/bloodline-similar-combined-table.test.ts

import { expect, test } from "vitest";

import type { Runner } from "../../../lib/race-types";
import {
  buildCombinedScoreRows,
  createDefaultScoreTargets,
} from "./bloodline-similar-combined-table";

const runner: Runner = {
  bamei: "テストホース",
  barei: "3",
  banushimei: null,
  bataiju: null,
  chokyoshimeiRyakusho: "調教師",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: null,
  kakuteiChakujun: null,
  kettoTorokuBango: null,
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: null,
  tanshoOdds: null,
  timeSa: null,
  umaban: "01",
  wakuban: "01",
  zogenFugo: null,
  zogenSa: null,
};

test("createDefaultScoreTargets returns a complete independent value", () => {
  const first = createDefaultScoreTargets();
  const second = createDefaultScoreTargets();

  expect(first).toStrictEqual({
    base: { correlation: true, time: true },
    bloodline: {
      damDamSire: true,
      damSire: true,
      damSireSire: true,
      sire: true,
      sireDamSire: true,
      sireSire: true,
      sireSireSire: true,
    },
    similar: { jockey: true, owner: true, trainer: true },
  });
  expect(first === second).toBe(false);
  expect(first.base === second.base).toBe(false);
  expect(first.bloodline === second.bloodline).toBe(false);
  expect(first.similar === second.similar).toBe(false);
});

test("buildCombinedScoreRows uses complete targets when scoreTargets is omitted", () => {
  const rows = buildCombinedScoreRows({
    bloodlineRows: [],
    correlationRows: [],
    rows: [],
    runners: [runner],
    timeRows: [],
  });

  expect(rows).toHaveLength(1);
  expect(rows[0]?.horseNumber).toBe("1");
  expect(rows[0]?.score).toBe(1);
});
