import { expect, it } from "vitest";

import type { Runner } from "./race-types";
import { getRunnerDisplayNames } from "./runner-display";

const baseRunner = (): Runner => ({
  banushimei: "短縮馬主",
  barei: "03",
  bataiju: "480",
  bamei: "短縮馬名",
  chokyoshimeiRyakusho: "短縮調教師",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "550",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2023100001",
  kishumeiRyakusho: "短縮騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: null,
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: "+",
  zogenSa: "0",
});

it("prefers supplemental overseas profile names", () => {
  expect(
    getRunnerDisplayNames({
      ...baseRunner(),
      horseNameFull: "Overseas Horse",
      jockeyNameFull: "正式騎手",
      ownerNameFull: "Official Owner",
      trainerNameFull: "正式調教師",
    }),
  ).toStrictEqual({
    horse: "Overseas Horse",
    jockey: "正式騎手",
    owner: "Official Owner",
    trainer: "正式調教師",
  });
});

it("falls back to JV names when no supplemental profile exists", () => {
  expect(getRunnerDisplayNames(baseRunner())).toStrictEqual({
    horse: "短縮馬名",
    jockey: "短縮騎手",
    owner: "短縮馬主",
    trainer: "短縮調教師",
  });
});

it("returns empty display names when neither source has a value", () => {
  expect(
    getRunnerDisplayNames({
      ...baseRunner(),
      banushimei: " ",
      bamei: null,
      chokyoshimeiRyakusho: "",
      horseNameFull: "",
      jockeyNameFull: null,
      kishumeiRyakusho: null,
      ownerNameFull: " ",
      trainerNameFull: null,
    }),
  ).toStrictEqual({ horse: "", jockey: "", owner: "", trainer: "" });
});
