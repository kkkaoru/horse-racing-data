import { describe, expect, it } from "vitest";

import {
  getPreferredJockeyName,
  isSameJockeyName,
  normalizeJockeyNameForDisplay,
  normalizeJockeyNameForComparison,
} from "./jockey-name";

describe("jockey name helpers", () => {
  it("normalizes whitespace for comparison", () => {
    expect(normalizeJockeyNameForComparison(" 増田 充 ")).toBe("増田充");
    expect(normalizeJockeyNameForComparison("団野 大成")).toBe("団野大成");
    expect(normalizeJockeyNameForComparison("団野　大成")).toBe("団野大成");
    expect(normalizeJockeyNameForComparison("団野\u200B大成")).toBe("団野大成");
    expect(normalizeJockeyNameForComparison("シャ ベス")).toBe("シャベス");
    expect(normalizeJockeyNameForComparison("櫻井光")).toBe("桜井光");
    expect(normalizeJockeyNameForComparison("D.レーン")).toBe("レーン");
    expect(normalizeJockeyNameForComparison("Ｄ．レーン")).toBe("レーン");
    expect(normalizeJockeyNameForComparison("Ｍ．デム")).toBe("デムーロ");
  });

  it("normalizes realtime JRA entry text for display and comparison", () => {
    expect(normalizeJockeyNameForDisplay("牝3/鹿 55.0kg 田口 貫太")).toBe("田口貫太");
    expect(normalizeJockeyNameForDisplay("▲森田 誠也")).toBe("森田誠也");
    expect(normalizeJockeyNameForDisplay("牝3/青 53.0kg △田山 旺佑")).toBe("田山旺佑");
    expect(isSameJockeyName("田口貫太", "牝3/鹿 55.0kg 田口 貫太")).toBe(true);
    expect(getPreferredJockeyName("田口貫太", "牝3/鹿 55.0kg 田口 貫太")).toBe("田口貫太");
  });

  it("treats names with the same first three characters as the same jockey", () => {
    expect(isSameJockeyName("増田充", "増田充宏")).toBe(true);
    expect(isSameJockeyName("シャベ", "シャベス")).toBe(true);
    expect(isSameJockeyName("櫻井光", "桜井光輔")).toBe(true);
    expect(isSameJockeyName("山田太郎", "山田太一")).toBe(true);
    expect(isSameJockeyName("レーン", "D.レーン")).toBe(true);
    expect(isSameJockeyName("デムーロ", "Ｍ．デム")).toBe(true);
  });

  it("treats local keiba abbreviated kanji names as the same jockey", () => {
    expect(isSameJockeyName("本田重", "本田正重")).toBe(true);
    expect(isSameJockeyName("本田正重", "本田重")).toBe(true);
    expect(isSameJockeyName("森泰斗", "森島斗")).toBe(false);
    expect(isSameJockeyName("シャベ", "シャス")).toBe(false);
  });

  it("does not treat short partial prefixes or empty names as the same jockey", () => {
    expect(isSameJockeyName("増田", "増田充")).toBe(false);
    expect(isSameJockeyName("", "増田充")).toBe(false);
    expect(isSameJockeyName(null, "増田充")).toBe(false);
    expect(isSameJockeyName("山田太郎", "山本太郎")).toBe(false);
  });

  it("prefers stored names when realtime names refer to the same jockey", () => {
    expect(getPreferredJockeyName("団野大成", "団野 大成")).toBe("団野大成");
    expect(getPreferredJockeyName("櫻井光", "桜井光輔")).toBe("櫻井光");
    expect(getPreferredJockeyName("増田充", "増田充宏")).toBe("増田充");
    expect(getPreferredJockeyName("本田正重", "本田重")).toBe("本田正重");
    expect(getPreferredJockeyName("レーン", "D.レーン")).toBe("レーン");
    expect(getPreferredJockeyName("元騎手", "替騎手")).toBe("替騎手");
    expect(getPreferredJockeyName("元騎手", null)).toBe("元騎手");
  });
});
