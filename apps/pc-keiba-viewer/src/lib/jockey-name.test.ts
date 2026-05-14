import { describe, expect, it } from "vitest";

import {
  getPreferredJockeyName,
  isSameJockeyName,
  normalizeJockeyNameForComparison,
} from "./jockey-name";

describe("jockey name helpers", () => {
  it("normalizes whitespace for comparison", () => {
    expect(normalizeJockeyNameForComparison(" 増田 充 ")).toBe("増田充");
    expect(normalizeJockeyNameForComparison("シャ ベス")).toBe("シャベス");
    expect(normalizeJockeyNameForComparison("櫻井光")).toBe("桜井光");
  });

  it("treats names with the same first three characters as the same jockey", () => {
    expect(isSameJockeyName("増田充", "増田充宏")).toBe(true);
    expect(isSameJockeyName("シャベ", "シャベス")).toBe(true);
    expect(isSameJockeyName("櫻井光", "桜井光輔")).toBe(true);
    expect(isSameJockeyName("山田太郎", "山田太一")).toBe(true);
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
    expect(getPreferredJockeyName("櫻井光", "桜井光輔")).toBe("櫻井光");
    expect(getPreferredJockeyName("増田充", "増田充宏")).toBe("増田充");
    expect(getPreferredJockeyName("本田正重", "本田重")).toBe("本田正重");
    expect(getPreferredJockeyName("元騎手", "替騎手")).toBe("替騎手");
    expect(getPreferredJockeyName("元騎手", null)).toBe("元騎手");
  });
});
