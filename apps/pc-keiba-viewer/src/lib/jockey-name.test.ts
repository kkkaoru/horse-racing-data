import { describe, expect, it } from "vitest";

import { isSameJockeyName, normalizeJockeyNameForComparison } from "./jockey-name";

describe("jockey name helpers", () => {
  it("normalizes whitespace for comparison", () => {
    expect(normalizeJockeyNameForComparison(" 増田 充 ")).toBe("増田充");
    expect(normalizeJockeyNameForComparison("シャ ベス")).toBe("シャベス");
  });

  it("treats names with the same first three characters as the same jockey", () => {
    expect(isSameJockeyName("増田充", "増田充宏")).toBe(true);
    expect(isSameJockeyName("シャベ", "シャベス")).toBe(true);
    expect(isSameJockeyName("山田太郎", "山田太一")).toBe(true);
  });

  it("does not treat short partial prefixes or empty names as the same jockey", () => {
    expect(isSameJockeyName("増田", "増田充")).toBe(false);
    expect(isSameJockeyName("", "増田充")).toBe(false);
    expect(isSameJockeyName(null, "増田充")).toBe(false);
    expect(isSameJockeyName("山田太郎", "山本太郎")).toBe(false);
  });
});
