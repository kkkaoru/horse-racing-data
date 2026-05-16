import { describe, expect, it } from "vitest";

import { formatTracen, formatTrainingTime, formatWoodCourse } from "./training-format";

describe("training format helpers", () => {
  it("formats training times from tenths of seconds", () => {
    expect(formatTrainingTime("0690")).toBe("69.0");
    expect(formatTrainingTime("196")).toBe("19.6");
    expect(formatTrainingTime("0000")).toBe("-");
    expect(formatTrainingTime("9999")).toBe("-");
    expect(formatTrainingTime("abc")).toBe("-");
    expect(formatTrainingTime(null)).toBe("-");
  });

  it("formats training location labels", () => {
    expect(formatTracen("0")).toBe("美浦");
    expect(formatTracen("1")).toBe("栗東");
    expect(formatTracen("9")).toBe("トレセン9");
    expect(formatTracen(null)).toBe("-");
    expect(formatTracen("")).toBe("-");
    expect(formatWoodCourse("2", "1")).toBe("Bコース / 外");
    expect(formatWoodCourse("9", "9")).toBe("9コース / 9");
    expect(formatWoodCourse("2", null)).toBe("Bコース");
    expect(formatWoodCourse(null, "1")).toBe("外");
    expect(formatWoodCourse(null, null)).toBe("-");
  });
});
