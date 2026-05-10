import { describe, expect, it } from "vitest";

import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
} from "./runner-format";

describe("runner format helpers", () => {
  it("formats horse weight with and without diff", () => {
    expect(formatHorseWeight("480", "+", "12")).toBe("480kg (+12)");
    expect(formatHorseWeight("480", "", "")).toBe("480kg");
    expect(formatHorseWeight("4AE", "+", "008", true)).toBe("1198kg (+8)");
    expect(formatHorseWeight("406", "+", "00B", true)).toBe("1030kg (+11)");
    expect(formatHorseWeight("FFF", "+", "FFF", true)).toBe("-");
    expect(formatHorseWeight(" ", "+", "12")).toBe("-");
  });

  it("formats carried weight with ban-ei hexadecimal values", () => {
    expect(formatCarriedWeight("550")).toBe("55.0");
    expect(formatCarriedWeight("525")).toBe("52.5");
    expect(formatCarriedWeight("262", true)).toBe("610");
    expect(formatCarriedWeight("26C", true)).toBe("620");
    expect(formatCarriedWeight("FFF", true)).toBe("-");
  });

  it("replaces sentinel runner values with dash", () => {
    expect(formatRunnerValue("0000", "0000")).toBe("-");
    expect(formatRunnerValue("00", "00")).toBe("-");
    expect(formatRunnerValue("0123", "0000")).toBe("0123");
    expect(formatRunnerValue(null, "0000")).toBe("-");
  });

  it("formats runner number without zero padding", () => {
    expect(formatRunnerNumber("01")).toBe("1");
    expect(formatRunnerNumber("12")).toBe("12");
    expect(formatRunnerNumber("00")).toBe("-");
  });

  it("formats sex and age with readable labels", () => {
    expect(formatSexAge("1", "03")).toBe("牡 / 3歳");
    expect(formatSexAge("2", "04")).toBe("牝 / 4歳");
    expect(formatSexAge("3", "05")).toBe("セ / 5歳");
    expect(formatSexAge(null, "03")).toBe("3歳");
    expect(formatSexAge("9", null)).toBe("-");
  });
});
