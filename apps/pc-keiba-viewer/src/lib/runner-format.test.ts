import { describe, expect, it } from "vitest";

import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
  inferRaceSourceFromKeibajoCode,
  isOverseasKeibajoCode,
} from "./runner-format";

describe("runner format helpers", () => {
  it("formats horse weight with and without diff", () => {
    expect(formatHorseWeight("480", "+", "12")).toBe("480kg (+12)");
    expect(formatHorseWeight("480", "", "")).toBe("480kg");
    expect(formatHorseWeight("不明", "+", "増減不明")).toBe("不明kg (+NaN)");
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
    expect(formatCarriedWeight("不明")).toBe("不明");
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
    expect(formatSexAge("1", null)).toBe("牡");
    expect(formatSexAge("9", null)).toBe("-");
  });

  it("identifies overseas keibajo codes (alphabetic) vs domestic (numeric)", () => {
    expect(isOverseasKeibajoCode("A6")).toBe(true);
    expect(isOverseasKeibajoCode("A0")).toBe(true);
    expect(isOverseasKeibajoCode("A2")).toBe(true);
    expect(isOverseasKeibajoCode("05")).toBe(false);
    expect(isOverseasKeibajoCode("30")).toBe(false);
    expect(isOverseasKeibajoCode("83")).toBe(false);
    expect(isOverseasKeibajoCode(null)).toBe(false);
    expect(isOverseasKeibajoCode("")).toBe(false);
  });

  it("infers jra vs nar from keibajo codes", () => {
    expect(inferRaceSourceFromKeibajoCode("05")).toBe("jra");
    expect(inferRaceSourceFromKeibajoCode("10")).toBe("jra");
    expect(inferRaceSourceFromKeibajoCode("A8")).toBe("jra");
    expect(inferRaceSourceFromKeibajoCode("45")).toBe("nar");
    expect(inferRaceSourceFromKeibajoCode("30")).toBe("nar");
    expect(inferRaceSourceFromKeibajoCode("83")).toBe("nar");
    expect(inferRaceSourceFromKeibajoCode("11")).toBe(null);
    expect(inferRaceSourceFromKeibajoCode("00")).toBe(null);
    expect(inferRaceSourceFromKeibajoCode(null)).toBe(null);
    expect(inferRaceSourceFromKeibajoCode("")).toBe(null);
  });
});
