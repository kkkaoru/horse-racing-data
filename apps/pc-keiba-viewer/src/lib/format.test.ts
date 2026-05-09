import { describe, expect, it } from "vitest";

import {
  cleanText,
  formatBaba,
  formatCount,
  formatDate,
  formatDisplayDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTime,
  formatTrack,
  formatWeather,
  getTrackSurfaceLabel,
  getTrackTurnLabel,
} from "./format";

describe("format helpers", () => {
  it("cleans empty text with fallback", () => {
    expect(cleanText("  東京  ")).toBe("東京");
    expect(cleanText("   ")).toBe("-");
    expect(cleanText(null, "未設定")).toBe("未設定");
  });

  it("formats date values", () => {
    expect(formatDate("2026", "0510")).toBe("2026-05-10");
    expect(formatDisplayDate("2026", "0510")).toBe("2026年5月10日");
  });

  it("formats time, race number, distance, and counts", () => {
    expect(formatTime("0945")).toBe("09:45");
    expect(formatTime("945")).toBe("--:--");
    expect(formatRaceNumber("01")).toBe("1R");
    expect(formatRaceNumber("00")).toBe("-");
    expect(formatDistance("1800")).toBe("1800m");
    expect(formatDistance("0000")).toBe("-");
    expect(formatCount(12345)).toBe("12,345");
  });

  it("formats known and unknown race codes", () => {
    expect(formatKeibajo("04")).toBe("新潟");
    expect(formatKeibajo("99")).toBe("競馬場 99");
    expect(formatTrack("23")).toBe("ダート・左");
    expect(getTrackSurfaceLabel("23")).toBe("ダート");
    expect(getTrackTurnLabel("23")).toBe("左");
    expect(getTrackSurfaceLabel("12")).toBe("芝");
    expect(getTrackTurnLabel("12")).toBe("左");
    expect(getTrackTurnLabel("24")).toBe("右");
    expect(getTrackTurnLabel("10")).toBe("直線");
    expect(getTrackSurfaceLabel(" ")).toBe("-");
    expect(getTrackTurnLabel("99")).toBe("-");
    expect(formatTrack("99")).toBe("コース 99");
    expect(formatTrack(" ")).toBe("-");
    expect(formatWeather("1")).toBe("晴");
    expect(formatWeather("9")).toBe("天候 9");
    expect(formatBaba("1")).toBe("良");
    expect(formatBaba("9")).toBe("馬場 9");
  });
});
