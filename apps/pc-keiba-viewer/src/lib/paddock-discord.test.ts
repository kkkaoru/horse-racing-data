import { describe, expect, it } from "vitest";

import { formatPaddockDiscordHorseLine, type DiscordPaddockHorsePayload } from "./paddock-discord";

const createHorsePayload = (
  overrides: Partial<DiscordPaddockHorsePayload>,
): DiscordPaddockHorsePayload => ({
  attention: 0,
  horseName: "ディルムッド",
  horseNumber: "4",
  jockeyName: "矢野貴之",
  kaeshi: 0,
  odds: "-",
  officialRank: "-",
  paddock: 0,
  popularity: "-",
  preference: 0,
  sexAge: "牡 / 4歳",
  total: "0",
  weight: "528kg (-10)",
  ...overrides,
});

describe("paddock discord notification", () => {
  it("omits the official rank medal icon and zero-valued metrics", () => {
    const line = formatPaddockDiscordHorseLine(
      createHorsePayload({
        attention: 1,
        paddock: 3,
        preference: 2,
        total: "4.1",
      }),
      0,
    );

    expect(line).toContain("　⭐ **4.1**　公式-　👀 気配3 注目1 好み2");
    expect(line).not.toContain("🏅");
    expect(line).not.toContain("返し0");
  });

  it("omits the metric block when all metrics are zero", () => {
    const line = formatPaddockDiscordHorseLine(
      createHorsePayload({
        horseName: "フェアゴー",
        horseNumber: "2",
        jockeyName: "御神本訓",
        officialRank: "1",
        sexAge: "牡 / 4歳",
        weight: "494kg (-5)",
      }),
      2,
    );

    expect(line.split("\n")[2]).toBe("　⭐ **0**　公式1");
  });

  it("uses rank icons and fallback placeholders", () => {
    expect(formatPaddockDiscordHorseLine(createHorsePayload({}), 1)).toMatch(/^🥈/u);
    expect(formatPaddockDiscordHorseLine(createHorsePayload({}), 3)).toMatch(/^▫️/u);
    const line = formatPaddockDiscordHorseLine(
      createHorsePayload({
        jockeyName: "",
        kaeshi: 2,
        sexAge: "",
        weight: "",
      }),
      4,
    );
    expect(line).toContain("（-）");
    expect(line).toContain("👤 -");
    expect(line).toContain("⚖️ -");
    expect(line).toContain("返し2");
  });
});
