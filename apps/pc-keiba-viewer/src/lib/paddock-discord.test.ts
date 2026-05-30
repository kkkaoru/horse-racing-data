// Run with bun.
import { describe, expect, it } from "vitest";

import {
  buildPaddockDiscordOfficialRankLines,
  chunkPaddockDiscordOfficialRankFields,
  formatPaddockDiscordHorseLine,
  formatPaddockDiscordOfficialRankLine,
  type DiscordPaddockHorsePayload,
} from "./paddock-discord";

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
  it("omits the official rank token when officialRank is the '-' placeholder", () => {
    const line = formatPaddockDiscordHorseLine(
      createHorsePayload({
        attention: 1,
        paddock: 3,
        preference: 2,
        total: "4.1",
      }),
      0,
    );

    expect(line.split("\n")[2]).toBe("　⭐ **4.1**　👀 気配3 注目1 好み2");
  });

  it("renders the official rank when officialRank is a numeric string", () => {
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

  it("renders only the total when both officialRank and metrics are absent", () => {
    const line = formatPaddockDiscordHorseLine(createHorsePayload({ total: "2" }), 1);

    expect(line.split("\n")[2]).toBe("　⭐ **2**");
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

it("formatPaddockDiscordOfficialRankLine builds a single ranked line with horse meta", () => {
  expect(
    formatPaddockDiscordOfficialRankLine(
      createHorsePayload({
        horseName: "テスト馬",
        horseNumber: "3",
        officialRank: "1",
        total: "4.5",
        weight: "500kg (+2)",
      }),
    ),
  ).toBe("1. **3 テスト馬** / ⭐ 4.5 / ⚖️ 500kg (+2)");
});

it("formatPaddockDiscordOfficialRankLine falls back to '-' for an empty weight", () => {
  expect(
    formatPaddockDiscordOfficialRankLine(
      createHorsePayload({
        horseName: "ノーウェイト",
        horseNumber: "7",
        officialRank: "5",
        total: "0",
        weight: "",
      }),
    ),
  ).toBe("5. **7 ノーウェイト** / ⭐ 0 / ⚖️ -");
});

it("buildPaddockDiscordOfficialRankLines drops '-' placeholders and sorts by official rank asc", () => {
  const horses: DiscordPaddockHorsePayload[] = [
    createHorsePayload({
      horseName: "B",
      horseNumber: "2",
      officialRank: "3",
      total: "1",
      weight: "500kg",
    }),
    createHorsePayload({
      horseName: "Z",
      horseNumber: "9",
      officialRank: "-",
      total: "9",
      weight: "490kg",
    }),
    createHorsePayload({
      horseName: "A",
      horseNumber: "1",
      officialRank: "1",
      total: "5",
      weight: "510kg",
    }),
    createHorsePayload({
      horseName: "C",
      horseNumber: "5",
      officialRank: "2",
      total: "2",
      weight: "470kg",
    }),
  ];

  expect(buildPaddockDiscordOfficialRankLines(horses)).toStrictEqual([
    "1. **1 A** / ⭐ 5 / ⚖️ 510kg",
    "2. **5 C** / ⭐ 2 / ⚖️ 470kg",
    "3. **2 B** / ⭐ 1 / ⚖️ 500kg",
  ]);
});

it("buildPaddockDiscordOfficialRankLines returns an empty list when no horse has a numeric rank", () => {
  expect(
    buildPaddockDiscordOfficialRankLines([
      createHorsePayload({ horseNumber: "1", officialRank: "-" }),
      createHorsePayload({ horseNumber: "2", officialRank: "" }),
    ]),
  ).toStrictEqual([]);
});

it("chunkPaddockDiscordOfficialRankFields returns an empty array for no lines", () => {
  expect(chunkPaddockDiscordOfficialRankFields([])).toStrictEqual([]);
});

it("chunkPaddockDiscordOfficialRankFields keeps a small list inside a single field", () => {
  expect(
    chunkPaddockDiscordOfficialRankFields([
      "1. **1 A** / ⭐ 5 / ⚖️ 500kg",
      "2. **2 B** / ⭐ 4 / ⚖️ 490kg",
    ]),
  ).toStrictEqual(["1. **1 A** / ⭐ 5 / ⚖️ 500kg\n2. **2 B** / ⭐ 4 / ⚖️ 490kg"]);
});

it("chunkPaddockDiscordOfficialRankFields splits an 18-horse list into multiple fields while preserving every line", () => {
  const longHorses: DiscordPaddockHorsePayload[] = Array.from({ length: 18 }, (_, index) => {
    const rank = index + 1;
    return createHorsePayload({
      horseName: `Horse${rank}`.padEnd(80, "x"),
      horseNumber: String(rank),
      officialRank: String(rank),
      total: "0",
      weight: "500kg",
    });
  });
  const fields = chunkPaddockDiscordOfficialRankFields(
    buildPaddockDiscordOfficialRankLines(longHorses),
  );
  const joinedAllLines = fields.flatMap((field) => field.split("\n"));

  expect(fields.length).toBeGreaterThan(1);
  expect(joinedAllLines.length).toBe(18);
  expect(fields.every((field) => field.length <= 1024)).toBe(true);
  expect(joinedAllLines[0]).toMatch(/^1\. \*\*1 Horse1/u);
  expect(joinedAllLines[17]).toMatch(/^18\. \*\*18 Horse18/u);
});

it("chunkPaddockDiscordOfficialRankFields keeps every horse when there are exactly 18 short entries below the cap", () => {
  const shortLines: string[] = Array.from({ length: 18 }, (_, index) => {
    const rank = index + 1;
    return `${rank}. **${rank} S** / ⭐ 1 / ⚖️ 5kg`;
  });
  const fields = chunkPaddockDiscordOfficialRankFields(shortLines);

  expect(fields).toStrictEqual([
    "1. **1 S** / ⭐ 1 / ⚖️ 5kg\n2. **2 S** / ⭐ 1 / ⚖️ 5kg\n3. **3 S** / ⭐ 1 / ⚖️ 5kg\n4. **4 S** / ⭐ 1 / ⚖️ 5kg\n5. **5 S** / ⭐ 1 / ⚖️ 5kg\n6. **6 S** / ⭐ 1 / ⚖️ 5kg\n7. **7 S** / ⭐ 1 / ⚖️ 5kg\n8. **8 S** / ⭐ 1 / ⚖️ 5kg\n9. **9 S** / ⭐ 1 / ⚖️ 5kg\n10. **10 S** / ⭐ 1 / ⚖️ 5kg\n11. **11 S** / ⭐ 1 / ⚖️ 5kg\n12. **12 S** / ⭐ 1 / ⚖️ 5kg\n13. **13 S** / ⭐ 1 / ⚖️ 5kg\n14. **14 S** / ⭐ 1 / ⚖️ 5kg\n15. **15 S** / ⭐ 1 / ⚖️ 5kg\n16. **16 S** / ⭐ 1 / ⚖️ 5kg\n17. **17 S** / ⭐ 1 / ⚖️ 5kg\n18. **18 S** / ⭐ 1 / ⚖️ 5kg",
  ]);
});
