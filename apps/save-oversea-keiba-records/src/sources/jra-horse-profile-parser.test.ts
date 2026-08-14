import { describe, expect, it } from "vitest";

import { parseJraVanHorseProfile } from "./jra-horse-profile-parser";

const row = (cells: string[]): string =>
  `<tr>${cells.map((cell: string): string => `<td>${cell}</td>`).join("")}</tr>`;

const profile = (rows: string): string => `
  <html><head>
    <link rel="canonical" href="https://world.jra-van.jp/db/horse/H1234567/">
  </head><body>
    <h1>テストホース<span class="horsefix__title__en">（Test &amp; Horse）</span></h1>
    <div id="horse--result" class="horsedata"><table><tbody>
      <tr><th>開催日</th><th>場所</th></tr>
      ${rows}
    </tbody></table></div>
    <div id="horse--long" class="horsedata"></div>
  </body></html>`;

const completeCells = [
  "<span>2026/</span><span>07/11</span>",
  "アスコット",
  '<a href="/schedule/result/R1019335/">クイーンアンステークス（G1）</a>',
  "",
  "4",
  '<a href="/db/jockey/18854/">R．ムーア</a>',
  "芝",
  "1600",
  "良",
];

describe("parseJraVanHorseProfile", () => {
  it("parses source-native identities and a numeric result", () => {
    expect(parseJraVanHorseProfile(profile(row(completeCells)))).toStrictEqual({
      horseName: "Test & Horse",
      sourceHorseId: "H1234567",
      sourceUrl: "https://world.jra-van.jp/db/horse/H1234567/",
      results: [
        {
          raceDate: "2026-07-11",
          raceDaySequence: 1,
          venue: "アスコット",
          raceName: "クイーンアンステークス（G1）",
          sourceRaceId: "R1019335",
          sourceRaceUrl: "https://world.jra-van.jp/schedule/result/R1019335/",
          finishPosition: 4,
          finishPositionText: "4",
          jockeyName: "R．ムーア",
          sourceJockeyId: "18854",
          surface: "芝",
          distanceMetres: 1600,
          going: "良",
        },
      ],
    });
  });

  it("preserves non-numeric source values and nullable native IDs", () => {
    const cells = [...completeCells];
    cells[2] = "条件戦";
    cells[4] = "取消";
    cells[5] = "C．リー";
    cells[7] = "不明";
    expect(parseJraVanHorseProfile(profile(row(cells))).results[0]).toMatchObject({
      sourceRaceId: null,
      sourceRaceUrl: null,
      finishPosition: null,
      finishPositionText: "取消",
      sourceJockeyId: null,
      distanceMetres: null,
    });
  });

  it("assigns a stable source-order sequence within the same horse, date, and venue", () => {
    const secondCells = [...completeCells];
    secondCells[2] = "別のレース";
    expect(
      parseJraVanHorseProfile(profile(`${row(completeCells)}${row(secondCells)}`)).results.map(
        (result) => result.raceDaySequence,
      ),
    ).toStrictEqual([1, 2]);
  });

  it("accepts a profile with no historical starts", () => {
    expect(parseJraVanHorseProfile(profile("")).results).toStrictEqual([]);
  });

  it("rejects malformed profile and result shapes", () => {
    expect(() => parseJraVanHorseProfile("<html></html>")).toThrow("missing canonical URL");
    expect(() =>
      parseJraVanHorseProfile(
        profile(row(completeCells)).replace('class="horsefix__title__en"', 'class="missing"'),
      ),
    ).toThrow("missing English horse name");
    expect(() =>
      parseJraVanHorseProfile(profile(row(completeCells)).replace('id="horse--long"', 'id="next"')),
    ).toThrow("missing the section after results");
    expect(() => parseJraVanHorseProfile(profile(row(completeCells.slice(0, 8))))).toThrow(
      "8 cells instead of 9",
    );
    const invalidDate = [...completeCells];
    invalidDate[0] = "unknown";
    expect(() => parseJraVanHorseProfile(profile(row(invalidDate)))).toThrow("invalid date");
  });
});
