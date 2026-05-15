import { describe, expect, it } from "vitest";

import {
  buildJraEntryUrlFromRace,
  parseJraHorseWeights,
  parseJraOddsByType,
  parseJraRaceEntries,
} from "../src/jra";
import { raceKeyFromRealtimePath } from "../src/race-key";

describe("JRA realtime helpers", () => {
  it("builds JRA race keys from realtime API paths", () => {
    expect(raceKeyFromRealtimePath("/api/jra/races/2026/05/12/08/01/realtime")).toBe(
      "jra:2026:0512:08:01",
    );
    expect(raceKeyFromRealtimePath("/api/nar/races/2026/05/12/45/12/realtime")).toBe(
      "nar:2026:0512:45:12",
    );
  });

  it("builds entry URLs from local JRA race rows", () => {
    expect(
      buildJraEntryUrlFromRace({
        hasso_jikoku: "1005",
        kaisai_kai: "02",
        kaisai_nen: "2026",
        kaisai_nichime: "05",
        kaisai_tsukihi: "0509",
        keibajo_code: "05",
        kyosomei_hondai: "test",
        race_bango: "1",
      }),
    ).toBe("https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0105202602050120260509/6A");
  });

  it("parses race entries, jockey changes, scratches, and horse weights", () => {
    const html = `
      <tr>
        <td class="num">2</td>
        <td class="horseName"><a>テストホース</a></td>
        <td class="jockey"><a>武豊</a></td>
        <td class="weight">460(+4)</td>
      </tr>
      <tr>
        <td class="num">9</td>
        <td class="horseName"><a>取消馬</a></td>
        <td class="jockey">騎手変更 ルメール</td>
        <td class="weight">454(-2)</td>
        <td>出走取消</td>
      </tr>
    `;
    expect(parseJraRaceEntries(html)).toEqual([
      {
        horseName: "テストホース",
        horseNumber: "2",
        jockeyName: "武豊",
        status: null,
      },
      {
        horseName: "取消馬",
        horseNumber: "9",
        jockeyName: "騎手変更 ルメール",
        status: "出走取消",
      },
    ]);
    expect(parseJraHorseWeights(html)).toEqual([
      {
        changeAmount: 4,
        changeSign: "+",
        horseName: "テストホース",
        horseNumber: "2",
        weight: 460,
      },
      {
        changeAmount: 2,
        changeSign: "-",
        horseName: "取消馬",
        horseNumber: "9",
        weight: 454,
      },
    ]);
  });

  it("parses JRA odds pages into the shared odds shape", () => {
    expect(
      parseJraOddsByType(
        "tansho",
        `
          <table class="tanpuku">
            <tr><td class="num">2</td><td></td><td></td><td class="odds_tan"><strong>4.6</strong></td></tr>
            <tr><td class="num">9</td><td></td><td></td><td class="odds_tan">113.8</td></tr>
          </table>
        `,
      ),
    ).toEqual([
      { combination: "2", odds: 4.6, rank: 1 },
      { combination: "9", odds: 113.8, rank: 2 },
    ]);

    expect(
      parseJraOddsByType(
        "wide",
        `
          <table class="wide">
            <caption>2</caption>
            <tr><th>9</th><td><span class="min">8.1</span>-<span class="max">10.2</span></td></tr>
          </table>
        `,
      ),
    ).toEqual([{ averageOdds: 9.15, combination: "2-9", maxOdds: 10.2, minOdds: 8.1, rank: 1 }]);
  });
});
