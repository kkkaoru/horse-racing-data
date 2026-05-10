import { describe, expect, it } from "vitest";

import { buildRaceKey, extractOddsLinks, parseHorseWeights } from "../src/keiba-go";

describe("keiba.go realtime helpers", () => {
  it("builds normalized NAR race keys", () => {
    expect(buildRaceKey("2026", "0510", "55", "4")).toBe("nar:2026:0510:55:04");
  });

  it("extracts odds links from keiba.go navigation", () => {
    const html = `
      <nav>
        <div></div><div></div>
        <div>
          <a href="../Odds/OddsTanFuku?k=1">単・複</a>
          <a href="/KeibaWeb/Odds/OddsWide?k=1">ワイド</a>
        </div>
      </nav>
    `;
    expect(
      extractOddsLinks(
        html,
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f10&k_raceNo=9&k_babaCode=22",
      ),
    ).toEqual({
      tansho: "https://www.keiba.go.jp/KeibaWeb/Odds/OddsTanFuku?k=1",
      wide: "https://www.keiba.go.jp/KeibaWeb/Odds/OddsWide?k=1",
    });
  });

  it("parses horse weights from table rows", () => {
    const html = `
      <table>
        <tr class="tBorder">
          <td rowspan="5" class="horseNum">1</td>
          <td colspan="3"><a class="horseName">テストホース</a></td>
          <td class="odds_weight" rowspan="2">1.8<br>482(+4)</td>
          <td>26.05.02　2.6　8頭<br>帯広　直200　8番</td>
        </tr>
        <tr class="tBorder">
          <td rowspan="5" class="horseNum">12</td>
          <td colspan="3"><a class="horseName">別馬</a></td>
          <td class="odds_weight" rowspan="2">7.2<br>510(-2)</td>
        </tr>
      </table>
    `;
    expect(parseHorseWeights(html)).toEqual([
      {
        changeAmount: 4,
        changeSign: "+",
        horseName: "テストホース",
        horseNumber: "1",
        weight: 482,
      },
      {
        changeAmount: 2,
        changeSign: "-",
        horseName: "別馬",
        horseNumber: "12",
        weight: 510,
      },
    ]);
  });

  it("does not parse past-race distances as horse weights", () => {
    const html = `
      <tr class="tBorder">
        <td rowspan="5" class="horseNum">6</td>
        <td colspan="3"><a class="horseName">ダイリンファイター</a></td>
        <td class="odds_weight" rowspan="2"><span>22.3</span><br>(8人気)</td>
        <td>26.05.02　2.6　8頭<br>帯広　直200　8番</td>
      </tr>
    `;
    expect(parseHorseWeights(html)).toEqual([]);
  });
});
