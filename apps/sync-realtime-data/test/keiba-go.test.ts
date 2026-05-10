import { describe, expect, it } from "vitest";

import {
  buildRaceKey,
  buildRaceResultUrl,
  extractOddsLinks,
  parseHorseWeights,
  parseRaceResultHorseWeights,
} from "../src/keiba-go";

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

  it("builds result table URLs from entry table URLs", () => {
    expect(
      buildRaceResultUrl(
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f10&k_raceNo=9&k_babaCode=22",
      ),
    ).toBe(
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceMarkTable?k_raceDate=2026%2f05%2f10&k_raceNo=9&k_babaCode=22",
    );
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

  it("does not parse high decimal odds as horse weights", () => {
    const html = `
      <tr class="tBorder">
        <td rowspan="5" class="horseNum">4</td>
        <td colspan="3"><a class="horseName">シルヴァバローズ</a></td>
        <td class="odds_weight" rowspan="2"><span>413.9</span><br>(12人気)</td>
      </tr>
    `;
    expect(parseHorseWeights(html)).toEqual([]);
  });

  it("parses horse weights from race result tables", () => {
    const html = `
      <tr bgcolor="#FFFFFF" align="center">
        <td nowrap><span class="bold">1</span></td>
        <td nowrap>1</td>
        <td nowrap>7</td>
        <td nowrap class="dbdata3"><span class="bold"><a href="/horse">グリーゼ</a></span></td>
        <td nowrap>金沢</td>
        <td nowrap><span>牝 3</span></td>
        <td nowrap class="dbdata2">55.0</td>
        <td nowrap>米倉知<br></td>
        <td nowrap>中川雅</td>
        <td nowrap class="dbdata2">1117</td>
        <td nowrap>1</td>
        <td nowrap class="dbdata2">1:54.3</td>
      </tr>
      <tr bgcolor="#FFFFFF" align="center">
        <td nowrap><span class="bold">2</span></td>
        <td nowrap>2</td>
        <td nowrap>12</td>
        <td nowrap class="dbdata3"><a href="/horse">ピカピカピロコ</a></td>
        <td nowrap>金沢</td>
        <td nowrap>牝 3</td>
        <td nowrap>55.0</td>
        <td nowrap>青柳正</td>
        <td nowrap>中川雅</td>
        <td nowrap class="dbdata2">446</td>
        <td nowrap>-3</td>
      </tr>
    `;
    expect(parseRaceResultHorseWeights(html)).toEqual([
      {
        changeAmount: 1,
        changeSign: "+",
        horseName: "グリーゼ",
        horseNumber: "7",
        weight: 1117,
      },
      {
        changeAmount: 3,
        changeSign: "-",
        horseName: "ピカピカピロコ",
        horseNumber: "12",
        weight: 446,
      },
    ]);
  });
});
