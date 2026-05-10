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
        <tr><td>1</td><td>テストホース</td><td>482(+4)</td></tr>
        <tr><td>12</td><td>別馬</td><td>510(-2)</td></tr>
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
});
