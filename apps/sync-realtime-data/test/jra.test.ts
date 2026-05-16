import { describe, expect, it } from "vitest";

import {
  buildJraEntryUrlFromRace,
  buildJraResultUrlFromRaceSource,
  parseJraHorseWeights,
  parseJraOddsByType,
  parseJraRaceResultExcludedHorseNumbers,
  parseJraRaceResults,
  parseJraRaceEntries,
  sanitizeJraRaceEntriesWithOdds,
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

  it("builds result URLs from stored JRA race sources", () => {
    expect(
      buildJraResultUrlFromRaceSource({
        babaCode: "08",
        debaUrl: "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0108202303080320231126/F9",
        kaisaiKai: "03",
        kaisaiNen: "2023",
        kaisaiNichime: "08",
        kaisaiTsukihi: "1126",
        keibajoCode: "08",
        lastOddsFetchAt: null,
        lastWeightFetchAt: null,
        oddsLinks: {},
        raceBango: "03",
        raceKey: "jra:2023:1126:08:03",
        raceName: "test",
        raceStartAtJst: "2023-11-26T11:00:00+09:00",
        source: "jra",
      }),
    ).toBe("https://www.jra.go.jp/JRADB/accessS.html?CNAME=pw01sde1008202303080320231126/6B");
  });

  it("parses race entries, jockey changes, scratches, and horse weights", () => {
    const html = `
      <tr>
        <td class="num">2<span class="horse_icon blinker"><img alt="ブリンカー使用" /></span></td>
        <td class="horseName">
          <a>テストホース</a>
          <div class="cell result">(1.0.0.3)</div>
          <div class="cell win" title="8,320,000円">832万円</div>
          <div class="cell weight">520kg<span class="transition">(+4)</span></div>
          <p class="past"><span class="h_weight">516<span>kg</span></span></p>
        </td>
        <td class="jockey"><a>△武 豊</a></td>
      </tr>
      <tr>
        <td class="num">9</td>
        <td class="horseName"><a>取消馬</a></td>
        <td class="jockey">騎手変更 ▲ルメール</td>
        <td class="misc">752</td>
        <td>出走取消</td>
      </tr>
      <tr>
        <td class="num">14</td>
        <td class="horseName"><a>現役馬</a></td>
        <td class="jockey"><a>角田 大和</a></td>
        <td class="weight">359(-2)</td>
        <td>取消</td>
      </tr>
      <tr>
        <td class="num">16</td>
        <td class="horseName"><a>過去取消あり</a></td>
        <td class="jockey"><a>川田 将雅</a></td>
        <td><span class="pastRank">取消</span></td>
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
        jockeyName: "騎手変更ルメール",
        status: "出走取消",
      },
      {
        horseName: "現役馬",
        horseNumber: "14",
        jockeyName: "角田大和",
        status: null,
      },
      {
        horseName: "過去取消あり",
        horseNumber: "16",
        jockeyName: "川田将雅",
        status: null,
      },
    ]);
    expect(parseJraHorseWeights(html)).toEqual([
      {
        changeAmount: 4,
        changeSign: "+",
        horseName: "テストホース",
        horseNumber: "2",
        weight: 520,
      },
      { changeAmount: 2, changeSign: "-", horseName: "現役馬", horseNumber: "14", weight: 359 },
    ]);
  });

  it("parses current jockey from the nested JRA entry cell", () => {
    const html = `
      <tr>
        <td class="num">1</td>
        <td class="horse">
          <div class="name_line">
            <div class="name"><a>メイショウルビー</a></div>
          </div>
        </td>
        <td class="jockey">
          <p class="age">牝3/鹿</p>
          <p class="weight">55.0<span>kg</span></p>
          <p class="jockey"><a>△田口 貫太</a></p>
        </td>
      </tr>
    `;
    expect(parseJraRaceEntries(html)).toEqual([
      {
        horseName: "メイショウルビー",
        horseNumber: "1",
        jockeyName: "田口貫太",
        status: null,
      },
    ]);
  });

  it("does not treat past JRA cancellation text as the current entry status", () => {
    const html = `
      <tr>
        <td class="num">3</td>
        <td class="horseName"><a>ビップディラン</a></td>
        <td class="jockey"><a>松若 風馬</a></td>
        <td class="recent-result">前走 取消</td>
      </tr>
      <tr>
        <td class="num">8</td>
        <td class="horseName"><a>現取消馬</a></td>
        <td class="jockey"><a>横山 武史</a></td>
        <td>競走除外</td>
      </tr>
    `;
    expect(parseJraRaceEntries(html)).toEqual([
      {
        horseName: "ビップディラン",
        horseNumber: "3",
        jockeyName: "松若風馬",
        status: null,
      },
      {
        horseName: "現取消馬",
        horseNumber: "8",
        jockeyName: "横山武史",
        status: "競走除外",
      },
    ]);
  });

  it("clears stale JRA scratch status when current win odds exist", () => {
    expect(
      sanitizeJraRaceEntriesWithOdds(
        [
          {
            horseName: "ビップディラン",
            horseNumber: "3",
            jockeyName: "松若風馬",
            status: "除外",
          },
          {
            horseName: "取消馬",
            horseNumber: "8",
            jockeyName: "騎手",
            status: "取消",
          },
          {
            horseName: "騎手変更馬",
            horseNumber: "9",
            jockeyName: "騎手",
            status: "騎手変更",
          },
        ],
        { tansho: [{ combination: "3", odds: 10.2, rank: 5 }] },
      ),
    ).toEqual([
      {
        horseName: "ビップディラン",
        horseNumber: "3",
        jockeyName: "松若風馬",
        status: null,
      },
      {
        horseName: "取消馬",
        horseNumber: "8",
        jockeyName: "騎手",
        status: "取消",
      },
      {
        horseName: "騎手変更馬",
        horseNumber: "9",
        jockeyName: "騎手",
        status: "騎手変更",
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

  it("parses JRA race results and excludes non-finish statuses", () => {
    const html = `
      <div id="race_result">
        <table>
          <tbody>
            <tr>
              <td class="place">1</td>
              <td class="waku"></td>
              <td class="num">10</td>
              <td class="horse"><a>ショウナンハウル</a></td>
              <td class="time">2:02.6</td>
            </tr>
            <tr>
              <td class="place">10</td>
              <td class="waku"></td>
              <td class="num">7</td>
              <td class="horse"><a>クロドラバール</a></td>
              <td class="time">2:04.2</td>
            </tr>
            <tr>
              <td class="place">競走中止</td>
              <td class="waku"></td>
              <td class="num">8</td>
              <td class="horse"><a>中止馬</a></td>
              <td class="time"></td>
            </tr>
            <tr>
              <td class="place">除外</td>
              <td class="waku"></td>
              <td class="num">9</td>
              <td class="horse"><a>除外馬</a></td>
              <td class="time"></td>
            </tr>
          </tbody>
        </table>
      </div>
    `;

    expect(parseJraRaceResults(html)).toEqual([
      {
        finishPosition: "01",
        horseName: "ショウナンハウル",
        horseNumber: "10",
        time: "2:02.6",
      },
      {
        finishPosition: "10",
        horseName: "クロドラバール",
        horseNumber: "7",
        time: "2:04.2",
      },
    ]);
    expect(parseJraRaceResultExcludedHorseNumbers(html)).toEqual(["8", "9"]);
  });
});
