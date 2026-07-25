// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import { parseJraCard } from "./jra-card-parser";

const CARD_HTML: string = `
<div class="cell date">2026年7月25日（土曜） アスコット競馬場<span class="country">（イギリス）</span></div>
<span class="race_name">テストステークス &amp; カップ (G1)</span>
<div class="cell course"><span class="cap">コース：</span>2,390<span class="unit">メートル</span><span class="detail">（芝・右）</span></div>
<div class="cell time">発走時刻：<strong>23時35分</strong><span class="local_time">（現地時間：7月25日 15時35分）</span></div>
<table><tbody>
<tr>
<td class="num">1</td>
<td class="horse"><div class="name"><div class="line"><div class="txt">テストホース</div></div></div>
<div class="odds"><strong class="red">1.6</strong></div><span class="pop_rank">(1<span>番人気</span>)</span>
<div class="cell result">(10.5.1.1)</div><p class="owner">TEST OWNER</p>
<p class="trainer">F.トレーナー<span class="division">(FR)</span></p>
<ul><li class="sire"><span>父：</span>Test Sire</li><li class="mare"><span>母：</span>Test Dam<span class="bloodmare">(母の父：Test Damsire)</span></li></ul></td>
<td class="jockey"><p class="age">せん5/鹿</p><p class="weight">61.0<span>kg</span></p><p class="jockey">M.ジョッキー</p></td>
<td class="waku">7</td>
</tr>
<tr>
<td class="num">2</td>
<td class="horse"><div class="name"><div class="txt">サンプルホース</div></div>
<div class="cell result">(2.3.4.5)</div><p class="owner">SAMPLE OWNER</p>
<p class="trainer">サンプル師</p>
<ul><li class="sire"><span>父：</span>Sample Sire</li><li class="mare"><span>母：</span>Sample Dam<span class="bloodmare">(母の父：Sample Damsire)</span></li></ul></td>
<td class="jockey"><p class="age">牝4/黒鹿</p><p class="weight">56.5<span>kg</span></p><p class="jockey"><a>R.サンプル</a></p></td>
<td class="waku">3</td>
</tr>
</tbody></table>`;

test("parses race metadata and runner details from a compact JRA card", () => {
  expect(parseJraCard(CARD_HTML)).toStrictEqual({
    raceName: "テストステークス & カップ",
    grade: "G1",
    date: "2026-07-25",
    venue: "アスコット",
    country: "イギリス",
    distanceMetres: 2390,
    surface: "芝",
    direction: "右",
    startTime: "23:35",
    localStartTime: "15:35",
    runners: [
      {
        horseNumber: 1,
        gate: 7,
        horseName: "テストホース",
        sex: "せん",
        age: 5,
        coatColour: "鹿",
        weightCarriedKg: 61,
        jockeyAbbrev: "M.ジョッキー",
        trainerAbbrev: "F.トレーナー",
        trainerCountry: "FR",
        owner: "TEST OWNER",
        winOdds: 1.6,
        popularity: 1,
        formRecord: "10.5.1.1",
        sire: "Test Sire",
        dam: "Test Dam",
        damsire: "Test Damsire",
      },
      {
        horseNumber: 2,
        gate: 3,
        horseName: "サンプルホース",
        sex: "牝",
        age: 4,
        coatColour: "黒鹿",
        weightCarriedKg: 56.5,
        jockeyAbbrev: "R.サンプル",
        trainerAbbrev: "サンプル師",
        trainerCountry: "",
        owner: "SAMPLE OWNER",
        winOdds: null,
        popularity: null,
        formRecord: "2.3.4.5",
        sire: "Sample Sire",
        dam: "Sample Dam",
        damsire: "Sample Damsire",
      },
    ],
  });
});

test("returns a null grade when the race name has no supported grade suffix", () => {
  expect(parseJraCard(CARD_HTML.replace(" (G1)", "")).grade).toBe(null);
});

test("rejects a card without race date metadata", () => {
  expect(() => parseJraCard(CARD_HTML.replace('class="cell date"', 'class="other"'))).toThrow(
    "JRA card is missing race date, venue, or country.",
  );
});

test("rejects a card without course metadata", () => {
  expect(() =>
    parseJraCard(CARD_HTML.replace('class="cell course"', 'class="other course"')),
  ).toThrow("JRA card is missing race course details.");
});

test("rejects a card without start time metadata", () => {
  expect(() => parseJraCard(CARD_HTML.replace("発走時刻：", "開始："))).toThrow(
    "JRA card is missing race start time.",
  );
});

test("rejects a card without local start time metadata", () => {
  expect(() => parseJraCard(CARD_HTML.replace("現地時間：", "LOCAL:"))).toThrow(
    "JRA card is missing local race start time.",
  );
});

test("rejects a card without a runner table", () => {
  expect(() => parseJraCard(CARD_HTML.replace("<tbody>", "<section>"))).toThrow(
    "JRA card is missing runner table.",
  );
});

test("rejects an empty runner table", () => {
  const emptyCard: string = CARD_HTML.replace(/<tbody>[\s\S]*<\/tbody>/, "<tbody></tbody>");
  expect(() => parseJraCard(emptyCard)).toThrow("JRA card has no runners.");
});

test("rejects an invalid runner age field", () => {
  expect(() => parseJraCard(CARD_HTML.replace("せん5/鹿", "invalid"))).toThrow(
    "JRA card has an invalid runner sex, age, or coat colour.",
  );
});

test("rejects a runner missing a required field", () => {
  expect(() => parseJraCard(CARD_HTML.replace('class="owner"', 'class="other"'))).toThrow(
    "JRA card is missing runner owner.",
  );
});
