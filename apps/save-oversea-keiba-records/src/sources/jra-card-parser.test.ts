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

const JRA_VAN_WORLD_HTML: string = `
<p class="raceInfo__txt">
2026/08/16(日)ドーヴィル競馬場<br>
<span class="raceInfo__txt__r">4R</span><span class="raceInfo__txt__name">ジャックルマロワ賞（G1）</span><br>
芝1600m（直線）2頭<br>
22:50発走（現地時間：2026/08/16 15:50）</p>
<dd class="raceTable__details">
<div class="raceTable__details__line active">
<div class="raceTable__details__line__item--horseNun"><p>1</p></div>
<div class="raceTable__details__line__item--gateNun"><p>6</p></div>
<span class="raceTable__details__line__item--horse__name"><a>ゼウスオリンピオス</a></span>
<span class="raceTable__details__line__item--horse__info">K．バーク<br />
<span class="raceTable__details__line__item--horse__info__father"><span>父</span>Night Of Thunder</span>
<span class="raceTable__details__line__item--horse__info__mother"><span>母</span>Rhea</span>
<span class="raceTable__details__line__item--horse__info__motherfather"><span>母父</span>Siyouni</span></span>
<span class="raceTable__details__line__item--odds__jra">3.4</span>
<span>牡4　栗毛</span>
<span class="raceTable__details__line__item--jockey__name">C．リー<br /></span>
<span class="raceTable__details__line__item--jockey__weight">59.5kg</span>
</div>
<div class="raceTable__details__line active">
<div class="raceTable__details__line__item--horseNun"><p>2</p></div>
<div class="raceTable__details__line__item--gateNun"><p>2</p></div>
<span class="raceTable__details__line__item--horse__name"><a>ドリームライナー</a></span>
<span class="raceTable__details__line__item--horse__info">S．ワッテル<br />
<span class="raceTable__details__line__item--horse__info__father"><span>父</span>Adlerflug</span>
<span class="raceTable__details__line__item--horse__info__mother"><span>母</span>Game Theory</span>
<span class="raceTable__details__line__item--horse__info__motherfather"><span>母父</span>Aussie Rules</span></span>
<span>牡4　鹿毛</span>
<span class="raceTable__details__line__item--jockey__name"><a>T．バシュロ</a><br /></span>
<span class="raceTable__details__line__item--jockey__weight">59.5kg</span>
</div>
</dd>`;

test("parses a JRA-VAN World card when the official JRA CNAME is unavailable", () => {
  expect(parseJraCard(JRA_VAN_WORLD_HTML)).toStrictEqual({
    raceName: "ジャックルマロワ賞",
    grade: "G1",
    date: "2026-08-16",
    venue: "ドーヴィル",
    country: "",
    distanceMetres: 1600,
    surface: "芝",
    direction: "直線",
    startTime: "22:50",
    localStartTime: "15:50",
    runners: [
      {
        horseNumber: 1,
        gate: 6,
        horseName: "ゼウスオリンピオス",
        sex: "牡",
        age: 4,
        coatColour: "栗毛",
        weightCarriedKg: 59.5,
        jockeyAbbrev: "C．リー",
        trainerAbbrev: "K．バーク",
        trainerCountry: "",
        owner: "",
        winOdds: 3.4,
        popularity: null,
        formRecord: "",
        sire: "Night Of Thunder",
        dam: "Rhea",
        damsire: "Siyouni",
      },
      {
        horseNumber: 2,
        gate: 2,
        horseName: "ドリームライナー",
        sex: "牡",
        age: 4,
        coatColour: "鹿毛",
        weightCarriedKg: 59.5,
        jockeyAbbrev: "T．バシュロ",
        trainerAbbrev: "S．ワッテル",
        trainerCountry: "",
        owner: "",
        winOdds: null,
        popularity: null,
        formRecord: "",
        sire: "Adlerflug",
        dam: "Game Theory",
        damsire: "Aussie Rules",
      },
    ],
  });
});

test("parses a JRA-VAN World dirt card without a supported grade", () => {
  const card: string = JRA_VAN_WORLD_HTML.replace("ジャックルマロワ賞（G1）", "テスト競走").replace(
    "芝1600m（直線）2頭",
    "ダ1600m（右）2頭",
  );
  const parsed = parseJraCard(card);
  expect(parsed.grade).toBe(null);
  expect(parsed.surface).toBe("ダート");
  expect(parsed.direction).toBe("右");
});

test("rejects JRA-VAN World metadata without a date and venue", () => {
  expect(() =>
    parseJraCard(JRA_VAN_WORLD_HTML.replace("2026/08/16(日)ドーヴィル競馬場", "開催未定")),
  ).toThrow("JRA-VAN World card is missing race date or venue.");
});

test("rejects JRA-VAN World metadata without a start time", () => {
  expect(() => parseJraCard(JRA_VAN_WORLD_HTML.replace("22:50発走", "発走未定"))).toThrow(
    "JRA-VAN World card is missing race start time.",
  );
});

test("rejects a JRA-VAN World runner with invalid age and coat metadata", () => {
  expect(() => parseJraCard(JRA_VAN_WORLD_HTML.replace("牡4　栗毛", "属性未定"))).toThrow(
    "JRA-VAN World card has an invalid runner sex, age, or coat colour.",
  );
});

test("rejects a JRA-VAN World runner without a required horse name", () => {
  expect(() => parseJraCard(JRA_VAN_WORLD_HTML.replace("--horse__name", "--other"))).toThrow(
    "JRA-VAN World card is missing runner horse name.",
  );
});

test("rejects JRA-VAN World metadata without a course", () => {
  expect(() =>
    parseJraCard(JRA_VAN_WORLD_HTML.replace("芝1600m（直線）2頭", "course pending")),
  ).toThrow("JRA-VAN World card is missing race course details.");
});

test("rejects a JRA-VAN World card without runners", () => {
  expect(() =>
    parseJraCard(JRA_VAN_WORLD_HTML.replaceAll("raceTable__details__line active", "other")),
  ).toThrow("JRA-VAN World card has no runners.");
});
