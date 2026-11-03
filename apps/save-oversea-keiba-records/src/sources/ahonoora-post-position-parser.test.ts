// This file runs with Bun and Vitest. Fixtures follow the live ahonoora.com markup.
import iconv from "iconv-lite";
import { expect, it } from "vitest";
import { decodeAhonoora, parseAhonooraRace, yearPageNames } from "./ahonoora-post-position-parser";

const CURRENT_PAGE: string = `<B><SMALL>Breeders' Cup Classic<BR><BIG>ブリーダーズカップクラシック
<FONT COLOR=RED>(GI)</FONT></center></BIG></B>
2025/11/1　米・デルマー9R　3歳以上　ダート10f（左）　速</small><BR>
<TABLE BORDER BGCOLOR=LIGHTYELLOW width=730>
<TR align=center bgcolor=silver><td width=35>着順<td width=35>馬番<td width=35>枠順<td>馬名<td>性齢<td width=35>斤量<td width=100>騎手</TR>

<tr align=center bgcolor=lightgreen><td>1着<td>5番<td>5番<td><B><A HREF="forever_young.html">Forever Young<td>牡4<td>57<td><B>坂井瑠星</tr>
<tr align=center bgcolor=yellow><td>2着<td>7番<td>6番<td><B>Sierra Leone<td>牝4<td>55.5<td>F.プラ</tr>
<tr align=center><td>中止<td>1a番<td>1番<td><B>Coupled<td>セン5<td>57<td>M.スミス</tr>
</TBODY></TABLE>`;
const OLD_PAGE: string = `<BIG>
<FONT COLOR=LIGHTYELLOW>■</FONT>ブリーダーズカップスプリント<FONT COLOR=RED>(GI)</FONT><BR>
2010.11. 6　速<P></TABLE>
<TABLE BORDER BGCOLOR=LIGHTYELLOW width=710>
<TR align=center bgcolor=silver><td width=35>着順<td width=35>枠順<td>馬名<td>性齢<td width=35>斤量</TR>

<tr align=center bgcolor=lightgreen><td>1着<td>1番<td><B>Big Drama<td>牡4<td>57
<tr align=center bgcolor=yellow><td>2着<td>4番<td><B>Hamazing Destiny<td>不明<td>-
</TABLE>`;

it("decodes Shift_JIS bytes", () => {
  expect(decodeAhonoora(iconv.encode("ブリーダーズカップ", "cp932"))).toBe("ブリーダーズカップ");
});

it("lists year pages of one series from the given year, once each", () => {
  expect(
    yearPageNames(
      '<A HREF="bcclassic_2025.html"><A HREF="bcclassic_1995.html"><A HREF="bcclassic_2024.html"><A HREF="bcclassic_2025.html"><A HREF="bcmile_2025.html">',
      "bcclassic",
      1996,
    ),
  ).toStrictEqual(["bcclassic_2024.html", "bcclassic_2025.html"]);
  expect(() => yearPageNames("", "bc.classic", 1996)).toThrow(
    "Invalid ahonoora series: bc.classic",
  );
});

it("reads post positions, horse numbers, sex, age and weight from the current layout", () => {
  expect(parseAhonooraRace(CURRENT_PAGE)).toStrictEqual({
    date: "20251101",
    title: "ブリーダーズカップクラシック",
    runners: [
      { finish: 1, umaban: 5, gate: 5, sexCode: "1", age: 4, weightKg: 57 },
      { finish: 2, umaban: 7, gate: 6, sexCode: "2", age: 4, weightKg: 55.5 },
      { finish: null, umaban: null, gate: 1, sexCode: "3", age: 5, weightKg: 57 },
    ],
  });
});

it("reads the older layout without a horse-number column and keeps unknown cells empty", () => {
  expect(parseAhonooraRace(OLD_PAGE)).toStrictEqual({
    date: "20101106",
    title: "ブリーダーズカップスプリント",
    runners: [
      { finish: 1, umaban: null, gate: 1, sexCode: "1", age: 4, weightKg: 57 },
      { finish: 2, umaban: null, gate: 4, sexCode: null, age: null, weightKg: null },
    ],
  });
});

it("reads a page saved through the browser proxy with quoted attributes", () => {
  expect(
    parseAhonooraRace(`<big>メルボルンカップ <font color="RED">(GI)</font></big></small></b></center><small>
2023/11/7　豪・フレミントン7R　3歳以上　芝3200m（左）　稍重</small>
<table border="" bgcolor="LIGHTYELLOW"><tbody><tr align="center" bgcolor="silver"><td width="35">着順</td><td width="35">枠順</td><td>馬名</td><td>性齢</td><td width="35">斤量</td></tr>
<tr align="center" bgcolor="lightgreen"><td>1着</td><td>4番</td><td><b>Without A Fight</b></td><td>セン6</td><td>56</td></tr>
<tr align="center"><td>2着</td><td>13番</td><td><b>Soulcombe</b></td><td>セン5</td><td>53.5</td></tr>
</tbody></table>`),
  ).toStrictEqual({
    date: "20231107",
    title: "メルボルンカップ",
    runners: [
      { finish: 1, umaban: null, gate: 4, sexCode: "3", age: 6, weightKg: 56 },
      { finish: 2, umaban: null, gate: 13, sexCode: "3", age: 5, weightKg: 53.5 },
    ],
  });
});

it("fails closed on pages it cannot read safely", () => {
  expect(() => parseAhonooraRace("<html></html>")).toThrow(
    "Ahonoora race page has no title or date.",
  );
  expect(() =>
    parseAhonooraRace(OLD_PAGE.replace("<td width=35>枠順", "<td width=35>番号")),
  ).toThrow("Ahonoora race page has no 着順/枠順 header.");
  expect(() => parseAhonooraRace(OLD_PAGE.replace("<td>2着<td>4番", "<td>2着<td>1番"))).toThrow(
    "Ahonoora race page repeats a post position.",
  );
  expect(() =>
    parseAhonooraRace(
      OLD_PAGE.replace("<td>1着<td>1番", "<td>1着<td>-").replace("<td>2着<td>4番", "<td>2着<td>-"),
    ),
  ).toThrow("Ahonoora race page has no runner rows.");
});
