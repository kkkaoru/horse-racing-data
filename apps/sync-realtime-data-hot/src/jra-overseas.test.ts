// Run with: bun run --filter sync-realtime-data-hot test
import { afterEach, expect, it, vi } from "vitest";

import {
  createJraOverseasRaceResolver,
  JraOverseasFetchError,
  parseJraOverseasRaceList,
  parseJraOverseasRacePage,
} from "./jra-overseas";

const ASCOT_RACE_LIST_HTML = `
  <table>
    <tr><th>施行日</th><th>国・競馬場</th><th>競走名</th><th>距離</th></tr>
    <tr>
      <td>2026年7月25日（土曜）</td>
      <td>イギリス／アスコット</td>
      <td><a href="/keiba/overseas/race/2026kgqes/index.html">キングジョージVI世＆クイーンエリザベスステークス（G1）</a></td>
      <td>芝2,390メートル</td>
    </tr>
    <tr>
      <td>2026年8月2日（日曜）</td>
      <td>フランス／ドーヴィル</td>
      <td><a href="/keiba/overseas/race/2026other/index.html">ロートシルト賞（G1）</a></td>
      <td>芝1,600メートル</td>
    </tr>
  </table>
`;

const ASCOT_RACE_PAGE_HTML = `
  <main>
    <dl>
      <dt>発走予定時刻</dt>
      <dd>日本時間 7月25日（土曜）23時35分</dd>
    </dl>
    <nav>
      <a href="/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32">出馬表</a>
    </nav>
  </main>
`;

const ASCOT_INPUT = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0725",
  kyosomeiHondai: "キングジョージ６世＆クイーンエリザベスステークス　　　　　　",
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("parseJraOverseasRaceList extracts official race pages with normalized dates and names", () => {
  expect(parseJraOverseasRaceList(ASCOT_RACE_LIST_HTML)).toStrictEqual([
    {
      kaisaiTsukihi: "0725",
      raceName: "キングジョージVI世＆クイーンエリザベスステークス（G1）",
      racePageUrl: "https://www.jra.go.jp/keiba/overseas/race/2026kgqes/index.html",
    },
    {
      kaisaiTsukihi: "0802",
      raceName: "ロートシルト賞（G1）",
      racePageUrl: "https://www.jra.go.jp/keiba/overseas/race/2026other/index.html",
    },
  ]);
});

it("parseJraOverseasRaceList decodes hexadecimal and decimal HTML entities", () => {
  expect(
    parseJraOverseasRaceList(`
      <table><tr>
        <td>2026年9月1日</td>
        <td><a href="/keiba/overseas/race/entity/index.html">A&#x42;&#67;</a></td>
      </tr></table>
    `),
  ).toStrictEqual([
    {
      kaisaiTsukihi: "0901",
      raceName: "ABC",
      racePageUrl: "https://www.jra.go.jp/keiba/overseas/race/entity/index.html",
    },
  ]);
});

it("parseJraOverseasRaceList ignores rows without a date or an official overseas race page", () => {
  expect(
    parseJraOverseasRaceList(`
      <table>
        <tr><td>日付なし</td><td><a href="/keiba/overseas/race/no-date/index.html">対象外</a></td></tr>
        <tr><td>2026年7月25日</td><td><a href="/keiba/domestic/index.html">国内</a></td></tr>
        <tr><td>2026年7月25日</td><td><a href="http://[">壊れたURL</a></td></tr>
      </table>
    `),
  ).toStrictEqual([]);
});

it("parseJraOverseasRacePage extracts the accessSD entry URL and real JST post time", () => {
  expect(parseJraOverseasRacePage(ASCOT_RACE_PAGE_HTML, ASCOT_INPUT)).toStrictEqual({
    debaUrl: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32",
    raceStartAtJst: "2026-07-25T23:35:00+09:00",
  });
});

it("parseJraOverseasRacePage rejects a post date that differs from the JV race date", () => {
  expect(
    parseJraOverseasRacePage(
      `
        <p>発走予定時刻 日本時間 7月26日（日曜）23時35分</p>
        <a href="/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32">出馬表</a>
      `,
      ASCOT_INPUT,
    ),
  ).toBeNull();
});

it("parseJraOverseasRacePage rejects pages without a parseable Japan post time", () => {
  expect(
    parseJraOverseasRacePage(
      `<a href="/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32">出馬表</a>`,
      ASCOT_INPUT,
    ),
  ).toBeNull();
});

it("parseJraOverseasRacePage rejects a domestic entry path", () => {
  expect(
    parseJraOverseasRacePage(
      `
        <p>日本時間 7月25日（土曜）23時35分</p>
        <a href="/JRADB/accessD.html?CNAME=pw01dde0105202602050120260509/6A">出馬表</a>
      `,
      ASCOT_INPUT,
    ),
  ).toBeNull();
});

it("parseJraOverseasRacePage rejects a non-JRA entry origin", () => {
  expect(
    parseJraOverseasRacePage(
      `
        <p>日本時間 7月25日（土曜）23時35分</p>
        <a href="https://example.com/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32">出馬表</a>
      `,
      ASCOT_INPUT,
    ),
  ).toBeNull();
});

it("parseJraOverseasRacePage rejects accessSD links without the pk01dde prefix", () => {
  expect(
    parseJraOverseasRacePage(
      `
        <p>日本時間 7月25日（土曜）23時35分</p>
        <a href="/JRADB/accessSD.html?CNAME=pw01dde0110420260101051/32">出馬表</a>
      `,
      ASCOT_INPUT,
    ),
  ).toBeNull();
});

it("parseJraOverseasRacePage rejects pages without an 出馬表 link", () => {
  expect(
    parseJraOverseasRacePage(
      `<p>日本時間 7月25日（土曜）23時35分</p><a href="/other">オッズ</a>`,
      ASCOT_INPUT,
    ),
  ).toBeNull();
});

it("createJraOverseasRaceResolver matches exact date plus normalized VI/６ race title and caches fetches", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    return Promise.resolve(
      new Response(
        url === "https://www.jra.go.jp/keiba/overseas/racelist/2026.html"
          ? ASCOT_RACE_LIST_HTML
          : ASCOT_RACE_PAGE_HTML,
        { status: 200 },
      ),
    );
  });
  const resolver = createJraOverseasRaceResolver();
  await expect(Promise.all([resolver(ASCOT_INPUT), resolver(ASCOT_INPUT)])).resolves.toStrictEqual([
    {
      debaUrl: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32",
      raceStartAtJst: "2026-07-25T23:35:00+09:00",
    },
    {
      debaUrl: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/32",
      raceStartAtJst: "2026-07-25T23:35:00+09:00",
    },
  ]);
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

it("createJraOverseasRaceResolver returns null when date and race name do not identify a row", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response(ASCOT_RACE_LIST_HTML, { status: 200 }));
  const resolver = createJraOverseasRaceResolver();
  await expect(
    resolver({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0725",
      kyosomeiHondai: "一致しない競走名",
    }),
  ).resolves.toBeNull();
  expect(fetchSpy).toHaveBeenCalledTimes(1);
});

it("createJraOverseasRaceResolver rejects ambiguous same-date normalized race names", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(
      `${ASCOT_RACE_LIST_HTML}
       <table><tr>
         <td>2026年7月25日（土曜）</td>
         <td><a href="/keiba/overseas/race/2026kgqes-copy/index.html">キングジョージVI世＆クイーンエリザベスステークス（G1）</a></td>
       </tr></table>`,
      { status: 200 },
    ),
  );
  const resolver = createJraOverseasRaceResolver();
  await expect(resolver(ASCOT_INPUT)).resolves.toBeNull();
  expect(fetchSpy).toHaveBeenCalledTimes(1);
});

it("createJraOverseasRaceResolver surfaces official page HTTP failures", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unavailable", { status: 503 }));
  const resolver = createJraOverseasRaceResolver();
  await expect(resolver(ASCOT_INPUT)).rejects.toStrictEqual(
    new JraOverseasFetchError("https://www.jra.go.jp/keiba/overseas/racelist/2026.html", 503),
  );
});
