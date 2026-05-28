import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildRaceListUrl,
  buildRaceKey,
  buildRaceResultUrl,
  convertToAbsoluteKeibaGoUrl,
  extractOddsLinks,
  fetchOdds,
  fetchRaceLinksFromRaceList,
  fetchRacePage,
  fetchTodayRaceListUrls,
  parseRaceMetadata,
  parseRaceEntries,
  parseHorseWeights,
  parseRaceEntryHorseNumbers,
  parseRaceResultExcludedHorseNumbers,
  parseRaceResults,
  parseRaceResultHorseWeights,
} from "./keiba-go";

const mockFetchHtml = (htmlByUrl: Record<string, string | Response>): void => {
  vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const value = htmlByUrl[url];
    if (value instanceof Response) {
      return Promise.resolve(value);
    }
    return Promise.resolve(
      new Response(value ?? "", {
        headers: { "content-type": "text/html" },
        status: value === undefined ? 404 : 200,
      }),
    );
  });
};

describe("keiba.go realtime helpers", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("builds normalized NAR race keys", () => {
    expect(buildRaceKey("2026", "0510", "55", "4")).toBe("nar:2026:0510:55:04");
  });

  it("builds race list URLs", () => {
    expect(buildRaceListUrl("20260510", "22")).toEqual({
      babaCode: "22",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F10&k_babaCode=22",
    });
  });

  it("converts relative odds URLs for normal and IPAT pages", () => {
    expect(
      convertToAbsoluteKeibaGoUrl(
        "../Odds/OddsTanFuku?k=1",
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1",
      ),
    ).toBe("https://www.keiba.go.jp/KeibaWeb/Odds/OddsTanFuku?k=1");
    expect(
      convertToAbsoluteKeibaGoUrl(
        "./Odds/OddsWide?k=1",
        "https://www.keiba.go.jp/KeibaWeb_IPAT/TodayRaceInfo/DebaTable?k=1",
      ),
    ).toBe("https://www.keiba.go.jp/KeibaWeb_IPAT/Odds/OddsWide?k=1");
    expect(
      convertToAbsoluteKeibaGoUrl(
        "Odds/OddsWide?k=1",
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1",
      ),
    ).toBe("https://www.keiba.go.jp/KeibaWeb/Odds/OddsWide?k=1");
    expect(
      convertToAbsoluteKeibaGoUrl(
        "https://example.test/odds",
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1",
      ),
    ).toBe("https://example.test/odds");
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
      fukusho: "https://www.keiba.go.jp/KeibaWeb/Odds/OddsTanFuku?k=1",
      tansho: "https://www.keiba.go.jp/KeibaWeb/Odds/OddsTanFuku?k=1",
      wide: "https://www.keiba.go.jp/KeibaWeb/Odds/OddsWide?k=1",
    });
  });

  it("extracts odds links from current keiba.go navigation", () => {
    const html = `
      <nav class="clearfix noWrapNav">
        <div class="chartNavi clearfix">
          <h3 class="naviTitle">オッズ</h3>
          <a href="/KeibaWeb/TodayRaceInfo/OddsTanFuku?k_raceDate=2026%2F05%2F21&amp;k_raceNo=1&amp;k_babaCode=27">単・複</a>
          <a href="/KeibaWeb/TodayRaceInfo/Odds3LenTan?k_raceDate=2026%2F05%2F21&amp;k_raceNo=1&amp;k_babaCode=27">三連単</a>
        </div>
      </nav>
    `;
    expect(
      extractOddsLinks(
        html,
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f21&k_raceNo=1&k_babaCode=27",
      ),
    ).toEqual({
      "3rentan":
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/Odds3LenTan?k_raceDate=2026%2F05%2F21&k_raceNo=1&k_babaCode=27",
      fukusho:
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/OddsTanFuku?k_raceDate=2026%2F05%2F21&k_raceNo=1&k_babaCode=27",
      tansho:
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/OddsTanFuku?k_raceDate=2026%2F05%2F21&k_raceNo=1&k_babaCode=27",
    });
  });

  it("returns empty odds links when the odds nav is missing", () => {
    expect(extractOddsLinks("<nav><div></div></nav>", "https://example.test")).toEqual({});
  });

  it("ignores unknown odds nav links", () => {
    expect(
      extractOddsLinks(
        `
          <nav>
            <div></div><div></div>
            <div>
              <a href="/KeibaWeb/Odds/Unknown">不明</a>
              <a href="/KeibaWeb/Odds/Empty"></a>
            </div>
          </nav>
        `,
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1",
      ),
    ).toEqual({});
  });

  it("fetches today's race list URLs for the requested date", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop": `
        <article class="todayRace">
          <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&amp;k_babaCode=22">target</a>
          <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22">dupe</a>
          <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F10&k_babaCode=23">target uppercase</a>
          <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f11&k_babaCode=22">other date</a>
          <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=99">unknown</a>
        </article>
      `,
    });

    expect(await fetchTodayRaceListUrls("20260510")).toEqual([
      {
        babaCode: "22",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22",
      },
      {
        babaCode: "23",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F10&k_babaCode=23",
      },
    ]);
  });

  it("falls back to the full page when today's race article is missing", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop": `
        <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22">target</a>
      `,
    });

    expect(await fetchTodayRaceListUrls("20260510")).toHaveLength(1);
  });

  it("fetches race links from a race list", async () => {
    const raceListUrl =
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F10&k_babaCode=22";
    mockFetchHtml({
      [raceListUrl]: `
        <a href="DebaTable?k_raceDate=2026%2F05%2F10&amp;k_raceNo=12&amp;k_babaCode=22">12</a>
        <button onclick="location.href='/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F10&amp;k_babaCode=22&amp;k_raceNo=4'">4</button>
        <a href="DebaTable?k_raceDate=2026%2F05%2F10&k_raceNo=2&k_babaCode=22">2</a>
        <a href="DebaTable?k_raceDate=2026%2F05%2F10&k_raceNo=2&k_babaCode=22">dupe</a>
        <a href="DebaTable?k_raceDate=2026%2F05%2F11&k_raceNo=1&k_babaCode=22">other date</a>
        <a href="DebaTable?k_raceDate=2026%2F05%2F10&k_raceNo=3&k_babaCode=23">other baba</a>
      `,
    });

    expect(await fetchRaceLinksFromRaceList(raceListUrl)).toEqual([
      {
        babaCode: "22",
        raceNumber: "02",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F10&k_raceNo=2&k_babaCode=22",
      },
      {
        babaCode: "22",
        raceNumber: "04",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F10&k_raceNo=4&k_babaCode=22",
      },
      {
        babaCode: "22",
        raceNumber: "12",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F10&k_raceNo=12&k_babaCode=22",
      },
    ]);
  });

  it("returns no race links when list URL query is invalid", async () => {
    expect(
      await fetchRaceLinksFromRaceList("https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList"),
    ).toEqual([]);
  });

  it("parses race metadata from a deba page", () => {
    expect(
      parseRaceMetadata(`
        <article class="raceCard">
          <h4>2026年5月14日（木）　川　崎　第4競走　16:30発走</h4>
          <section class="raceTitle">
            <p class="subTitle"></p>
            <h3>Ｃ２五 六</h3>
          </section>
        </article>
      `),
    ).toEqual({
      raceName: "Ｃ２五 六",
      startTime: "1630",
    });
  });

  it("throws on failed page fetches", async () => {
    mockFetchHtml({
      "https://example.test/fail": new Response("ng", { status: 503 }),
    });

    await expect(fetchRacePage("https://example.test/fail")).rejects.toThrow(
      "Failed to fetch https://example.test/fail: 503",
    );
  });

  it("fetchRaceLinksFromRaceList returns [] when raceListUrl lacks k_raceDate", async () => {
    const result = await fetchRaceLinksFromRaceList(
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_babaCode=22",
    );
    expect(result).toStrictEqual([]);
  });

  it("fetchRaceLinksFromRaceList returns [] when raceListUrl lacks k_babaCode", async () => {
    const result = await fetchRaceLinksFromRaceList(
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10",
    );
    expect(result).toStrictEqual([]);
  });

  it("fetchRaceLinksFromRaceList skips links with mismatched race date or babaCode", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22": `
        <a href="/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f10&k_raceNo=1&k_babaCode=22">match</a>
        <a href="/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f11&k_raceNo=2&k_babaCode=22">other date</a>
        <a href="/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f10&k_raceNo=3&k_babaCode=30">other code</a>
        <a href="/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f10&k_raceNo=1&k_babaCode=22">duplicate</a>
        `,
    });
    const result = await fetchRaceLinksFromRaceList(
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22",
    );
    expect(result).toHaveLength(1);
    expect(result[0]?.raceNumber).toBe("01");
  });

  it("fetchTodayRaceListUrls accepts absolute http URLs and skips entries with missing babaCode", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop": `
        <article class="todayRace">
          <a href="https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22">absolute</a>
          <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10">no-baba</a>
        </article>
      `,
    });
    const result = await fetchTodayRaceListUrls("20260510");
    expect(result).toStrictEqual([
      {
        babaCode: "22",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22",
      },
    ]);
  });

  it("fetchRaceLinksFromRaceList parses single-quoted DebaTable URLs", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22": `<a href='/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f10&k_raceNo=4&k_babaCode=22'>r4</a>`,
    });
    const result = await fetchRaceLinksFromRaceList(
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22",
    );
    expect(result).toHaveLength(1);
    expect(result[0]?.raceNumber).toBe("04");
  });

  it("fetchRaceLinksFromRaceList parses location.href bare DebaTable URLs", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22": `<button onclick=location.href=/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2f05%2f10&k_raceNo=2&k_babaCode=22>go</button>`,
    });
    const result = await fetchRaceLinksFromRaceList(
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22",
    );
    expect(result[0]?.raceNumber).toBe("02");
  });

  it("fetchOdds returns empty for tansho when tbody absent", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/tansho": "<div>no table</div>",
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    await expect(fetchOdds(baseUrl, { tansho: "/KeibaWeb/Odds/tansho" })).resolves.toStrictEqual({
      tansho: [],
    });
  });

  it("fetchOdds returns empty array for tansho with invalid horseNum/odds", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/tansho": `
        <tbody>
          <tr><td></td><td>99</td><td></td><td>2.0</td></tr>
          <tr><td></td><td>1</td><td></td><td>not-a-number</td></tr>
          <tr><td></td><td>2</td><td></td><td></td></tr>
        </tbody>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    await expect(fetchOdds(baseUrl, { tansho: "/KeibaWeb/Odds/tansho" })).resolves.toStrictEqual({
      tansho: [],
    });
  });

  it("fetchOdds drops fukusho rows missing the odds cell entirely", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/fukusho": `
        <tbody>
          <tr><td></td><td>1</td><td></td><td></td></tr>
        </tbody>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    await expect(fetchOdds(baseUrl, { fukusho: "/KeibaWeb/Odds/fukusho" })).resolves.toStrictEqual({
      fukusho: [],
    });
  });

  it("fetchOdds drops fukusho rows whose odds cell has no numbers", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/fukusho": `
        <tbody>
          <tr><td></td><td>1</td><td></td><td></td><td>not-numeric</td></tr>
          <tr><td></td><td>2</td><td></td><td></td><td>1.5 - 2.3</td></tr>
        </tbody>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { fukusho: "/KeibaWeb/Odds/fukusho" });
    expect(result.fukusho).toHaveLength(1);
    expect(result.fukusho?.[0]?.combination).toBe("2");
  });

  it("fetchOdds catches per-type fetch errors and excludes that type from the result", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/tansho": new Response("err", { status: 503 }),
      "https://www.keiba.go.jp/KeibaWeb/Odds/fukusho": `
        <tbody>
          <tr><td></td><td>1</td><td></td><td></td><td>1.5 - 2.3</td></tr>
        </tbody>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, {
      fukusho: "/KeibaWeb/Odds/fukusho",
      tansho: "/KeibaWeb/Odds/tansho",
    });
    expect(result.tansho).toBeUndefined();
    expect(result.fukusho).toHaveLength(1);
  });

  it("parses fukusho odds with min/max range cells", async () => {
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/fukusho": `
        <tbody>
          <tr><td></td><td>1</td><td></td><td></td><td>1.5 - 2.3</td></tr>
          <tr><td></td><td>2</td><td></td><td></td><td>2.0 - 4.0</td></tr>
          <tr><td></td><td>invalid</td><td></td><td></td><td>3.0 - 5.0</td></tr>
        </tbody>
      `,
    });
    await expect(fetchOdds(baseUrl, { fukusho: "/KeibaWeb/Odds/fukusho" })).resolves.toMatchObject({
      fukusho: [
        { averageOdds: 1.9, combination: "1", maxOdds: 2.3, minOdds: 1.5, rank: 1 },
        { averageOdds: 3, combination: "2", maxOdds: 4, minOdds: 2, rank: 2 },
      ],
    });
  });

  it("fetches and parses all supported odds types", async () => {
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/tansho": `
        <tbody>
          <tr><td></td><td>2</td><td></td><td>3.456</td></tr>
          <tr><td></td><td>1</td><td></td><td>1.2</td></tr>
          <tr><td></td><td>19</td><td></td><td>9.9</td></tr>
        </tbody>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/wakuren": `
        <ul class="odd_horse_number_list">
          <table>
            <th class="odd_post1">1</th>
            <tr><td>2</td><td>4.444</td></tr>
          </table>
          <table>
            <th class="odd_post2">2</th>
            <tr><td>1</td><td>5.555</td></tr>
            <tr><td>9</td><td>99.9</td></tr>
          </table>
        </ul>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/umaren": `
        <table class="odd_ranking_table">
          <tr><td>2-1</td><td>8.888</td></tr>
          <tr><td>1-2</td><td>9.9</td></tr>
        </table>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/umatan": `
        <table class="odd_ranking_table">
          <tr><td>2-1</td><td>7.777</td></tr>
        </table>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/wide": `
        <table class="odd_ranking_table">
          <tr><td>3-1</td><td>2.0</br> - 4.0</td></tr>
          <tr><td>1-2</td><td>1.0</br> - 3.0</td></tr>
        </table>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/3renpuku": `
        <table class="odd_ranking_table">
          <tr><td>3-1-2</td><td>12.345</td></tr>
        </table>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/3rentan": `
        <table class="formation_table"><tr><td>1-2-3</td><td>0</td></tr></table>
        <table class="odd_ranking_table">
          <tr><td>3-1-2</td><td>123.456</td></tr>
        </table>
      `,
    });

    await expect(
      fetchOdds(baseUrl, {
        "3renpuku": "/KeibaWeb/Odds/3renpuku",
        "3rentan": "/KeibaWeb/Odds/3rentan",
        tansho: "/KeibaWeb/Odds/tansho",
        umaren: "/KeibaWeb/Odds/umaren",
        umatan: "/KeibaWeb/Odds/umatan",
        wakuren: "/KeibaWeb/Odds/wakuren",
        wide: "/KeibaWeb/Odds/wide",
      }),
    ).resolves.toMatchObject({
      "3renpuku": [{ combination: "1-2-3", odds: 12.35, rank: 1 }],
      "3rentan": [{ combination: "3-1-2", odds: 123.46, rank: 1 }],
      tansho: [
        { combination: "1", odds: 1.2, rank: 1 },
        { combination: "2", odds: 3.46, rank: 2 },
      ],
      umaren: [{ combination: "1-2", odds: 9.9, rank: 1 }],
      umatan: [{ combination: "2-1", odds: 7.78, rank: 1 }],
      wakuren: [{ combination: "1-2", odds: 5.56, rank: 1 }],
      wide: [
        { averageOdds: 2, combination: "1-2", maxOdds: 3, minOdds: 1, rank: 1 },
        { averageOdds: 3, combination: "1-3", maxOdds: 4, minOdds: 2, rank: 2 },
      ],
    });
  });

  it("falls back to plain triple odds text when ranking tables are absent", async () => {
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/3renpuku": "3-1-2 12.345",
      "https://www.keiba.go.jp/KeibaWeb/Odds/3rentan": "3→1→2 123.456",
    });

    await expect(
      fetchOdds(baseUrl, {
        "3renpuku": "/KeibaWeb/Odds/3renpuku",
        "3rentan": "/KeibaWeb/Odds/3rentan",
      }),
    ).resolves.toMatchObject({
      "3renpuku": [{ combination: "1-2-3", odds: 12.35, rank: 1 }],
      "3rentan": [{ combination: "3-1-2", odds: 123.46, rank: 1 }],
    });
  });

  it("skips odds links that fail to fetch", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/tansho": new Response("ng", { status: 500 }),
    });

    await expect(
      fetchOdds("https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1", {
        tansho: "/KeibaWeb/Odds/tansho",
      }),
    ).resolves.toEqual({});
    expect(console.warn).toHaveBeenCalledOnce();
  });

  it("handles empty and malformed odds pages", async () => {
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/tansho": `
        <tbody>
          <tr><td></td><td>A</td><td></td><td>1.2</td></tr>
          <tr><td></td><td></td><td></td><td>1.2</td></tr>
          <tr><td></td><td>1</td><td></td><td>bad</td></tr>
        </tbody>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/wakuren": `
        <ul class="odd_horse_number_list">
          <table><th class="odd_post9">9</th><tr><td>1</td><td>2.0</td></tr></table>
          <table><th class="odd_post2">2</th><tr><td>9</td><td>2.0</td></tr></table>
        </ul>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/umaren": `
        <table class="odd_ranking_table">
          <tr><td>bad</td><td>1.0</td></tr>
          <tr><td>1-2</td><td>bad</td></tr>
        </table>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/umatan": `
        <table class="odd_ranking_table"><tr><td></td><td>1.0</td></tr></table>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/wide": `
        <table class="odd_ranking_table">
          <tr><td>1-2</td><td>bad</td></tr>
          <tr><td></td><td>2.0</br> - 4.0</td></tr>
        </table>
      `,
      "https://www.keiba.go.jp/KeibaWeb/Odds/3renpuku": "19-1-2 10.0",
      "https://www.keiba.go.jp/KeibaWeb/Odds/3rentan": "1-2-19 10.0",
    });

    await expect(
      fetchOdds(baseUrl, {
        "3renpuku": "/KeibaWeb/Odds/3renpuku",
        "3rentan": "/KeibaWeb/Odds/3rentan",
        tansho: "/KeibaWeb/Odds/tansho",
        umaren: "/KeibaWeb/Odds/umaren",
        umatan: "/KeibaWeb/Odds/umatan",
        wakuren: "/KeibaWeb/Odds/wakuren",
        wide: "/KeibaWeb/Odds/wide",
      }),
    ).resolves.toEqual({
      "3renpuku": [],
      "3rentan": [],
      tansho: [],
      umaren: [],
      umatan: [],
      wakuren: [],
      wide: [],
    });
  });

  it("returns empty odds arrays for missing odds containers", async () => {
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/tansho": "<table></table>",
      "https://www.keiba.go.jp/KeibaWeb/Odds/wakuren": "<table></table>",
    });

    await expect(
      fetchOdds(baseUrl, {
        tansho: "/KeibaWeb/Odds/tansho",
        wakuren: "/KeibaWeb/Odds/wakuren",
      }),
    ).resolves.toEqual({
      tansho: [],
      wakuren: [],
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

  it("parses horse weights when odds and weight are separate odds_weight cells", () => {
    const html = `
      <table>
        <tr class="tBorder">
          <td rowspan="5" class="horseNum">10</td>
          <td colspan="3"><a class="horseName">オメガドライヴ</a></td>
          <td class="odds_weight" rowspan="2"><span class="odds_Black odds">4.3</span><br>(2人気)</td>
        </tr>
        <tr>
          <td class="noBorder">セン4</td>
        </tr>
        <tr>
          <td colspan="3">マインドユアビスケッツ</td>
          <td rowspan="2" class="odds_weight">493<br>(+13)</td>
        </tr>
      </table>
    `;
    expect(parseHorseWeights(html)).toEqual([
      {
        changeAmount: 13,
        changeSign: "+",
        horseName: "オメガドライヴ",
        horseNumber: "10",
        weight: 493,
      },
    ]);
  });

  it("parses horse weights without change amounts or names", () => {
    const html = `
      <tr class="tBorder">
        <td class="horseNum">3</td>
        <td class="odds_weight">399</td>
      </tr>
    `;
    expect(parseHorseWeights(html)).toEqual([
      {
        changeAmount: null,
        changeSign: null,
        horseName: null,
        horseNumber: "3",
        weight: 399,
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

  it("parses race finish results from race result tables", () => {
    const html = `
      <tr bgcolor="#FFFFFF" align="center">
        <td nowrap><span class="bold">2</span></td>
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
        <td nowrap><span class="bold">1</span></td>
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
        <td nowrap class="dbdata2">1:53.9</td>
      </tr>
    `;
    expect(parseRaceResults(html)).toEqual([
      {
        finishPosition: "01",
        horseName: "ピカピカピロコ",
        horseNumber: "12",
        time: "1:53.9",
      },
      {
        finishPosition: "02",
        horseName: "グリーゼ",
        horseNumber: "7",
        time: "1:54.3",
      },
    ]);
  });

  it("parses entry horse numbers from entry tables", () => {
    const html = `
      <tr class="tBorder"><td class="horseNum">12</td><td>horse</td></tr>
      <tr class="tBorder"><td class="horseNum">3</td><td>horse</td></tr>
      <tr class="tBorder"><td class="horseNum">12</td><td>duplicate</td></tr>
      <tr class="tBorder"><td class="horseNum">20</td><td>invalid</td></tr>
    `;
    expect(parseRaceEntryHorseNumbers(html)).toEqual(["3", "12"]);
  });

  it("parses realtime race entries with cancellations and jockey changes", () => {
    const html = `
      <tr class="tBorder">
        <td rowspan="5" class="horseNum">7</td>
        <td colspan="3"><a class="horseName">取消ホース</a></td>
        <td><a class="jockeyName">新騎手<span class="jockeyarea">（川崎）</span></a></td>
        <td class="info"><span>出走取消</span></td>
      </tr>
      <tr class="tBorder">
        <td rowspan="5" class="horseNum">8</td>
        <td colspan="3"><a class="horseName">乗替ホース</a></td>
        <td><a class="jockeyName">替騎手<span class="jockeyarea">（船橋）</span></a></td>
        <td class="odds_weight"><span>5.5</span></td>
        <td><span class="pastRank ">取消</span></td>
      </tr>
    `;

    expect(parseRaceEntries(html)).toEqual([
      {
        horseName: "取消ホース",
        horseNumber: "7",
        jockeyName: "新騎手",
        status: "出走取消",
      },
      {
        horseName: "乗替ホース",
        horseNumber: "8",
        jockeyName: "替騎手",
        status: null,
      },
    ]);
  });

  it("keeps non-excluded race result statuses and detects excluded horses", () => {
    const html = `
      <tr bgcolor="#FFFFFF" align="center">
        <td nowrap>中止</td><td></td><td>5</td><td>競走中止馬</td>
      </tr>
      <tr bgcolor="#FFFFFF" align="center">
        <td nowrap>出場停止</td><td></td><td>7</td><td>対象外</td>
      </tr>
      <tr bgcolor="#FFFFFF" align="center">
        <td nowrap>除外</td><td></td><td>8</td><td>対象外</td>
      </tr>
    `;
    expect(parseRaceResults(html)).toEqual([
      {
        finishPosition: "中止",
        horseName: "競走中止馬",
        horseNumber: "5",
        time: null,
      },
    ]);
    expect(parseRaceResultExcludedHorseNumbers(html)).toEqual(["7", "8"]);
  });

  it("parses current keiba.go race result tables", () => {
    const html = `
      <tr>
        <td class="a"><span class="bold">1</span></td>
        <td class="b">3</td>
        <td class="c">4</td>
        <td class="d horseName"><a>ナムラハカ</a></td>
        <td class="e">兵庫</td><td class="f">牝 9</td><td class="g">52.0</td>
        <td class="h">塩津璃</td><td class="i">長南和</td>
        <td class="j horseWeight">488<span>(-6)</span></td>
        <td class="k">1:35.0</td>
        <td class="o">4</td><td class="p">7.5</td>
      </tr>
      <tr>
        <td class="a">除外</td>
        <td class="b">5</td>
        <td class="c">8</td>
        <td class="d horseName">対象外</td>
        <td class="j horseWeight">452<span>(0)</span></td>
      </tr>
    `;
    expect(parseRaceResults(html)).toEqual([
      {
        finishPosition: "01",
        horseName: "ナムラハカ",
        horseNumber: "4",
        time: "1:35.0",
      },
    ]);
    expect(parseRaceResultExcludedHorseNumbers(html)).toEqual(["8"]);
    expect(parseRaceResultHorseWeights(html)).toEqual([
      {
        changeAmount: 6,
        changeSign: "-",
        horseName: "ナムラハカ",
        horseNumber: "4",
        weight: 488,
      },
      {
        changeAmount: 0,
        changeSign: "+",
        horseName: "対象外",
        horseNumber: "8",
        weight: 452,
      },
    ]);
  });

  it("ignores invalid race result rows", () => {
    const html = `
      <tr bgcolor="#FFFFFF"><td>除外</td><td></td><td>1</td><td>invalid</td></tr>
      <tr bgcolor="#FFFFFF"><td>1</td><td></td><td>20</td><td>invalid</td></tr>
    `;
    expect(parseRaceResults(html)).toEqual([]);
  });

  it("ignores invalid race result weight rows", () => {
    const html = `
      <tr bgcolor="#FFFFFF"><td></td><td></td><td>A</td><td>invalid</td><td></td><td></td><td></td><td></td><td></td><td>500</td><td>0</td></tr>
      <tr bgcolor="#FFFFFF"><td></td><td></td><td>20</td><td>invalid</td><td></td><td></td><td></td><td></td><td></td><td>200</td><td>abc</td></tr>
      <tr bgcolor="#FFFFFF"><td></td><td></td><td>5</td><td>valid</td><td></td><td></td><td></td><td></td><td></td><td>500</td><td></td></tr>
    `;
    expect(parseRaceResultHorseWeights(html)).toEqual([
      {
        changeAmount: null,
        changeSign: null,
        horseName: "valid",
        horseNumber: "5",
        weight: 500,
      },
    ]);
  });

  it("fetchTodayRaceListUrls parses single-quoted RaceList URLs", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop": `
        <article class="todayRace">
          <a href='/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22'>single</a>
        </article>
      `,
    });
    expect(await fetchTodayRaceListUrls("20260510")).toStrictEqual([
      {
        babaCode: "22",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22",
      },
    ]);
  });

  it("fetchTodayRaceListUrls parses bare/unquoted RaceList URLs", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop": `
        <article class="todayRace">
          <button onclick=location.href=/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22>bare</button>
        </article>
      `,
    });
    expect(await fetchTodayRaceListUrls("20260510")).toStrictEqual([
      {
        babaCode: "22",
        url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2f05%2f10&k_babaCode=22",
      },
    ]);
  });

  it("parseRaceMetadata returns null fields when the h4 heading is missing", () => {
    expect(
      parseRaceMetadata(`
        <article class="raceCard">
          <section class="raceTitle">
            <h3>Ｃ２五 六</h3>
          </section>
        </article>
      `),
    ).toStrictEqual({
      raceName: "Ｃ２五 六",
      startTime: null,
    });
  });

  it("parseRaceMetadata returns null raceName when raceTitle has no h3", () => {
    expect(
      parseRaceMetadata(`
        <article class="raceCard">
          <h4>2026年5月14日（木）　川　崎　第4競走　16:30発走</h4>
          <section class="raceTitle">
            <p class="subTitle"></p>
          </section>
        </article>
      `),
    ).toStrictEqual({
      raceName: null,
      startTime: "1630",
    });
  });

  it("parseRaceResultHorseWeights sets horseName to null for an empty name cell", () => {
    const html = `
      <tr bgcolor="#FFFFFF"><td></td><td></td><td>7</td><td></td><td></td><td></td><td></td><td></td><td></td><td>480</td><td>0</td></tr>
    `;
    expect(parseRaceResultHorseWeights(html)).toStrictEqual([
      {
        changeAmount: 0,
        changeSign: "+",
        horseName: null,
        horseNumber: "7",
        weight: 480,
      },
    ]);
  });

  it("parseRaceResults sets horseName and time to null for empty cells", () => {
    const html = `
      <tr bgcolor="#FFFFFF"><td>1</td><td></td><td>3</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
    `;
    expect(parseRaceResults(html)).toStrictEqual([
      {
        finishPosition: "01",
        horseName: null,
        horseNumber: "3",
        time: null,
      },
    ]);
  });

  it("parseRaceResults falls through to horseNumber when both rows share a non-numeric finishPosition", () => {
    const html = `
      <tr bgcolor="#FFFFFF"><td>－</td><td></td><td>5</td><td>laterA</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
      <tr bgcolor="#FFFFFF"><td>－</td><td></td><td>2</td><td>laterB</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
    `;
    expect(parseRaceResults(html)).toStrictEqual([
      {
        finishPosition: "－",
        horseName: "laterB",
        horseNumber: "2",
        time: null,
      },
      {
        finishPosition: "－",
        horseName: "laterA",
        horseNumber: "5",
        time: null,
      },
    ]);
  });

  it("extractBabaCode returns null when k_babaCode is missing", async () => {
    const { extractOddsLinks: _extract } = await import("../src/keiba-go");
    expect(_extract).toBeDefined();
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop": `
        <article class="todayRace">
          <a href="/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F10">no-baba</a>
        </article>
      `,
    });
    expect(await fetchTodayRaceListUrls("20260510")).toStrictEqual([]);
  });

  it("collectLegacyOddsLinksFromNav returns {} when the third div is missing the known odds links", () => {
    expect(
      extractOddsLinks(
        `
          <nav>
            <div></div>
            <div></div>
            <div>
              <a href="/KeibaWeb/Odds/Unknown">不明</a>
            </div>
          </nav>
        `,
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1",
      ),
    ).toStrictEqual({});
  });

  it("parseOddsByType wakuren swaps left/right when base frame is greater than target", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/wakuren": `
        <ul class="odd_horse_number_list">
          <table>
            <th class="odd_post8">8</th>
            <tr><td>2</td><td>11.5</td></tr>
          </table>
        </ul>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { wakuren: "/KeibaWeb/Odds/wakuren" });
    expect(result.wakuren?.[0]?.combination).toBe("2-8");
  });

  it("parseOddsByType wakuren returns [] when list is missing", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/wakuren": `<div>no list</div>`,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    await expect(fetchOdds(baseUrl, { wakuren: "/KeibaWeb/Odds/wakuren" })).resolves.toStrictEqual({
      wakuren: [],
    });
  });

  it("parseOddsByType wakuren skips inner rows when base/target frame is invalid", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/wakuren": `
        <ul class="odd_horse_number_list">
          <table>
            <th class="odd_post1">99</th>
            <tr><td>2</td><td>5.5</td></tr>
          </table>
          <table>
            <th class="odd_post1">1</th>
            <tr><td>99</td><td>5.5</td></tr>
            <tr><td>2</td><td>3.5</td></tr>
          </table>
        </ul>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { wakuren: "/KeibaWeb/Odds/wakuren" });
    expect(result.wakuren?.length).toBe(1);
  });

  it("parseOddsByType umaren swaps left/right when first horse is greater than second", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/umaren": `
        <table class="odd_ranking_table">
          <tr><td>9-1</td><td>15.5</td></tr>
        </table>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { umaren: "/KeibaWeb/Odds/umaren" });
    expect(result.umaren?.[0]?.combination).toBe("1-9");
  });

  it("parseOddsByType umaren skips rows with non-finite odds", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/umaren": `
        <table class="odd_ranking_table">
          <tr><td>1-2</td><td>not-a-number</td></tr>
          <tr><td>3-4</td><td>11.5</td></tr>
        </table>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { umaren: "/KeibaWeb/Odds/umaren" });
    expect(result.umaren?.length).toBe(1);
  });

  it("parseOddsByType 3renpuku skips text rows with invalid horses or non-positive odds", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/3renpuku": `
        <p>1-2-3 5.5</p>
        <p>99-2-3 8.5</p>
        <p>1-2-4 0</p>
        <p>1-2-5 not-a-number</p>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { "3renpuku": "/KeibaWeb/Odds/3renpuku" });
    expect(result["3renpuku"]?.length).toBe(1);
    expect(result["3renpuku"]?.[0]?.combination).toBe("1-2-3");
  });

  it("parseOddsByType 3rentan skips ranking rows with invalid horse numbers", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/3rentan": `
        <table class="odd_ranking_table">
          <tr><td>1-2-99</td><td>15.5</td></tr>
          <tr><td>1-2-3</td><td>5.5</td></tr>
        </table>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { "3rentan": "/KeibaWeb/Odds/3rentan" });
    expect(result["3rentan"]?.length).toBe(1);
    expect(result["3rentan"]?.[0]?.combination).toBe("1-2-3");
  });

  it("parseOddsByType 3rentan skips ranking rows with non-positive odds", async () => {
    mockFetchHtml({
      "https://www.keiba.go.jp/KeibaWeb/Odds/3rentan": `
        <table class="odd_ranking_table">
          <tr><td>1-2-3</td><td>0</td></tr>
          <tr><td>4-5-6</td><td>15.5</td></tr>
        </table>
      `,
    });
    const baseUrl = "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k=1";
    const result = await fetchOdds(baseUrl, { "3rentan": "/KeibaWeb/Odds/3rentan" });
    expect(result["3rentan"]?.length).toBe(1);
    expect(result["3rentan"]?.[0]?.combination).toBe("4-5-6");
  });

  it("parseRaceResults sorts non-numeric finishPositions to the back via the 999 fallback", () => {
    const html = `
      <tr bgcolor="#FFFFFF"><td>－</td><td></td><td>2</td><td>late</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
      <tr bgcolor="#FFFFFF"><td>1</td><td></td><td>5</td><td>winner</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td>1:23.4</td></tr>
    `;
    expect(parseRaceResults(html)).toStrictEqual([
      {
        finishPosition: "01",
        horseName: "winner",
        horseNumber: "5",
        time: "1:23.4",
      },
      {
        finishPosition: "－",
        horseName: "late",
        horseNumber: "2",
        time: null,
      },
    ]);
  });
});
