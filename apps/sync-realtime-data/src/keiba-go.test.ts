// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  fetchRaceLinksFromRaceList,
  fetchRacePage,
  fetchTodayRaceListUrls,
  isRaceResultDisabledOnRaceList,
  parseRaceResultTanshoOdds,
  TOP_PAGE_RETRYABLE_STATUSES,
} from "./keiba-go";

const TEST_RACE_LIST_URL =
  "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F07%2F04&k_babaCode=36";

const SJIS_TOKYO_BYTES = [0x93, 0x8c, 0x8b, 0x9e];
const TEST_URL = "https://www.keiba.go.jp/test";

const asciiBytes = (text: string): Uint8Array => new TextEncoder().encode(text);

const concatBytes = (left: Uint8Array, right: Uint8Array): Uint8Array => {
  const merged = new Uint8Array(left.length + right.length);
  merged.set(left, 0);
  merged.set(right, left.length);
  return merged;
};

interface SjisHtmlArgs {
  metaCharsetTag: string;
}

const toArrayBuffer = (bytes: Uint8Array): ArrayBuffer => {
  const fresh = new ArrayBuffer(bytes.length);
  new Uint8Array(fresh).set(bytes);
  return fresh;
};

const buildSjisBuffer = (args: SjisHtmlArgs): ArrayBuffer => {
  const headHtml = `<html><head>${args.metaCharsetTag}</head><body>`;
  const tail = "</body></html>";
  const merged = concatBytes(
    concatBytes(asciiBytes(headHtml), Uint8Array.from(SJIS_TOKYO_BYTES)),
    asciiBytes(tail),
  );
  return toArrayBuffer(merged);
};

const buildSjisBufferNoMeta = (): ArrayBuffer => {
  const headHtml = "<html><head></head><body>";
  const tail = "</body></html>";
  const merged = concatBytes(
    concatBytes(asciiBytes(headHtml), Uint8Array.from(SJIS_TOKYO_BYTES)),
    asciiBytes(tail),
  );
  return toArrayBuffer(merged);
};

beforeEach(() => {
  vi.unstubAllGlobals();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it("fetchRacePage decodes SHIFT_JIS body when meta charset is Shift_JIS", async () => {
  const buffer = buildSjisBuffer({ metaCharsetTag: '<meta charset="Shift_JIS">' });
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () => new Response(buffer, { headers: { "Content-Type": "text/html" }, status: 200 }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe('<html><head><meta charset="Shift_JIS"></head><body>東京</body></html>');
});

it("fetchRacePage uses Content-Type charset when provided", async () => {
  const buffer = buildSjisBufferNoMeta();
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(buffer, {
          headers: { "Content-Type": "text/html; charset=Shift_JIS" },
          status: 200,
        }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe("<html><head></head><body>東京</body></html>");
});

it("fetchRacePage defaults to UTF-8 when no charset hint", async () => {
  const utf8Buffer = toArrayBuffer(
    new TextEncoder().encode("<html><body>example utf8</body></html>"),
  );
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(utf8Buffer, {
          headers: { "Content-Type": "text/html" },
          status: 200,
        }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe("<html><body>example utf8</body></html>");
});

it("fetchRacePage normalizes shift_jis alias to shift-jis", async () => {
  const buffer = buildSjisBuffer({ metaCharsetTag: '<meta charset="shift_jis">' });
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () => new Response(buffer, { headers: { "Content-Type": "text/html" }, status: 200 }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe('<html><head><meta charset="shift_jis"></head><body>東京</body></html>');
});

it("fetchRacePage normalizes sjis alias to shift-jis", async () => {
  const buffer = buildSjisBuffer({ metaCharsetTag: '<meta charset="sjis">' });
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () => new Response(buffer, { headers: { "Content-Type": "text/html" }, status: 200 }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe('<html><head><meta charset="sjis"></head><body>東京</body></html>');
});

it("fetchRacePage normalizes x-sjis alias to shift-jis", async () => {
  const buffer = buildSjisBuffer({ metaCharsetTag: '<meta charset="x-sjis">' });
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () => new Response(buffer, { headers: { "Content-Type": "text/html" }, status: 200 }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe('<html><head><meta charset="x-sjis"></head><body>東京</body></html>');
});

it("fetchRacePage normalizes shift-jis alias to shift-jis", async () => {
  const buffer = buildSjisBuffer({ metaCharsetTag: '<meta charset="shift-jis">' });
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () => new Response(buffer, { headers: { "Content-Type": "text/html" }, status: 200 }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe('<html><head><meta charset="shift-jis"></head><body>東京</body></html>');
});

it("fetchRacePage throws when response is not ok", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("nope", { status: 500 })),
  );
  await expect(fetchRacePage(TEST_URL)).rejects.toThrowError(
    "Failed to fetch https://www.keiba.go.jp/test: 500",
  );
});

it("fetchRacePage prefers Content-Type charset over meta tag", async () => {
  const buffer = buildSjisBuffer({ metaCharsetTag: '<meta charset="utf-8">' });
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(buffer, {
          headers: { "Content-Type": "text/html; charset=Shift_JIS" },
          status: 200,
        }),
    ),
  );
  const html = await fetchRacePage(TEST_URL);
  expect(html).toBe('<html><head><meta charset="utf-8"></head><body>東京</body></html>');
});

it("TOP_PAGE_RETRYABLE_STATUSES enumerates 404 and transient 4xx/5xx codes for the NAR top page", () => {
  const sortNumeric = (left: number, right: number): number => left - right;
  expect(Array.from(TOP_PAGE_RETRYABLE_STATUSES).toSorted(sortNumeric)).toStrictEqual([
    404, 408, 425, 429, 502, 503, 504,
  ]);
});

it("fetchTodayRaceListUrls retries on transient 404 and succeeds on second attempt", async () => {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("nope", { status: 404 }))
    .mockResolvedValueOnce(
      new Response("<html><body></body></html>", {
        headers: { "Content-Type": "text/html; charset=utf-8" },
        status: 200,
      }),
    );
  vi.stubGlobal("fetch", fetchMock);
  const result = await fetchTodayRaceListUrls("20260609");
  expect(result.length).toBeGreaterThan(0);
  expect(result[0]?.url).toContain("RaceList?k_raceDate=2026%2F06%2F09");
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("fetchTodayRaceListUrls falls back to deterministic RaceList URLs after top-page 404s", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("nope", { status: 404 }));
  vi.stubGlobal("fetch", fetchMock);
  const result = await fetchTodayRaceListUrls("20260609");
  expect(result.length).toBeGreaterThan(0);
  expect(result.every((item) => item.url.includes("RaceList?k_raceDate=2026%2F06%2F09"))).toBe(
    true,
  );
  expect(fetchMock).toHaveBeenCalledTimes(3);
});

it("fetchRacePage does not retry on 404 (sub-page 404 bubbles immediately)", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("nope", { status: 404 }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(fetchRacePage(TEST_URL)).rejects.toThrowError(
    "Failed to fetch https://www.keiba.go.jp/test: 404",
  );
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("fetchRaceLinksFromRaceList returns an empty array when the RaceList page 404s (no race today for this venue)", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("nope", { status: 404 }));
  vi.stubGlobal("fetch", fetchMock);
  const result = await fetchRaceLinksFromRaceList(TEST_RACE_LIST_URL);
  expect(result).toStrictEqual([]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("fetchRaceLinksFromRaceList propagates a 500 RaceList response instead of treating it as no-race", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("nope", { status: 500 }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(fetchRaceLinksFromRaceList(TEST_RACE_LIST_URL)).rejects.toThrowError(
    "Failed to fetch https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F07%2F04&k_babaCode=36: 500",
  );
});

it("fetchRaceLinksFromRaceList propagates a raw network failure unmodified", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error("network unreachable"));
  vi.stubGlobal("fetch", fetchMock);
  await expect(fetchRaceLinksFromRaceList(TEST_RACE_LIST_URL)).rejects.toThrowError(
    "network unreachable",
  );
});

it("fetchRaceLinksFromRaceList parses race links from a successful RaceList response", async () => {
  const html =
    '<a href="/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F07%2F04&amp;k_raceNo=1&amp;k_babaCode=36">1R</a>';
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
    new Response(html, {
      headers: { "Content-Type": "text/html; charset=utf-8" },
      status: 200,
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
  const result = await fetchRaceLinksFromRaceList(TEST_RACE_LIST_URL);
  expect(result).toStrictEqual([
    {
      babaCode: "36",
      raceNumber: "01",
      url: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F07%2F04&k_raceNo=1&k_babaCode=36",
    },
  ]);
});

const buildRaceListRow = (args: { raceNumber: number; resultsDisabled: boolean }): string =>
  `<tr class="data">
    <td>
      ${args.raceNumber}R
    </td>
    <td>
      16:58
    </td>
    <td><span class="timechange"></span>
    </td>
    <td>
    </td>
    <td>
      <a href=/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F07%2F24&amp;k_raceNo=${args.raceNumber}&amp;k_babaCode=20>３歳一 二 三</a>
    </td>
    <td>
      右1400m
    </td>
    <td>
      曇
    </td>
    <td>
      良
    </td>
    <td>
      14
    </td>
    <td>
      ${
        args.resultsDisabled
          ? `<a class="chartBtn disable">オッズ</a>
      <a class="chartBtn disable">映像</a>
      <a class="chartBtn disable">成績</a>`
          : `<a class="chartBtn" href=/KeibaWeb/TodayRaceInfo/OddsTanFuku?k_raceDate=2026%2F07%2F24&amp;k_raceNo=${args.raceNumber}&amp;k_babaCode=20>オッズ</a>
      <a target="_blank" href=http://keiba-lv-st.jp/movie/player?date=20260724&amp;race=${args.raceNumber}&amp;track=ooi class="chartBtn">映像</a>
      <a class="chartBtn" href=/KeibaWeb/TodayRaceInfo/RaceMarkTable?k_raceDate=2026%2F07%2F24&amp;k_raceNo=${args.raceNumber}&amp;k_babaCode=20>成績</a>`
      }
    </td>
  </tr>`;

it("isRaceResultDisabledOnRaceList returns false for a race whose 成績 link is enabled", () => {
  const html = buildRaceListRow({ raceNumber: 4, resultsDisabled: false });
  expect(isRaceResultDisabledOnRaceList(html, "04")).toBe(false);
});

it("isRaceResultDisabledOnRaceList returns true for a race whose 成績 link is disabled", () => {
  const html = buildRaceListRow({ raceNumber: 5, resultsDisabled: true });
  expect(isRaceResultDisabledOnRaceList(html, "05")).toBe(true);
});

it("isRaceResultDisabledOnRaceList picks the matching race out of several rows, not the first", () => {
  const html = [
    buildRaceListRow({ raceNumber: 4, resultsDisabled: false }),
    buildRaceListRow({ raceNumber: 5, resultsDisabled: true }),
    buildRaceListRow({ raceNumber: 6, resultsDisabled: true }),
    buildRaceListRow({ raceNumber: 7, resultsDisabled: false }),
  ].join("\n");
  expect(isRaceResultDisabledOnRaceList(html, "04")).toBe(false);
  expect(isRaceResultDisabledOnRaceList(html, "06")).toBe(true);
  expect(isRaceResultDisabledOnRaceList(html, "07")).toBe(false);
});

it("isRaceResultDisabledOnRaceList accepts an un-padded race number the same as a zero-padded one", () => {
  const html = buildRaceListRow({ raceNumber: 5, resultsDisabled: true });
  expect(isRaceResultDisabledOnRaceList(html, "5")).toBe(true);
});

it("isRaceResultDisabledOnRaceList returns null when the race's row is not present on the page", () => {
  const html = buildRaceListRow({ raceNumber: 4, resultsDisabled: false });
  expect(isRaceResultDisabledOnRaceList(html, "12")).toBeNull();
});

it("isRaceResultDisabledOnRaceList returns null when the row has no 成績 link at all (layout drift)", () => {
  const html = `<tr class="data"><td>5R</td></tr>`;
  expect(isRaceResultDisabledOnRaceList(html, "05")).toBeNull();
});

it("parseRaceResultTanshoOdds extracts horseNumber + popularity + tanshoOdds from current layout rows", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">テールヴェール</td><td class="e">大井</td>' +
    '<td class="o">6</td><td class="p">24.9</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([
    { horseNumber: "5", popularity: 6, tanshoOdds: 24.9 },
  ]);
});

it("parseRaceResultTanshoOdds extracts popularity from a cell with extra class names", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">2</td><td class="b">6</td><td class="c">8</td>' +
    '<td class="d horseName">ヨロシクユウキ</td>' +
    '<td class="o popularNum course_03">3</td><td class="p femal">6.4</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([
    { horseNumber: "8", popularity: 3, tanshoOdds: 6.4 },
  ]);
});

it("parseRaceResultTanshoOdds extracts multiple rows from the same result table", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">6</td><td class="p">24.9</td>' +
    "</tr>" +
    '<tr class="tBorder">' +
    '<td class="a">2</td><td class="b">6</td><td class="c">8</td>' +
    '<td class="d horseName">B</td>' +
    '<td class="o">3</td><td class="p">6.4</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([
    { horseNumber: "5", popularity: 6, tanshoOdds: 24.9 },
    { horseNumber: "8", popularity: 3, tanshoOdds: 6.4 },
  ]);
});

it("parseRaceResultTanshoOdds returns empty array when HTML has no result rows", () => {
  expect(parseRaceResultTanshoOdds("<html><body><p>nothing</p></body></html>")).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the horseNumber cell is non-numeric", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">abc</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">6</td><td class="p">24.9</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the horseNumber cell is out of range", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">99</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">6</td><td class="p">24.9</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the popularity cell is missing", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="p">24.9</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the popularity cell is empty", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o"></td><td class="p">24.9</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the popularity cell is non-numeric", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">abc</td><td class="p">24.9</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where popularity is zero", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">0</td><td class="p">24.9</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the tansho odds cell is missing", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">6</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the tansho odds cell is non-numeric", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">6</td><td class="p">abc</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops rows where the tansho odds value is zero", () => {
  const html =
    '<table><tr class="tBorder">' +
    '<td class="a">1</td><td class="b">3</td><td class="c">5</td>' +
    '<td class="d horseName">A</td>' +
    '<td class="o">6</td><td class="p">0</td>' +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});

it("parseRaceResultTanshoOdds drops legacy bgcolor result rows even when class-named cells are absent", () => {
  const html =
    '<table><tr bgcolor="#FFFFFF">' +
    "<td>1</td><td>3</td><td>5</td><td>テールヴェール</td>" +
    "<td>大井</td><td>牡 3</td><td>54.0</td><td>藤本現</td>" +
    "<td>鷹見浩</td><td>440(2)</td><td></td><td>1:30.0</td>" +
    "<td></td><td>39.4</td><td>4-4-4</td><td>6</td><td>24.9</td>" +
    "</tr></table>";
  expect(parseRaceResultTanshoOdds(html)).toStrictEqual([]);
});
