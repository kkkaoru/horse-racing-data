// Run with: bunx vitest run src/lib/win5/jra-parse.test.ts
import { afterEach, expect, test, vi } from "vitest";

import {
  decodeJraHtml,
  fetchWin5SchedulesFromJra,
  inferScheduleYearFromHtml,
  JRA_WIN5_RACELIST_URL,
  parseWin5SchedulesFromJraHtml,
} from "./jra-parse";

const FIXED_FETCHED_AT = "2026-05-25T01:23:45.000Z";

const buildLegHtml = (venue: string, raceNumber: number, hour: number, minute: number): string =>
  `<td><span class="race">${venue}${raceNumber}R</span><span class="time">${hour}時${String(minute).padStart(2, "0")}分 発走</span></td>`;

const buildRowHtml = (params: { monthDay: string; deadline?: string; legs: string[] }): string => {
  const deadlineCell = params.deadline
    ? `<td><strong>${params.deadline}</strong></td>`
    : "<td></td>";
  return `<tr><td>${params.monthDay}</td>${deadlineCell}${params.legs.join("")}</tr>`;
};

const buildTableHtml = (rowsHtml: string): string =>
  `<header>2026年5月25日（日）までのWIN5対象レース</header><table class="win5list"><tbody>${rowsHtml}</tbody></table>`;

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

test("decodeJraHtml decodes ASCII-compatible bytes from an ArrayBuffer", () => {
  const asciiBytes = new TextEncoder().encode("ABC123");
  expect(decodeJraHtml(asciiBytes.buffer)).toStrictEqual("ABC123");
});

test("decodeJraHtml accepts a Uint8Array as input", () => {
  expect(decodeJraHtml(new TextEncoder().encode("hello-world"))).toStrictEqual("hello-world");
});

test("inferScheduleYearFromHtml returns the matched header year", () => {
  expect(
    inferScheduleYearFromHtml("2026年5月25日（日）までのWIN5対象レース", "1999"),
  ).toStrictEqual("2026");
});

test("inferScheduleYearFromHtml falls back to provided year when header is missing", () => {
  expect(inferScheduleYearFromHtml("<html>no header</html>", "2027")).toStrictEqual("2027");
});

test("parseWin5SchedulesFromJraHtml returns an empty array when the win5list table is absent", () => {
  expect(
    parseWin5SchedulesFromJraHtml("<html>no table</html>", { fetchedAt: FIXED_FETCHED_AT }),
  ).toStrictEqual([]);
});

test("parseWin5SchedulesFromJraHtml skips rows without a date marker", () => {
  const html = buildTableHtml("<tr><td>no date here</td><td><strong>15時00分</strong></td></tr>");
  expect(parseWin5SchedulesFromJraHtml(html, { fetchedAt: FIXED_FETCHED_AT })).toStrictEqual([]);
});

test("parseWin5SchedulesFromJraHtml skips rows that do not contain five race spans", () => {
  const html = buildTableHtml(
    buildRowHtml({
      monthDay: "5月25日",
      deadline: "15時00分",
      legs: [buildLegHtml("東京", 9, 14, 0), buildLegHtml("東京", 10, 14, 35)],
    }),
  );
  expect(parseWin5SchedulesFromJraHtml(html, { fetchedAt: FIXED_FETCHED_AT })).toStrictEqual([]);
});

test("parseWin5SchedulesFromJraHtml skips rows whose race labels reference an unknown venue", () => {
  const html = buildTableHtml(
    buildRowHtml({
      monthDay: "5月25日",
      deadline: "15時00分",
      legs: [
        buildLegHtml("謎", 9, 14, 0),
        buildLegHtml("東京", 10, 14, 35),
        buildLegHtml("東京", 11, 15, 5),
        buildLegHtml("中山", 10, 14, 25),
        buildLegHtml("中山", 11, 15, 0),
      ],
    }),
  );
  expect(parseWin5SchedulesFromJraHtml(html, { fetchedAt: FIXED_FETCHED_AT })).toStrictEqual([]);
});

test("parseWin5SchedulesFromJraHtml parses a fully-formed row into a schedule", () => {
  const html = buildTableHtml(
    buildRowHtml({
      monthDay: "5月25日",
      deadline: "15時00分",
      legs: [
        buildLegHtml("東京", 9, 14, 0),
        buildLegHtml("東京", 10, 14, 35),
        buildLegHtml("東京", 11, 15, 5),
        buildLegHtml("中山", 10, 14, 25),
        buildLegHtml("中山", 11, 15, 0),
      ],
    }),
  );
  const result = parseWin5SchedulesFromJraHtml(html, { fetchedAt: FIXED_FETCHED_AT });
  expect(result).toStrictEqual([
    {
      fetchedAt: FIXED_FETCHED_AT,
      kaisaiNen: "2026",
      kaisaiTsukihi: "0525",
      saleDeadline: "15時00分",
      source: "jra_web",
      legs: [
        {
          legIndex: 1,
          kaisaiKai: "00",
          kaisaiNichime: "00",
          startTime: "14:00",
          keibajoCode: "05",
          keibajoName: "東京",
          raceBango: "9",
          raceLabel: "東京9R",
        },
        {
          legIndex: 2,
          kaisaiKai: "00",
          kaisaiNichime: "00",
          startTime: "14:35",
          keibajoCode: "05",
          keibajoName: "東京",
          raceBango: "10",
          raceLabel: "東京10R",
        },
        {
          legIndex: 3,
          kaisaiKai: "00",
          kaisaiNichime: "00",
          startTime: "15:05",
          keibajoCode: "05",
          keibajoName: "東京",
          raceBango: "11",
          raceLabel: "東京11R",
        },
        {
          legIndex: 4,
          kaisaiKai: "00",
          kaisaiNichime: "00",
          startTime: "14:25",
          keibajoCode: "06",
          keibajoName: "中山",
          raceBango: "10",
          raceLabel: "中山10R",
        },
        {
          legIndex: 5,
          kaisaiKai: "00",
          kaisaiNichime: "00",
          startTime: "15:00",
          keibajoCode: "06",
          keibajoName: "中山",
          raceBango: "11",
          raceLabel: "中山11R",
        },
      ],
    },
  ]);
});

test("parseWin5SchedulesFromJraHtml leaves saleDeadline null when no <strong> deadline is present", () => {
  const html = buildTableHtml(
    buildRowHtml({
      monthDay: "5月25日",
      legs: [
        buildLegHtml("東京", 9, 14, 0),
        buildLegHtml("東京", 10, 14, 35),
        buildLegHtml("東京", 11, 15, 5),
        buildLegHtml("中山", 10, 14, 25),
        buildLegHtml("中山", 11, 15, 0),
      ],
    }),
  );
  const result = parseWin5SchedulesFromJraHtml(html, { fetchedAt: FIXED_FETCHED_AT });
  expect(result[0]?.saleDeadline).toBeNull();
});

test("parseWin5SchedulesFromJraHtml falls back to the provided fallbackYear when header is missing", () => {
  const html = `<table class="win5list"><tbody>${buildRowHtml({
    monthDay: "5月25日",
    deadline: "15時00分",
    legs: [
      buildLegHtml("東京", 9, 14, 0),
      buildLegHtml("東京", 10, 14, 35),
      buildLegHtml("東京", 11, 15, 5),
      buildLegHtml("中山", 10, 14, 25),
      buildLegHtml("中山", 11, 15, 0),
    ],
  })}</tbody></table>`;
  const result = parseWin5SchedulesFromJraHtml(html, {
    fallbackYear: "2099",
    fetchedAt: FIXED_FETCHED_AT,
  });
  expect(result[0]?.kaisaiNen).toStrictEqual("2099");
});

test("parseWin5SchedulesFromJraHtml falls back to the current calendar year when no options are given", () => {
  const html = `<table class="win5list"><tbody>${buildRowHtml({
    monthDay: "5月25日",
    deadline: "15時00分",
    legs: [
      buildLegHtml("東京", 9, 14, 0),
      buildLegHtml("東京", 10, 14, 35),
      buildLegHtml("東京", 11, 15, 5),
      buildLegHtml("中山", 10, 14, 25),
      buildLegHtml("中山", 11, 15, 0),
    ],
  })}</tbody></table>`;
  const expectedYear = String(new Date().getFullYear());
  const result = parseWin5SchedulesFromJraHtml(html);
  expect(result[0]?.kaisaiNen).toStrictEqual(expectedYear);
});

const raceOnlyLegHtml = (venue: string, raceNumber: number): string =>
  `<td><span class="race">${venue}${raceNumber}R</span></td>`;

test("parseWin5SchedulesFromJraHtml leaves startTime undefined when no time span is recognized for any leg", () => {
  const html = buildTableHtml(
    `<tr><td>5月25日</td><td><strong>15時00分</strong></td>${raceOnlyLegHtml(
      "東京",
      9,
    )}${raceOnlyLegHtml("東京", 10)}${raceOnlyLegHtml("東京", 11)}${raceOnlyLegHtml(
      "中山",
      10,
    )}${raceOnlyLegHtml("中山", 11)}</tr>`,
  );
  const result = parseWin5SchedulesFromJraHtml(html, { fetchedAt: FIXED_FETCHED_AT });
  expect(result[0]?.legs[0]?.startTime).toBeUndefined();
});

type FetchHeaderRecord = Record<string, string>;

const extractStringHeaders = (init: RequestInit | undefined): FetchHeaderRecord => {
  const headers = init?.headers;
  if (!headers || Array.isArray(headers) || headers instanceof Headers) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(headers).filter(
      (entry): entry is [string, string] => typeof entry[1] === "string",
    ),
  );
};

test("fetchWin5SchedulesFromJra fetches the JRA URL with required headers", async () => {
  const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>(
    async (url, init) => {
      expect(url).toStrictEqual(JRA_WIN5_RACELIST_URL);
      const headers = extractStringHeaders(init);
      expect(headers.Accept).toStrictEqual("text/html");
      expect(headers["User-Agent"]).toStrictEqual("pc-keiba-viewer/1.0");
      return new Response(new TextEncoder().encode("<html>no table here</html>"), { status: 200 });
    },
  );
  vi.stubGlobal("fetch", fetchMock);
  const result = await fetchWin5SchedulesFromJra({ fetchedAt: FIXED_FETCHED_AT });
  expect(result).toStrictEqual([]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

test("parseWin5SchedulesFromJraHtml preserves a raceBango of all zeros", () => {
  const html = buildTableHtml(
    buildRowHtml({
      monthDay: "5月25日",
      deadline: "15時00分",
      legs: [
        `<td><span class="race">東京00R</span><span class="time">14時00分 発走</span></td>`,
        buildLegHtml("東京", 10, 14, 35),
        buildLegHtml("東京", 11, 15, 5),
        buildLegHtml("中山", 10, 14, 25),
        buildLegHtml("中山", 11, 15, 0),
      ],
    }),
  );
  const result = parseWin5SchedulesFromJraHtml(html, { fetchedAt: FIXED_FETCHED_AT });
  expect(result[0]?.legs[0]?.raceBango).toStrictEqual("00");
});

test("fetchWin5SchedulesFromJra throws when the JRA endpoint returns a non-ok status", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("nope", { status: 503 })),
  );
  await expect(fetchWin5SchedulesFromJra()).rejects.toThrow(
    "Failed to fetch JRA WIN5 racelist: HTTP 503",
  );
});
