// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { fetchRacePage, fetchTodayRaceListUrls, TOP_PAGE_RETRYABLE_STATUSES } from "./keiba-go";

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
  expect(result).toStrictEqual([]);
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("fetchTodayRaceListUrls bubbles up 404 after exhausting retry attempts", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("nope", { status: 404 }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(fetchTodayRaceListUrls("20260609")).rejects.toThrowError(
    "Failed to fetch https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop: 404",
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
