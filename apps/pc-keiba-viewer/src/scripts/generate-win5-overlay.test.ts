import { expect, test } from "vitest";

import {
  buildOverlayModelVersion,
  formatTodayYyyymmddJst,
  offsetYyyymmdd,
  splitDate,
} from "./generate-win5-overlay";

test("buildOverlayModelVersion appends rs-overlay suffix", () => {
  expect(buildOverlayModelVersion("20260524")).toBe("win5-xgb-v7-lineage-v1-rs-overlay-20260524");
});

test("splitDate decomposes YYYYMMDD into year + month-day pieces", () => {
  expect(splitDate("20260524")).toStrictEqual({
    year: "2026",
    monthDay: "0524",
    yyyymmdd: "20260524",
  });
});

test("offsetYyyymmdd subtracts a day across month boundary", () => {
  expect(offsetYyyymmdd("20260601", -1)).toBe("20260531");
});

test("offsetYyyymmdd subtracts a 14-day lookback window", () => {
  expect(offsetYyyymmdd("20260524", -14)).toBe("20260510");
});

test("offsetYyyymmdd adds days across year boundary", () => {
  expect(offsetYyyymmdd("20251231", 1)).toBe("20260101");
});

test("formatTodayYyyymmddJst formats a known UTC instant into JST date", () => {
  // 2026-05-23T18:00:00Z = 2026-05-24T03:00:00+09:00 → JST date is 20260524
  expect(formatTodayYyyymmddJst(new Date(Date.UTC(2026, 4, 23, 18, 0, 0)))).toBe("20260524");
});

test("formatTodayYyyymmddJst handles JST midnight rollover", () => {
  // 2026-05-23T14:59:59Z = 2026-05-23T23:59:59+09:00 → JST date is 20260523
  expect(formatTodayYyyymmddJst(new Date(Date.UTC(2026, 4, 23, 14, 59, 59)))).toBe("20260523");
  // 2026-05-23T15:00:00Z = 2026-05-24T00:00:00+09:00 → JST date is 20260524
  expect(formatTodayYyyymmddJst(new Date(Date.UTC(2026, 4, 23, 15, 0, 0)))).toBe("20260524");
});
