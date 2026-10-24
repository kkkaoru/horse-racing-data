import { expect, it } from "vitest";

import {
  buildShareAppName,
  buildShareAssetDestinations,
  buildShareDestinations,
  buildShareEntryTtlSeconds,
  parseShareAppName,
  parseSharedPagePath,
  parseShareExpiry,
} from "./shared-page-share";

const NOW_MS = Date.parse("2026-10-25T08:00:00+09:00");

it("parses the race page path from a viewer URL", () => {
  expect(parseSharedPagePath("https://pc-keiba-viewer.kkk4oru.com/races/2026/10/25/08/11")).toBe(
    "/races/2026/10/25/08/11",
  );
});

it("rejects URLs on another host", () => {
  expect(() => parseSharedPagePath("https://example.com/races/2026/10/25/08/11")).toThrow(
    "Not a pc-keiba-viewer.kkk4oru.com race page URL: https://example.com/races/2026/10/25/08/11",
  );
});

it("rejects viewer URLs that are not a race page", () => {
  expect(() =>
    parseSharedPagePath("https://pc-keiba-viewer.kkk4oru.com/races/2026/10/25/08/11/paddock"),
  ).toThrow(
    "Not a pc-keiba-viewer.kkk4oru.com race page URL: https://pc-keiba-viewer.kkk4oru.com/races/2026/10/25/08/11/paddock",
  );
});

it("accepts a future expiry with an offset", () => {
  expect(parseShareExpiry("2026-10-25T23:59:59+09:00", NOW_MS)).toBe("2026-10-25T23:59:59+09:00");
});

it("rejects an expiry without an offset", () => {
  expect(() => parseShareExpiry("2026-10-25T23:59:59", NOW_MS)).toThrow(
    "Expiry must be an ISO date-time with an offset: 2026-10-25T23:59:59",
  );
});

it("rejects an expiry that is not a date", () => {
  expect(() => parseShareExpiry("tomorrowZ", NOW_MS)).toThrow(
    "Expiry must be an ISO date-time with an offset: tomorrowZ",
  );
});

it("rejects an expiry in the past", () => {
  expect(() => parseShareExpiry("2026-10-25T07:00:00+09:00", NOW_MS)).toThrow(
    "Expiry is in the past: 2026-10-25T07:00:00+09:00",
  );
});

it("lists the page, its API and its clef bets as Access destinations", () => {
  expect(buildShareDestinations("/races/2026/10/25/08/11")).toStrictEqual([
    "pc-keiba-viewer.kkk4oru.com/races/2026/10/25/08/11",
    "pc-keiba-viewer.kkk4oru.com/api/races/2026/10/25/08/11/*",
    "pc-keiba-viewer.kkk4oru.com/api/clef-rerank/bets/20261025/08/11",
  ]);
});

it("rejects destinations for a non-race path", () => {
  expect(() => buildShareDestinations("/races")).toThrow("Not a race page path: /races");
});

it("lists the shared static assets as Access destinations", () => {
  expect(buildShareAssetDestinations()).toStrictEqual([
    "pc-keiba-viewer.kkk4oru.com/_next/static/*",
    "pc-keiba-viewer.kkk4oru.com/icon.svg",
    "pc-keiba-viewer.kkk4oru.com/favicon.ico",
  ]);
});

it("round-trips the share app name", () => {
  const name = buildShareAppName({
    expiresAt: "2026-10-25T23:59:59+09:00",
    pagePath: "/races/2026/10/25/08/11",
  });
  expect(name).toBe(
    "PC Keiba Viewer share /races/2026/10/25/08/11 until 2026-10-25T23:59:59+09:00",
  );
  expect(parseShareAppName(name)).toStrictEqual({
    expiresAt: "2026-10-25T23:59:59+09:00",
    pagePath: "/races/2026/10/25/08/11",
  });
});

it("does not parse unrelated Access app names", () => {
  expect(parseShareAppName("PC Keiba Viewer")).toBeNull();
});

it("keeps the share entry for 30 days past the expiry", () => {
  expect(buildShareEntryTtlSeconds("2026-10-25T09:00:00+09:00", NOW_MS)).toBe(2_595_600);
});
