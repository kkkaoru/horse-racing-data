// Run with bun: `bun run --filter pc-keiba-viewer test`
import { expect, it } from "vitest";

import {
  STALE_DETAIL_SECTION_MAX_AGE_MS,
  getJstMidnightMsForToday,
  isStaleDetailSectionEnvelope,
  parseStaleDetailSectionEnvelope,
  serializeStaleDetailSectionEnvelope,
} from "./race-detail-section-stale";

it("STALE_DETAIL_SECTION_MAX_AGE_MS equals 4 hours in milliseconds", () => {
  expect(STALE_DETAIL_SECTION_MAX_AGE_MS).toBe(14400000);
});

it("isStaleDetailSectionEnvelope accepts a well-formed envelope", () => {
  expect(isStaleDetailSectionEnvelope({ payload: "{}", writtenAt: 1 })).toBe(true);
});

it("isStaleDetailSectionEnvelope rejects null", () => {
  expect(isStaleDetailSectionEnvelope(null)).toBe(false);
});

it("isStaleDetailSectionEnvelope rejects a non-object primitive", () => {
  expect(isStaleDetailSectionEnvelope("string")).toBe(false);
});

it("isStaleDetailSectionEnvelope rejects an envelope missing writtenAt", () => {
  expect(isStaleDetailSectionEnvelope({ payload: "body" })).toBe(false);
});

it("isStaleDetailSectionEnvelope rejects an envelope where writtenAt is NaN", () => {
  expect(isStaleDetailSectionEnvelope({ payload: "body", writtenAt: Number.NaN })).toBe(false);
});

it("isStaleDetailSectionEnvelope rejects an envelope where payload is not a string", () => {
  expect(isStaleDetailSectionEnvelope({ payload: 42, writtenAt: 1 })).toBe(false);
});

it("parseStaleDetailSectionEnvelope returns the envelope for valid JSON envelope", () => {
  expect(parseStaleDetailSectionEnvelope('{"payload":"body","writtenAt":1}')).toStrictEqual({
    payload: "body",
    writtenAt: 1,
  });
});

it("parseStaleDetailSectionEnvelope returns null for raw legacy payload (string)", () => {
  expect(parseStaleDetailSectionEnvelope('"just-a-string"')).toBeNull();
});

it("parseStaleDetailSectionEnvelope returns null for malformed JSON", () => {
  expect(parseStaleDetailSectionEnvelope("{not-json")).toBeNull();
});

it("parseStaleDetailSectionEnvelope returns null for envelope-shaped JSON missing writtenAt", () => {
  expect(parseStaleDetailSectionEnvelope('{"payload":"body"}')).toBeNull();
});

it("serializeStaleDetailSectionEnvelope round-trips through parseStaleDetailSectionEnvelope", () => {
  expect(
    parseStaleDetailSectionEnvelope(serializeStaleDetailSectionEnvelope("body", 42)),
  ).toStrictEqual({
    payload: "body",
    writtenAt: 42,
  });
});

it("serializeStaleDetailSectionEnvelope produces the expected JSON shape", () => {
  expect(serializeStaleDetailSectionEnvelope("body", 42)).toBe('{"payload":"body","writtenAt":42}');
});

it("getJstMidnightMsForToday returns the most recent JST midnight at JST 00:00:00", () => {
  // 2026-06-28T00:00:00+09:00 == 2026-06-27T15:00:00Z
  expect(getJstMidnightMsForToday(Date.parse("2026-06-28T00:00:00+09:00"))).toBe(
    Date.parse("2026-06-28T00:00:00+09:00"),
  );
});

it("getJstMidnightMsForToday returns earlier-today JST midnight when called mid-day JST", () => {
  // mid-day JST returns the same-day JST midnight, not yesterday's
  expect(getJstMidnightMsForToday(Date.parse("2026-06-28T12:34:56+09:00"))).toBe(
    Date.parse("2026-06-28T00:00:00+09:00"),
  );
});

it("getJstMidnightMsForToday rolls over to next-day midnight exactly at JST midnight", () => {
  // 2026-06-28T23:59:59+09:00 should return 2026-06-28T00:00:00+09:00
  expect(getJstMidnightMsForToday(Date.parse("2026-06-28T23:59:59+09:00"))).toBe(
    Date.parse("2026-06-28T00:00:00+09:00"),
  );
});
