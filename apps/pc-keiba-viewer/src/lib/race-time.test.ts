import { expect, it } from "vitest";

import {
  formatEncodedRaceTime,
  formatRaceTimeTenths,
  parseEncodedRaceTimeTenths,
} from "./race-time";

it("decodes PC-KEIBA MSSD race times into tenths of seconds", () => {
  expect(parseEncodedRaceTimeTenths("1008")).toBe(608);
  expect(parseEncodedRaceTimeTenths("0595")).toBe(595);
  expect(parseEncodedRaceTimeTenths("3188")).toBe(1988);
});

it("rejects missing, sentinel, malformed, and impossible encoded race times", () => {
  expect(parseEncodedRaceTimeTenths(null)).toBeNull();
  expect(parseEncodedRaceTimeTenths("0000")).toBeNull();
  expect(parseEncodedRaceTimeTenths("9999")).toBeNull();
  expect(parseEncodedRaceTimeTenths("12x3")).toBeNull();
  expect(parseEncodedRaceTimeTenths("12345")).toBeNull();
});

it("formats encoded PC-KEIBA race times", () => {
  expect(formatEncodedRaceTime("1008")).toBe("1:00.8");
  expect(formatEncodedRaceTime("0595")).toBe("59.5");
  expect(formatEncodedRaceTime("3188")).toBe("3:18.8");
  expect(formatEncodedRaceTime("9999")).toBe("-");
});

it("formats calculated tenths-of-seconds values", () => {
  expect(formatRaceTimeTenths(608)).toBe("1:00.8");
  expect(formatRaceTimeTenths(595)).toBe("59.5");
  expect(formatRaceTimeTenths(608.4)).toBe("1:00.8");
  expect(formatRaceTimeTenths(null)).toBe("-");
  expect(formatRaceTimeTenths(Number.NaN)).toBe("-");
});
