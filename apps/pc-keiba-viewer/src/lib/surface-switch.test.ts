// Run with bun (exercised via `bunx vitest run`).
import { expect, test } from "vitest";

import { classifySurfaceSwitch, getSurfaceSwitchClassName } from "./surface-switch";

test("returns null for null raceTrackCode", () => {
  expect(classifySurfaceSwitch(null, ["23", "24"])).toStrictEqual(null);
});

test("returns null for undefined raceTrackCode", () => {
  expect(classifySurfaceSwitch(undefined, ["23", "24"])).toStrictEqual(null);
});

test("returns null for empty pastTrackCodes", () => {
  expect(classifySurfaceSwitch("11", [])).toStrictEqual(null);
});

test("returns 芝替わり when race is turf and all past races are dirt", () => {
  expect(classifySurfaceSwitch("11", ["23", "24"])).toStrictEqual("芝替わり");
});

test("returns ダート替わり when race is dirt and all past races are turf", () => {
  expect(classifySurfaceSwitch("23", ["11", "12"])).toStrictEqual("ダート替わり");
});

test("returns null when horse has run on both surfaces", () => {
  expect(classifySurfaceSwitch("11", ["23", "11"])).toStrictEqual(null);
});

test("returns null when race is mixed/steeplechase track code", () => {
  expect(classifySurfaceSwitch("52", ["23", "24"])).toStrictEqual(null);
});

test("returns null when past includes a mixed track code", () => {
  expect(classifySurfaceSwitch("11", ["23", "52"])).toStrictEqual(null);
});

test("returns null when past contains null entries mixed with dirt-only", () => {
  expect(classifySurfaceSwitch("11", ["23", null, "24"])).toStrictEqual(null);
});

test("returns 芝替わり for single past race on dirt and current race on turf", () => {
  expect(classifySurfaceSwitch("11", ["23"])).toStrictEqual("芝替わり");
});

test("returns null when past has only null and undefined entries", () => {
  expect(classifySurfaceSwitch("11", [null, undefined])).toStrictEqual(null);
});

test('getSurfaceSwitchClassName returns "surface-turf" for 芝替わり', () => {
  expect(getSurfaceSwitchClassName("芝替わり")).toStrictEqual("surface-turf");
});

test('getSurfaceSwitchClassName returns "surface-dirt" for ダート替わり', () => {
  expect(getSurfaceSwitchClassName("ダート替わり")).toStrictEqual("surface-dirt");
});
