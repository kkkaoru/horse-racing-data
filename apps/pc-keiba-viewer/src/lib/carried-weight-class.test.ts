// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  CARRIED_WEIGHT_CLASSES,
  getCarriedWeightClass,
  parseCarriedWeightKg,
} from "./carried-weight-class";

it("defines six carried-weight classes from 49kg and below through 57.5-59kg", () => {
  expect(CARRIED_WEIGHT_CLASSES).toStrictEqual([
    { key: "le49", label: "49kg以下", maxKg: 49, minKg: null },
    { key: "49.5-51", label: "49.5以上51kg以下", maxKg: 51, minKg: 49.5 },
    { key: "51.5-53", label: "51.5kg以上53kg以下", maxKg: 53, minKg: 51.5 },
    { key: "53.5-55", label: "53.5kg以上55kg以下", maxKg: 55, minKg: 53.5 },
    { key: "55.5-57", label: "55.5kg以上57kg以下", maxKg: 57, minKg: 55.5 },
    { key: "57.5-59", label: "57.5kg以上59kg以下", maxKg: 59, minKg: 57.5 },
  ]);
});

it("parses JV-Link 0.1kg futan strings into kilograms", () => {
  expect(parseCarriedWeightKg("490")).toBe(49);
  expect(parseCarriedWeightKg("495")).toBe(49.5);
  expect(parseCarriedWeightKg("570")).toBe(57);
  expect(parseCarriedWeightKg("575")).toBe(57.5);
});

it("treats blank, 000, FFF, and non-positive carried weights as missing", () => {
  expect(parseCarriedWeightKg(null)).toBe(null);
  expect(parseCarriedWeightKg(" ")).toBe(null);
  expect(parseCarriedWeightKg("000")).toBe(null);
  expect(parseCarriedWeightKg("FFF")).toBe(null);
  expect(parseCarriedWeightKg("fff")).toBe(null);
  expect(parseCarriedWeightKg("0")).toBe(null);
  expect(parseCarriedWeightKg("abc")).toBe(null);
});

it("classifies carried weights on the six inclusive kilogram bands", () => {
  expect(getCarriedWeightClass(48.5)).toStrictEqual({
    key: "le49",
    label: "49kg以下",
    maxKg: 49,
    minKg: null,
  });
  expect(getCarriedWeightClass(49)).toStrictEqual({
    key: "le49",
    label: "49kg以下",
    maxKg: 49,
    minKg: null,
  });
  expect(getCarriedWeightClass(49.5)).toStrictEqual({
    key: "49.5-51",
    label: "49.5以上51kg以下",
    maxKg: 51,
    minKg: 49.5,
  });
  expect(getCarriedWeightClass(51)).toStrictEqual({
    key: "49.5-51",
    label: "49.5以上51kg以下",
    maxKg: 51,
    minKg: 49.5,
  });
  expect(getCarriedWeightClass(51.5)).toStrictEqual({
    key: "51.5-53",
    label: "51.5kg以上53kg以下",
    maxKg: 53,
    minKg: 51.5,
  });
  expect(getCarriedWeightClass(53)).toStrictEqual({
    key: "51.5-53",
    label: "51.5kg以上53kg以下",
    maxKg: 53,
    minKg: 51.5,
  });
  expect(getCarriedWeightClass(53.5)).toStrictEqual({
    key: "53.5-55",
    label: "53.5kg以上55kg以下",
    maxKg: 55,
    minKg: 53.5,
  });
  expect(getCarriedWeightClass(55)).toStrictEqual({
    key: "53.5-55",
    label: "53.5kg以上55kg以下",
    maxKg: 55,
    minKg: 53.5,
  });
  expect(getCarriedWeightClass(55.5)).toStrictEqual({
    key: "55.5-57",
    label: "55.5kg以上57kg以下",
    maxKg: 57,
    minKg: 55.5,
  });
  expect(getCarriedWeightClass(57)).toStrictEqual({
    key: "55.5-57",
    label: "55.5kg以上57kg以下",
    maxKg: 57,
    minKg: 55.5,
  });
  expect(getCarriedWeightClass(57.5)).toStrictEqual({
    key: "57.5-59",
    label: "57.5kg以上59kg以下",
    maxKg: 59,
    minKg: 57.5,
  });
  expect(getCarriedWeightClass(59)).toStrictEqual({
    key: "57.5-59",
    label: "57.5kg以上59kg以下",
    maxKg: 59,
    minKg: 57.5,
  });
  expect(getCarriedWeightClass(60)).toStrictEqual({
    key: "57.5-59",
    label: "57.5kg以上59kg以下",
    maxKg: 59,
    minKg: 57.5,
  });
  expect(getCarriedWeightClass(Number.NaN)).toStrictEqual({
    key: "57.5-59",
    label: "57.5kg以上59kg以下",
    maxKg: 59,
    minKg: 57.5,
  });
});
