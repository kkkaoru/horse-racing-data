// Run with: bun run test src/db/corner-running-style-parsers.test.ts

import { describe, expect, test } from "vitest";

import {
  buildRaceKey,
  isRunningStyleLabel,
  numericOrNull,
  parseRaceRunningStyleRow,
  requireNumber,
  requireRunningStyleLabel,
  requireString,
  RUNNING_STYLE_LABELS,
  stringOrNull,
} from "./corner-running-style-parsers";

describe("RUNNING_STYLE_LABELS", () => {
  test("lists labels in nige/senkou/sashi/oikomi order", () => {
    expect(RUNNING_STYLE_LABELS).toStrictEqual(["nige", "senkou", "sashi", "oikomi"]);
  });
});

describe("isRunningStyleLabel", () => {
  test("accepts each canonical label", () => {
    expect(isRunningStyleLabel("nige")).toBe(true);
    expect(isRunningStyleLabel("senkou")).toBe(true);
    expect(isRunningStyleLabel("sashi")).toBe(true);
    expect(isRunningStyleLabel("oikomi")).toBe(true);
  });

  test("rejects unknown labels", () => {
    expect(isRunningStyleLabel("front-runner")).toBe(false);
    expect(isRunningStyleLabel("")).toBe(false);
  });
});

describe("numericOrNull", () => {
  test("returns the value for finite numbers", () => {
    expect(numericOrNull(0)).toBe(0);
    expect(numericOrNull(0.5)).toBe(0.5);
    expect(numericOrNull(-1.25)).toBe(-1.25);
  });

  test("returns null for NaN and infinities", () => {
    expect(numericOrNull(Number.NaN)).toBe(null);
    expect(numericOrNull(Number.POSITIVE_INFINITY)).toBe(null);
  });

  test("parses numeric strings", () => {
    expect(numericOrNull("0.62")).toBe(0.62);
  });

  test("returns null for non-numeric strings", () => {
    expect(numericOrNull("abc")).toBe(null);
  });

  test("returns null for nullish input", () => {
    expect(numericOrNull(null)).toBe(null);
    expect(numericOrNull(undefined)).toBe(null);
  });
});

describe("buildRaceKey", () => {
  test("emits source:YYYYMMDD:keibajo:race_bango", () => {
    const key = buildRaceKey({
      source: "jra",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "05",
      raceBango: "11",
    });
    expect(key).toBe("jra:20250517:05:11");
  });

  test("works for nar races", () => {
    const key = buildRaceKey({
      source: "nar",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0228",
      keibajoCode: "42",
      raceBango: "07",
    });
    expect(key).toBe("nar:20250228:42:07");
  });
});

describe("requireNumber", () => {
  test("returns the parsed number when present", () => {
    expect(requireNumber(0.5, "p")).toBe(0.5);
    expect(requireNumber("0.5", "p")).toBe(0.5);
  });

  test("throws when value is null or not numeric", () => {
    expect(() => requireNumber(null, "p_nige")).toThrowError(/p_nige/);
    expect(() => requireNumber("abc", "p_nige")).toThrowError(/p_nige/);
  });
});

describe("requireString", () => {
  test("returns the string when present", () => {
    expect(requireString("hello", "field")).toBe("hello");
  });

  test("throws when value is not a string", () => {
    expect(() => requireString(123, "field")).toThrowError(/field/);
    expect(() => requireString(null, "field")).toThrowError(/field/);
  });
});

describe("stringOrNull", () => {
  test("returns the string when value is a string", () => {
    expect(stringOrNull("hi")).toBe("hi");
  });

  test("returns null for non-string values", () => {
    expect(stringOrNull(null)).toBe(null);
    expect(stringOrNull(123)).toBe(null);
  });
});

describe("requireRunningStyleLabel", () => {
  test("returns canonical labels", () => {
    expect(requireRunningStyleLabel("nige")).toBe("nige");
    expect(requireRunningStyleLabel("oikomi")).toBe("oikomi");
  });

  test("throws on unknown labels", () => {
    expect(() => requireRunningStyleLabel("front-runner")).toThrowError(
      /nige\/senkou\/sashi\/oikomi/,
    );
    expect(() => requireRunningStyleLabel(123)).toThrowError(/nige\/senkou\/sashi\/oikomi/);
  });
});

describe("parseRaceRunningStyleRow", () => {
  test("normalizes raw D1 columns into the typed row", () => {
    const row = parseRaceRunningStyleRow({
      race_key: "jra:20250517:05:11",
      horse_number: 3,
      ketto_toroku_bango: "2020100001",
      bamei: "ロードカナロア",
      category: "jra",
      kaisai_nen: "2025",
      model_version: "jra-rs-v1.0",
      p_nige: 0.05,
      p_senkou: 0.62,
      p_sashi: 0.25,
      p_oikomi: 0.08,
      predicted_label: "senkou",
      predicted_at: "2025-05-17T01:00:00Z",
    });
    expect(row.raceKey).toBe("jra:20250517:05:11");
    expect(row.horseNumber).toBe(3);
    expect(row.bamei).toBe("ロードカナロア");
    expect(row.predictedLabel).toBe("senkou");
    expect(row.p_senkou).toBe(0.62);
  });

  test("tolerates null bamei", () => {
    const row = parseRaceRunningStyleRow({
      race_key: "nar:20250228:42:07",
      horse_number: 1,
      ketto_toroku_bango: "2020100002",
      bamei: null,
      category: "nar",
      kaisai_nen: "2025",
      model_version: "nar-rs-v1.0",
      p_nige: 0.6,
      p_senkou: 0.2,
      p_sashi: 0.15,
      p_oikomi: 0.05,
      predicted_label: "nige",
      predicted_at: "2025-02-28T07:00:00Z",
    });
    expect(row.bamei).toBe(null);
    expect(row.predictedLabel).toBe("nige");
  });

  test("throws when predicted_label is outside the canonical set", () => {
    expect(() =>
      parseRaceRunningStyleRow({
        race_key: "jra:20250517:05:11",
        horse_number: 1,
        ketto_toroku_bango: "h1",
        bamei: null,
        category: "jra",
        kaisai_nen: "2025",
        model_version: "jra-rs-v1.0",
        p_nige: 0.25,
        p_senkou: 0.25,
        p_sashi: 0.25,
        p_oikomi: 0.25,
        predicted_label: "front-runner",
        predicted_at: "2025-05-17T01:00:00Z",
      }),
    ).toThrowError(/predicted_label/);
  });

  test("throws when numeric column is missing", () => {
    expect(() =>
      parseRaceRunningStyleRow({
        race_key: "jra:20250517:05:11",
        horse_number: null,
        ketto_toroku_bango: "h1",
        bamei: null,
        category: "jra",
        kaisai_nen: "2025",
        model_version: "jra-rs-v1.0",
        p_nige: 0.25,
        p_senkou: 0.25,
        p_sashi: 0.25,
        p_oikomi: 0.25,
        predicted_label: "nige",
        predicted_at: "2025-05-17T01:00:00Z",
      }),
    ).toThrowError(/horse_number/);
  });
});
