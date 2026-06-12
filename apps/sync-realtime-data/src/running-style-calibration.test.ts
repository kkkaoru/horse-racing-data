// Run with bun test apps/sync-realtime-data/src/running-style-calibration.test.ts
import { expect, test, vi } from "vitest";

import {
  applyRunningStyleCalibration,
  buildCalibrationR2Key,
  linearInterp,
  loadCalibratorsFromR2,
} from "./running-style-calibration";
import type { RunningStyleCalibrationTable } from "./running-style-calibration";
import type { RunningStylePrediction } from "./running-style-lightgbm-tree";

const IDENTITY_CALIBRATORS: RunningStyleCalibrationTable = {
  calibrators: {
    nige: { x: [0, 1], y: [0, 1] },
    oikomi: { x: [0, 1], y: [0, 1] },
    sashi: { x: [0, 1], y: [0, 1] },
    senkou: { x: [0, 1], y: [0, 1] },
  },
  category: "jra",
  classes: ["nige", "senkou", "sashi", "oikomi"],
  fit_year: 2024,
};

const PREDICTION_NIGE: RunningStylePrediction = {
  predictedClass: 0,
  predictedLabel: "nige",
  probabilities: { nige: 0.7, oikomi: 0.1, sashi: 0.1, senkou: 0.1 },
};

test("linearInterp with identity knots returns value unchanged", () => {
  const result = linearInterp([0, 1], [0, 1], 0.5);
  expect(result).toBeCloseTo(0.5);
});

test("linearInterp with 2 knots does correct linear interpolation at midpoint", () => {
  const result = linearInterp([0, 10], [0, 20], 5);
  expect(result).toBeCloseTo(10);
});

test("linearInterp clamps below-range values to left boundary", () => {
  const result = linearInterp([2, 8], [10, 20], 0);
  expect(result).toBe(10);
});

test("linearInterp clamps above-range values to right boundary", () => {
  const result = linearInterp([2, 8], [10, 20], 100);
  expect(result).toBe(20);
});

test("linearInterp with single knot returns that y value", () => {
  const result = linearInterp([0.5], [0.42], 0.9);
  expect(result).toBe(0.42);
});

test("applyRunningStyleCalibration with identity calibrators preserves argmax and probs", () => {
  const result = applyRunningStyleCalibration(PREDICTION_NIGE, IDENTITY_CALIBRATORS);
  expect(result.predictedLabel).toBe("nige");
  expect(result.probabilities.nige).toBeCloseTo(0.7);
});

test("applyRunningStyleCalibration with identity calibrators is a complete no-op for all classes", () => {
  const prediction: RunningStylePrediction = {
    predictedClass: 0,
    predictedLabel: "nige",
    probabilities: { nige: 0.4, senkou: 0.3, sashi: 0.2, oikomi: 0.1 },
  };
  const result = applyRunningStyleCalibration(prediction, IDENTITY_CALIBRATORS);
  expect(result.predictedClass).toBe(0);
  expect(result.predictedLabel).toBe("nige");
  expect(result.probabilities.nige).toBeCloseTo(0.4);
  expect(result.probabilities.senkou).toBeCloseTo(0.3);
  expect(result.probabilities.sashi).toBeCloseTo(0.2);
  expect(result.probabilities.oikomi).toBeCloseTo(0.1);
});

test("applyRunningStyleCalibration with 100-knot identity calibrators is a no-op", () => {
  const knots100x: number[] = Array.from({ length: 100 }, (_, i) => i / 99);
  const knots100y: number[] = Array.from({ length: 100 }, (_, i) => i / 99);
  const identity100: RunningStyleCalibrationTable = {
    calibrators: {
      nige: { x: knots100x, y: knots100y },
      senkou: { x: knots100x, y: knots100y },
      sashi: { x: knots100x, y: knots100y },
      oikomi: { x: knots100x, y: knots100y },
    },
    category: "jra",
    classes: ["nige", "senkou", "sashi", "oikomi"],
    fit_year: 9999,
  };
  const prediction: RunningStylePrediction = {
    predictedClass: 2,
    predictedLabel: "sashi",
    probabilities: { nige: 0.1, senkou: 0.15, sashi: 0.5, oikomi: 0.25 },
  };
  const result = applyRunningStyleCalibration(prediction, identity100);
  expect(result.predictedLabel).toBe("sashi");
  expect(result.predictedClass).toBe(2);
  expect(result.probabilities.sashi).toBeCloseTo(0.5);
  expect(result.probabilities.oikomi).toBeCloseTo(0.25);
});

test("applyRunningStyleCalibration renormalizes: result probabilities sum to 1.0", () => {
  const result = applyRunningStyleCalibration(PREDICTION_NIGE, IDENTITY_CALIBRATORS);
  const total =
    result.probabilities.nige +
    result.probabilities.senkou +
    result.probabilities.sashi +
    result.probabilities.oikomi;
  expect(total).toBeCloseTo(1.0);
});

test("applyRunningStyleCalibration with all-zero calibrated fallback returns uniform distribution", () => {
  const zeroCalibrators: RunningStyleCalibrationTable = {
    calibrators: {
      nige: { x: [0, 1], y: [0, 0] },
      oikomi: { x: [0, 1], y: [0, 0] },
      sashi: { x: [0, 1], y: [0, 0] },
      senkou: { x: [0, 1], y: [0, 0] },
    },
    category: "jra",
    classes: ["nige", "senkou", "sashi", "oikomi"],
    fit_year: 2024,
  };
  const result = applyRunningStyleCalibration(PREDICTION_NIGE, zeroCalibrators);
  expect(result.probabilities.nige).toBeCloseTo(0.25);
  expect(result.probabilities.senkou).toBeCloseTo(0.25);
  expect(result.probabilities.sashi).toBeCloseTo(0.25);
  expect(result.probabilities.oikomi).toBeCloseTo(0.25);
  const total =
    result.probabilities.nige +
    result.probabilities.senkou +
    result.probabilities.sashi +
    result.probabilities.oikomi;
  expect(total).toBeCloseTo(1.0);
});

test("applyRunningStyleCalibration correctly recomputes predictedLabel after calibration changes argmax", () => {
  const flipCalibrators: RunningStyleCalibrationTable = {
    calibrators: {
      nige: { x: [0, 1], y: [0, 0.05] },
      oikomi: { x: [0, 1], y: [0, 0.9] },
      sashi: { x: [0, 1], y: [0, 1] },
      senkou: { x: [0, 1], y: [0, 1] },
    },
    category: "jra",
    classes: ["nige", "senkou", "sashi", "oikomi"],
    fit_year: 2024,
  };
  const result = applyRunningStyleCalibration(PREDICTION_NIGE, flipCalibrators);
  expect(result.predictedLabel).not.toBe("nige");
});

test("buildCalibrationR2Key returns correct key for jra", () => {
  expect(buildCalibrationR2Key("jra")).toBe("running-style/models/jra/calibrators.json");
});

test("buildCalibrationR2Key returns correct key for nar", () => {
  expect(buildCalibrationR2Key("nar")).toBe("running-style/models/nar/calibrators.json");
});

test("loadCalibratorsFromR2 returns parsed calibrators on happy path", async () => {
  const payload: RunningStyleCalibrationTable = {
    calibrators: {
      nige: { x: [0, 1], y: [0, 1] },
      oikomi: { x: [0, 1], y: [0, 1] },
      sashi: { x: [0, 1], y: [0, 1] },
      senkou: { x: [0, 1], y: [0, 1] },
    },
    category: "nar",
    classes: ["nige", "senkou", "sashi", "oikomi"],
    fit_year: 2023,
  };
  const mockBucket = {
    get: vi.fn(async () => ({ text: async () => JSON.stringify(payload) })),
  } as unknown as R2Bucket;
  const result = await loadCalibratorsFromR2(mockBucket, "some/key");
  expect(result.category).toBe("nar");
  expect(result.fit_year).toBe(2023);
  expect(result.calibrators.nige.x).toStrictEqual([0, 1]);
});

test("loadCalibratorsFromR2 throws when R2 returns null", async () => {
  const mockBucket = {
    get: vi.fn(async () => null),
  } as unknown as R2Bucket;
  await expect(loadCalibratorsFromR2(mockBucket, "missing/key")).rejects.toThrow(
    "Calibration table not found in R2",
  );
});

test("loadCalibratorsFromR2 throws when JSON is missing required keys", async () => {
  const mockBucket = {
    get: vi.fn(async () => ({ text: async () => JSON.stringify({ category: "jra" }) })),
  } as unknown as R2Bucket;
  await expect(loadCalibratorsFromR2(mockBucket, "bad/key")).rejects.toThrow(
    "Invalid calibration table",
  );
});

test("loadCalibratorsFromR2 throws when calibrators object is missing a class key", async () => {
  const partial = {
    calibrators: { nige: { x: [0, 1], y: [0, 1] } },
    category: "jra",
    classes: ["nige"],
    fit_year: 2024,
  };
  const mockBucket = {
    get: vi.fn(async () => ({ text: async () => JSON.stringify(partial) })),
  } as unknown as R2Bucket;
  await expect(loadCalibratorsFromR2(mockBucket, "partial/key")).rejects.toThrow(
    "Invalid calibration table",
  );
});

test("loadCalibratorsFromR2 throws when calibrators value is not an object", async () => {
  const bad = {
    calibrators: null,
    category: "jra",
    classes: ["nige"],
    fit_year: 2024,
  };
  const mockBucket = {
    get: vi.fn(async () => ({ text: async () => JSON.stringify(bad) })),
  } as unknown as R2Bucket;
  await expect(loadCalibratorsFromR2(mockBucket, "null-cal/key")).rejects.toThrow(
    "Invalid calibration table",
  );
});

test("loadCalibratorsFromR2 falls back to empty array when classes is not an array", async () => {
  const payload = {
    calibrators: {
      nige: { x: [0, 1], y: [0, 1] },
      oikomi: { x: [0, 1], y: [0, 1] },
      sashi: { x: [0, 1], y: [0, 1] },
      senkou: { x: [0, 1], y: [0, 1] },
    },
    category: "nar",
    classes: null,
    fit_year: 2023,
  };
  const mockBucket = {
    get: vi.fn(async () => ({ text: async () => JSON.stringify(payload) })),
  } as unknown as R2Bucket;
  const result = await loadCalibratorsFromR2(mockBucket, "null-classes/key");
  expect(result.classes).toStrictEqual([]);
});
