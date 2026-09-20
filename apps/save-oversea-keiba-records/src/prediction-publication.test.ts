// This file runs with Bun and Vitest.
import { expect, test, vi } from "vitest";
import layouts from "../../daily-keiba-sync/src/generated/record-layouts.json";
import {
  buildPredictionStatements,
  publishPredictions,
  type PredictionPublicationInput,
} from "./prediction-publication";
import type { ProductionRequestInput } from "./production-request";

interface PredictionFixture {
  readonly umaban: number;
  readonly ketto_toroku_bango: string;
  readonly predicted_score: number;
  readonly predicted_rank: number;
  readonly predicted_top1_prob: number;
  readonly predicted_top3_prob: number;
  readonly predicted_finish_position: number;
}

const REQUEST: ProductionRequestInput = {
  runId: "12345678-abcd-4321-9876-123456789abc",
  createdAt: "2026-09-18T18:00:00Z",
  race: {
    ...Object.fromEntries(layouts.tables.jvd_ra.columns.map((c) => [c.name, ""])),
    kaisai_nen: "2026",
    kaisai_tsukihi: "0919",
    keibajo_code: "A4",
    race_bango: "05",
    shusso_tosu: "01",
  },
  runners: [
    {
      ...Object.fromEntries(layouts.tables.jvd_se.columns.map((c) => [c.name, ""])),
      kaisai_nen: "2026",
      kaisai_tsukihi: "0919",
      keibajo_code: "A4",
      race_bango: "05",
      umaban: "07",
      ketto_toroku_bango: "2021105727",
    },
  ],
};
const PREDICTION: PredictionFixture = {
  umaban: 7,
  ketto_toroku_bango: "2021105727",
  predicted_score: 2,
  predicted_rank: 1,
  predicted_top1_prob: 0.3,
  predicted_top3_prob: 0.9,
  predicted_finish_position: 0.8,
};
const INPUT: PredictionPublicationInput = {
  request: REQUEST,
  predictions: [PREDICTION],
  generatedAt: "2026-09-18T18:05:00Z",
};

test("builds only parameterized target-model/race upserts without model activation", () => {
  const statements = buildPredictionStatements(INPUT);
  expect(statements).toHaveLength(1);
  expect(statements[0]?.values).toStrictEqual([
    "overseas-lgbm-fp-v3",
    "2026",
    "0919",
    "A4",
    "05",
    "2021105727",
    "7",
    "2",
    "1",
    "0.3",
    "0.9",
    "0.8",
    "2026-09-18T18:05:00Z",
  ]);
  expect(statements[0]?.text).not.toMatch(/delete|create|finish_position_active_models/i);
  expect(statements[0]?.text).toMatch(/on conflict/);
});
test("preserves prediction-only UMABAN identities without inventing JV keys", () => {
  const request: ProductionRequestInput = {
    ...REQUEST,
    runners: REQUEST.runners.map((r) => ({ ...r, ketto_toroku_bango: "0000000000" })),
  };
  const statements = buildPredictionStatements({
    ...INPUT,
    request,
    predictions: [{ ...PREDICTION, ketto_toroku_bango: "UMABAN_07" }],
  });
  expect(statements[0]?.values[5]).toBe("UMABAN_07");
});
test("rejects missing generations", () => {
  expect(() => buildPredictionStatements({ ...INPUT, predictions: null })).toThrow(
    "Prediction generation is incomplete",
  );
});
test("rejects incomplete generations", () => {
  expect(() => buildPredictionStatements({ ...INPUT, predictions: [] })).toThrow(
    "Prediction generation is incomplete",
  );
});
test("rejects invalid generation times", () => {
  expect(() => buildPredictionStatements({ ...INPUT, generatedAt: "bad" })).toThrow(
    "Invalid prediction generation time",
  );
});
test("rejects a null row", () => {
  expect(() => buildPredictionStatements({ ...INPUT, predictions: [null] })).toThrow(
    "Invalid prediction row",
  );
});
test("rejects a primitive row", () => {
  expect(() => buildPredictionStatements({ ...INPUT, predictions: [7] })).toThrow(
    "Invalid prediction row",
  );
});
test("rejects an array row", () => {
  expect(() => buildPredictionStatements({ ...INPUT, predictions: [[]] })).toThrow(
    "Invalid prediction row",
  );
});
test("rejects a missing horse identity", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, ketto_toroku_bango: null }],
    }),
  ).toThrow("Invalid prediction row");
});
test("rejects string scores", () => {
  expect(() =>
    buildPredictionStatements({ ...INPUT, predictions: [{ ...PREDICTION, predicted_score: "2" }] }),
  ).toThrow("Prediction contains an invalid numeric value");
});
test("rejects non-finite scores", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, predicted_score: Infinity }],
    }),
  ).toThrow("Prediction contains an invalid numeric value");
});
test("rejects non-integer runner numbers", () => {
  expect(() =>
    buildPredictionStatements({ ...INPUT, predictions: [{ ...PREDICTION, umaban: 7.5 }] }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects non-integer ranks", () => {
  expect(() =>
    buildPredictionStatements({ ...INPUT, predictions: [{ ...PREDICTION, predicted_rank: 1.5 }] }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects zero rank", () => {
  expect(() =>
    buildPredictionStatements({ ...INPUT, predictions: [{ ...PREDICTION, predicted_rank: 0 }] }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects rank beyond the field", () => {
  expect(() =>
    buildPredictionStatements({ ...INPUT, predictions: [{ ...PREDICTION, predicted_rank: 2 }] }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects negative win probabilities", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, predicted_top1_prob: -0.1 }],
    }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects win probabilities above one", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, predicted_top1_prob: 1.1 }],
    }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects negative place probabilities", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, predicted_top3_prob: -0.1 }],
    }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects place probabilities above one", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, predicted_top3_prob: 1.1 }],
    }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects negative finish positions", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, predicted_finish_position: -1 }],
    }),
  ).toThrow("Prediction rank or probability is out of range");
});
test("rejects a runner outside the registered field", () => {
  expect(() =>
    buildPredictionStatements({ ...INPUT, predictions: [{ ...PREDICTION, umaban: 6 }] }),
  ).toThrow("Prediction identity does not match the registered field");
});
test("rejects a mismatched horse identity", () => {
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      predictions: [{ ...PREDICTION, ketto_toroku_bango: "UMABAN_07" }],
    }),
  ).toThrow("Prediction identity does not match the registered field");
});
test("rejects repeated runner numbers", () => {
  const request: ProductionRequestInput = {
    ...REQUEST,
    race: { ...REQUEST.race, shusso_tosu: "02" },
    runners: [...REQUEST.runners, ...REQUEST.runners.map((r) => ({ ...r, umaban: "01" }))],
  };
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      request,
      predictions: [PREDICTION, { ...PREDICTION, predicted_rank: 2 }],
    }),
  ).toThrow("Prediction generation has duplicate runners or ranks");
});
test("rejects repeated ranks", () => {
  const request: ProductionRequestInput = {
    ...REQUEST,
    race: { ...REQUEST.race, shusso_tosu: "02" },
    runners: [...REQUEST.runners, ...REQUEST.runners.map((r) => ({ ...r, umaban: "01" }))],
  };
  expect(() =>
    buildPredictionStatements({
      ...INPUT,
      request,
      predictions: [PREDICTION, { ...PREDICTION, umaban: 1 }],
    }),
  ).toThrow("Prediction generation has duplicate runners or ranks");
});
test("publishes the entire generation in one transaction", async () => {
  const withTransaction = vi.fn().mockResolvedValue(undefined);
  await publishPredictions(INPUT, { withTransaction });
  expect(withTransaction).toHaveBeenCalledTimes(1);
  expect(withTransaction.mock.calls[0]?.[0]).toHaveLength(1);
});
test("publishes all four mixed-identity runners in one generation", async () => {
  const request: ProductionRequestInput = {
    ...REQUEST,
    race: { ...REQUEST.race, shusso_tosu: "04" },
    runners: [
      { ...REQUEST.runners[0], umaban: "01", ketto_toroku_bango: "0000000000" },
      { ...REQUEST.runners[0], umaban: "02", ketto_toroku_bango: "0000000000" },
      { ...REQUEST.runners[0], umaban: "04", ketto_toroku_bango: "0000000000" },
      { ...REQUEST.runners[0], umaban: "07", ketto_toroku_bango: "2021105727" },
    ],
  };
  const predictions: readonly PredictionFixture[] = [
    { ...PREDICTION, umaban: 1, ketto_toroku_bango: "UMABAN_01", predicted_rank: 3 },
    { ...PREDICTION, umaban: 2, ketto_toroku_bango: "UMABAN_02", predicted_rank: 4 },
    { ...PREDICTION, umaban: 4, ketto_toroku_bango: "UMABAN_04", predicted_rank: 2 },
    PREDICTION,
  ];
  const statements = buildPredictionStatements({ ...INPUT, request, predictions });
  expect(
    statements.map((statement) => [
      statement.values[5],
      statement.values[6],
      statement.values[8],
      statement.values[12],
    ]),
  ).toStrictEqual([
    ["UMABAN_01", "1", "3", "2026-09-18T18:05:00Z"],
    ["UMABAN_02", "2", "4", "2026-09-18T18:05:00Z"],
    ["UMABAN_04", "4", "2", "2026-09-18T18:05:00Z"],
    ["2021105727", "7", "1", "2026-09-18T18:05:00Z"],
  ]);
  const withTransaction = vi.fn().mockResolvedValue(undefined);
  await publishPredictions({ ...INPUT, request, predictions }, { withTransaction });
  expect(withTransaction).toHaveBeenCalledTimes(1);
  expect(withTransaction.mock.calls[0]?.[0]).toHaveLength(4);
});

test("does not start a transaction for invalid input", async () => {
  const withTransaction = vi.fn();
  await expect(
    publishPredictions({ ...INPUT, predictions: [] }, { withTransaction }),
  ).rejects.toThrow("Prediction generation is incomplete");
  expect(withTransaction).not.toHaveBeenCalled();
});
test("propagates transaction failure without reporting publication", async () => {
  const withTransaction = vi.fn().mockRejectedValue(new Error("transaction failed"));
  await expect(publishPredictions(INPUT, { withTransaction })).rejects.toThrow(
    "transaction failed",
  );
});
