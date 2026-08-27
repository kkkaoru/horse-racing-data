// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  buildFinishPredictionSummary,
  createFinishPredictionSummaryError,
} from "./mcp-finish-prediction-summary";

it("builds a compact ranked prediction and joins zero-padded runner identities", () => {
  const result = buildFinishPredictionSummary(
    {
      bucket: {
        bucketEvaluation: {
          ndcgAt3Avg: 0.81,
          pairScoreAvg: 0.72,
          predictionCount: 400,
          raceCount: 40,
          smallSampleWarning: false,
          top1Accuracy: 0.45,
          top3BoxAccuracy: 0.3,
          top3ExactAccuracy: 0.12,
          top3WinnerCaptureRate: 0.8,
          top5WinnerCaptureRate: 0.95,
        },
        bucketRace: { kyosomeiHondai: "園田テスト", raceName: "fallback" },
      },
      evaluation: { top1Accuracy: 99 },
      inputs: {
        currentDistance: "1400",
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        currentTrackCode: "24",
        modelPredictionFeatures: [
          {
            confidenceTier: "mid",
            horseNumber: "02",
            modelVersion: "model-v2",
            predictedFinishNorm: 0.7,
            predictedScoreStddev: 0.2,
            predictionGeneratedAt: "2026-08-27T01:02:03.000Z",
            showProbability: 0.4,
            winProbability: 0.2,
          },
          {
            confidenceTier: "high",
            horseNumber: "1",
            modelVersion: "model-v2",
            predictedFinishNorm: 0.1,
            predictedScoreStddev: 0.2,
            predictionGeneratedAt: "2026-08-27T01:02:03.000Z",
            showProbability: 0.8,
            winProbability: 0.5,
          },
        ],
        results: [{ history: "must never be copied" }],
        runners: [
          {
            bamei: "Short Alpha",
            horseNameFull: "Alpha Full",
            jockeyNameFull: "Jockey Alpha Full",
            kishumeiRyakusho: "Jockey A",
            umaban: "01",
          },
          { bamei: "Beta", kishumeiRyakusho: "Jockey B", umaban: "2" },
        ],
        sameDayVenueJockeyWins: [{ jockeyName: "Jockey A", latestRaceNumber: "04", winCount: 2 }],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );

  expect(JSON.parse(JSON.stringify(result))).toStrictEqual({
    status: "ok",
    summary: {
      evaluation: {
        ndcgAt3Avg: 0.81,
        pairScoreAvg: 0.72,
        predictionCount: 400,
        raceCount: 40,
        smallSampleWarning: false,
        top1Accuracy: 0.45,
        top3BoxAccuracy: 0.3,
        top3ExactAccuracy: 0.12,
        top3WinnerCaptureRate: 0.8,
        top5WinnerCaptureRate: 0.95,
      },
      prediction: [
        {
          confidenceTier: "high",
          horseName: "Alpha Full",
          horseNumber: "01",
          jockeyName: "Jockey Alpha Full",
          modelVersion: "model-v2",
          predictedFinishNorm: 0.1,
          predictedScoreStddev: 0.2,
          predictionGeneratedAt: "2026-08-27T01:02:03.000Z",
          rank: 1,
          showProbability: 0.8,
          winProbability: 0.5,
        },
        {
          confidenceTier: "mid",
          horseName: "Beta",
          horseNumber: "02",
          jockeyName: "Jockey B",
          modelVersion: "model-v2",
          predictedFinishNorm: 0.7,
          predictedScoreStddev: 0.2,
          predictionGeneratedAt: "2026-08-27T01:02:03.000Z",
          rank: 2,
          showProbability: 0.4,
          winProbability: 0.2,
        },
      ],
      race: {
        distance: "1400",
        keibajoCode: "50",
        raceDate: "2026-08-27",
        raceName: "園田テスト",
        raceNumber: "05",
        source: "nar",
        trackCode: "24",
      },
      sameDayVenueJockeyWins: [{ jockeyName: "Jockey A", latestRaceNumber: "04", winCount: 2 }],
    },
  });
});

it("uses legacy evaluation names and keeps null predictions after scored runners", () => {
  const result = buildFinishPredictionSummary(
    {
      bucket: { bucketRace: { raceName: "JRA Test" } },
      evaluation: {
        pairScore: 70,
        raceCount: 100,
        top1Accuracy: 40,
        top3BoxAccuracy: 20,
        top3ExactOrderAccuracy: 10,
        top3WinnerCapture: 80,
        top5WinnerCapture: 95,
      },
      inputs: {
        currentDistance: 1600,
        currentKeibajoCode: "05",
        currentRaceDate: "20260827",
        currentSource: "jra",
        currentTrackCode: null,
        modelPredictionFeatures: [
          {
            horseNumber: "02",
            modelVersion: null,
            predictedFinishNorm: null,
            predictedScoreStddev: null,
            predictionGeneratedAt: null,
            showProbability: null,
            winProbability: null,
          },
          {
            horseNumber: "01",
            modelVersion: "jra-v1",
            predictedFinishNorm: 0.2,
            predictedScoreStddev: null,
            predictionGeneratedAt: null,
            showProbability: null,
            winProbability: null,
          },
        ],
        runners: [
          { bamei: "Alpha", umaban: "1" },
          { bamei: "Beta", umaban: "02" },
        ],
        sameDayVenueJockeyWins: [null, { jockeyName: "", winCount: "two" }],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "05",
      month: "08",
      raceNumber: "11",
      source: "jra",
      year: "2026",
    },
  );

  expect(JSON.parse(JSON.stringify(result))).toStrictEqual({
    status: "ok",
    summary: {
      evaluation: {
        pairScoreAvg: 70,
        raceCount: 100,
        top1Accuracy: 40,
        top3BoxAccuracy: 20,
        top3ExactAccuracy: 10,
        top3WinnerCaptureRate: 80,
        top5WinnerCaptureRate: 95,
      },
      prediction: [
        {
          confidenceTier: null,
          horseName: "Alpha",
          horseNumber: "01",
          modelVersion: "jra-v1",
          predictedFinishNorm: 0.2,
          predictedScoreStddev: null,
          predictionGeneratedAt: null,
          rank: 1,
          showProbability: null,
          winProbability: null,
        },
        {
          confidenceTier: null,
          horseName: "Beta",
          horseNumber: "02",
          modelVersion: null,
          predictedFinishNorm: null,
          predictedScoreStddev: null,
          predictionGeneratedAt: null,
          rank: 2,
          showProbability: null,
          winProbability: null,
        },
      ],
      race: {
        distance: 1600,
        keibajoCode: "05",
        raceDate: "2026-08-27",
        raceName: "JRA Test",
        raceNumber: "11",
        source: "jra",
      },
      sameDayVenueJockeyWins: [],
    },
  });
});

it("distinguishes unavailable predictions from malformed prediction payloads", () => {
  const unavailable = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [],
        runners: [],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(unavailable).toStrictEqual({
    error: {
      code: "PREDICTION_NOT_AVAILABLE",
      message: "Finish prediction has not been generated for this race.",
    },
    status: "error",
  });

  const malformed = buildFinishPredictionSummary(
    { inputs: { modelPredictionFeatures: [], runners: "bad" }, type: "finish-prediction" },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(malformed).toStrictEqual({
    error: {
      code: "PREDICTION_PAYLOAD_MALFORMED",
      message: "The finish prediction API returned a payload that cannot be summarized safely.",
    },
    status: "error",
  });
});

it("rejects all-null output, missing runners, invalid scores, and mismatched race metadata", () => {
  const allNull = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [{ horseNumber: "1", predictedFinishNorm: null }],
        runners: [{ bamei: "Alpha", umaban: "01" }],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(allNull).toStrictEqual({
    error: {
      code: "PREDICTION_NOT_AVAILABLE",
      message: "Finish prediction has not been generated for this race.",
    },
    status: "error",
  });

  const missingRunner = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [{ horseNumber: "2", predictedFinishNorm: 0.1 }],
        runners: [{ bamei: "Alpha", umaban: "01" }, null],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(missingRunner.status).toBe("error");

  const invalidScore = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [{ horseNumber: "1", predictedFinishNorm: "first" }],
        runners: [{ bamei: "Alpha", umaban: "01" }],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(invalidScore.status).toBe("error");

  const mismatchedRace = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "43",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [{ horseNumber: "1", predictedFinishNorm: 0.1 }],
        runners: [{ bamei: "Alpha", umaban: "01" }],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(mismatchedRace.status).toBe("error");
});

it("omits unrecognized optional data and creates typed errors", () => {
  const result = buildFinishPredictionSummary(
    {
      bucket: { bucketEvaluation: {}, bucketRace: null },
      evaluation: null,
      inputs: {
        currentDistance: null,
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [
          {
            confidenceTier: 1,
            horseNumber: "1",
            modelVersion: 2,
            predictedFinishNorm: 0.1,
            predictedScoreStddev: "wide",
            predictionGeneratedAt: 3,
            showProbability: "high",
            winProbability: "high",
          },
        ],
        runners: [{ bamei: "Alpha", umaban: "01" }],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(JSON.parse(JSON.stringify(result))).toStrictEqual({
    status: "ok",
    summary: {
      prediction: [
        {
          confidenceTier: null,
          horseName: "Alpha",
          horseNumber: "01",
          modelVersion: null,
          predictedFinishNorm: 0.1,
          predictedScoreStddev: null,
          predictionGeneratedAt: null,
          rank: 1,
          showProbability: null,
          winProbability: null,
        },
      ],
      race: {
        keibajoCode: "50",
        raceDate: "2026-08-27",
        raceNumber: "05",
        source: "nar",
      },
    },
  });
  expect(createFinishPredictionSummaryError("TIMEOUT", "Timed out.")).toStrictEqual({
    code: "TIMEOUT",
    message: "Timed out.",
  });
});

it("rejects invalid core shapes and ignores unusable unreferenced runner rows", () => {
  const invalidPayload = buildFinishPredictionSummary(null, {
    day: "27",
    keibajoCode: "50",
    month: "08",
    raceNumber: "05",
    source: "nar",
    year: "2026",
  });
  expect(invalidPayload.status).toBe("error");

  const invalidFeatures = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: {},
        runners: [],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(invalidFeatures.status).toBe("error");

  const invalidFeatureRow = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [null],
        runners: [],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(invalidFeatureRow.status).toBe("error");

  const invalidFeatureHorseNumber = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [{ horseNumber: "not-a-number", predictedFinishNorm: 0.1 }],
        runners: [],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(invalidFeatureHorseNumber.status).toBe("error");

  const valid = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [{ horseNumber: "1", predictedFinishNorm: 0.1 }],
        runners: [
          { bamei: "Bad Number", umaban: "x" },
          { bamei: "Zero", umaban: "0" },
          { umaban: "03" },
          { bamei: "Alpha", umaban: "01" },
        ],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(valid.status).toBe("ok");
});

it("uses horse number tie-breakers for equal and null prediction scores", () => {
  const result = buildFinishPredictionSummary(
    {
      inputs: {
        currentKeibajoCode: "50",
        currentRaceDate: "20260827",
        currentSource: "nar",
        modelPredictionFeatures: [
          { horseNumber: "4", predictedFinishNorm: null },
          { horseNumber: "2", predictedFinishNorm: 0.1 },
          { horseNumber: "3", predictedFinishNorm: null },
          { horseNumber: "1", predictedFinishNorm: 0.1 },
        ],
        runners: [
          { bamei: "Horse 1", umaban: "01" },
          { bamei: "Horse 2", umaban: "02" },
          { bamei: "Horse 3", umaban: "03" },
          { bamei: "Horse 4", umaban: "04" },
        ],
      },
      type: "finish-prediction",
    },
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
  );
  expect(result.status).toBe("ok");
  expect(
    result.status === "ok" ? result.summary.prediction.map((item) => item.horseNumber) : [],
  ).toStrictEqual(["01", "02", "03", "04"]);
});
