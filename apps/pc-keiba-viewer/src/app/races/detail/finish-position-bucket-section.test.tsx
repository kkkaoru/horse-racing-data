// Run with: bunx vitest run src/app/races/detail/finish-position-bucket-section.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test } from "vitest";

import { FinishPositionBucketEvaluationPanel } from "./finish-position-bucket-section";

afterEach(() => {
  cleanup();
});

test("renders the exact-scope headline with active dimension labels", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.63,
        pairScoreAvg: 0.7,
        place1Accuracy: 0.52,
        place2Accuracy: 0.28,
        place3Accuracy: 0.2,
        predictionCount: 1500,
        raceCount: 120,
        smallSampleWarning: false,
        top1Accuracy: 0.525,
        top1AccuracyCI: { lower: 0.49, upper: 0.56 },
        top3BoxAccuracy: 0.12,
        top3ExactAccuracy: 0.03,
        top3PlaceRelationAvg: 0.57,
        top3WinnerCaptureRate: 0.71,
        top5WinnerCaptureRate: 0.86,
      }}
      gradeCode={null}
      modelVersion="jra-cb-v7-lineage-wf-21y"
      race={{
        gradeCode: null,
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "010",
        kyosoJokenMeisho: null,
        kyosomeiHondai: null,
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: true,
          grade: false,
          keibajo: true,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "exact",
      }}
      source="jra"
    />,
  );
  expect(screen.getByText("Top1正解率")).toBeTruthy();
  expect(screen.getByText("52.5%")).toBeTruthy();
  expect(screen.getByText("モデル: jra-cb-v7-lineage-wf-21y")).toBeTruthy();
});

test("renders all ten ranking metric card labels and the NDCG card as a decimal", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.612,
        pairScoreAvg: 0.7,
        place1Accuracy: 0.52,
        place2Accuracy: 0.28,
        place3Accuracy: 0.2,
        predictionCount: 1500,
        raceCount: 120,
        smallSampleWarning: false,
        top1Accuracy: 0.525,
        top1AccuracyCI: { lower: 0.49, upper: 0.56 },
        top3BoxAccuracy: 0.12,
        top3ExactAccuracy: 0.03,
        top3PlaceRelationAvg: 0.57,
        top3WinnerCaptureRate: 0.71,
        top5WinnerCaptureRate: 0.86,
      }}
      gradeCode={null}
      modelVersion={null}
      race={{
        gradeCode: null,
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "010",
        kyosoJokenMeisho: null,
        kyosomeiHondai: null,
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: true,
          grade: false,
          keibajo: true,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "exact",
      }}
      source="jra"
    />,
  );
  expect(screen.getByText("1着的中率")).toBeTruthy();
  expect(screen.getByText("2着的中率")).toBeTruthy();
  expect(screen.getByText("3着的中率")).toBeTruthy();
  expect(screen.getByText("三連複的中率")).toBeTruthy();
  expect(screen.getByText("三連単的中率")).toBeTruthy();
  expect(screen.getByText("NDCG@3")).toBeTruthy();
  expect(screen.getByText("ペアスコア")).toBeTruthy();
  expect(screen.getByText("上位3頭で勝馬捕捉")).toBeTruthy();
  expect(screen.getByText("上位5頭で勝馬捕捉")).toBeTruthy();
  expect(screen.getByText("0.612")).toBeTruthy();
});

test("renders the keibajo-fallback scope notice and label", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.6,
        pairScoreAvg: 0.68,
        place1Accuracy: 0.34,
        place2Accuracy: 0.21,
        place3Accuracy: 0.15,
        predictionCount: 2000,
        raceCount: 200,
        smallSampleWarning: false,
        top1Accuracy: 0.347,
        top1AccuracyCI: { lower: 0.32, upper: 0.37 },
        top3BoxAccuracy: 0.12,
        top3ExactAccuracy: 0.03,
        top3PlaceRelationAvg: 0.57,
        top3WinnerCaptureRate: 0.7,
        top5WinnerCaptureRate: 0.87,
      }}
      gradeCode={null}
      modelVersion={null}
      race={{
        gradeCode: null,
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "010",
        kyosoJokenMeisho: null,
        kyosomeiHondai: null,
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: false,
          grade: false,
          keibajo: true,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "keibajo",
      }}
      source="jra"
    />,
  );
  expect(screen.getByText("東京（全レース） の着順予測精度")).toBeTruthy();
  expect(
    screen.getByText("該当条件のデータが無いため東京（全レース）で集計しています"),
  ).toBeTruthy();
});

test("renders the category-fallback scope using the JRA category label", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.6,
        pairScoreAvg: 0.68,
        place1Accuracy: 0.34,
        place2Accuracy: 0.21,
        place3Accuracy: 0.15,
        predictionCount: 9000,
        raceCount: 900,
        smallSampleWarning: false,
        top1Accuracy: 0.52,
        top1AccuracyCI: { lower: 0.5, upper: 0.54 },
        top3BoxAccuracy: 0.12,
        top3ExactAccuracy: 0.03,
        top3PlaceRelationAvg: 0.57,
        top3WinnerCaptureRate: 0.71,
        top5WinnerCaptureRate: 0.86,
      }}
      gradeCode={null}
      modelVersion={null}
      race={{
        gradeCode: null,
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "010",
        kyosoJokenMeisho: null,
        kyosomeiHondai: null,
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: false,
          grade: false,
          keibajo: false,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "category",
      }}
      source="jra"
    />,
  );
  expect(screen.getByText("JRA 全体 の着順予測精度")).toBeTruthy();
  expect(screen.getByText("該当条件のデータが無いためJRA 全体で集計しています")).toBeTruthy();
});

test("renders the small-sample badge when smallSampleWarning is true", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.6,
        pairScoreAvg: 0.68,
        place1Accuracy: 0.34,
        place2Accuracy: 0.21,
        place3Accuracy: 0.15,
        predictionCount: 120,
        raceCount: 10,
        smallSampleWarning: true,
        top1Accuracy: 0.5,
        top1AccuracyCI: { lower: 0.3, upper: 0.7 },
        top3BoxAccuracy: 0.1,
        top3ExactAccuracy: 0.02,
        top3PlaceRelationAvg: 0.5,
        top3WinnerCaptureRate: 0.6,
        top5WinnerCaptureRate: 0.8,
      }}
      gradeCode={null}
      modelVersion={null}
      race={{
        gradeCode: null,
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "010",
        kyosoJokenMeisho: null,
        kyosomeiHondai: null,
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: true,
          grade: false,
          keibajo: true,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "exact",
      }}
      source="jra"
    />,
  );
  expect(screen.getByText("(n=10, small sample)")).toBeTruthy();
});

test("renders the all-tiers-miss notice when the evaluation is null", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={null}
      gradeCode={null}
      modelVersion={null}
      race={null}
      scope={null}
      source={null}
    />,
  );
  expect(screen.getByText("該当する分類の精度データがまだ蓄積されていません")).toBeTruthy();
});

test("builds an exact JRA scope label from every active dimension including grade and race name", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.63,
        pairScoreAvg: 0.7,
        place1Accuracy: 0.52,
        place2Accuracy: 0.28,
        place3Accuracy: 0.2,
        predictionCount: 1500,
        raceCount: 120,
        smallSampleWarning: false,
        top1Accuracy: 0.525,
        top1AccuracyCI: { lower: 0.49, upper: 0.56 },
        top3BoxAccuracy: 0.12,
        top3ExactAccuracy: 0.03,
        top3PlaceRelationAvg: 0.57,
        top3WinnerCaptureRate: 0.71,
        top5WinnerCaptureRate: 0.86,
      }}
      gradeCode="A"
      modelVersion="jra-cb-v7-lineage-wf-21y"
      race={{
        gradeCode: "A",
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "999",
        kyosoJokenMeisho: "オープン",
        kyosomeiHondai: "東京新聞杯",
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: true,
          grade: true,
          keibajo: true,
          kyosoJoken: true,
          kyosoShubetsu: true,
          raceName: true,
          track: true,
        },
        level: "exact",
      }}
      source="jra"
    />,
  );
  expect(screen.getByText(/東京新聞杯/u)).toBeTruthy();
  expect(screen.getByText(/2000m/u)).toBeTruthy();
});

test("falls back to the condition placeholder when a NAR race has no condition name", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.6,
        pairScoreAvg: 0.7,
        place1Accuracy: 0.58,
        place2Accuracy: 0.35,
        place3Accuracy: 0.27,
        predictionCount: 3000,
        raceCount: 300,
        smallSampleWarning: false,
        top1Accuracy: 0.585,
        top1AccuracyCI: { lower: 0.56, upper: 0.61 },
        top3BoxAccuracy: 0.34,
        top3ExactAccuracy: 0.05,
        top3PlaceRelationAvg: 0.61,
        top3WinnerCaptureRate: 0.77,
        top5WinnerCaptureRate: 0.9,
      }}
      gradeCode={null}
      modelVersion={null}
      race={{
        gradeCode: null,
        keibajoCode: "44",
        kyori: 1800,
        kyosoJokenCode: null,
        kyosoJokenMeisho: null,
        kyosomeiHondai: null,
        kyosoShubetsuCode: "11",
        source: "nar",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: true,
          distance: false,
          grade: false,
          keibajo: false,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "exact",
      }}
      source="nar"
    />,
  );
  expect(screen.getByText("条件 の着順予測精度")).toBeTruthy();
});

test("uses the category label for an exact scope when no dimension flags are active", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.6,
        pairScoreAvg: 0.7,
        place1Accuracy: 0.52,
        place2Accuracy: 0.28,
        place3Accuracy: 0.2,
        predictionCount: 9000,
        raceCount: 900,
        smallSampleWarning: false,
        top1Accuracy: 0.52,
        top1AccuracyCI: { lower: 0.5, upper: 0.54 },
        top3BoxAccuracy: 0.12,
        top3ExactAccuracy: 0.03,
        top3PlaceRelationAvg: 0.57,
        top3WinnerCaptureRate: 0.71,
        top5WinnerCaptureRate: 0.86,
      }}
      gradeCode={null}
      modelVersion={null}
      race={{
        gradeCode: null,
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "999",
        kyosoJokenMeisho: "オープン",
        kyosomeiHondai: null,
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: false,
          grade: false,
          keibajo: false,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "exact",
      }}
      source="jra"
    />,
  );
  expect(screen.getByText("JRA 全体 の着順予測精度")).toBeTruthy();
});

test("omits the race name from the scope label when the grade is not eligible for race names", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.63,
        pairScoreAvg: 0.7,
        place1Accuracy: 0.52,
        place2Accuracy: 0.28,
        place3Accuracy: 0.2,
        predictionCount: 1500,
        raceCount: 120,
        smallSampleWarning: false,
        top1Accuracy: 0.525,
        top1AccuracyCI: { lower: 0.49, upper: 0.56 },
        top3BoxAccuracy: 0.12,
        top3ExactAccuracy: 0.03,
        top3PlaceRelationAvg: 0.57,
        top3WinnerCaptureRate: 0.71,
        top5WinnerCaptureRate: 0.86,
      }}
      gradeCode="C"
      modelVersion={null}
      race={{
        gradeCode: "C",
        keibajoCode: "05",
        kyori: 2000,
        kyosoJokenCode: "999",
        kyosoJokenMeisho: "オープン",
        kyosomeiHondai: "重賞ではないレース",
        kyosoShubetsuCode: "13",
        source: "jra",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: false,
          distance: false,
          grade: true,
          keibajo: true,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: true,
          track: false,
        },
        level: "exact",
      }}
      source="jra"
    />,
  );
  expect(screen.queryByText(/重賞ではないレース/u)).toBe(null);
});

test("renders the NAR condition label in the exact scope when the condition flag is on", () => {
  render(
    <FinishPositionBucketEvaluationPanel
      evaluation={{
        ndcgAt3Avg: 0.6,
        pairScoreAvg: 0.7,
        place1Accuracy: 0.58,
        place2Accuracy: 0.35,
        place3Accuracy: 0.27,
        predictionCount: 3000,
        raceCount: 300,
        smallSampleWarning: false,
        top1Accuracy: 0.585,
        top1AccuracyCI: { lower: 0.56, upper: 0.61 },
        top3BoxAccuracy: 0.34,
        top3ExactAccuracy: 0.05,
        top3PlaceRelationAvg: 0.61,
        top3WinnerCaptureRate: 0.77,
        top5WinnerCaptureRate: 0.9,
      }}
      gradeCode={null}
      modelVersion="nar-xgb-v7-lineage-wf-21y"
      race={{
        gradeCode: null,
        keibajoCode: "44",
        kyori: 1800,
        kyosoJokenCode: null,
        kyosoJokenMeisho: "B3",
        kyosomeiHondai: null,
        kyosoShubetsuCode: "11",
        source: "nar",
        trackCode: "10",
      }}
      scope={{
        flags: {
          condition: true,
          distance: false,
          grade: false,
          keibajo: true,
          kyosoJoken: false,
          kyosoShubetsu: false,
          raceName: false,
          track: false,
        },
        level: "exact",
      }}
      source="nar"
    />,
  );
  expect(screen.getByText("58.5%")).toBeTruthy();
  expect(screen.getByText("モデル: nar-xgb-v7-lineage-wf-21y")).toBeTruthy();
});
