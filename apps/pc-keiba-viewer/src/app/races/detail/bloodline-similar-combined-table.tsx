"use client";

import { Fragment, memo, useMemo, useState } from "react";

import { cleanText } from "../../../lib/format";
import type { BloodlineStatsRow, Runner, SimilarRaceStatsRow } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";

type BloodlineCategory = BloodlineStatsRow["category"];
type SimilarCategory = SimilarRaceStatsRow["category"];

type CombinedRow = {
  bloodline: ScoreGroup<BloodlineCategory>;
  bloodlineScore: number;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  rawScore: number;
  score: number;
  similar: ScoreGroup<SimilarCategory>;
  similarScore: number;
};

type ScoreGroup<Category extends string> = {
  categoryRows: Partial<Record<Category, ScoredRateRow>>;
  categoryScores: Record<Category, number>;
  horseCount: number;
  starts: number;
};

type ScoredRateRow = (BloodlineStatsRow | SimilarRaceStatsRow) & {
  score: number;
};

interface BloodlineSimilarCombinedTableProps {
  bloodlineRows: BloodlineStatsRow[];
  rows: SimilarRaceStatsRow[];
  runners: Runner[];
}

const BLOODLINE_CATEGORY_LABELS: Record<BloodlineCategory, string> = {
  damSire: "母父",
  sire: "父",
  sireSire: "父父",
};

const SIMILAR_CATEGORY_LABELS: Record<SimilarCategory, string> = {
  jockey: "騎手",
  owner: "馬主",
  trainer: "調教師",
};

const BLOODLINE_CATEGORY_ORDER: BloodlineCategory[] = ["sire", "damSire", "sireSire"];
const SIMILAR_CATEGORY_ORDER: SimilarCategory[] = ["jockey", "trainer", "owner"];

const BLOODLINE_SCORE_WEIGHTS: Record<BloodlineCategory, number> = {
  damSire: 0.35,
  sire: 0.45,
  sireSire: 0.2,
};

const METRIC_SCORE_WEIGHTS = {
  horseCount: 0.05,
  quinellaRate: 0.25,
  showRate: 0.35,
  starts: 0.1,
  winRate: 0.25,
};

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

const formatScore = (value: number): string => value.toFixed(2);

const normalize = (value: number, max: number): number => (max > 0 ? value / max : 0);

const splitHorseNumbers = (value: string): string[] =>
  value
    .split(",")
    .map((horseNumber) => cleanText(horseNumber, ""))
    .filter(Boolean);

const scoreRateRow = (
  row: BloodlineStatsRow | SimilarRaceStatsRow,
  maxStarts: number,
  maxHorseCount: number,
): number =>
  (row.showRate / 100) * METRIC_SCORE_WEIGHTS.showRate +
  (row.quinellaRate / 100) * METRIC_SCORE_WEIGHTS.quinellaRate +
  (row.winRate / 100) * METRIC_SCORE_WEIGHTS.winRate +
  normalize(row.starts, maxStarts) * METRIC_SCORE_WEIGHTS.starts +
  normalize(row.horseCount, maxHorseCount) * METRIC_SCORE_WEIGHTS.horseCount;

const toScoredRows = <Row extends BloodlineStatsRow | SimilarRaceStatsRow>(
  rows: Row[],
): Array<Row & { score: number }> => {
  const maxStarts = Math.max(...rows.map((row) => row.starts), 0);
  const maxHorseCount = Math.max(...rows.map((row) => row.horseCount), 0);
  const rawRows = rows.map((row) => ({
    rawScore: scoreRateRow(row, maxStarts, maxHorseCount),
    row,
  }));
  const normalized = normalizeScores(rawRows.map((row) => row.rawScore));
  return rawRows.map(({ row }, index) => Object.assign({}, row, { score: normalized[index] ?? 0 }));
};

const normalizeScores = (scores: number[]): number[] => {
  if (scores.length === 0) {
    return [];
  }
  if (scores.length === 1) {
    return [1];
  }
  const minScore = Math.min(...scores);
  const maxScore = Math.max(...scores);
  if (maxScore === minScore) {
    return scores.map((_, index) => (index === 0 ? 1 : index === scores.length - 1 ? 0 : 0.5));
  }
  return scores.map((score) => (score - minScore) / (maxScore - minScore));
};

const getRowsByHorse = <Category extends string>(
  rows: Array<ScoredRateRow & { category: Category }>,
): Map<string, Partial<Record<Category, ScoredRateRow>>> => {
  const rowsByHorse = new Map<string, Partial<Record<Category, ScoredRateRow>>>();
  for (const row of rows) {
    for (const horseNumber of splitHorseNumbers(row.currentHorseNumbers)) {
      const currentRows = rowsByHorse.get(horseNumber);
      rowsByHorse.set(horseNumber, { ...currentRows, [row.category]: row });
    }
  }
  return rowsByHorse;
};

export const BloodlineSimilarCombinedTable = memo(function BloodlineSimilarCombinedTable({
  bloodlineRows,
  rows,
  runners,
}: BloodlineSimilarCombinedTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);

  const combinedRows = useMemo(() => {
    const scoredBloodlineRows = BLOODLINE_CATEGORY_ORDER.flatMap((category) =>
      toScoredRows(bloodlineRows.filter((row) => row.category === category)),
    );
    const scoredSimilarRows = SIMILAR_CATEGORY_ORDER.flatMap((category) =>
      toScoredRows(rows.filter((row) => row.category === category)),
    );
    const bloodlineRowsByHorse = getRowsByHorse(scoredBloodlineRows);
    const similarRowsByHorse = getRowsByHorse(scoredSimilarRows);

    const rawRows = runners.map((runner) => {
      const horseNumber = formatRunnerNumber(runner.umaban);
      const bloodlineCategoryRows = bloodlineRowsByHorse.get(horseNumber) ?? {};
      const similarCategoryRows = similarRowsByHorse.get(horseNumber) ?? {};

      const bloodline: ScoreGroup<BloodlineCategory> = {
        categoryRows: {},
        categoryScores: {
          damSire: 0,
          sire: 0,
          sireSire: 0,
        },
        horseCount: 0,
        starts: 0,
      };
      bloodline.categoryRows = bloodlineCategoryRows;
      for (const category of BLOODLINE_CATEGORY_ORDER) {
        bloodline.categoryScores[category] = bloodlineCategoryRows[category]?.score ?? 0;
      }
      const bloodlineValues = Object.values(bloodlineCategoryRows).filter(
        (row): row is ScoredRateRow => row !== undefined,
      );
      bloodline.starts = bloodlineValues.reduce((total, row) => total + row.starts, 0);
      bloodline.horseCount = bloodlineValues.reduce((total, row) => total + row.horseCount, 0);

      const similar: ScoreGroup<SimilarCategory> = {
        categoryRows: {},
        categoryScores: {
          jockey: 0,
          owner: 0,
          trainer: 0,
        },
        horseCount: 0,
        starts: 0,
      };
      similar.categoryRows = similarCategoryRows;
      for (const category of SIMILAR_CATEGORY_ORDER) {
        similar.categoryScores[category] = similarCategoryRows[category]?.score ?? 0;
      }
      const similarValues = Object.values(similarCategoryRows).filter(
        (row): row is ScoredRateRow => row !== undefined,
      );
      similar.starts = similarValues.reduce((total, row) => total + row.starts, 0);
      similar.horseCount = similarValues.reduce((total, row) => total + row.horseCount, 0);

      const bloodlineScore = BLOODLINE_CATEGORY_ORDER.reduce(
        (total, category) =>
          total + bloodline.categoryScores[category] * BLOODLINE_SCORE_WEIGHTS[category],
        0,
      );
      const similarScore =
        SIMILAR_CATEGORY_ORDER.reduce(
          (total, category) => total + similar.categoryScores[category],
          0,
        ) / SIMILAR_CATEGORY_ORDER.length;

      return {
        bloodline,
        bloodlineScore,
        horseName: cleanText(runner.bamei, "-"),
        horseNumber,
        jockeyName: cleanText(runner.kishumeiRyakusho, "-"),
        rawScore: (bloodlineScore + similarScore) / 2,
        similar,
        similarScore,
      };
    });

    const normalized = normalizeScores(rawRows.map((row) => row.rawScore));
    return rawRows
      .map((row, index) => Object.assign(row, { score: normalized[index] ?? 0 }))
      .toSorted((left, right) =>
        right.score === left.score
          ? Number(left.horseNumber) - Number(right.horseNumber)
          : right.score - left.score,
      );
  }, [bloodlineRows, rows, runners]);

  const renderDetail = (row: CombinedRow) => {
    const bloodlineDetails = BLOODLINE_CATEGORY_ORDER.map((category) => ({
      category: BLOODLINE_CATEGORY_LABELS[category],
      row: row.bloodline.categoryRows[category],
      score: row.bloodline.categoryScores[category],
      type: "血統",
    }));
    const similarDetails = SIMILAR_CATEGORY_ORDER.map((category) => ({
      category: SIMILAR_CATEGORY_LABELS[category],
      row: row.similar.categoryRows[category],
      score: row.similar.categoryScores[category],
      type: "同条件",
    }));

    return (
      <tr className="stats-detail-row">
        <td colSpan={8}>
          <div className="stats-detail-panel">
            <table className="stats-detail-table combined-score-detail-table">
              <thead>
                <tr>
                  <th>種別</th>
                  <th>項目</th>
                  <th>名前</th>
                  <th>スコア</th>
                  <th>複勝率</th>
                  <th>連対率</th>
                  <th>勝率</th>
                  <th>出走回数</th>
                  <th>出馬数</th>
                </tr>
              </thead>
              <tbody>
                {[...bloodlineDetails, ...similarDetails].map((detail) => (
                  <tr key={`${detail.type}-${detail.category}`}>
                    <td>{detail.type}</td>
                    <td>{detail.category}</td>
                    <td className="stats-name-cell">{detail.row?.name ?? "-"}</td>
                    <td className="stats-score-cell">{formatScore(detail.score)}</td>
                    <td>{detail.row ? formatRate(detail.row.showRate) : "-"}</td>
                    <td>{detail.row ? formatRate(detail.row.quinellaRate) : "-"}</td>
                    <td>{detail.row ? formatRate(detail.row.winRate) : "-"}</td>
                    <td>{detail.row ? detail.row.starts.toLocaleString("ja-JP") : "-"}</td>
                    <td>{detail.row ? detail.row.horseCount.toLocaleString("ja-JP") : "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </td>
      </tr>
    );
  };

  return (
    <section className="stats-category-section">
      <div className="section-heading compact">
        <h3>血統・同条件 合計スコア</h3>
      </div>
      <div className="stats-table-wrap">
        <table className="stats-table combined-score-table">
          <colgroup>
            <col className="combined-score-col-number" />
            <col className="combined-score-col-name" />
            <col className="combined-score-col-name" />
            <col className="combined-score-col-score" />
            <col className="combined-score-col-score" />
            <col className="combined-score-col-score" />
            <col className="combined-score-col-count" />
            <col className="combined-score-col-count" />
          </colgroup>
          <thead>
            <tr>
              <th>馬番</th>
              <th>馬名</th>
              <th>騎手</th>
              <th>合計スコア</th>
              <th>血統スコア</th>
              <th>勝率スコア</th>
              <th>出走回数</th>
              <th>出馬数</th>
            </tr>
          </thead>
          <tbody>
            {combinedRows.map((row) => {
              const isExpanded = expandedHorseNumber === row.horseNumber;
              return (
                <Fragment key={row.horseNumber}>
                  <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                    <td>{row.horseNumber}</td>
                    <td className="stats-name-cell">{row.horseName}</td>
                    <td className="stats-name-cell">{row.jockeyName}</td>
                    <td className="stats-score-cell">
                      <span className="combined-score-value">{formatScore(row.score)}</span>
                      <button
                        aria-expanded={isExpanded}
                        className="stats-detail-toggle combined-score-detail-toggle"
                        type="button"
                        onClick={() => {
                          setExpandedHorseNumber((current) =>
                            current === row.horseNumber ? null : row.horseNumber,
                          );
                        }}
                      >
                        {isExpanded ? "非表示" : "詳細"}
                      </button>
                    </td>
                    <td>{formatScore(row.bloodlineScore)}</td>
                    <td>{formatScore(row.similarScore)}</td>
                    <td>{(row.bloodline.starts + row.similar.starts).toLocaleString("ja-JP")}</td>
                    <td>
                      {(row.bloodline.horseCount + row.similar.horseCount).toLocaleString("ja-JP")}
                    </td>
                  </tr>
                  {isExpanded ? renderDetail(row) : null}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
});
