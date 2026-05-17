"use client";

import { Fragment, memo, useMemo, useState } from "react";

import { cleanText } from "../../../lib/format";
import { getPreferredJockeyName } from "../../../lib/jockey-name";
import type {
  BloodlineStatsRow,
  ConditionCorrelationDetail,
  ConditionCorrelationRow,
  Runner,
  SimilarRaceStatsRow,
  TimeScoreDetail,
  TimeScoreRow,
} from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

type BloodlineCategory = BloodlineStatsRow["category"];
type SimilarCategory = SimilarRaceStatsRow["category"];

type CombinedRow = {
  bloodline: ScoreGroup<BloodlineCategory>;
  bloodlineScore: number;
  correlationDetails: ConditionCorrelationDetail[];
  correlationScore: number;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  rawScore: number;
  score: number;
  similar: ScoreGroup<SimilarCategory>;
  similarScore: number;
  timeDetails: TimeScoreDetail[];
  timeScore: number;
};

type ScoreTargets = {
  base: {
    correlation: boolean;
    time: boolean;
  };
  bloodline: Record<BloodlineCategory, boolean>;
  similar: Record<SimilarCategory, boolean>;
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
  correlationRows: ConditionCorrelationRow[];
  realtimeRequest?: RealtimeRaceRequest;
  rows: SimilarRaceStatsRow[];
  runners: Runner[];
  timeRows: TimeScoreRow[];
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

const DEFAULT_SCORE_TARGETS: ScoreTargets = {
  base: {
    correlation: true,
    time: true,
  },
  bloodline: {
    damSire: true,
    sire: true,
    sireSire: true,
  },
  similar: {
    jockey: true,
    owner: true,
    trainer: true,
  },
};

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

const formatScore = (value: number): string => value.toFixed(2);

const normalize = (value: number, max: number): number => (max > 0 ? value / max : 0);

const normalizeHorseNumber = (value: string): string =>
  value.replace(/^0+/u, "") || (value ? "0" : "");

const roundScore = (value: number): number => Math.round(value * 100) / 100;

const clampScore = (value: number): number => Math.max(0, Math.min(1, value));

const formatDetailNumber = (value: number | null): string =>
  value === null ? "-" : value.toFixed(1);

const similarityScore = (value: number | null, target: number | null, scale: number): number => {
  if (value === null || target === null) {
    return 0.5;
  }
  return Math.max(0, Math.min(1, 1 - Math.abs(value - target) / Math.max(target, scale)));
};

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

const applyRealtimeCorrelationRows = (
  rows: ConditionCorrelationRow[],
  realtimeValues: Map<string, { odds: number | null; popularity: number | null }>,
): ConditionCorrelationRow[] => {
  if (realtimeValues.size === 0) {
    return rows;
  }

  return rows.map((row) => {
    const realtime = realtimeValues.get(normalizeHorseNumber(row.horseNumber));
    if (!realtime) {
      return row;
    }
    const details = row.details.map((detail): ConditionCorrelationDetail => {
      if (detail.key === "popularity" && realtime.popularity !== null) {
        return Object.assign({}, detail, {
          reason: `${detail.reason}。最新オッズ取得値で再計算`,
          score: roundScore(similarityScore(realtime.popularity, detail.target, 5)),
          value: realtime.popularity,
        });
      }
      if (detail.key === "odds" && realtime.odds !== null) {
        return Object.assign({}, detail, {
          reason: `${detail.reason}。最新オッズ取得値で再計算`,
          score: roundScore(similarityScore(realtime.odds, detail.target, 10)),
          value: realtime.odds,
        });
      }
      return detail;
    });
    const score = roundScore(
      details.reduce((total, detail) => total + detail.score * detail.weight, 0),
    );
    return Object.assign({}, row, { details, score });
  });
};

export const BloodlineSimilarCombinedTable = memo(function BloodlineSimilarCombinedTable({
  bloodlineRows,
  correlationRows,
  realtimeRequest,
  rows,
  runners,
  timeRows,
}: BloodlineSimilarCombinedTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);
  const [scoreTargets, setScoreTargets] = useState<ScoreTargets>(DEFAULT_SCORE_TARGETS);
  const { payload } = useRealtimeRacePayload(
    realtimeRequest ?? {
      apiBaseUrl: "",
      day: "",
      keibajoCode: "",
      month: "",
      raceNumber: "",
      source: "jra",
      year: "",
    },
    null,
  );
  const realtimeValues = useMemo(() => {
    const values = new Map<string, { odds: number | null; popularity: number | null }>();
    for (const row of payload?.odds?.latest.tansho ?? []) {
      values.set(normalizeHorseNumber(row.combination), {
        odds: row.odds ?? null,
        popularity: row.rank ?? null,
      });
    }
    return values;
  }, [payload]);
  const realtimeJockeyByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceEntries?.horses ?? []).map((horse) => [
          normalizeHorseNumber(horse.horseNumber),
          horse.jockeyName ?? "",
        ]),
      ),
    [payload],
  );

  const combinedRows = useMemo(() => {
    const displayedCorrelationRows = applyRealtimeCorrelationRows(correlationRows, realtimeValues);
    const timeByHorse = new Map(
      timeRows.map((row) => [normalizeHorseNumber(row.horseNumber), row]),
    );
    const correlationByHorse = new Map(
      displayedCorrelationRows.map((row) => [normalizeHorseNumber(row.horseNumber), row]),
    );
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
      const normalizedHorseNumber = normalizeHorseNumber(horseNumber);
      const timeRow = timeByHorse.get(normalizedHorseNumber);
      const correlationRow = correlationByHorse.get(normalizedHorseNumber);
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

      const selectedBloodlineCategories = BLOODLINE_CATEGORY_ORDER.filter(
        (category) => scoreTargets.bloodline[category],
      );
      const selectedBloodlineWeight = selectedBloodlineCategories.reduce(
        (total, category) => total + BLOODLINE_SCORE_WEIGHTS[category],
        0,
      );
      const bloodlineScore =
        selectedBloodlineWeight > 0
          ? selectedBloodlineCategories.reduce(
              (total, category) =>
                total + bloodline.categoryScores[category] * BLOODLINE_SCORE_WEIGHTS[category],
              0,
            ) / selectedBloodlineWeight
          : 0;
      const selectedSimilarCategories = SIMILAR_CATEGORY_ORDER.filter(
        (category) => scoreTargets.similar[category],
      );
      const similarScore =
        selectedSimilarCategories.length > 0
          ? selectedSimilarCategories.reduce(
              (total, category) => total + similar.categoryScores[category],
              0,
            ) / selectedSimilarCategories.length
          : 0;
      const selectedGroupScores = [
        scoreTargets.base.time ? clampScore(timeRow?.score ?? 0.5) : null,
        scoreTargets.base.correlation ? clampScore(correlationRow?.score ?? 0.5) : null,
        selectedBloodlineCategories.length > 0 ? bloodlineScore : null,
        selectedSimilarCategories.length > 0 ? similarScore : null,
      ].filter((score): score is number => score !== null);

      return {
        bloodline,
        bloodlineScore,
        correlationDetails: correlationRow?.details ?? [],
        correlationScore: clampScore(correlationRow?.score ?? 0.5),
        horseName: cleanText(runner.bamei, "-"),
        horseNumber,
        jockeyName:
          getPreferredJockeyName(
            cleanText(timeRow?.jockeyName ?? runner.kishumeiRyakusho, "-"),
            realtimeJockeyByHorse.get(normalizedHorseNumber),
          ) || "-",
        rawScore:
          selectedGroupScores.length > 0
            ? selectedGroupScores.reduce((total, score) => total + score, 0) /
              selectedGroupScores.length
            : 0,
        similar,
        similarScore,
        timeDetails: timeRow?.details ?? [],
        timeScore: clampScore(timeRow?.score ?? 0.5),
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
  }, [bloodlineRows, correlationRows, realtimeJockeyByHorse, realtimeValues, rows, runners, scoreTargets, timeRows]);

  const toggleBaseTarget = (key: keyof ScoreTargets["base"]) => {
    setScoreTargets((current) => ({
      ...current,
      base: {
        ...current.base,
        [key]: !current.base[key],
      },
    }));
  };

  const toggleBloodlineTarget = (category: BloodlineCategory) => {
    setScoreTargets((current) => ({
      ...current,
      bloodline: {
        ...current.bloodline,
        [category]: !current.bloodline[category],
      },
    }));
  };

  const toggleSimilarTarget = (category: SimilarCategory) => {
    setScoreTargets((current) => ({
      ...current,
      similar: {
        ...current.similar,
        [category]: !current.similar[category],
      },
    }));
  };

  const renderDetail = (row: CombinedRow) => {
    const baseDetails = [
      ...row.timeDetails.map((detail) => ({
        current: formatDetailNumber(detail.value),
        item: detail.label,
        reason: detail.reason,
        score: detail.score,
        target: formatDetailNumber(detail.target),
        type: "タイム",
        weight: detail.weight.toFixed(3),
      })),
      ...row.correlationDetails.map((detail) => ({
        current: formatDetailNumber(detail.value),
        item: detail.label,
        reason: detail.reason,
        score: detail.score,
        target: formatDetailNumber(detail.target),
        type: "1〜3着相関",
        weight: detail.weight.toFixed(3),
      })),
    ];
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
        <td colSpan={10}>
          <div className="stats-detail-panel">
            <table className="stats-detail-table combined-score-detail-table">
              <thead>
                <tr>
                  <th>種別</th>
                  <th>項目</th>
                  <th>名前</th>
                  <th>スコア</th>
                  <th>現在値</th>
                  <th>対象平均</th>
                  <th>重み</th>
                  <th>複勝率</th>
                  <th>連対率</th>
                  <th>勝率</th>
                  <th>出走回数</th>
                  <th>出馬数</th>
                  <th>理由</th>
                </tr>
              </thead>
              <tbody>
                {baseDetails.map((detail) => (
                  <tr key={`${detail.type}-${detail.item}`}>
                    <td>{detail.type}</td>
                    <td>{detail.item}</td>
                    <td className="stats-name-cell">-</td>
                    <td className="stats-score-cell">{formatScore(detail.score)}</td>
                    <td>{detail.current}</td>
                    <td>{detail.target}</td>
                    <td>{detail.weight}</td>
                    <td>-</td>
                    <td>-</td>
                    <td>-</td>
                    <td>-</td>
                    <td>-</td>
                    <td className="stats-name-cell">{detail.reason}</td>
                  </tr>
                ))}
                {[...bloodlineDetails, ...similarDetails].map((detail) => (
                  <tr key={`${detail.type}-${detail.category}`}>
                    <td>{detail.type}</td>
                    <td>{detail.category}</td>
                    <td className="stats-name-cell">{detail.row?.name ?? "-"}</td>
                    <td className="stats-score-cell">{formatScore(detail.score)}</td>
                    <td>-</td>
                    <td>-</td>
                    <td>-</td>
                    <td>{detail.row ? formatRate(detail.row.showRate) : "-"}</td>
                    <td>{detail.row ? formatRate(detail.row.quinellaRate) : "-"}</td>
                    <td>{detail.row ? formatRate(detail.row.winRate) : "-"}</td>
                    <td>{detail.row ? detail.row.starts.toLocaleString("ja-JP") : "-"}</td>
                    <td>{detail.row ? detail.row.horseCount.toLocaleString("ja-JP") : "-"}</td>
                    <td>-</td>
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
        <h3>タイム・相関・血統・同条件 合計スコア</h3>
      </div>
      <div className="combined-score-targets" aria-label="合計スコア対象">
        <fieldset>
          <legend>基本スコア</legend>
          <label>
            <input
              checked={scoreTargets.base.time}
              type="checkbox"
              onChange={() => {
                toggleBaseTarget("time");
              }}
            />
            <span>タイム</span>
          </label>
          <label>
            <input
              checked={scoreTargets.base.correlation}
              type="checkbox"
              onChange={() => {
                toggleBaseTarget("correlation");
              }}
            />
            <span>1〜3着相関</span>
          </label>
        </fieldset>
        <fieldset>
          <legend>血統スコア</legend>
          {BLOODLINE_CATEGORY_ORDER.map((category) => (
            <label key={category}>
              <input
                checked={scoreTargets.bloodline[category]}
                type="checkbox"
                onChange={() => {
                  toggleBloodlineTarget(category);
                }}
              />
              <span>{BLOODLINE_CATEGORY_LABELS[category]}</span>
            </label>
          ))}
        </fieldset>
        <fieldset>
          <legend>勝率スコア</legend>
          {SIMILAR_CATEGORY_ORDER.map((category) => (
            <label key={category}>
              <input
                checked={scoreTargets.similar[category]}
                type="checkbox"
                onChange={() => {
                  toggleSimilarTarget(category);
                }}
              />
              <span>{SIMILAR_CATEGORY_LABELS[category]}</span>
            </label>
          ))}
        </fieldset>
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
              <th>タイムスコア</th>
              <th>1〜3着相関スコア</th>
              <th>血統スコア</th>
              <th>同条件スコア</th>
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
                        {isExpanded ? "閉じる" : "詳細"}
                      </button>
                    </td>
                    <td>{formatScore(row.timeScore)}</td>
                    <td>{formatScore(row.correlationScore)}</td>
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
