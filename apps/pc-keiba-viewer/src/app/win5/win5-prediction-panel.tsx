"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

import { formatKeibajo, formatRaceNumber } from "../../lib/format";
import { getWin5PlanForBudget } from "../../lib/win5/prediction";
import { WIN5_DEFAULT_BUDGET_YEN, type Win5PredictionPayload } from "../../lib/win5/types";

interface Win5PredictionPanelProps {
  day: string;
  month: string;
  prediction: Win5PredictionPayload;
  year: string;
}

const formatPercent = (value: number): string => `${(value * 100).toFixed(2)}%`;
const formatYen = (value: number): string => `${value.toLocaleString("ja-JP")}円`;
const cleanHorseName = (value: string): string => value.replace(/\s+/gu, " ").trim();
const formatHorseNumber = (value: string): string => `${value}番`;
const formatHorseNumbers = (values: readonly string[], separator = " / "): string =>
  values.map(formatHorseNumber).join(separator);

const buildRaceHref = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceBango: string,
): string => `/races/${year}/${month}/${day}/${keibajoCode}/${raceBango.padStart(2, "0")}`;

export function Win5PredictionPanel({ day, month, prediction, year }: Win5PredictionPanelProps) {
  const [budgetYen, setBudgetYen] = useState(prediction.defaultBudgetYen);
  const [showAllHorses, setShowAllHorses] = useState(false);
  const plan = useMemo(() => getWin5PlanForBudget(prediction, budgetYen), [budgetYen, prediction]);

  return (
    <section aria-label="WIN5買い目提案" className="win5-section">
      <div className="section-heading">
        <h2>買い目提案</h2>
        <span>{prediction.modelVersion}</span>
      </div>

      <dl aria-label="買い目サマリー" className="course-facts win5-summary-facts">
        <div>
          <dt>予算</dt>
          <dd>{formatYen(budgetYen)}</dd>
        </div>
        <div>
          <dt>点数</dt>
          <dd>{plan.combinationCount.toLocaleString("ja-JP")}点</dd>
        </div>
        <div>
          <dt>購入合計</dt>
          <dd>{formatYen(plan.totalCostYen)}</dd>
        </div>
        <div>
          <dt>的中期待</dt>
          <dd>{formatPercent(plan.expectedHitProbability)}</dd>
        </div>
      </dl>

      <div className="filter-panel win5-budget-panel">
        <label htmlFor="win5-budget-slider">
          <span>予算を調整</span>
          <input
            aria-valuemin={1000}
            aria-valuemax={100000}
            aria-valuenow={budgetYen}
            id="win5-budget-slider"
            max={100000}
            min={1000}
            onChange={(event) => setBudgetYen(Number(event.target.value))}
            step={100}
            type="range"
            value={budgetYen}
          />
        </label>
        <output className="win5-budget-output" htmlFor="win5-budget-slider">
          {formatYen(budgetYen)}
        </output>
      </div>

      <div aria-label="予算プリセット" className="running-style-tabs" role="group">
        <button
          aria-pressed={budgetYen === WIN5_DEFAULT_BUDGET_YEN}
          className="running-style-tab"
          onClick={() => setBudgetYen(WIN5_DEFAULT_BUDGET_YEN)}
          type="button"
        >
          標準 {formatYen(WIN5_DEFAULT_BUDGET_YEN)}
        </button>
        <button
          aria-pressed={budgetYen === prediction.recommendedBudgetYen}
          className="running-style-tab"
          onClick={() => setBudgetYen(prediction.recommendedBudgetYen)}
          type="button"
        >
          推奨 {formatYen(prediction.recommendedBudgetYen)}
        </button>
      </div>

      <div aria-label="選択中の買い目" className="win5-pick-summary">
        {plan.selections.map((selection) => {
          const leg = prediction.legs.find((item) => item.leg.legIndex === selection.legIndex);
          const label = leg?.leg.raceLabel ?? `${selection.legIndex}レース目`;
          return (
            <div className="win5-pick-card" key={selection.legIndex}>
              <p className="win5-pick-label">{label}</p>
              <p className="win5-pick-numbers">{formatHorseNumbers(selection.horseNumbers)}</p>
            </div>
          );
        })}
      </div>

      <div className="section-heading compact win5-leg-list-heading">
        <h3>レース別の買い目</h3>
        <label className="win5-show-all-toggle">
          <input
            checked={showAllHorses}
            onChange={(event) => setShowAllHorses(event.target.checked)}
            type="checkbox"
          />
          <span>全ての馬を常に表示</span>
        </label>
      </div>

      <div className="win5-leg-list">
        {prediction.legs.map((legPrediction) => {
          const selection = plan.selections.find(
            (item) => item.legIndex === legPrediction.leg.legIndex,
          );
          const selectedNumbers = new Set(selection?.horseNumbers ?? []);
          const raceHref = buildRaceHref(
            year,
            month,
            day,
            legPrediction.leg.keibajoCode,
            legPrediction.leg.raceBango,
          );
          const venueName =
            legPrediction.leg.keibajoName ?? formatKeibajo(legPrediction.leg.keibajoCode);
          const raceTitle =
            legPrediction.leg.raceLabel ??
            `${venueName}${formatRaceNumber(legPrediction.leg.raceBango)}`;
          const visibleHorses = showAllHorses
            ? legPrediction.horses
            : legPrediction.horses.filter((horse) => selectedNumbers.has(horse.horseNumber));

          return (
            <section className="win5-leg-section" key={legPrediction.leg.legIndex}>
              <div className="section-heading compact">
                <div>
                  <h3>
                    第{legPrediction.leg.legIndex}レース {raceTitle}
                  </h3>
                  {legPrediction.leg.startTime ? (
                    <p className="win5-leg-meta">{legPrediction.leg.startTime} 発走</p>
                  ) : null}
                </div>
                <Link href={raceHref}>レース詳細</Link>
              </div>

              <div className="runner-table-wrap">
                <table className="runner-table win5-leg-table">
                  <thead>
                    <tr>
                      <th scope="col">買い</th>
                      <th scope="col">馬番</th>
                      <th scope="col">馬名</th>
                      <th scope="col">騎手</th>
                      <th scope="col">勝率</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleHorses.length === 0 ? (
                      <tr>
                        <td colSpan={5}>選択された馬がありません。</td>
                      </tr>
                    ) : (
                      visibleHorses.map((horse) => {
                        const isSelected = selectedNumbers.has(horse.horseNumber);
                        return (
                          <tr
                            className={isSelected ? "win5-pick-row" : undefined}
                            key={horse.horseNumber}
                          >
                            <td>
                              {isSelected ? (
                                <span className="win5-pick-badge">選択</span>
                              ) : (
                                <span className="win5-pick-badge muted">-</span>
                              )}
                            </td>
                            <td className="runner-number-cell">
                              <span>{horse.horseNumber}</span>
                            </td>
                            <td className="stats-name-cell">{cleanHorseName(horse.horseName)}</td>
                            <td className="stats-name-cell">{horse.jockeyName ?? "-"}</td>
                            <td>{formatPercent(horse.winProbability)}</td>
                          </tr>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>
            </section>
          );
        })}
      </div>

      <section className="win5-combination-section">
        <div className="section-heading compact">
          <h3>上位の組み合わせ</h3>
          <span>{plan.topCombinations.length}件</span>
        </div>
        <div className="runner-table-wrap">
          <table className="runner-table win5-combination-table">
            <thead>
              <tr>
                <th scope="col">順位</th>
                <th scope="col">5レースの馬番</th>
                <th scope="col">推定確率</th>
              </tr>
            </thead>
            <tbody>
              {plan.topCombinations.map((combination, index) => (
                <tr key={combination.legs.join("-")}>
                  <td>{index + 1}</td>
                  <td className="win5-combination-numbers">{combination.legs.join(" - ")}</td>
                  <td>{formatPercent(combination.probability)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
}
