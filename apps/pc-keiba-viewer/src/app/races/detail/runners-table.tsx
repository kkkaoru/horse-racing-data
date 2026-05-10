"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { useMemo, useState } from "react";

import { cleanText } from "../../../lib/format";
import type { Runner } from "../../../lib/race-types";
import {
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
} from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

type SortKey = "umaban" | "tanshoOdds" | "kakuteiChakujun";
type SortDirection = "asc" | "desc";

interface RunnersTableProps {
  decodeHexHorseWeight?: boolean;
  initialRealtimePayload?: RealtimeRacePayload | null;
  realtimeRequest?: RealtimeRaceRequest;
  runners: Runner[];
}

interface SortState {
  direction: SortDirection;
  key: SortKey;
}

const SORT_LABELS: Record<SortKey, string> = {
  umaban: "馬番号",
  tanshoOdds: "単勝",
  kakuteiChakujun: "着順",
};

const parseSortValue = (value: string | null | undefined, emptyValue?: string): number | null => {
  const cleaned = cleanText(value, "");
  if (!cleaned || cleaned === emptyValue) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const compareNullableNumber = (
  left: number | null,
  right: number | null,
  direction: SortDirection,
): number => {
  if (left === null && right === null) {
    return 0;
  }
  if (left === null) {
    return 1;
  }
  if (right === null) {
    return -1;
  }
  return direction === "asc" ? left - right : right - left;
};

const getSortValue = (
  runner: Runner,
  key: SortKey,
  realtimeOddsByHorse: Map<string, number>,
): number | null => {
  if (key === "tanshoOdds") {
    const horseNumber = formatRunnerNumber(runner.umaban);
    const realtimeOdds = realtimeOddsByHorse.get(horseNumber);
    if (realtimeOdds !== undefined) {
      return realtimeOdds;
    }
    return parseSortValue(runner.tanshoOdds, "0000");
  }
  if (key === "kakuteiChakujun") {
    return parseSortValue(runner.kakuteiChakujun, "00");
  }
  return parseSortValue(runner.umaban);
};

const formatRealtimeOdds = (value: number | undefined): string =>
  value === undefined ? "-" : value.toFixed(1);

export function RunnersTable({
  decodeHexHorseWeight = false,
  initialRealtimePayload = null,
  realtimeRequest,
  runners,
}: RunnersTableProps) {
  const [sort, setSort] = useState<SortState>({ direction: "asc", key: "umaban" });
  const { payload } = useRealtimeRacePayload(
    realtimeRequest ?? {
      apiBaseUrl: "",
      day: "",
      keibajoCode: "",
      month: "",
      raceNumber: "",
      source: "",
      year: "",
    },
    initialRealtimePayload,
  );
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (payload?.odds?.latest.tansho ?? [])
          .filter((row) => row.odds !== undefined)
          .map((row) => [row.combination, Number(row.odds)] as const),
      ),
    [payload],
  );
  const realtimeWeightByHorse = useMemo(
    () =>
      new Map(
        (payload?.horseWeights?.horses ?? []).map((horse) => [
          horse.horseNumber,
          formatHorseWeight(
            horse.weight === null ? null : String(horse.weight),
            horse.changeSign,
            horse.changeAmount === null ? null : String(horse.changeAmount),
          ),
        ]),
      ),
    [payload],
  );

  const sortedRunners = useMemo(
    () =>
      runners
        .map((runner, index) => ({ index, runner }))
        .toSorted((left, right) => {
          const compared = compareNullableNumber(
            getSortValue(left.runner, sort.key, realtimeOddsByHorse),
            getSortValue(right.runner, sort.key, realtimeOddsByHorse),
            sort.direction,
          );
          return compared === 0 ? left.index - right.index : compared;
        })
        .map(({ runner }) => runner),
    [realtimeOddsByHorse, runners, sort],
  );

  const changeSort = (key: SortKey) => {
    setSort((current) => ({
      direction: current.key === key && current.direction === "asc" ? "desc" : "asc",
      key,
    }));
  };

  const renderSortButton = (key: SortKey) => {
    const isCurrent = sort.key === key;
    const directionLabel = sort.direction === "asc" ? "昇順" : "降順";
    const nextDirectionLabel = isCurrent && sort.direction === "asc" ? "降順" : "昇順";

    return (
      <button
        aria-label={`${SORT_LABELS[key]}を${nextDirectionLabel}で並び替え`}
        className="runner-sort-button"
        type="button"
        onClick={() => {
          changeSort(key);
        }}
      >
        <span>{SORT_LABELS[key]}</span>
        <small>{isCurrent ? directionLabel : "並替"}</small>
      </button>
    );
  };

  const renderRunnerRow = (runner: Runner) => {
    const horseNumber = formatRunnerNumber(runner.umaban);
    const realtimeOdds = realtimeOddsByHorse.get(horseNumber);
    const realtimeWeight = realtimeWeightByHorse.get(horseNumber);

    return (
      <tr key={`${runner.umaban}-${runner.kettoTorokuBango}`}>
        <td>{horseNumber}</td>
        <td>{cleanText(runner.wakuban)}</td>
        <td className="runner-horse-cell">
          <strong>{cleanText(runner.bamei)}</strong>
        </td>
        <td>{formatSexAge(runner.seibetsuCode, runner.barei)}</td>
        <td>{cleanText(runner.futanJuryo)}</td>
        <td>{cleanText(runner.kishumeiRyakusho)}</td>
        <td>{cleanText(runner.chokyoshimeiRyakusho)}</td>
        <td>{cleanText(runner.banushimei)}</td>
        <td>
          {realtimeWeight ??
            formatHorseWeight(
              runner.bataiju,
              runner.zogenFugo,
              runner.zogenSa,
              decodeHexHorseWeight,
            )}
        </td>
        <td>
          {realtimeOdds === undefined
            ? formatRunnerValue(runner.tanshoOdds, "0000")
            : formatRealtimeOdds(realtimeOdds)}
        </td>
        <td>{formatRunnerValue(runner.kakuteiChakujun, "00")}</td>
      </tr>
    );
  };

  return (
    <div className="runner-table-wrap">
      <table className="runner-table">
        <colgroup>
          <col className="runner-col-number" />
          <col className="runner-col-frame" />
          <col className="runner-col-horse" />
          <col className="runner-col-sex-age" />
          <col className="runner-col-weight" />
          <col className="runner-col-person" />
          <col className="runner-col-person" />
          <col className="runner-col-owner" />
          <col className="runner-col-body" />
          <col className="runner-col-odds" />
          <col className="runner-col-finish" />
        </colgroup>
        <thead>
          <tr>
            <th>{renderSortButton("umaban")}</th>
            <th className="runner-frame-header">枠</th>
            <th>馬名</th>
            <th>性齢</th>
            <th>負担</th>
            <th>騎手</th>
            <th>調教師</th>
            <th>馬主</th>
            <th>馬体重</th>
            <th>{renderSortButton("tanshoOdds")}</th>
            <th>{renderSortButton("kakuteiChakujun")}</th>
          </tr>
        </thead>
        <tbody>{sortedRunners.map(renderRunnerRow)}</tbody>
      </table>
    </div>
  );
}
