"use client";

import { useMemo, useState } from "react";

import { cleanText, formatDate } from "../../../lib/format";
import type { Training } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { formatTracen, formatTrainingTime, formatWoodCourse } from "../../../lib/training-format";

interface TrainingTableProps {
  sourceLabel: string;
  trainings: Training[];
}

type SortDirection = "asc" | "desc";
type SortKey =
  | "umaban"
  | "timeGokei6f"
  | "timeGokei5f"
  | "timeGokei4f"
  | "timeGokei3f"
  | "timeGokei2f"
  | "lapTime1f";

interface SortState {
  direction: SortDirection;
  key: SortKey;
}

const ALL_FILTER = "all";

const SORT_LABELS: Record<SortKey, string> = {
  umaban: "馬番号",
  timeGokei6f: "6F",
  timeGokei5f: "5F",
  timeGokei4f: "4F",
  timeGokei3f: "3F",
  timeGokei2f: "2F",
  lapTime1f: "1F",
};

const PREMIUM_REVIEW_LABELS = {
  grade: process.env.NEXT_PUBLIC_PREMIUM_RACE_WORK_LABEL_GRADE ?? "記号",
  text: process.env.NEXT_PUBLIC_PREMIUM_RACE_WORK_LABEL_TEXT ?? "評価",
};

const parseTime = (value: string | null | undefined): number | null => {
  const formatted = formatTrainingTime(value);
  if (formatted === "-") {
    return null;
  }
  const parsed = Number(formatted);
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

const getSortValue = (training: Training, key: SortKey): number | null => {
  if (key === "umaban") {
    const parsed = Number(cleanText(training.umaban, ""));
    return Number.isFinite(parsed) ? parsed : null;
  }
  return parseTime(training[key]);
};

const getTrainingDateTimeValue = (training: Training): number | null => {
  const parsed = Number(`${training.chokyoNengappi}${training.chokyoJikoku}`);
  return Number.isFinite(parsed) ? parsed : null;
};

const compareByFastestTraining = (left: Training, right: Training): number => {
  const timeKeys: SortKey[] = [
    "lapTime1f",
    "timeGokei2f",
    "timeGokei3f",
    "timeGokei4f",
    "timeGokei5f",
    "timeGokei6f",
  ];

  for (const key of timeKeys) {
    const compared = compareNullableNumber(
      getSortValue(left, key),
      getSortValue(right, key),
      "asc",
    );
    if (compared !== 0) {
      return compared;
    }
  }

  return compareNullableNumber(
    getTrainingDateTimeValue(left),
    getTrainingDateTimeValue(right),
    "desc",
  );
};

const getTrainingCourseLabel = (training: Training): string =>
  formatWoodCourse(training.course, training.babamawari);

const getUniqueOptions = (values: string[]): string[] =>
  [...new Set(values)]
    .filter((value) => value !== "-")
    .toSorted((a, b) => a.localeCompare(b, "ja"));

export function TrainingTable({ sourceLabel, trainings }: TrainingTableProps) {
  const [sort, setSort] = useState<SortState>({ direction: "asc", key: "lapTime1f" });
  const [typeFilter, setTypeFilter] = useState(ALL_FILTER);
  const [tracenFilter, setTracenFilter] = useState(ALL_FILTER);
  const [courseFilter, setCourseFilter] = useState(ALL_FILTER);
  const [fastestOnly, setFastestOnly] = useState(true);
  const hasPremiumReviews = trainings.some(
    (training) =>
      cleanText(training.premiumEvaluationText, "") ||
      cleanText(training.premiumEvaluationGrade, ""),
  );

  const filterOptions = useMemo(
    () => ({
      courses: getUniqueOptions(trainings.map(getTrainingCourseLabel)),
      tracens: getUniqueOptions(trainings.map((training) => formatTracen(training.tracenKubun))),
      types: getUniqueOptions(trainings.map((training) => training.trainingType)),
    }),
    [trainings],
  );

  const filteredTrainings = useMemo(() => {
    const matchedTrainings = trainings.filter((training) => {
      if (typeFilter !== ALL_FILTER && training.trainingType !== typeFilter) {
        return false;
      }
      if (tracenFilter !== ALL_FILTER && formatTracen(training.tracenKubun) !== tracenFilter) {
        return false;
      }
      if (courseFilter !== ALL_FILTER && getTrainingCourseLabel(training) !== courseFilter) {
        return false;
      }
      return true;
    });

    if (!fastestOnly) {
      return matchedTrainings;
    }

    return [
      ...matchedTrainings
        .reduce((byRunner, training) => {
          const key = cleanText(training.umaban, "");
          const current = byRunner.get(key);
          if (!current || compareByFastestTraining(training, current) < 0) {
            byRunner.set(key, training);
          }
          return byRunner;
        }, new Map<string, Training>())
        .values(),
    ];
  }, [courseFilter, fastestOnly, tracenFilter, trainings, typeFilter]);

  const sortedTrainings = useMemo(
    () =>
      filteredTrainings
        .map((training, index) => ({ index, training }))
        .toSorted((left, right) => {
          const compared = compareNullableNumber(
            getSortValue(left.training, sort.key),
            getSortValue(right.training, sort.key),
            sort.direction,
          );
          return compared === 0 ? left.index - right.index : compared;
        })
        .map(({ training }) => training),
    [filteredTrainings, sort],
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
        className="training-sort-button"
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

  if (trainings.length === 0) {
    return (
      <p className="empty-state">
        {sourceLabel}の馬ごとの調教・追い切りデータは見つかりませんでした。
      </p>
    );
  }

  return (
    <>
      <section className="training-filter-panel" aria-label="training filters">
        <label>
          <span>種別</span>
          <select
            value={typeFilter}
            onChange={(event) => {
              setTypeFilter(event.currentTarget.value);
            }}
          >
            <option value={ALL_FILTER}>すべて</option>
            {filterOptions.types.map((option) => (
              <option value={option} key={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>場所</span>
          <select
            value={tracenFilter}
            onChange={(event) => {
              setTracenFilter(event.currentTarget.value);
            }}
          >
            <option value={ALL_FILTER}>すべて</option>
            {filterOptions.tracens.map((option) => (
              <option value={option} key={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>コース</span>
          <select
            value={courseFilter}
            onChange={(event) => {
              setCourseFilter(event.currentTarget.value);
            }}
          >
            <option value={ALL_FILTER}>すべて</option>
            {filterOptions.courses.map((option) => (
              <option value={option} key={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <label className="training-checkbox-label">
          <span>最速のレコードのみを表示</span>
          <span className="training-checkbox-control">
            <input
              aria-label="最速のレコードのみを表示"
              checked={fastestOnly}
              type="checkbox"
              onChange={(event) => {
                setFastestOnly(event.currentTarget.checked);
              }}
            />
          </span>
        </label>
        <span className="training-filter-count">
          {sortedTrainings.length} / {trainings.length} 件
        </span>
      </section>
      <div className="training-table-wrap">
        <table className="training-table">
          <colgroup>
            <col className="training-col-runner-number" />
            <col className="training-col-horse" />
            <col className="training-col-date" />
            <col className="training-col-place" />
            <col className="training-col-type" />
            <col className="training-col-course" />
            <col className="training-col-time" />
            <col className="training-col-time" />
            <col className="training-col-time" />
            <col className="training-col-time" />
            <col className="training-col-time" />
            <col className="training-col-time" />
            {hasPremiumReviews ? <col className="training-col-review" /> : null}
            {hasPremiumReviews ? <col className="training-col-review" /> : null}
          </colgroup>
          <thead>
            <tr>
              <th>{renderSortButton("umaban")}</th>
              <th>馬名</th>
              <th>日付</th>
              <th>場所</th>
              <th>種別</th>
              <th>コース</th>
              <th>{renderSortButton("timeGokei6f")}</th>
              <th>{renderSortButton("timeGokei5f")}</th>
              <th>{renderSortButton("timeGokei4f")}</th>
              <th>{renderSortButton("timeGokei3f")}</th>
              <th>{renderSortButton("timeGokei2f")}</th>
              <th>{renderSortButton("lapTime1f")}</th>
              {hasPremiumReviews ? <th>{PREMIUM_REVIEW_LABELS.text}</th> : null}
              {hasPremiumReviews ? <th>{PREMIUM_REVIEW_LABELS.grade}</th> : null}
            </tr>
          </thead>
          <tbody>
            {sortedTrainings.map((training) => (
              <tr
                key={`${training.umaban}-${training.trainingType}-${training.chokyoNengappi}-${training.chokyoJikoku}`}
              >
                <td>{formatRunnerNumber(training.umaban)}</td>
                <td className="training-horse-cell">{cleanText(training.bamei)}</td>
                <td className="training-date-cell">
                  {formatDate(
                    training.chokyoNengappi.slice(0, 4),
                    training.chokyoNengappi.slice(4),
                  )}
                </td>
                <td>{formatTracen(training.tracenKubun)}</td>
                <td>{training.trainingType}</td>
                <td className="training-course-cell">{getTrainingCourseLabel(training)}</td>
                <td>{formatTrainingTime(training.timeGokei6f)}</td>
                <td>{formatTrainingTime(training.timeGokei5f)}</td>
                <td>{formatTrainingTime(training.timeGokei4f)}</td>
                <td>{formatTrainingTime(training.timeGokei3f)}</td>
                <td>{formatTrainingTime(training.timeGokei2f)}</td>
                <td>{formatTrainingTime(training.lapTime1f)}</td>
                {hasPremiumReviews ? (
                  <td>{cleanText(training.premiumEvaluationText, "-")}</td>
                ) : null}
                {hasPremiumReviews ? (
                  <td>{cleanText(training.premiumEvaluationGrade, "-")}</td>
                ) : null}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
