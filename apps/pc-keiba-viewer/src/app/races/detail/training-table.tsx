"use client";

import { useMemo, useState } from "react";

import { cleanText, formatDate } from "../../../lib/format";
import type { Training } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { formatTracen, formatTrainingTime, formatWoodCourse } from "../../../lib/training-format";
import { MobileFilterDisclosure } from "./mobile-filter-disclosure";

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

const FURLONG_COLUMNS = [
  { key: "timeGokei6f", label: "6F" },
  { key: "timeGokei5f", label: "5F" },
  { key: "timeGokei4f", label: "4F" },
  { key: "timeGokei3f", label: "3F" },
  { key: "timeGokei2f", label: "2F" },
  { key: "lapTime1f", label: "1F" },
] as const satisfies readonly { key: SortKey; label: string }[];

const DEFAULT_VISIBLE_FURLONG_KEYS: SortKey[] = [
  "timeGokei4f",
  "timeGokei3f",
  "timeGokei2f",
  "lapTime1f",
];

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

const GRADE_PRIORITY = new Map<string, number>([
  ["◎", 1],
  ["SS", 1],
  ["S", 1],
  ["○", 2],
  ["◯", 2],
  ["A", 2],
  ["▲", 3],
  ["B", 3],
  ["△", 4],
  ["C", 4],
]);

const getTrainingGradePriority = (training: Training): number => {
  const grade = cleanText(training.premiumEvaluationGrade, "").toUpperCase();
  if (!grade) {
    return Number.POSITIVE_INFINITY;
  }
  const numericGrade = Number(grade);
  if (Number.isFinite(numericGrade)) {
    return numericGrade;
  }
  return GRADE_PRIORITY.get(grade) ?? Number.POSITIVE_INFINITY;
};

const compareByBestGradeThenOneF = (left: Training, right: Training): number => {
  const gradeCompared = compareNullableNumber(
    getTrainingGradePriority(left),
    getTrainingGradePriority(right),
    "asc",
  );
  if (gradeCompared !== 0) {
    return gradeCompared;
  }

  const oneFCompared = compareNullableNumber(
    getSortValue(left, "lapTime1f"),
    getSortValue(right, "lapTime1f"),
    "asc",
  );
  if (oneFCompared !== 0) {
    return oneFCompared;
  }

  return compareByFastestTraining(left, right);
};

const getTrainingCourseLabel = (training: Training): string =>
  formatWoodCourse(training.course, training.babamawari);

const getTrainingPlaceSummary = (training: Training): string => {
  const values = [
    formatTracen(training.tracenKubun),
    cleanText(training.trainingType, "-"),
    getTrainingCourseLabel(training),
  ].filter((value) => value && value !== "-");
  return values.length > 0 ? values.join(" / ") : "-";
};

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
  const [gradeOnly, setGradeOnly] = useState(true);
  const [visibleFurlongKeys, setVisibleFurlongKeys] = useState<SortKey[]>(
    DEFAULT_VISIBLE_FURLONG_KEYS,
  );
  const hasPremiumReviews = trainings.some(
    (training) =>
      cleanText(training.premiumEvaluationText, "") ||
      cleanText(training.premiumEvaluationGrade, ""),
  );
  const hasPremiumGrades = trainings.some((training) =>
    Boolean(cleanText(training.premiumEvaluationGrade, "")),
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
      if (hasPremiumGrades && gradeOnly && !cleanText(training.premiumEvaluationGrade, "")) {
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
          const compared =
            hasPremiumGrades && gradeOnly
              ? current
                ? compareByBestGradeThenOneF(training, current)
                : -1
              : current
                ? compareByFastestTraining(training, current)
                : -1;
          if (compared < 0) {
            byRunner.set(key, training);
          }
          return byRunner;
        }, new Map<string, Training>())
        .values(),
    ];
  }, [courseFilter, fastestOnly, gradeOnly, hasPremiumGrades, tracenFilter, trainings, typeFilter]);

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
  const visibleFurlongColumns = useMemo(
    () => FURLONG_COLUMNS.filter((column) => visibleFurlongKeys.includes(column.key)),
    [visibleFurlongKeys],
  );

  const toggleFurlongColumn = (key: SortKey) => {
    setVisibleFurlongKeys((current) => {
      if (current.includes(key)) {
        if (current.length === 1) {
          return current;
        }
        const next = current.filter((item) => item !== key);
        setSort((currentSort) =>
          currentSort.key === key ? { direction: "asc", key: "lapTime1f" } : currentSort,
        );
        return next;
      }
      return FURLONG_COLUMNS.map((column) => column.key).filter(
        (item) => item === key || current.includes(item),
      );
    });
  };

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
      <MobileFilterDisclosure title="検索メニュー">
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
          {hasPremiumGrades ? (
            <label className="training-checkbox-label">
              <span>記号ありのみ</span>
              <span className="training-checkbox-control">
                <input
                  aria-label="記号ありのみを表示"
                  checked={gradeOnly}
                  type="checkbox"
                  onChange={(event) => {
                    setGradeOnly(event.currentTarget.checked);
                  }}
                />
              </span>
            </label>
          ) : null}
          <fieldset className="training-furlong-fieldset">
            <legend>表示ハロン</legend>
            <span className="training-furlong-control">
              {FURLONG_COLUMNS.map((column) => (
                <label className="training-furlong-option" key={column.key}>
                  <input
                    aria-label={`${column.label}を表示`}
                    checked={visibleFurlongKeys.includes(column.key)}
                    type="checkbox"
                    onChange={() => {
                      toggleFurlongColumn(column.key);
                    }}
                  />
                  <span>{column.label}</span>
                </label>
              ))}
            </span>
          </fieldset>
          <span className="training-filter-count">
            {sortedTrainings.length} / {trainings.length} 件
          </span>
        </section>
      </MobileFilterDisclosure>
      <div className="training-table-wrap">
        <table className="training-table">
          <colgroup>
            <col className="training-col-runner-number" />
            <col className="training-col-horse" />
            <col className="training-col-jockey" />
            <col className="training-col-rider" />
            <col className="training-col-trainer" />
            <col className="training-col-date" />
            <col className="training-col-place-summary" />
            {visibleFurlongColumns.map((column) => (
              <col className="training-col-time" key={column.key} />
            ))}
            {hasPremiumReviews ? <col className="training-col-review" /> : null}
            {hasPremiumReviews ? <col className="training-col-review" /> : null}
          </colgroup>
          <thead>
            <tr>
              <th>{renderSortButton("umaban")}</th>
              <th>馬名</th>
              <th>騎手名</th>
              <th>騎乗</th>
              <th>調教師名</th>
              <th>日付</th>
              <th>場所 / 種別 / コース</th>
              {visibleFurlongColumns.map((column) => (
                <th key={column.key}>{renderSortButton(column.key)}</th>
              ))}
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
                <td>{cleanText(training.currentJockeyName, "-")}</td>
                <td>{cleanText(training.trainingRiderName, "-")}</td>
                <td>{cleanText(training.trainerName, "-")}</td>
                <td className="training-date-cell">
                  {formatDate(
                    training.chokyoNengappi.slice(0, 4),
                    training.chokyoNengappi.slice(4),
                  )}
                </td>
                <td className="training-course-cell">{getTrainingPlaceSummary(training)}</td>
                {visibleFurlongColumns.map((column) => (
                  <td key={column.key}>{formatTrainingTime(training[column.key])}</td>
                ))}
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
