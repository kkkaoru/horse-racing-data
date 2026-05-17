"use client";

import Link from "next/link";
import { memo, type CSSProperties, useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatTrack,
} from "../../../lib/format";
import {
  getPreferredJockeyName,
  isSameJockeyName,
  normalizeJockeyNameForComparison,
} from "../../../lib/jockey-name";
import {
  isPaddockState,
  applyPaddockAction,
  normalizePaddockHorseScore,
  type PaddockAction,
  type PaddockMetric,
  type PaddockOfficialRank,
  type PaddockState,
} from "../../../lib/paddock";
import { getRaceTags } from "../../../lib/race-classification";
import type { HorseRaceResult, PremiumPaddockBulletin, Runner } from "../../../lib/race-types";
import {
  formatHorseWeight,
  formatRunnerNumber,
  formatSexAge,
  isBanEiKeibajoCode,
} from "../../../lib/runner-format";
import { FrameNumberBadge, HorseNameBadge } from "./frame-number-badge";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface PaddockSectionProps {
  day: string;
  detailUrl?: string;
  decodeHexHorseWeight?: boolean;
  editFooterDetailPath?: string;
  editable?: boolean;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  raceNumberLabel?: string;
  racePlace?: string;
  raceMeta?: string;
  raceStartsAt?: string | null;
  raceStartsAtLabel?: string;
  raceTitle?: string;
  realtimeRequest?: RealtimeRaceRequest;
  recentResults?: HorseRaceResult[];
  runners: Runner[];
  source: RaceSource;
  year: string;
}

interface PaddockHorseRowProps {
  editable: boolean;
  horseName: string;
  horseNumber: string;
  frameNumber: string | null;
  jockeyName: string;
  moshokuCode?: string | null;
  onScore: (action: PaddockAction) => void;
  originalJockeyName: string;
  recentResults: HorseRaceResult[] | null;
  realtimeOdds: number | null;
  realtimeJockeyName: string | null;
  realtimePopularity: number | null;
  scores: {
    attention: number;
    kaeshi: number;
    officialRank: PaddockOfficialRank | null;
    paddock: number;
    preference: number;
    total: number;
  };
  sexAge: string;
  status: string | null;
  trainingEvaluationGrade: string | null;
  weight: string;
}

interface PaddockRunnerRow {
  horseName: string;
  horseNumber: string;
  frameNumber: string | null;
  index: number;
  jockeyName: string;
  moshokuCode?: string | null;
  sexAge: string;
  status: string;
  weight: string;
}

type PaddockTableRowStyle = CSSProperties & {
  "--paddock-row-cell-padding-y": string;
  "--paddock-row-font-size": string;
};

type PaddockTableStyle = CSSProperties & {
  "--paddock-jockey-col-width": string;
  "--paddock-mobile-table-width": string;
  "--paddock-name-col-width": string;
};

type PaddockScoreTooltipState = {
  horseName: string;
  horseNumber: string;
  left: number;
  placement: "bottom" | "top";
  top: number;
  total: number;
};

const METRIC_LABELS: Record<PaddockMetric, { minus: string; plus: string; title: string }> = {
  attention: { minus: "注目-", plus: "注目+", title: "注目度" },
  kaeshi: { minus: "返し-", plus: "返し+", title: "返し" },
  paddock: { minus: "気配-", plus: "気配+", title: "パドック" },
  preference: { minus: "嫌い", plus: "好き", title: "好み" },
};
const METRIC_ORDER = [
  "paddock",
  "attention",
  "preference",
  "kaeshi",
] as const satisfies readonly PaddockMetric[];
const DEFAULT_REMOTE_PADDOCK_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const PUBLIC_DETAIL_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const OFFICIAL_RANK_OPTIONS: PaddockOfficialRank[] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
const PAST_RACE_EDIT_UNLOCK_MS = 10 * 60 * 1000;

const isLocalHost = (hostname: string): boolean =>
  hostname === "localhost" || hostname === "127.0.0.1" || hostname === "0.0.0.0";

const getPaddockRequestUrl = (path: string): string => {
  if (
    typeof window !== "undefined" &&
    isLocalHost(window.location.hostname) &&
    process.env.NEXT_PUBLIC_PC_KEIBA_PADDOCK_REMOTE_BINDINGS !== "0"
  ) {
    return `${process.env.NEXT_PUBLIC_PC_KEIBA_PADDOCK_REMOTE_ORIGIN ?? DEFAULT_REMOTE_PADDOCK_ORIGIN}${path}`;
  }
  return path;
};

const getPaddockLiveUrl = (path: string): string => {
  const requestUrl = getPaddockRequestUrl(path);
  if (!requestUrl.startsWith("http")) {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${protocol}//${window.location.host}${requestUrl}`;
  }
  const liveUrl = new URL(requestUrl);
  liveUrl.protocol = liveUrl.protocol === "http:" ? "ws:" : "wss:";
  return liveUrl.toString();
};

const getPastRaceEditSessionKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}): string => `paddock:past-edit-until:${year}${month}${day}:${keibajoCode}:${raceNumber}`;

const isPastRace = (raceStartsAt: string | null | undefined): boolean => {
  if (!raceStartsAt) {
    return false;
  }
  const startsAt = new Date(raceStartsAt).getTime();
  return Number.isFinite(startsAt) && startsAt <= Date.now();
};

const readPastRaceEditUnlocked = (storageKey: string): boolean => {
  try {
    const expiresAt = Number(window.sessionStorage.getItem(storageKey));
    return Number.isFinite(expiresAt) && expiresAt > Date.now();
  } catch {
    return false;
  }
};

const writePastRaceEditUnlocked = (storageKey: string): void => {
  try {
    window.sessionStorage.setItem(storageKey, String(Date.now() + PAST_RACE_EDIT_UNLOCK_MS));
  } catch {
    // sessionStorage が使えない環境でも、その場の編集操作は許可する。
  }
};

const formatOfficialRank = (rank: PaddockOfficialRank | null | undefined): string =>
  rank ? `${rank}` : "-";

const formatPaddockScore = (value: number): string =>
  Number.isInteger(value) ? String(value) : value.toFixed(1);

const CIRCLED_NUMBER_LABELS = ["", "①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⑩"] as const;

const formatPaddockScoreRuby = (value: number): string =>
  Number.isInteger(value) && value >= 1 && value <= 10
    ? (CIRCLED_NUMBER_LABELS[value] ?? String(value))
    : formatPaddockScore(value);

const parseHorseWeightLabel = (
  value: string,
): {
  change: string | null;
  weight: string;
} => {
  const match = /^(.+?)\s*\(([^)]+)\)$/u.exec(value);
  if (!match) {
    return { change: null, weight: value };
  }
  return { change: match[2] ?? null, weight: match[1] ?? value };
};

function PaddockWeightValue({ value }: { value: string }) {
  const parsed = parseHorseWeightLabel(value);
  const changeClassName = parsed.change?.startsWith("+")
    ? "paddock-weight-change-plus"
    : parsed.change?.startsWith("-")
      ? "paddock-weight-change-minus"
      : parsed.change === "0"
        ? "paddock-weight-change-zero"
        : "paddock-weight-change-neutral";
  return (
    <span className="paddock-weight-value">
      <strong>{parsed.weight}</strong>
      {parsed.change ? <em className={changeClassName}>{parsed.change}</em> : null}
    </span>
  );
}

const formatRealtimePopularity = (value: number | null): string =>
  value === null ? "-" : `${value}`;

const formatRealtimeOdds = (value: number | null): string =>
  value === null ? "-" : value.toFixed(1);

const parseStoredNumber = (value: string | null | undefined, emptyValue = ""): number | null => {
  const cleaned = cleanText(value, "").trim();
  if (!cleaned || cleaned === emptyValue || cleaned.toUpperCase() === "FFF") {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const formatPastRank = (value: string | null | undefined): string => {
  const rank = parseStoredNumber(value, "00");
  return rank === null ? "-" : `${rank}着`;
};

const formatPastOdds = (value: string | null | undefined): string => {
  const odds = parseStoredNumber(value, "0000");
  return odds === null ? "-" : (odds / 10).toFixed(1);
};

const formatPastPopularity = (value: string | null | undefined): string => {
  const popularity = parseStoredNumber(value, "00");
  return popularity === null ? "-" : `${popularity}番 人気`;
};

const formatPastRaceName = (result: HorseRaceResult): string => {
  const names = [
    cleanText(result.kyosomeiHondai, ""),
    cleanText(result.kyosomeiFukudai, ""),
    cleanText(result.kyosomeiKakkonai, ""),
  ].filter(Boolean);
  return names.length > 0 ? names.join(" / ") : "一般競走";
};

const formatPastRaceConditions = (result: HorseRaceResult): string => {
  const tags = getRaceTags(result);
  return tags.length > 0 ? tags.join(" / ") : cleanText(result.kyosoJokenMeisho, "-");
};

const formatPastResultMeta = (result: HorseRaceResult): string =>
  [
    formatDate(result.kaisaiNen, result.kaisaiTsukihi),
    formatKeibajo(result.keibajoCode),
    formatTrack(result.trackCode),
    formatDistance(result.kyori),
  ].join(" / ");

function PaddockRecentResults({ results }: { results: HorseRaceResult[] | null }) {
  if (results === null) {
    return null;
  }

  if (results.length === 0) {
    return (
      <section className="paddock-recent-results paddock-recent-results-empty">
        <h3>近走</h3>
        <p>
          <strong>新馬</strong>
          <span>初出走</span>
        </p>
      </section>
    );
  }

  return (
    <section className="paddock-recent-results" aria-label="近走成績">
      <h3>近走</h3>
      <ol>
        {results.slice(0, 3).map((result) => (
          <li
            key={`${result.kaisaiNen}${result.kaisaiTsukihi}-${result.keibajoCode}-${result.raceBango}`}
          >
            <span className="paddock-recent-finish">{formatPastRank(result.kakuteiChakujun)}</span>
            <span className="paddock-recent-race">
              <strong>{formatPastRaceName(result)}</strong>
              <small>{formatPastRaceConditions(result)}</small>
              <small>{formatPastResultMeta(result)}</small>
            </span>
            <span className="paddock-recent-stats">
              <span>{formatPastPopularity(result.tanshoNinkijun)}</span>
              <span>{formatPastOdds(result.tanshoOdds)}</span>
              <span>
                {formatHorseWeight(
                  result.bataiju,
                  result.zogenFugo,
                  result.zogenSa,
                  isBanEiKeibajoCode(result.keibajoCode),
                )}
              </span>
            </span>
          </li>
        ))}
      </ol>
    </section>
  );
}

const isChangedJockey = (
  storedName: string,
  realtimeName: string | null,
  displayName: string,
): boolean => {
  if (!realtimeName || storedName === "") {
    return false;
  }
  if (
    normalizeJockeyNameForComparison(storedName) === normalizeJockeyNameForComparison(displayName)
  ) {
    return false;
  }
  return !isSameJockeyName(storedName, realtimeName);
};

const getOfficialRankClassName = (rank: PaddockOfficialRank | null | undefined): string =>
  rank ? `paddock-rank-badge rank-${rank}` : "paddock-rank-badge";

function PaddockOfficialRankQuickPanel({
  rows,
  state,
  onOfficialRank,
}: {
  rows: PaddockRunnerRow[];
  state: PaddockState | null;
  onOfficialRank: (action: PaddockAction) => void;
}) {
  const scoredRows = rows.map((runner) => ({
    runner,
    scores: normalizePaddockHorseScore(state?.horses[runner.horseNumber], runner),
  }));
  const activeRows = scoredRows.filter(({ runner }) => runner.status === "");
  const selectedByRank = new Map<PaddockOfficialRank, (typeof scoredRows)[number]>();
  for (const row of scoredRows) {
    if (row.scores.officialRank) {
      selectedByRank.set(row.scores.officialRank, row);
    }
  }

  const submitOfficialRank = (
    runner: PaddockRunnerRow,
    currentRank: PaddockOfficialRank | null,
    nextRank: PaddockOfficialRank | null,
  ) => {
    onOfficialRank({
      horseName: runner.horseName,
      horseNumber: runner.horseNumber,
      rank: currentRank === nextRank ? null : nextRank,
      type: "official-rank",
    });
  };

  return (
    <section className="paddock-official-rank-quick" aria-label="公式評価順の馬番指定">
      <header>
        <h3>公式評価順</h3>
        <span>馬番だけで指定</span>
      </header>
      <div className="paddock-official-rank-quick-list">
        {OFFICIAL_RANK_OPTIONS.map((rank) => {
          const selected = selectedByRank.get(rank);
          return (
            <section className={`paddock-official-rank-slot rank-${rank}`} key={rank}>
              <div className="paddock-official-rank-slot-head">
                <span>{rank}</span>
                <strong>{selected ? formatRunnerNumber(selected.runner.horseNumber) : "-"}</strong>
                {selected ? (
                  <button
                    aria-label={`公式評価順 ${rank} を解除`}
                    type="button"
                    onClick={() => submitOfficialRank(selected.runner, rank, null)}
                  >
                    解除
                  </button>
                ) : null}
              </div>
              <div className="paddock-official-horse-number-grid">
                {activeRows.map(({ runner, scores }) => (
                  <button
                    aria-label={`公式評価順 ${rank} に ${runner.horseNumber}番を指定`}
                    aria-pressed={scores.officialRank === rank}
                    className={scores.officialRank === rank ? "selected" : undefined}
                    key={runner.horseNumber}
                    type="button"
                    onClick={() => submitOfficialRank(runner, scores.officialRank, rank)}
                  >
                    {formatRunnerNumber(runner.horseNumber)}
                  </button>
                ))}
              </div>
            </section>
          );
        })}
      </div>
    </section>
  );
}

const formatHistoryDate = (value: string): string => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("ja-JP", {
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    month: "2-digit",
    second: "2-digit",
  }).format(date);
};

const getPaddockApiPath = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  year,
}: Omit<PaddockSectionProps, "runners" | "source">): string =>
  `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/paddock`;

const getPaddockDiscordApiPath = (props: Omit<PaddockSectionProps, "runners" | "source">): string =>
  `${getPaddockApiPath(props)}/discord`;

const getPremiumRaceRequestPath = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: Pick<
  PaddockSectionProps,
  "day" | "keibajoCode" | "month" | "raceNumber" | "source" | "year"
>): string =>
  `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/premium?source=${encodeURIComponent(source)}`;

const isPremiumPaddockBulletin = (value: unknown): value is PremiumPaddockBulletin => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "groupKey" in value &&
    "horseNumber" in value &&
    typeof value.groupKey === "string" &&
    typeof value.horseNumber === "string"
  );
};

interface PremiumTrainingReview {
  evaluationGrade: string | null;
  horseNumber: string;
  trainingDate: string;
}

const isPremiumTrainingReview = (value: unknown): value is PremiumTrainingReview => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "horseNumber" in value &&
    "trainingDate" in value &&
    typeof value.horseNumber === "string" &&
    typeof value.trainingDate === "string" &&
    (!("evaluationGrade" in value) ||
      value.evaluationGrade === null ||
      typeof value.evaluationGrade === "string")
  );
};

const TRAINING_GRADE_PRIORITY = new Map<string, number>([
  ["1", 0],
  ["◎", 0],
  ["SS", 0],
  ["S", 1],
  ["2", 2],
  ["○", 2],
  ["◯", 2],
  ["A+", 2],
  ["A", 3],
  ["3", 4],
  ["▲", 4],
  ["B", 4],
  ["△", 5],
  ["C", 5],
  ["D", 6],
]);

const getTrainingGradePriority = (value: string): number => {
  const normalized = cleanText(value, "").toUpperCase();
  if (!normalized) {
    return Number.POSITIVE_INFINITY;
  }
  return TRAINING_GRADE_PRIORITY.get(normalized) ?? 100 + (normalized.codePointAt(0) ?? 0);
};

const toBestTrainingGradeByHorse = (reviews: PremiumTrainingReview[]): Map<string, string> => {
  const byHorse = new Map<string, string>();
  for (const review of reviews) {
    const horseNumber = formatRunnerNumber(review.horseNumber);
    const grade = cleanText(review.evaluationGrade, "");
    if (!horseNumber || horseNumber === "-" || !grade) {
      continue;
    }
    const current = byHorse.get(horseNumber);
    if (!current || getTrainingGradePriority(grade) < getTrainingGradePriority(current)) {
      byHorse.set(horseNumber, grade);
    }
  }
  return byHorse;
};

const getPremiumPaddockEvaluationRank = (evaluationText: string | null | undefined): number => {
  const normalized = cleanText(evaluationText, "").toUpperCase();
  const grade = normalized.match(/[ABC]/)?.[0];
  if (grade === "A") {
    return 0;
  }
  if (grade === "B") {
    return 1;
  }
  if (grade === "C") {
    return 2;
  }
  return 3;
};

const comparePremiumPaddockBulletins = (
  left: PremiumPaddockBulletin,
  right: PremiumPaddockBulletin,
): number => {
  const gradeDiff =
    getPremiumPaddockEvaluationRank(left.evaluationText) -
    getPremiumPaddockEvaluationRank(right.evaluationText);
  if (gradeDiff !== 0) {
    return gradeDiff;
  }
  return Number(left.horseNumber) - Number(right.horseNumber);
};

function PremiumPaddockBulletinTable({
  notifyDisabled,
  notifyLabel,
  onNotify,
  rows,
}: {
  notifyDisabled: boolean;
  notifyLabel: string;
  onNotify: () => void;
  rows: PremiumPaddockBulletin[];
}) {
  if (rows.length === 0) {
    return null;
  }
  const sortedRows = rows.toSorted(comparePremiumPaddockBulletins);
  const labels = {
    comment: process.env.NEXT_PUBLIC_PREMIUM_RACE_PADDOCK_LABEL_COMMENT ?? "コメント",
    evaluation: process.env.NEXT_PUBLIC_PREMIUM_RACE_PADDOCK_LABEL_EVALUATION ?? "評価",
    horseName: process.env.NEXT_PUBLIC_PREMIUM_RACE_PADDOCK_LABEL_HORSE_NAME ?? "馬名",
    horseNumber: process.env.NEXT_PUBLIC_PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER ?? "馬番",
  };
  return (
    <section className="premium-paddock-section">
      <div className="section-heading compact">
        <h2>パドック速報</h2>
        <button
          className="paddock-external-discord-button"
          disabled={notifyDisabled}
          type="button"
          onClick={onNotify}
        >
          {notifyLabel}
        </button>
      </div>
      <div className="stats-table-wrap premium-paddock-table-wrap">
        <table className="stats-table premium-paddock-table">
          <thead>
            <tr>
              <th>{labels.horseNumber}</th>
              <th>{labels.horseName}</th>
              <th>{labels.evaluation}</th>
              <th>{labels.comment}</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => (
              <tr key={`${row.groupKey}-${row.horseNumber}`}>
                <td>{row.horseNumber}</td>
                <td>{row.horseName ?? "-"}</td>
                <td className="stats-score-cell">{row.evaluationText ?? "-"}</td>
                <td className="premium-paddock-comment-cell">{row.commentText ?? "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

const PaddockHorseRow = memo(function PaddockHorseRow({
  editable,
  frameNumber,
  horseName,
  horseNumber,
  jockeyName,
  moshokuCode,
  onScore,
  originalJockeyName,
  recentResults,
  realtimeOdds,
  realtimeJockeyName,
  realtimePopularity,
  scores,
  sexAge,
  status,
  trainingEvaluationGrade,
  weight,
}: PaddockHorseRowProps) {
  const score = (category: PaddockMetric, delta: -1 | 1) => {
    onScore({ category, delta, horseName, horseNumber });
  };
  const displayJockeyName = getPreferredJockeyName(jockeyName, realtimeJockeyName);
  const isScratched = Boolean(status);

  return (
    <article
      aria-label={`${formatRunnerNumber(horseNumber)}番 ${horseName}`}
      className={
        isScratched ? "paddock-horse-row paddock-horse-row-scratched" : "paddock-horse-row"
      }
      data-entry-status={status ?? undefined}
    >
      <header className="paddock-horse-summary">
        <dl className="paddock-horse-ids">
          <div>
            <dt>枠番</dt>
            <dd>
              <FrameNumberBadge value={frameNumber} />
            </dd>
          </div>
          <div>
            <dt>馬番</dt>
            <dd>{formatRunnerNumber(horseNumber)}</dd>
            {status ? <em className="entry-status-mini">{status}</em> : null}
          </div>
        </dl>
        <div className="paddock-horse-name-block">
          <span className="paddock-horse-name">
            <HorseNameBadge coatCode={moshokuCode} name={horseName} showCoatLabel={false} />
          </span>
          {displayJockeyName ? (
            <span aria-label={`騎手名 ${displayJockeyName}`} className="paddock-horse-jockey-line">
              <strong>{displayJockeyName}</strong>
              {isChangedJockey(originalJockeyName, realtimeJockeyName, displayJockeyName) ? (
                <small>元 {originalJockeyName}</small>
              ) : null}
            </span>
          ) : null}
          {status ? <span className="paddock-status-badge">{status}</span> : null}
        </div>
        <dl className="paddock-horse-race-facts">
          <div>
            <dt>性齢</dt>
            <dd>{sexAge}</dd>
          </div>
          <div className="paddock-horse-weight-fact">
            <dt>馬体重</dt>
            <dd>
              <PaddockWeightValue value={weight} />
            </dd>
          </div>
          <div>
            <dt>人気</dt>
            <dd>{formatRealtimePopularity(realtimePopularity)}</dd>
          </div>
          <div>
            <dt>単勝</dt>
            <dd>{formatRealtimeOdds(realtimeOdds)}</dd>
          </div>
          {editable && scores.officialRank ? (
            <div className="paddock-official-rank-fact">
              <dt>公式評価順</dt>
              <dd>
                <span className={getOfficialRankClassName(scores.officialRank)}>
                  {formatOfficialRank(scores.officialRank)}
                </span>
              </dd>
            </div>
          ) : null}
          {trainingEvaluationGrade ? (
            <div className="paddock-training-grade-fact">
              <dt>調教</dt>
              <dd>
                <span className="paddock-training-grade-badge">{trainingEvaluationGrade}</span>
              </dd>
            </div>
          ) : null}
        </dl>
        <b>{formatPaddockScore(scores.total)}</b>
      </header>
      <PaddockRecentResults results={recentResults} />
      {editable && isScratched ? (
        <div className="paddock-score-unavailable" aria-disabled="true">
          <strong>{status}</strong>
          <span>評価対象外</span>
        </div>
      ) : editable ? (
        <ul className="paddock-score-controls" aria-label={`${horseName}のパドック評価`}>
          {METRIC_ORDER.map((metric) => (
            <li className="paddock-score-control" key={metric}>
              <span>{METRIC_LABELS[metric].title}</span>
              <button
                aria-label={`${horseName} ${METRIC_LABELS[metric].minus}`}
                type="button"
                onClick={() => score(metric, -1)}
              >
                -
              </button>
              <output>{scores[metric]}</output>
              <button
                aria-label={`${horseName} ${METRIC_LABELS[metric].plus}`}
                type="button"
                onClick={() => score(metric, 1)}
              >
                +
              </button>
            </li>
          ))}
        </ul>
      ) : null}
      {!editable ? (
        <div className="paddock-score-readout">
          <span>
            <small>公式評価順</small>
            <strong
              className={
                scores.officialRank ? getOfficialRankClassName(scores.officialRank) : undefined
              }
            >
              {formatOfficialRank(scores.officialRank)}
            </strong>
          </span>
          {METRIC_ORDER.map((metric) => (
            <span key={metric}>
              <small>{METRIC_LABELS[metric].title}</small>
              <strong>{scores[metric]}</strong>
            </span>
          ))}
        </div>
      ) : null}
    </article>
  );
});

function PaddockReadOnlyTable({
  oddsByHorse,
  rows,
  state,
}: {
  oddsByHorse: Map<
    string,
    {
      odds: number | null;
      popularity: number | null;
    }
  >;
  rows: {
    frameNumber: string | null;
    horseName: string;
    horseNumber: string;
    jockeyName: string;
    moshokuCode?: string | null;
    weight: string;
  }[];
  state: PaddockState | null;
}) {
  const [activeScoreTooltip, setActiveScoreTooltip] = useState<PaddockScoreTooltipState | null>(
    null,
  );
  const evaluatedRows = rows
    .map((runner) => ({
      ...runner,
      scores: state?.horses[runner.horseNumber]
        ? normalizePaddockHorseScore(state.horses[runner.horseNumber], runner)
        : undefined,
    }))
    .filter((row) => row.scores !== undefined)
    .toSorted((left, right) => {
      const leftRank = left.scores?.officialRank ?? Number.POSITIVE_INFINITY;
      const rightRank = right.scores?.officialRank ?? Number.POSITIVE_INFINITY;
      if (leftRank !== rightRank) {
        return leftRank - rightRank;
      }
      const totalDiff = (right.scores?.total ?? 0) - (left.scores?.total ?? 0);
      if (totalDiff !== 0) {
        return totalDiff;
      }
      return Number(left.horseNumber) - Number(right.horseNumber);
    });

  const showScoreTooltip = useCallback(
    (row: (typeof evaluatedRows)[number], target: HTMLElement) => {
      const total = row.scores?.total ?? 0;
      if (total === 0) {
        setActiveScoreTooltip(null);
        return;
      }
      const rect = target.getBoundingClientRect();
      const estimatedWidth = 280;
      const edgePadding = 16;
      const left = Math.min(
        window.innerWidth - estimatedWidth / 2 - edgePadding,
        Math.max(estimatedWidth / 2 + edgePadding, rect.left + rect.width / 2),
      );
      const showAbove = rect.top > 92;
      setActiveScoreTooltip({
        horseName: row.horseName,
        horseNumber: row.horseNumber,
        left,
        placement: showAbove ? "top" : "bottom",
        top: showAbove ? rect.top - 10 : rect.bottom + 10,
        total,
      });
    },
    [],
  );

  if (evaluatedRows.length === 0) {
    return <p className="empty-state">パドック評価はまだありません。</p>;
  }
  const maxTotal = Math.max(...evaluatedRows.map((row) => row.scores?.total ?? 0));
  const minTotal = Math.min(...evaluatedRows.map((row) => row.scores?.total ?? 0));
  const totalRange = Math.max(1, maxTotal - minTotal);
  const requiredNameWidth = Math.max(
    ...evaluatedRows.map((row) => {
      const total = row.scores?.total ?? 0;
      const scoreRatio = (total - minTotal) / totalRange;
      const fontSize = 16 + scoreRatio * 9.6;
      const nameLength = Array.from(row.horseName).length;
      return nameLength * fontSize * 0.9 + 56;
    }),
  );
  const requiredJockeyWidth = Math.max(
    ...evaluatedRows.map((row) => {
      const total = row.scores?.total ?? 0;
      const scoreRatio = (total - minTotal) / totalRange;
      const fontSize = 16 + scoreRatio * 9.6;
      const jockeyLength = Array.from(row.jockeyName || "-").length;
      return jockeyLength * fontSize * 0.88 + 40;
    }),
  );
  const nameColumnWidth = Math.round(Math.min(360, Math.max(176, requiredNameWidth)));
  const mobileNameColumnWidth = Math.round(Math.min(300, Math.max(164, requiredNameWidth)));
  const jockeyColumnWidth = Math.round(Math.min(260, Math.max(112, requiredJockeyWidth)));
  const mobileJockeyColumnWidth = Math.round(Math.min(230, Math.max(104, requiredJockeyWidth)));
  const tableStyle: PaddockTableStyle = {
    "--paddock-jockey-col-width": `${jockeyColumnWidth}px`,
    "--paddock-mobile-table-width": `${1012 + mobileNameColumnWidth + mobileJockeyColumnWidth}px`,
    "--paddock-name-col-width": `${nameColumnWidth}px`,
  };

  return (
    <div className="paddock-table-shell">
      <div className="paddock-table-wrap">
        <table className="stats-table paddock-table" style={tableStyle}>
          <colgroup>
            <col className="paddock-col-frame" />
            <col className="paddock-col-horse-number" />
            <col className="paddock-col-name" />
            <col className="paddock-col-jockey" />
            <col className="paddock-col-popularity" />
            <col className="paddock-col-odds" />
            <col className="paddock-col-weight" />
            <col className="paddock-col-rank" />
            <col className="paddock-col-score" />
            <col className="paddock-col-score" />
            <col className="paddock-col-score" />
            <col className="paddock-col-score" />
            <col className="paddock-col-score" />
          </colgroup>
          <thead>
            <tr>
              <th>枠</th>
              <th>馬番</th>
              <th>馬名</th>
              <th>騎手名</th>
              <th>人気</th>
              <th>単勝</th>
              <th>馬体重</th>
              <th>公式評価順</th>
              <th>合計</th>
              <th>パドック</th>
              <th>返し</th>
              <th>注目度</th>
              <th>好み</th>
            </tr>
          </thead>
          <tbody>
            {evaluatedRows.map((row) => {
              const total = row.scores?.total ?? 0;
              const scoreTooltipId = `paddock-horse-number-score-${row.horseNumber}`;
              const isScoreTooltipOpen = activeScoreTooltip?.horseNumber === row.horseNumber;
              const scoreRatio = (total - minTotal) / totalRange;
              const fontSize = 16 + scoreRatio * 9.6;
              const cellPaddingY = 7 + scoreRatio * 7;
              const isTopTotal = total === maxTotal;
              const rowStyle: PaddockTableRowStyle = {
                "--paddock-row-cell-padding-y": `${cellPaddingY.toFixed(1)}px`,
                "--paddock-row-font-size": `${fontSize.toFixed(1)}px`,
              };
              return (
                <tr
                  className={isTopTotal ? "paddock-table-top-total-row" : undefined}
                  key={row.horseNumber}
                  style={rowStyle}
                >
                  <td>
                    <FrameNumberBadge value={row.frameNumber} />
                  </td>
                  <td
                    className="paddock-table-horse-number"
                    onMouseEnter={(event) => showScoreTooltip(row, event.currentTarget)}
                    onMouseLeave={() => setActiveScoreTooltip(null)}
                  >
                    <button
                      aria-describedby={total !== 0 ? scoreTooltipId : undefined}
                      aria-expanded={isScoreTooltipOpen}
                      aria-label={`${row.horseName}の馬番${formatRunnerNumber(row.horseNumber)}${total !== 0 ? `、パドック合計値 ${formatPaddockScore(total)} の説明` : ""}`}
                      className={`paddock-horse-number-cell-trigger${isScoreTooltipOpen ? " tooltip-open" : ""}`}
                      type="button"
                      onBlur={() => setActiveScoreTooltip(null)}
                      onClick={(event) => {
                        if (isScoreTooltipOpen) {
                          setActiveScoreTooltip(null);
                          return;
                        }
                        showScoreTooltip(row, event.currentTarget);
                      }}
                      onFocus={(event) => showScoreTooltip(row, event.currentTarget)}
                    >
                      <span className="paddock-horse-number-with-score">
                        <span>{formatRunnerNumber(row.horseNumber)}</span>
                        {total !== 0 ? (
                          <span className="paddock-horse-number-score" aria-hidden="true">
                            {formatPaddockScoreRuby(total)}
                          </span>
                        ) : null}
                      </span>
                    </button>
                  </td>
                  <td className="stats-name-cell" data-label="馬名">
                    <HorseNameBadge
                      coatCode={row.moshokuCode}
                      name={row.horseName}
                      showCoatLabel={false}
                    />
                  </td>
                  <td className="paddock-table-jockey-name-cell" data-label="騎手名">
                    {row.jockeyName ? (
                      <span className="paddock-table-jockey-name">{row.jockeyName}</span>
                    ) : (
                      "-"
                    )}
                  </td>
                  <td className="paddock-table-dynamic-cell" data-label="人気">
                    {formatRealtimePopularity(oddsByHorse.get(row.horseNumber)?.popularity ?? null)}
                  </td>
                  <td className="paddock-table-dynamic-cell" data-label="単勝">
                    {formatRealtimeOdds(oddsByHorse.get(row.horseNumber)?.odds ?? null)}
                  </td>
                  <td className="paddock-table-weight" data-label="馬体重">
                    <PaddockWeightValue value={row.weight} />
                  </td>
                  <td className="paddock-table-dynamic-cell" data-label="公式評価順">
                    {row.scores?.officialRank ? (
                      <span className={getOfficialRankClassName(row.scores.officialRank)}>
                        {formatOfficialRank(row.scores.officialRank)}
                      </span>
                    ) : (
                      "-"
                    )}
                  </td>
                  <td className="stats-score-cell" data-label="合計">
                    {formatPaddockScore(total)}
                  </td>
                  <td className="paddock-table-dynamic-cell" data-label="パドック">
                    {row.scores?.paddock ?? 0}
                  </td>
                  <td className="paddock-table-dynamic-cell" data-label="返し">
                    {row.scores?.kaeshi ?? 0}
                  </td>
                  <td className="paddock-table-dynamic-cell" data-label="注目度">
                    {row.scores?.attention ?? 0}
                  </td>
                  <td className="paddock-table-dynamic-cell" data-label="好み">
                    {row.scores?.preference ?? 0}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {activeScoreTooltip ? (
        <div
          className={`paddock-horse-number-score-tooltip ${activeScoreTooltip.placement}`}
          id={`paddock-horse-number-score-${activeScoreTooltip.horseNumber}`}
          role="tooltip"
          style={{
            left: `${activeScoreTooltip.left}px`,
            top: `${activeScoreTooltip.top}px`,
          }}
        >
          <strong>{activeScoreTooltip.horseName}</strong>
          <span>
            合計値 <b>{formatPaddockScore(activeScoreTooltip.total)}</b>
          </span>
          <small>パドック・返し + 注目度x0.5 + 好みx0.3</small>
        </div>
      ) : null}
    </div>
  );
}

export function PaddockSection({
  day,
  decodeHexHorseWeight = false,
  detailUrl,
  editFooterDetailPath,
  editable = false,
  keibajoCode,
  month,
  raceNumberLabel = "",
  racePlace = "",
  raceMeta = "",
  raceNumber,
  raceStartsAt = null,
  raceStartsAtLabel = "",
  raceTitle = "",
  realtimeRequest,
  recentResults,
  runners,
  source,
  year,
}: PaddockSectionProps) {
  const [state, setState] = useState<PaddockState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [premiumBulletins, setPremiumBulletins] = useState<PremiumPaddockBulletin[]>([]);
  const [premiumTrainingGradesByHorse, setPremiumTrainingGradesByHorse] = useState<
    Map<string, string>
  >(new Map());
  const [liveUrl, setLiveUrl] = useState<string | null>(null);
  const [realtimeEnabled, setRealtimeEnabled] = useState(false);
  const [discordStatus, setDiscordStatus] = useState<"idle" | "sending" | "sent" | "failed">(
    "idle",
  );
  const [externalDiscordStatus, setExternalDiscordStatus] = useState<
    "idle" | "sending" | "sent" | "failed"
  >("idle");
  const [lastDiscordSentAt, setLastDiscordSentAt] = useState<number | null>(null);
  const [discordCooldownNow, setDiscordCooldownNow] = useState(() => Date.now());
  const submitSequenceRef = useRef(0);
  const optimisticUntilRef = useRef(0);
  const apiPath = getPaddockApiPath({ day, keibajoCode, month, raceNumber, year });
  const pastRaceEditSessionKey = getPastRaceEditSessionKey({
    day,
    keibajoCode,
    month,
    raceNumber,
    year,
  });
  const discordApiPath = getPaddockDiscordApiPath({ day, keibajoCode, month, raceNumber, year });
  const premiumApiPath = getPremiumRaceRequestPath({
    day,
    keibajoCode,
    month,
    raceNumber,
    source,
    year,
  });
  const livePath = `${apiPath}/live`;
  const editPath = `/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/paddock`;
  const raceDetailPath = `/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}`;
  const publicRaceDetailUrl = `${PUBLIC_DETAIL_ORIGIN}${raceDetailPath}`;
  const { payload: realtimePayload } = useRealtimeRacePayload(
    realtimeRequest ?? {
      apiBaseUrl: "",
      day: "",
      keibajoCode: "",
      month: "",
      raceNumber: "",
      source: "",
      year: "",
    },
    null,
  );
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (realtimePayload?.odds?.latest.tansho ?? []).map((row) => [
          formatRunnerNumber(row.combination),
          {
            odds: row.odds ?? null,
            popularity: row.rank ?? null,
          },
        ]),
      ),
    [realtimePayload],
  );
  const realtimeEntryByHorse = useMemo(
    () =>
      new Map(
        (realtimePayload?.raceEntries?.horses ?? []).map((horse) => [
          formatRunnerNumber(horse.horseNumber),
          {
            jockeyName: cleanText(horse.jockeyName, ""),
            status: cleanText(horse.status, ""),
          },
        ]),
      ),
    [realtimePayload],
  );
  const recentResultsByHorse = useMemo(() => {
    if (!recentResults) {
      return null;
    }
    const grouped = new Map<string, HorseRaceResult[]>();
    for (const result of recentResults) {
      const horseNumber = formatRunnerNumber(result.currentUmaban);
      grouped.set(horseNumber, [...(grouped.get(horseNumber) ?? []), result]);
    }
    return grouped;
  }, [recentResults]);
  const realtimeWeightByHorse = useMemo(
    () =>
      new Map(
        (realtimePayload?.horseWeights?.horses ?? []).map((horse) => [
          formatRunnerNumber(horse.horseNumber),
          formatHorseWeight(
            horse.weight === null ? null : String(horse.weight),
            horse.changeSign,
            horse.changeAmount === null ? null : String(horse.changeAmount),
          ),
        ]),
      ),
    [realtimePayload],
  );
  const runnerRows = useMemo<PaddockRunnerRow[]>(
    () =>
      runners
        .map((runner, index) => {
          const horseNumber = formatRunnerNumber(runner.umaban);
          return {
            horseName: cleanText(runner.bamei),
            horseNumber,
            frameNumber: cleanText(runner.wakuban, ""),
            index,
            jockeyName: cleanText(runner.kishumeiRyakusho),
            moshokuCode: runner.moshokuCode,
            sexAge: formatSexAge(runner.seibetsuCode, runner.barei),
            status: realtimeEntryByHorse.get(horseNumber)?.status || "",
            weight:
              realtimeWeightByHorse.get(horseNumber) ??
              formatHorseWeight(
                runner.bataiju,
                runner.zogenFugo,
                runner.zogenSa,
                decodeHexHorseWeight,
              ),
          };
        })
        .toSorted((left, right) => {
          const leftScratched = left.status !== "";
          const rightScratched = right.status !== "";
          if (leftScratched !== rightScratched) {
            return leftScratched ? 1 : -1;
          }
          return left.index - right.index;
        }),
    [decodeHexHorseWeight, realtimeEntryByHorse, realtimeWeightByHorse, runners],
  );

  useEffect(() => {
    if (lastDiscordSentAt === null || Date.now() - lastDiscordSentAt >= 30_000) {
      return undefined;
    }

    const timer = window.setInterval(() => {
      setDiscordCooldownNow(Date.now());
    }, 1_000);
    return () => {
      window.clearInterval(timer);
    };
  }, [lastDiscordSentAt]);

  useEffect(() => {
    let cancelled = false;
    const requestUrl = getPaddockRequestUrl(apiPath);
    const load = async () => {
      try {
        const response = await fetchWithRetry(requestUrl, {
          cache: "no-store",
          credentials: "include",
        });
        if (!response.ok) {
          throw new Error(`paddock api ${response.status}`);
        }
        const payload: unknown = await response.json();
        if (!isPaddockState(payload)) {
          throw new Error("invalid paddock payload");
        }
        if (!cancelled && Date.now() >= optimisticUntilRef.current) {
          setState(payload);
          setLiveUrl(response.headers.get("x-paddock-live-url"));
          setRealtimeEnabled(response.headers.get("x-paddock-realtime") === "1");
          setError(null);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof Error ? caught.message : String(caught));
        }
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 10_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [apiPath]);

  useEffect(() => {
    if (source !== "jra") {
      return undefined;
    }
    let cancelled = false;
    const load = async () => {
      try {
        const response = await fetchWithRetry(premiumApiPath, { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const payload: unknown = await response.json();
        const rows =
          typeof payload === "object" && payload !== null && "paddockBulletins" in payload
            ? payload.paddockBulletins
            : [];
        const trainingReviews =
          typeof payload === "object" && payload !== null && "trainingReviews" in payload
            ? payload.trainingReviews
            : [];
        if (!cancelled && Array.isArray(rows)) {
          setPremiumBulletins(rows.filter(isPremiumPaddockBulletin));
        }
        if (!cancelled && Array.isArray(trainingReviews)) {
          setPremiumTrainingGradesByHorse(
            toBestTrainingGradeByHorse(trainingReviews.filter(isPremiumTrainingReview)),
          );
        }
      } catch {
        if (!cancelled) {
          setPremiumBulletins([]);
          setPremiumTrainingGradesByHorse(new Map());
        }
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 30_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [premiumApiPath, source]);

  useEffect(() => {
    if (!realtimeEnabled) {
      return undefined;
    }

    const socket = new WebSocket(liveUrl ?? getPaddockLiveUrl(livePath));
    socket.addEventListener("message", (event) => {
      const payload: unknown = JSON.parse(String(event.data));
      const nextState =
        typeof payload === "object" && payload !== null && "state" in payload
          ? payload.state
          : null;
      if (isPaddockState(nextState) && Date.now() >= optimisticUntilRef.current) {
        setState(nextState);
      }
    });
    socket.addEventListener("error", () => {
      socket.close();
    });
    return () => {
      socket.close();
    };
  }, [livePath, liveUrl, realtimeEnabled]);

  const submitScore = useCallback(
    (action: PaddockAction) => {
      if (
        editable &&
        isPastRace(raceStartsAt) &&
        !readPastRaceEditUnlocked(pastRaceEditSessionKey)
      ) {
        window.alert(
          "発走済みのレースは誤操作防止のためデフォルトでは編集できません。編集する場合は次の確認で許可してください。",
        );
        const accepted = window.confirm(
          "この発走済みレースのパドック編集を10分間だけ許可しますか？",
        );
        if (!accepted) {
          return;
        }
        writePastRaceEditUnlocked(pastRaceEditSessionKey);
      }

      const sequence = submitSequenceRef.current + 1;
      submitSequenceRef.current = sequence;
      optimisticUntilRef.current = Date.now() + 8_000;
      setState((current) => (current ? applyPaddockAction(current, action) : current));
      setError(null);
      void (async () => {
        const response = await fetchWithRetry(
          getPaddockRequestUrl(apiPath),
          {
            body: JSON.stringify(action),
            credentials: "include",
            method: "POST",
          },
          { attempts: 4, baseDelayMs: 250, maxDelayMs: 2000 },
        );
        if (response.ok) {
          const payload: unknown = await response.json();
          if (submitSequenceRef.current === sequence) {
            optimisticUntilRef.current = 0;
          }
          if (isPaddockState(payload) && submitSequenceRef.current === sequence) {
            setState(payload);
          }
          setError(null);
        } else {
          if (submitSequenceRef.current === sequence) {
            optimisticUntilRef.current = 0;
          }
          setError(`paddock api ${response.status}`);
        }
      })().catch((caught) => {
        if (submitSequenceRef.current === sequence) {
          optimisticUntilRef.current = 0;
        }
        setError(caught instanceof Error ? caught.message : String(caught));
      });
    },
    [apiPath, editable, pastRaceEditSessionKey, raceStartsAt],
  );

  const notifyDiscord = useCallback(() => {
    const cooldownRemaining = lastDiscordSentAt
      ? Math.max(0, 30_000 - (Date.now() - lastDiscordSentAt))
      : 0;
    if (!detailUrl || discordStatus === "sending" || cooldownRemaining > 0) {
      return;
    }

    void (async () => {
      setDiscordStatus("sending");
      const horses = runnerRows.map((runner) => {
        const scores = normalizePaddockHorseScore(state?.horses[runner.horseNumber], runner);
        const realtimeEntry = realtimeEntryByHorse.get(runner.horseNumber);
        const displayJockeyName = getPreferredJockeyName(
          runner.jockeyName,
          realtimeEntry?.jockeyName || null,
        );
        const realtimeOdds = realtimeOddsByHorse.get(runner.horseNumber);
        return {
          attention: scores.attention,
          horseName: runner.horseName,
          horseNumber: formatRunnerNumber(runner.horseNumber),
          jockeyName: displayJockeyName,
          kaeshi: scores.kaeshi,
          odds: formatRealtimeOdds(realtimeOdds?.odds ?? null),
          officialRank: formatOfficialRank(scores.officialRank),
          paddock: scores.paddock,
          popularity: formatRealtimePopularity(realtimeOdds?.popularity ?? null),
          preference: scores.preference,
          sexAge: runner.sexAge,
          total: formatPaddockScore(scores.total),
          weight: runner.weight,
        };
      });

      const response = await fetch(getPaddockRequestUrl(discordApiPath), {
        body: JSON.stringify({
          detailUrl,
          horses,
          raceNumberLabel,
          racePlace,
          raceMeta,
          raceStartsAtLabel,
          raceTitle,
        }),
        credentials: "include",
        method: "POST",
      });

      if (response.ok) {
        const sentAt = Date.now();
        setLastDiscordSentAt(sentAt);
        setDiscordCooldownNow(sentAt);
      }
      setDiscordStatus(response.ok ? "sent" : "failed");
      window.setTimeout(() => setDiscordStatus("idle"), 3_000);
    })().catch(() => {
      setDiscordStatus("failed");
      window.setTimeout(() => setDiscordStatus("idle"), 3_000);
    });
  }, [
    detailUrl,
    discordApiPath,
    discordStatus,
    lastDiscordSentAt,
    raceMeta,
    raceNumberLabel,
    racePlace,
    raceStartsAtLabel,
    raceTitle,
    realtimeEntryByHorse,
    realtimeOddsByHorse,
    runnerRows,
    state,
  ]);
  const notifyExternalDiscord = useCallback(() => {
    if (premiumBulletins.length === 0 || externalDiscordStatus === "sending") {
      return;
    }
    const groupLabels: Record<PremiumPaddockBulletin["groupKey"], string> = {
      favorite: process.env.NEXT_PUBLIC_PREMIUM_RACE_PADDOCK_GROUP_FAVORITE_LABEL ?? "人気馬",
      value: process.env.NEXT_PUBLIC_PREMIUM_RACE_PADDOCK_GROUP_VALUE_LABEL ?? "穴馬",
    };

    void (async () => {
      setExternalDiscordStatus("sending");
      const response = await fetch(getPaddockRequestUrl(discordApiPath), {
        body: JSON.stringify({
          bulletins: premiumBulletins.map((row) => ({
            commentText: row.commentText ?? "",
            evaluationText: row.evaluationText ?? "",
            frameNumber: row.frameNumber ?? "",
            groupLabel: groupLabels[row.groupKey],
            horseName: row.horseName ?? "",
            horseNumber: row.horseNumber,
          })),
          detailUrl: detailUrl ?? publicRaceDetailUrl,
          raceMeta,
          raceNumberLabel,
          racePlace,
          raceStartsAtLabel,
          raceTitle,
          type: "external-paddock",
        }),
        credentials: "include",
        method: "POST",
      });
      setExternalDiscordStatus(response.ok ? "sent" : "failed");
      window.setTimeout(() => setExternalDiscordStatus("idle"), 3_000);
    })().catch(() => {
      setExternalDiscordStatus("failed");
      window.setTimeout(() => setExternalDiscordStatus("idle"), 3_000);
    });
  }, [
    detailUrl,
    discordApiPath,
    externalDiscordStatus,
    premiumBulletins,
    publicRaceDetailUrl,
    raceMeta,
    raceNumberLabel,
    racePlace,
    raceStartsAtLabel,
    raceTitle,
  ]);
  const discordCooldownRemainingSeconds =
    lastDiscordSentAt === null
      ? 0
      : Math.ceil(Math.max(0, 30_000 - (discordCooldownNow - lastDiscordSentAt)) / 1_000);
  const isDiscordButtonDisabled =
    discordStatus === "sending" || discordCooldownRemainingSeconds > 0;
  const externalDiscordLabel =
    externalDiscordStatus === "sending"
      ? "外部速報を通知中"
      : externalDiscordStatus === "sent"
        ? "外部速報を通知済み"
        : externalDiscordStatus === "failed"
          ? "外部速報の通知失敗"
          : "外部速報を通知";

  return (
    <section className={editable ? "paddock-section paddock-section-edit" : "paddock-section"}>
      <header className="section-heading compact">
        <h2>パドック</h2>
        {editable ? null : (
          <Link className="paddock-edit-link" href={editPath}>
            編集
          </Link>
        )}
        <span>{state ? `更新 ${formatHistoryDate(state.updatedAt)}` : "読み込み中"}</span>
      </header>
      {error ? <p className="empty-state">パドック評価を取得できません: {error}</p> : null}
      {editable ? (
        <section className="paddock-board" aria-label="出走馬のパドック評価">
          {runnerRows.map((runner) => {
            const scores = normalizePaddockHorseScore(state?.horses[runner.horseNumber], runner);
            const realtimeEntry = realtimeEntryByHorse.get(runner.horseNumber);
            const status = realtimeEntry?.status || runner.status || null;
            return (
              <PaddockHorseRow
                editable
                frameNumber={runner.frameNumber}
                horseName={runner.horseName}
                horseNumber={runner.horseNumber}
                jockeyName={runner.jockeyName}
                moshokuCode={runner.moshokuCode}
                key={runner.horseNumber}
                originalJockeyName={runner.jockeyName}
                recentResults={
                  recentResultsByHorse === null
                    ? null
                    : (recentResultsByHorse.get(runner.horseNumber) ?? [])
                }
                realtimeOdds={realtimeOddsByHorse.get(runner.horseNumber)?.odds ?? null}
                realtimeJockeyName={realtimeEntry?.jockeyName || null}
                realtimePopularity={realtimeOddsByHorse.get(runner.horseNumber)?.popularity ?? null}
                scores={scores}
                sexAge={runner.sexAge}
                status={status}
                trainingEvaluationGrade={
                  premiumTrainingGradesByHorse.get(runner.horseNumber) ?? null
                }
                weight={runner.weight}
                onScore={submitScore}
              />
            );
          })}
        </section>
      ) : (
        <PaddockReadOnlyTable oddsByHorse={realtimeOddsByHorse} rows={runnerRows} state={state} />
      )}
      {editable ? (
        <details className="paddock-history">
          <summary>履歴</summary>
          {state?.history.length ? (
            <ol>
              {state.history.map((entry) => (
                <li key={entry.id}>
                  <time>{formatHistoryDate(entry.at)}</time>
                  <span>
                    {formatRunnerNumber(entry.horseNumber)} {entry.horseName}
                  </span>
                  <strong>
                    {entry.type === "official-rank"
                      ? `公式評価順 ${formatOfficialRank(entry.officialRank)}`
                      : `${METRIC_LABELS[entry.category ?? "paddock"].title} ${
                          entry.delta && entry.delta > 0 ? "+1" : "-1"
                        }`}
                  </strong>
                </li>
              ))}
            </ol>
          ) : (
            <p className="empty-state">履歴はまだありません。</p>
          )}
        </details>
      ) : null}
      {editable ? (
        <PaddockOfficialRankQuickPanel
          rows={runnerRows}
          state={state}
          onOfficialRank={submitScore}
        />
      ) : null}
      {editable && editFooterDetailPath ? (
        <footer className="paddock-edit-footer paddock-edit-footer-sticky">
          <Link className="paddock-edit-link" href={editFooterDetailPath}>
            詳細へ戻る
          </Link>
          <button
            className="paddock-discord-button"
            disabled={isDiscordButtonDisabled}
            type="button"
            onClick={notifyDiscord}
          >
            <span className="paddock-discord-icon" aria-hidden="true" />
            <span>
              {discordStatus === "sending"
                ? "通知中"
                : discordStatus === "sent"
                  ? "通知済み"
                  : discordStatus === "failed"
                    ? "通知失敗"
                    : discordCooldownRemainingSeconds > 0
                      ? `再通知まで ${discordCooldownRemainingSeconds}秒`
                      : lastDiscordSentAt
                        ? "Discordへ再通知"
                        : "Discordへ通知"}
            </span>
          </button>
        </footer>
      ) : null}
      {!editable ? (
        <PremiumPaddockBulletinTable
          notifyDisabled={externalDiscordStatus === "sending"}
          notifyLabel={externalDiscordLabel}
          rows={premiumBulletins}
          onNotify={notifyExternalDiscord}
        />
      ) : null}
    </section>
  );
}
