"use client";

import Link from "next/link";
import { memo, useCallback, useEffect, useMemo, useState } from "react";

import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { cleanText } from "../../../lib/format";
import {
  isPaddockState,
  normalizePaddockHorseScore,
  type PaddockAction,
  type PaddockMetric,
  type PaddockOfficialRank,
  type PaddockState,
} from "../../../lib/paddock";
import type { Runner } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";

interface PaddockSectionProps {
  day: string;
  editable?: boolean;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  runners: Runner[];
  year: string;
}

interface PaddockHorseRowProps {
  editable: boolean;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  onOfficialRank: (action: PaddockAction) => void;
  onScore: (action: PaddockAction) => void;
  scores: {
    attention: number;
    kaeshi: number;
    officialRank: PaddockOfficialRank | null;
    paddock: number;
    preference: number;
    total: number;
  };
}

const METRIC_LABELS: Record<PaddockMetric, { minus: string; plus: string; title: string }> = {
  attention: { minus: "注目-", plus: "注目+", title: "注目度" },
  kaeshi: { minus: "返し-", plus: "返し+", title: "返し" },
  paddock: { minus: "気配-", plus: "気配+", title: "パドック" },
  preference: { minus: "嫌い", plus: "好き", title: "好み" },
};
const METRIC_ORDER = ["paddock", "attention", "preference", "kaeshi"] as const satisfies readonly PaddockMetric[];
const DEFAULT_REMOTE_PADDOCK_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const OFFICIAL_RANK_OPTIONS: PaddockOfficialRank[] = [1, 2, 3, 4, 5, 6];

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

const formatOfficialRank = (rank: PaddockOfficialRank | null | undefined): string =>
  rank ? `${rank}` : "-";

const formatPaddockScore = (value: number): string =>
  Number.isInteger(value) ? String(value) : value.toFixed(1);

const getOfficialRankClassName = (rank: PaddockOfficialRank | null | undefined): string =>
  rank ? `paddock-rank-badge rank-${rank}` : "paddock-rank-badge";

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
}: Omit<PaddockSectionProps, "runners">): string =>
  `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/paddock`;

const PaddockHorseRow = memo(function PaddockHorseRow({
  editable,
  horseName,
  horseNumber,
  jockeyName,
  onOfficialRank,
  onScore,
  scores,
}: PaddockHorseRowProps) {
  const score = (category: PaddockMetric, delta: -1 | 1) => {
    onScore({ category, delta, horseName, horseNumber });
  };
  const setOfficialRank = (rank: PaddockOfficialRank | null) => {
    onOfficialRank({ horseName, horseNumber, rank, type: "official-rank" });
  };

  return (
    <div className="paddock-horse-row">
      <div className="paddock-horse-summary">
        <strong>{formatRunnerNumber(horseNumber)}</strong>
        <span>{horseName}</span>
        {jockeyName ? <em className="paddock-horse-jockey">{jockeyName}</em> : null}
        <b>{formatPaddockScore(scores.total)}</b>
      </div>
      {editable ? (
        <div className="paddock-score-controls">
          {METRIC_ORDER.map((metric) => (
            <div className="paddock-score-control" key={metric}>
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
            </div>
          ))}
          <div className="paddock-official-rank-control">
            <span>公式評価順</span>
            <div className="paddock-rank-buttons">
              {OFFICIAL_RANK_OPTIONS.map((rank) => (
                <button
                  aria-label={`${horseName} 公式パドック評価 ${rank}番手`}
                  aria-pressed={scores.officialRank === rank}
                  className={`rank-${rank}${scores.officialRank === rank ? " selected" : ""}`}
                  type="button"
                  key={rank}
                  onClick={() => {
                    setOfficialRank(scores.officialRank === rank ? null : rank);
                  }}
                >
                  {rank}
                </button>
              ))}
              <button
                aria-label={`${horseName} 公式パドック評価を解除`}
                className="clear"
                type="button"
                onClick={() => setOfficialRank(null)}
              >
                解除
              </button>
            </div>
          </div>
        </div>
      ) : (
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
      )}
    </div>
  );
});

function PaddockReadOnlyTable({
  rows,
  state,
}: {
  rows: { horseName: string; horseNumber: string; jockeyName: string }[];
  state: PaddockState | null;
}) {
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

  if (evaluatedRows.length === 0) {
    return <p className="empty-state">パドック評価はまだありません。</p>;
  }

  return (
    <div className="paddock-table-wrap">
      <table className="stats-table paddock-table">
        <colgroup>
          <col className="paddock-col-number" />
          <col className="paddock-col-name" />
          <col className="paddock-col-rank" />
          <col className="paddock-col-score" />
          <col className="paddock-col-score" />
          <col className="paddock-col-score" />
          <col className="paddock-col-score" />
          <col className="paddock-col-score" />
        </colgroup>
        <thead>
          <tr>
            <th>馬番</th>
            <th>馬名</th>
            <th>公式評価順</th>
            <th>合計</th>
            <th>パドック</th>
            <th>返し</th>
            <th>注目度</th>
            <th>好み</th>
          </tr>
        </thead>
        <tbody>
          {evaluatedRows.map((row) => (
            <tr key={row.horseNumber}>
              <td>{formatRunnerNumber(row.horseNumber)}</td>
              <td className="stats-name-cell">{row.horseName}</td>
              <td>
                {row.scores?.officialRank ? (
                  <span className={getOfficialRankClassName(row.scores.officialRank)}>
                    {formatOfficialRank(row.scores.officialRank)}
                  </span>
                ) : (
                  "-"
                )}
              </td>
              <td className="stats-score-cell">{formatPaddockScore(row.scores?.total ?? 0)}</td>
              <td>{row.scores?.paddock ?? 0}</td>
              <td>{row.scores?.kaeshi ?? 0}</td>
              <td>{row.scores?.attention ?? 0}</td>
              <td>{row.scores?.preference ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function PaddockSection({
  day,
  editable = false,
  keibajoCode,
  month,
  raceNumber,
  runners,
  year,
}: PaddockSectionProps) {
  const [state, setState] = useState<PaddockState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [liveUrl, setLiveUrl] = useState<string | null>(null);
  const [realtimeEnabled, setRealtimeEnabled] = useState(false);
  const apiPath = getPaddockApiPath({ day, keibajoCode, month, raceNumber, year });
  const livePath = `${apiPath}/live`;
  const editPath = `/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/paddock`;
  const runnerRows = useMemo(
    () =>
      runners.map((runner) => {
        const horseNumber = formatRunnerNumber(runner.umaban);
        return {
          horseName: cleanText(runner.bamei),
          horseNumber,
          jockeyName: cleanText(runner.kishumeiRyakusho),
        };
      }),
    [runners],
  );

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
        if (!cancelled) {
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
      if (isPaddockState(nextState)) {
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
      void (async () => {
        const response = await fetch(getPaddockRequestUrl(apiPath), {
          body: JSON.stringify(action),
          credentials: "include",
          method: "POST",
        });
        if (response.ok) {
          const payload: unknown = await response.json();
          if (isPaddockState(payload)) {
            setState(payload);
          }
          setError(null);
        } else {
          setError(`paddock api ${response.status}`);
        }
      })();
    },
    [apiPath],
  );

  return (
    <section className={editable ? "paddock-section paddock-section-edit" : "paddock-section"}>
      <div className="section-heading compact">
        <h2>パドック</h2>
        <span>{state ? `更新 ${formatHistoryDate(state.updatedAt)}` : "読み込み中"}</span>
        {editable ? null : (
          <Link className="paddock-edit-link" href={editPath}>
            編集
          </Link>
        )}
      </div>
      {error ? <p className="empty-state">パドック評価を取得できません: {error}</p> : null}
      {editable ? (
        <div className="paddock-board">
          {runnerRows.map((runner) => {
            const scores = normalizePaddockHorseScore(state?.horses[runner.horseNumber], runner);
            return (
              <PaddockHorseRow
                editable
                horseName={runner.horseName}
                horseNumber={runner.horseNumber}
                jockeyName={runner.jockeyName}
                key={runner.horseNumber}
                scores={scores}
                onOfficialRank={submitScore}
                onScore={submitScore}
              />
            );
          })}
        </div>
      ) : (
        <PaddockReadOnlyTable rows={runnerRows} state={state} />
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
    </section>
  );
}
