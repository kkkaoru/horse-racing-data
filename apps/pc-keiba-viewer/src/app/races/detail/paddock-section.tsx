"use client";

import Link from "next/link";
import { memo, useCallback, useEffect, useMemo, useState } from "react";

import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { cleanText } from "../../../lib/format";
import { getPreferredJockeyName, isSameJockeyName } from "../../../lib/jockey-name";
import {
  isPaddockState,
  normalizePaddockHorseScore,
  type PaddockAction,
  type PaddockMetric,
  type PaddockOfficialRank,
  type PaddockState,
} from "../../../lib/paddock";
import type { Runner } from "../../../lib/race-types";
import { formatRunnerNumber, formatSexAge } from "../../../lib/runner-format";
import { FrameNumberBadge, HorseNameBadge } from "./frame-number-badge";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface PaddockSectionProps {
  day: string;
  detailUrl?: string;
  editFooterDetailPath?: string;
  editable?: boolean;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  raceNumberLabel?: string;
  racePlace?: string;
  raceMeta?: string;
  raceStartsAtLabel?: string;
  raceTitle?: string;
  realtimeRequest?: RealtimeRaceRequest;
  runners: Runner[];
  year: string;
}

interface PaddockHorseRowProps {
  editable: boolean;
  horseName: string;
  horseNumber: string;
  frameNumber: string | null;
  jockeyName: string;
  moshokuCode?: string | null;
  onOfficialRank: (action: PaddockAction) => void;
  onScore: (action: PaddockAction) => void;
  originalJockeyName: string;
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
}

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

const formatRealtimePopularity = (value: number | null): string =>
  value === null ? "-" : `${value}`;

const formatRealtimeOdds = (value: number | null): string =>
  value === null ? "-" : value.toFixed(1);

const isChangedJockey = (storedName: string, realtimeName: string | null): boolean =>
  Boolean(realtimeName) && storedName !== "" && !isSameJockeyName(storedName, realtimeName);

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

const getPaddockDiscordApiPath = (props: Omit<PaddockSectionProps, "runners">): string =>
  `${getPaddockApiPath(props)}/discord`;

const PaddockHorseRow = memo(function PaddockHorseRow({
  editable,
  frameNumber,
  horseName,
  horseNumber,
  jockeyName,
  moshokuCode,
  onOfficialRank,
  onScore,
  originalJockeyName,
  realtimeOdds,
  realtimeJockeyName,
  realtimePopularity,
  scores,
  sexAge,
  status,
}: PaddockHorseRowProps) {
  const score = (category: PaddockMetric, delta: -1 | 1) => {
    onScore({ category, delta, horseName, horseNumber });
  };
  const setOfficialRank = (rank: PaddockOfficialRank | null) => {
    onOfficialRank({ horseName, horseNumber, rank, type: "official-rank" });
  };
  const displayJockeyName = getPreferredJockeyName(jockeyName, realtimeJockeyName);
  const isScratched = Boolean(status);

  return (
    <div
      className={
        isScratched ? "paddock-horse-row paddock-horse-row-scratched" : "paddock-horse-row"
      }
      data-entry-status={status ?? undefined}
    >
      <div className="paddock-horse-summary">
        <div className="paddock-horse-ids">
          <span>
            <small>馬番</small>
            <strong>{formatRunnerNumber(horseNumber)}</strong>
            {status ? <em className="entry-status-mini">{status}</em> : null}
          </span>
          <span>
            <small>枠番</small>
            <FrameNumberBadge value={frameNumber} />
          </span>
        </div>
        <span className="paddock-horse-name-block">
          <span className="paddock-horse-name">
            <HorseNameBadge coatCode={moshokuCode} name={horseName} showCoatLabel={false} />
          </span>
          {displayJockeyName ? (
            <span aria-label={`騎手名 ${displayJockeyName}`} className="paddock-horse-jockey-line">
              <strong>{displayJockeyName}</strong>
              {isChangedJockey(originalJockeyName, realtimeJockeyName) ? (
                <small>元 {originalJockeyName}</small>
              ) : null}
            </span>
          ) : null}
          {status ? <span className="paddock-status-badge">{status}</span> : null}
        </span>
        <div className="paddock-horse-race-facts">
          <span>
            <small>性齢</small>
            <strong>{sexAge}</strong>
          </span>
          <span>
            <small>人気</small>
            <strong>{formatRealtimePopularity(realtimePopularity)}</strong>
          </span>
          <span>
            <small>単勝</small>
            <strong>{formatRealtimeOdds(realtimeOdds)}</strong>
          </span>
        </div>
        <b>{formatPaddockScore(scores.total)}</b>
      </div>
      {editable && isScratched ? (
        <div className="paddock-score-unavailable" aria-disabled="true">
          <strong>{status}</strong>
          <span>評価対象外</span>
        </div>
      ) : editable ? (
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
  }[];
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
          <col className="paddock-col-number" />
          <col className="paddock-col-name" />
          <col className="paddock-col-score" />
          <col className="paddock-col-score" />
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
            <th>枠</th>
            <th>馬名</th>
            <th>人気</th>
            <th>単勝</th>
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
              <td className="paddock-table-horse-number">{formatRunnerNumber(row.horseNumber)}</td>
              <td>
                <FrameNumberBadge value={row.frameNumber} />
              </td>
              <td className="stats-name-cell">
                <HorseNameBadge
                  coatCode={row.moshokuCode}
                  name={row.horseName}
                  showCoatLabel={false}
                />
              </td>
              <td>
                {formatRealtimePopularity(oddsByHorse.get(row.horseNumber)?.popularity ?? null)}
              </td>
              <td>{formatRealtimeOdds(oddsByHorse.get(row.horseNumber)?.odds ?? null)}</td>
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
  detailUrl,
  editFooterDetailPath,
  editable = false,
  keibajoCode,
  month,
  raceNumberLabel = "",
  racePlace = "",
  raceMeta = "",
  raceNumber,
  raceStartsAtLabel = "",
  raceTitle = "",
  realtimeRequest,
  runners,
  year,
}: PaddockSectionProps) {
  const [state, setState] = useState<PaddockState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [liveUrl, setLiveUrl] = useState<string | null>(null);
  const [realtimeEnabled, setRealtimeEnabled] = useState(false);
  const [discordStatus, setDiscordStatus] = useState<"idle" | "sending" | "sent" | "failed">(
    "idle",
  );
  const apiPath = getPaddockApiPath({ day, keibajoCode, month, raceNumber, year });
  const discordApiPath = getPaddockDiscordApiPath({ day, keibajoCode, month, raceNumber, year });
  const livePath = `${apiPath}/live`;
  const editPath = `/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/paddock`;
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
  const runnerRows = useMemo(
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
    [realtimeEntryByHorse, runners],
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

  const notifyDiscord = useCallback(() => {
    if (!detailUrl || discordStatus === "sending") {
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
                realtimeOdds={realtimeOddsByHorse.get(runner.horseNumber)?.odds ?? null}
                realtimeJockeyName={realtimeEntry?.jockeyName || null}
                realtimePopularity={realtimeOddsByHorse.get(runner.horseNumber)?.popularity ?? null}
                scores={scores}
                sexAge={runner.sexAge}
                status={status}
                onOfficialRank={submitScore}
                onScore={submitScore}
              />
            );
          })}
        </div>
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
      {editable && editFooterDetailPath ? (
        <div className="paddock-edit-footer paddock-edit-footer-sticky">
          <Link className="paddock-edit-link" href={editFooterDetailPath}>
            詳細へ戻る
          </Link>
          <button
            className="paddock-discord-button"
            disabled={discordStatus === "sending"}
            type="button"
            onClick={notifyDiscord}
          >
            <span className="paddock-discord-icon" aria-hidden="true">
              Discord
            </span>
            <span>
              {discordStatus === "sending"
                ? "通知中"
                : discordStatus === "sent"
                  ? "通知済み"
                  : discordStatus === "failed"
                    ? "通知失敗"
                    : "Discordへ通知"}
            </span>
          </button>
        </div>
      ) : null}
    </section>
  );
}
