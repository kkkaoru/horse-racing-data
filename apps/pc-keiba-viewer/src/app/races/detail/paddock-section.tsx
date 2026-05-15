"use client";

import Link from "next/link";
import { memo, useCallback, useEffect, useMemo, useState } from "react";

import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatTrack,
} from "../../../lib/format";
import { getPreferredJockeyName, isSameJockeyName } from "../../../lib/jockey-name";
import {
  isPaddockState,
  normalizePaddockHorseScore,
  type PaddockAction,
  type PaddockMetric,
  type PaddockOfficialRank,
  type PaddockState,
} from "../../../lib/paddock";
import type { HorseRaceResult, Runner } from "../../../lib/race-types";
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
  raceStartsAtLabel?: string;
  raceTitle?: string;
  realtimeRequest?: RealtimeRaceRequest;
  recentResults?: HorseRaceResult[];
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
  weight: string;
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
  return (
    <span className="paddock-weight-value">
      <strong>{parsed.weight}</strong>
      {parsed.change ? <em>{parsed.change}</em> : null}
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
  return popularity === null ? "-" : `${popularity}人気`;
};

const formatPastRaceName = (result: HorseRaceResult): string => {
  const names = [
    cleanText(result.kyosomeiHondai, ""),
    cleanText(result.kyosomeiFukudai, ""),
    cleanText(result.kyosomeiKakkonai, ""),
  ].filter(Boolean);
  return names.length > 0 ? names.join(" / ") : "一般競走";
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
        <p>新馬</p>
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
              <small>{formatPastResultMeta(result)}</small>
            </span>
            <span className="paddock-recent-stats">
              <span>{formatPastPopularity(result.tanshoNinkijun)}</span>
              <span>単 {formatPastOdds(result.tanshoOdds)}</span>
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
  recentResults,
  realtimeOdds,
  realtimeJockeyName,
  realtimePopularity,
  scores,
  sexAge,
  status,
  weight,
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
            <dt>馬番</dt>
            <dd>{formatRunnerNumber(horseNumber)}</dd>
            {status ? <em className="entry-status-mini">{status}</em> : null}
          </div>
          <div>
            <dt>枠番</dt>
            <dd>
              <FrameNumberBadge value={frameNumber} />
            </dd>
          </div>
        </dl>
        <div className="paddock-horse-name-block">
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
          <li className="paddock-official-rank-control">
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
          </li>
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
              <td className="paddock-table-weight">
                <PaddockWeightValue value={row.weight} />
              </td>
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
  raceStartsAtLabel = "",
  raceTitle = "",
  realtimeRequest,
  recentResults,
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
  const [lastDiscordSentAt, setLastDiscordSentAt] = useState<number | null>(null);
  const [discordCooldownNow, setDiscordCooldownNow] = useState(() => Date.now());
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
  const discordCooldownRemainingSeconds =
    lastDiscordSentAt === null
      ? 0
      : Math.ceil(Math.max(0, 30_000 - (discordCooldownNow - lastDiscordSentAt)) / 1_000);
  const isDiscordButtonDisabled =
    discordStatus === "sending" || discordCooldownRemainingSeconds > 0;

  return (
    <section className={editable ? "paddock-section paddock-section-edit" : "paddock-section"}>
      <header className="section-heading compact">
        <h2>パドック</h2>
        <span>{state ? `更新 ${formatHistoryDate(state.updatedAt)}` : "読み込み中"}</span>
        {editable ? null : (
          <Link className="paddock-edit-link" href={editPath}>
            編集
          </Link>
        )}
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
                recentResults={recentResultsByHorse?.get(runner.horseNumber) ?? null}
                realtimeOdds={realtimeOddsByHorse.get(runner.horseNumber)?.odds ?? null}
                realtimeJockeyName={realtimeEntry?.jockeyName || null}
                realtimePopularity={realtimeOddsByHorse.get(runner.horseNumber)?.popularity ?? null}
                scores={scores}
                sexAge={runner.sexAge}
                status={status}
                weight={runner.weight}
                onOfficialRank={submitScore}
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
    </section>
  );
}
