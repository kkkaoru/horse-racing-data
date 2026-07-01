"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import Link from "next/link";
import { useMemo, useState } from "react";

import {
  BLINKER_PATTERN_LABELS,
  BLINKER_PATTERN_SHORT_LABELS,
  type BlinkerPattern,
} from "../../../lib/blinker-pattern";
import { cleanText } from "../../../lib/format";
import { useHorseWeightStream } from "../../../lib/horse-weight-stream-client";
import {
  getPreferredJockeyName,
  isSameJockeyName,
  normalizeJockeyNameForComparison,
} from "../../../lib/jockey-name";
import { buildD1FinishMap } from "../../../lib/race-finish-position";
import type { Runner } from "../../../lib/race-types";
import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
} from "../../../lib/runner-format";
import { getSurfaceSwitchClassName, type SurfaceSwitch } from "../../../lib/surface-switch";
import { FrameNumberBadge, HorseNameBadge } from "./frame-number-badge";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

type SortKey = "umaban" | "tanshoOdds" | "kakuteiChakujun";
type SortDirection = "asc" | "desc";

interface D1FinishPositionEntry {
  finishPosition: string;
  horseNumber: string;
}

interface BlinkerPatternEntry {
  kettoTorokuBango: string;
  pattern: BlinkerPattern;
}

interface SurfaceSwitchEntry {
  kettoTorokuBango: string;
  surfaceSwitch: SurfaceSwitch;
}

interface RunnersTableProps {
  blinkerPatterns?: ReadonlyArray<BlinkerPatternEntry>;
  d1FinishPositions?: ReadonlyArray<D1FinishPositionEntry>;
  decodeHexHorseWeight?: boolean;
  initialRealtimePayload?: RealtimeRacePayload | null;
  realtimeRequest?: RealtimeRaceRequest;
  runners: Runner[];
  surfaceSwitches?: ReadonlyArray<SurfaceSwitchEntry>;
}

interface GetSortValueParams {
  d1FinishByHorse: Map<string, string>;
  key: SortKey;
  realtimeOddsByHorse: Map<string, number>;
  realtimeResultByHorse: Map<string, string>;
  runner: Runner;
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

const getSortValue = ({
  d1FinishByHorse,
  key,
  realtimeOddsByHorse,
  realtimeResultByHorse,
  runner,
}: GetSortValueParams): number | null => {
  const horseNumber = formatRunnerNumber(runner.umaban);
  if (key === "tanshoOdds") {
    const realtimeOdds = realtimeOddsByHorse.get(horseNumber);
    if (realtimeOdds !== undefined) {
      return realtimeOdds;
    }
    return parseSortValue(runner.tanshoOdds, "0000");
  }
  if (key === "kakuteiChakujun") {
    const realtimeResult = realtimeResultByHorse.get(horseNumber);
    if (realtimeResult !== undefined) {
      return parseSortValue(realtimeResult, "00");
    }
    return parseSortValue(d1FinishByHorse.get(horseNumber) ?? runner.kakuteiChakujun, "00");
  }
  return parseSortValue(runner.umaban);
};

const formatRealtimeOdds = (value: number | undefined): string =>
  value === undefined ? "-" : value.toFixed(1);

const formatStoredOdds = (value: string | null | undefined): string => {
  const parsed = parseSortValue(value, "0000");
  return parsed === null ? "-" : (parsed / 10).toFixed(1);
};

const formatCornerRank = (value: string | null | undefined): string | null => {
  const cleaned = cleanText(value, "");
  if (cleaned === "" || cleaned === "00") {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) && parsed > 0 ? String(parsed) : cleaned;
};

const formatCornerRanks = (runner: Runner): string => {
  const corners = [
    formatCornerRank(runner.corner1),
    formatCornerRank(runner.corner2),
    formatCornerRank(runner.corner3),
    formatCornerRank(runner.corner4),
  ].filter((rank): rank is string => rank !== null);
  return corners.length > 0 ? corners.join("-") : "-";
};

const isLinkableText = (value: string): boolean => value !== "" && value !== "-";

const isChangedJockey = (
  storedName: string,
  realtimeName: string | null | undefined,
  displayName: string,
): boolean => {
  if (!realtimeName || !isLinkableText(storedName)) {
    return false;
  }
  if (
    normalizeJockeyNameForComparison(storedName) === normalizeJockeyNameForComparison(displayName)
  ) {
    return false;
  }
  return !isSameJockeyName(storedName, realtimeName);
};

export function RunnersTable({
  blinkerPatterns,
  d1FinishPositions,
  decodeHexHorseWeight = false,
  initialRealtimePayload = null,
  realtimeRequest,
  runners,
  surfaceSwitches,
}: RunnersTableProps) {
  const [sort, setSort] = useState<SortState | null>(null);
  const blinkerPatternByHorse = useMemo(
    () => new Map((blinkerPatterns ?? []).map((entry) => [entry.kettoTorokuBango, entry.pattern])),
    [blinkerPatterns],
  );
  const surfaceSwitchByHorse = useMemo(
    () =>
      new Map(
        (surfaceSwitches ?? []).map((entry) => [entry.kettoTorokuBango, entry.surfaceSwitch]),
      ),
    [surfaceSwitches],
  );
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
  const horseWeightSnapshot = useHorseWeightStream({
    day: realtimeRequest?.day ?? "",
    initial: initialRealtimePayload?.horseWeights ?? null,
    keibajoCode: realtimeRequest?.keibajoCode ?? "",
    month: realtimeRequest?.month ?? "",
    raceNumber: realtimeRequest?.raceNumber ?? "",
    source: realtimeRequest?.source ?? "",
    year: realtimeRequest?.year ?? "",
  });
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (payload?.odds?.latest.tansho ?? [])
          .filter((row) => row.odds !== undefined)
          .map((row) => [row.combination, Number(row.odds)] as const),
      ),
    [payload],
  );
  const realtimeWeightByHorse = useMemo(() => {
    const effectiveSnapshot = horseWeightSnapshot ?? payload?.horseWeights ?? null;
    const horses = effectiveSnapshot?.horses ?? [];
    return new Map(
      horses.map((horse) => [
        formatRunnerNumber(horse.horseNumber),
        formatHorseWeight(
          horse.weight === null ? null : String(horse.weight),
          horse.changeSign,
          horse.changeAmount === null ? null : String(horse.changeAmount),
        ),
      ]),
    );
  }, [horseWeightSnapshot, payload]);
  const realtimeResultByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceResults?.horses ?? []).map((horse) => [
          horse.horseNumber,
          horse.finishPosition,
        ]),
      ),
    [payload],
  );
  const d1FinishByHorse = useMemo(
    () => buildD1FinishMap(d1FinishPositions ?? []),
    [d1FinishPositions],
  );
  const realtimeEntryByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceEntries?.horses ?? []).map((horse) => [
          formatRunnerNumber(horse.horseNumber),
          {
            jockeyName: cleanText(horse.jockeyName, ""),
            status: cleanText(horse.status, ""),
          },
        ]),
      ),
    [payload],
  );
  const defaultSort = useMemo<SortState>(() => {
    if (
      runners.some(
        (runner) =>
          getSortValue({
            d1FinishByHorse,
            key: "kakuteiChakujun",
            realtimeOddsByHorse,
            realtimeResultByHorse,
            runner,
          }) !== null,
      )
    ) {
      return { direction: "asc", key: "kakuteiChakujun" };
    }
    if (
      runners.some(
        (runner) =>
          getSortValue({
            d1FinishByHorse,
            key: "tanshoOdds",
            realtimeOddsByHorse,
            realtimeResultByHorse,
            runner,
          }) !== null,
      )
    ) {
      return { direction: "asc", key: "tanshoOdds" };
    }
    return { direction: "asc", key: "umaban" };
  }, [d1FinishByHorse, realtimeOddsByHorse, realtimeResultByHorse, runners]);
  const activeSort = sort ?? defaultSort;
  const showCornerRanks = runners.some(
    (runner) => parseSortValue(runner.kakuteiChakujun, "00") !== null,
  );

  const sortedRunners = useMemo(
    () =>
      runners
        .map((runner, index) => ({ index, runner }))
        .toSorted((left, right) => {
          const compared = compareNullableNumber(
            getSortValue({
              d1FinishByHorse,
              key: activeSort.key,
              realtimeOddsByHorse,
              realtimeResultByHorse,
              runner: left.runner,
            }),
            getSortValue({
              d1FinishByHorse,
              key: activeSort.key,
              realtimeOddsByHorse,
              realtimeResultByHorse,
              runner: right.runner,
            }),
            activeSort.direction,
          );
          return compared === 0 ? left.index - right.index : compared;
        })
        .map(({ runner }) => runner),
    [activeSort, d1FinishByHorse, realtimeOddsByHorse, realtimeResultByHorse, runners],
  );

  const changeSort = (key: SortKey) => {
    setSort((current) => ({
      direction:
        (current ?? activeSort).key === key && (current ?? activeSort).direction === "asc"
          ? "desc"
          : "asc",
      key,
    }));
  };

  const renderSortButton = (key: SortKey) => {
    const isCurrent = activeSort.key === key;
    const directionLabel = activeSort.direction === "asc" ? "昇順" : "降順";
    const nextDirectionLabel = isCurrent && activeSort.direction === "asc" ? "降順" : "昇順";

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
    const realtimeFinishPosition = realtimeResultByHorse.get(horseNumber);
    const realtimeEntry = realtimeEntryByHorse.get(horseNumber);
    const horseName = cleanText(runner.bamei);
    const horseId = cleanText(runner.kettoTorokuBango);
    const jockeyName = cleanText(runner.kishumeiRyakusho);
    const displayJockeyName = getPreferredJockeyName(jockeyName, realtimeEntry?.jockeyName);
    const trainerName = cleanText(runner.chokyoshimeiRyakusho);
    const ownerName = cleanText(runner.banushimei);
    const entryStatus = realtimeEntry?.status || "";
    const blinkerPattern = blinkerPatternByHorse.get(cleanText(runner.kettoTorokuBango, ""));
    const surfaceSwitch = surfaceSwitchByHorse.get(cleanText(runner.kettoTorokuBango, ""));

    return (
      <tr
        className={entryStatus ? "runner-row-scratched" : undefined}
        data-entry-status={entryStatus || undefined}
        key={`${runner.umaban}-${runner.kettoTorokuBango}`}
      >
        <td>
          <FrameNumberBadge value={runner.wakuban} />
        </td>
        <td className="runner-number-cell">
          <span>{horseNumber}</span>
        </td>
        <td className="runner-horse-cell">
          {isLinkableText(horseName) && isLinkableText(horseId) ? (
            <Link href={`/horses/${encodeURIComponent(horseId)}`}>
              <HorseNameBadge coatCode={runner.moshokuCode} name={horseName} />
            </Link>
          ) : (
            <HorseNameBadge coatCode={runner.moshokuCode} name={horseName} />
          )}
          {entryStatus ? <span className="runner-status-badge">{entryStatus}</span> : null}
        </td>
        <td className="runner-blinker-cell">
          {blinkerPattern ? (
            <span
              className={`runner-blinker-pattern-badge pattern-${blinkerPattern}`}
              title={BLINKER_PATTERN_LABELS[blinkerPattern]}
            >
              {BLINKER_PATTERN_SHORT_LABELS[blinkerPattern]}
            </span>
          ) : null}
          {surfaceSwitch ? (
            <span
              className={`runner-surface-switch-badge ${getSurfaceSwitchClassName(surfaceSwitch)}`}
            >
              <strong>{surfaceSwitch}</strong>
            </span>
          ) : null}
          {!blinkerPattern && !surfaceSwitch ? "-" : null}
        </td>
        <td>{formatSexAge(runner.seibetsuCode, runner.barei)}</td>
        <td>{formatCarriedWeight(runner.futanJuryo, decodeHexHorseWeight)}</td>
        <td>
          {isLinkableText(displayJockeyName) ? (
            <Link
              className="runner-person-link"
              href={`/jockeys/${encodeURIComponent(displayJockeyName)}`}
            >
              {displayJockeyName}
            </Link>
          ) : (
            displayJockeyName
          )}
          {isChangedJockey(jockeyName, realtimeEntry?.jockeyName, displayJockeyName) ? (
            <small className="runner-change-note">元 {jockeyName}</small>
          ) : null}
        </td>
        <td>
          {isLinkableText(trainerName) ? (
            <Link
              className="runner-person-link"
              href={`/trainers/${encodeURIComponent(trainerName)}`}
            >
              {trainerName}
            </Link>
          ) : (
            trainerName
          )}
        </td>
        <td>
          {isLinkableText(ownerName) ? (
            <Link className="runner-person-link" href={`/owners/${encodeURIComponent(ownerName)}`}>
              {ownerName}
            </Link>
          ) : (
            ownerName
          )}
        </td>
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
            ? formatStoredOdds(runner.tanshoOdds)
            : formatRealtimeOdds(realtimeOdds)}
        </td>
        <td>
          {formatRunnerValue(
            realtimeFinishPosition ?? d1FinishByHorse.get(horseNumber) ?? runner.kakuteiChakujun,
            "00",
          )}
        </td>
        {showCornerRanks ? <td>{formatCornerRanks(runner)}</td> : null}
      </tr>
    );
  };

  return (
    <div className="runner-table-wrap">
      <table className="runner-table">
        <colgroup>
          <col className="runner-col-frame" />
          <col className="runner-col-number" />
          <col className="runner-col-horse" />
          <col className="runner-col-blinker" />
          <col className="runner-col-sex-age" />
          <col className="runner-col-weight" />
          <col className="runner-col-person" />
          <col className="runner-col-person" />
          <col className="runner-col-owner" />
          <col className="runner-col-body" />
          <col className="runner-col-odds" />
          <col className="runner-col-finish" />
          {showCornerRanks ? <col className="runner-col-corner" /> : null}
        </colgroup>
        <thead>
          <tr>
            <th className="runner-frame-header">枠</th>
            <th>{renderSortButton("umaban")}</th>
            <th>馬名</th>
            <th className="runner-blinker-header">
              <span>ブリンカー</span>
              <span>転向</span>
            </th>
            <th>性齢</th>
            <th>負担</th>
            <th>騎手</th>
            <th>調教師</th>
            <th>馬主</th>
            <th>馬体重</th>
            <th>{renderSortButton("tanshoOdds")}</th>
            <th>{renderSortButton("kakuteiChakujun")}</th>
            {showCornerRanks ? <th>コーナー通過順</th> : null}
          </tr>
        </thead>
        <tbody>{sortedRunners.map(renderRunnerRow)}</tbody>
      </table>
    </div>
  );
}
