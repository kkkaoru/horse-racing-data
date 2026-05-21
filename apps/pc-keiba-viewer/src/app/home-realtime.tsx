"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTime,
  getTrackSurfaceLabel,
} from "../lib/format";
import { getNextOddsFetchAt } from "../lib/odds-schedule";
import type { TopRaceSummary } from "../lib/race-types";

interface HomeRealtimeProps {
  initialFinished: TopRaceSummary[];
  initialLoadFailed: boolean;
  initialNow: number;
  initialUpcoming: TopRaceSummary[];
}

type RaceWindowsPayload = {
  finished: TopRaceSummary[];
  upcoming: TopRaceSummary[];
};

type ScheduledTask = {
  id: string;
  label: string;
  scheduledAt: string;
  race: TopRaceSummary;
};

type AsyncStatus = "loading" | "ready" | "error";
const SCHEDULE_PAGE_SIZE = 6;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRaceWindowsPayload = (data: unknown): data is RaceWindowsPayload => {
  if (!isRecord(data)) {
    return false;
  }
  return Array.isArray(data.finished) && Array.isArray(data.upcoming);
};

const racePath = (race: TopRaceSummary): string =>
  `/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}`;

const raceKey = (race: TopRaceSummary): string =>
  `${race.source}-${race.kaisaiNen}${race.kaisaiTsukihi}-${race.keibajoCode}-${race.raceBango}`;

const formatCountdown = (target: string, now: number, includeSeconds: boolean): string => {
  const diff = new Date(target).getTime() - now;
  if (diff <= 0) {
    return "発走済み";
  }
  const totalSeconds = Math.floor(diff / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (includeSeconds) {
    return hours > 0
      ? `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`
      : `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  if (hours > 0) {
    return `${hours}時間${String(minutes).padStart(2, "0")}分`;
  }
  return `${minutes}分`;
};

const countdownIcon = (target: string, now: number): string => {
  const remainingMinutes = Math.max(0, Math.floor((new Date(target).getTime() - now) / 60000));
  if (remainingMinutes <= 5) {
    return "🔥";
  }
  if (remainingMinutes <= 30) {
    return "⏱️";
  }
  return "🕒";
};

const getRaceStartMs = (race: TopRaceSummary): number => new Date(race.raceStartAt).getTime();

const formatRaceLine = (race: TopRaceSummary): string =>
  [
    formatKeibajo(race.keibajoCode),
    formatRaceNumber(race.raceBango),
    formatTime(race.hassoJikoku),
    getTrackSurfaceLabel(race.trackCode),
    formatDistance(race.kyori),
  ].join(" / ");

const getVenueMeetingKey = (race: TopRaceSummary): string =>
  `${race.source}:${race.kaisaiNen}${race.kaisaiTsukihi}:${race.keibajoCode}`;

const getVenueLastRaceStartAtByMeeting = (races: TopRaceSummary[]): Map<string, string> => {
  const result = new Map<string, string>();
  for (const race of races) {
    const key = getVenueMeetingKey(race);
    const current = result.get(key);
    if (!current || getRaceStartMs(race) > new Date(current).getTime()) {
      result.set(key, race.raceStartAt);
    }
  }
  return result;
};

const buildScheduledTasks = (races: TopRaceSummary[], now: number): ScheduledTask[] => {
  const venueLastRaceStartAt = getVenueLastRaceStartAtByMeeting(races);
  return races
    .flatMap((race): ScheduledTask[] => {
      if (race.source !== "nar" && race.source !== "jra") {
        return [];
      }
      const scheduledAt = getNextOddsFetchAt(race.raceStartAt, now, race.source, {
        keibajoCode: race.keibajoCode,
        venueLastRaceStartAt: venueLastRaceStartAt.get(getVenueMeetingKey(race)),
      });
      if (!scheduledAt) {
        return [];
      }
      return [
        {
          id: `${raceKey(race)}-odds`,
          label: "オッズ更新",
          race,
          scheduledAt,
        },
      ];
    })
    .toSorted(
      (left, right) =>
        new Date(left.scheduledAt).getTime() - new Date(right.scheduledAt).getTime() ||
        getRaceStartMs(left.race) - getRaceStartMs(right.race),
    );
};

const formatTaskCountdown = (target: string, now: number): string => {
  const diff = new Date(target).getTime() - now;
  if (diff <= 0) {
    return "まもなく";
  }
  const totalSeconds = Math.ceil(diff / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
};

const homeRaceListMinHeight = (count: number): string =>
  `${count * 42 + Math.max(0, count - 1) * 8}px`;

function HomeRaceListSkeleton({ count = 5 }: { count?: number }) {
  return (
    <div
      className="home-race-list home-race-list-skeleton"
      aria-busy="true"
      style={{ minHeight: homeRaceListMinHeight(count) }}
    >
      {Array.from({ length: count }, (_, itemIndex) => (
        <div className="home-race-skeleton-item" key={itemIndex}>
          <span className="skeleton-text short" />
          <span className="skeleton-text medium" />
        </div>
      ))}
    </div>
  );
}

function HomeRaceListMessage({ children, count = 5 }: { children: string; count?: number }) {
  return (
    <div className="home-race-list">
      <p
        className="empty-state home-race-list-message"
        style={{ minHeight: homeRaceListMinHeight(count) }}
      >
        {children}
      </p>
    </div>
  );
}

export function HomeRealtime({
  initialFinished,
  initialLoadFailed,
  initialNow,
  initialUpcoming,
}: HomeRealtimeProps) {
  const [now, setNow] = useState(initialNow);
  const [raceWindowsStatus, setRaceWindowsStatus] = useState<AsyncStatus>(
    initialLoadFailed ? "error" : "ready",
  );
  const [raceWindows, setRaceWindows] = useState<RaceWindowsPayload>({
    finished: initialFinished,
    upcoming: initialUpcoming,
  });
  const [schedulePage, setSchedulePage] = useState(1);
  const races = useMemo(
    () =>
      [...raceWindows.finished, ...raceWindows.upcoming]
        .filter((race) => Number.isFinite(getRaceStartMs(race)))
        .toSorted((left, right) => getRaceStartMs(left) - getRaceStartMs(right)),
    [raceWindows],
  );
  const upcoming = races.filter((race) => getRaceStartMs(race) > now).slice(0, 5);
  const finished = races
    .filter((race) => getRaceStartMs(race) <= now)
    .slice(-5)
    .toReversed();
  const scheduledTasks = useMemo(() => buildScheduledTasks(races, now), [races, now]);
  const nextScheduledTaskAt = scheduledTasks[0]?.scheduledAt ?? null;
  const totalSchedulePages = Math.max(1, Math.ceil(scheduledTasks.length / SCHEDULE_PAGE_SIZE));
  const currentSchedulePage = Math.min(schedulePage, totalSchedulePages);
  const visibleScheduledTasks = scheduledTasks.slice(
    (currentSchedulePage - 1) * SCHEDULE_PAGE_SIZE,
    currentSchedulePage * SCHEDULE_PAGE_SIZE,
  );

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const load = async () => {
      try {
        const response = await fetch("/api/top-races", { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const data: unknown = await response.json();
        if (isRaceWindowsPayload(data)) {
          setRaceWindows(data);
          setRaceWindowsStatus("ready");
        }
      } catch {
        // Keep the initial server-rendered race windows if refresh fails.
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 30_000);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <div className="home-live-grid">
      <section className="home-panel">
        <div className="section-heading compact">
          <h2>次のレース</h2>
          <span>自動更新</span>
        </div>
        {raceWindowsStatus === "loading" ? (
          <HomeRaceListSkeleton />
        ) : raceWindowsStatus === "error" ? (
          <HomeRaceListMessage>次のレースを読み込めませんでした。</HomeRaceListMessage>
        ) : upcoming.length > 0 ? (
          <div className="home-race-list">
            {upcoming.map((race, index) => (
              <Link href={racePath(race)} key={raceKey(race)}>
                <strong className="home-race-countdown">
                  <span className="home-race-countdown-icon" aria-hidden="true">
                    {countdownIcon(race.raceStartAt, now)}
                  </span>
                  <span>{formatCountdown(race.raceStartAt, now, index === 0)}</span>
                </strong>
                <span>{formatRaceLine(race)}</span>
              </Link>
            ))}
          </div>
        ) : (
          <HomeRaceListMessage>次のレースは見つかりませんでした。</HomeRaceListMessage>
        )}
      </section>
      <section className="home-panel">
        <div className="section-heading compact">
          <h2>直近の発走済み</h2>
          <span>最大5件</span>
        </div>
        {raceWindowsStatus === "loading" ? (
          <HomeRaceListSkeleton />
        ) : raceWindowsStatus === "error" ? (
          <HomeRaceListMessage>発走済みレースを読み込めませんでした。</HomeRaceListMessage>
        ) : finished.length > 0 ? (
          <div className="home-race-list">
            {finished.map((race) => (
              <Link href={racePath(race)} key={raceKey(race)}>
                <strong>{formatRaceLine(race)}</strong>
                <span>{race.raceStartAt.slice(5, 16).replace("T", " ")}</span>
              </Link>
            ))}
          </div>
        ) : (
          <HomeRaceListMessage>発走済みのレースは見つかりませんでした。</HomeRaceListMessage>
        )}
      </section>
      <section className="home-panel home-panel-wide">
        <div className="section-heading compact">
          <h2>スケジュール一覧</h2>
          <span>
            次回予定{" "}
            {nextScheduledTaskAt ? new Date(nextScheduledTaskAt).toLocaleTimeString("ja-JP") : "-"}
          </span>
        </div>
        {raceWindowsStatus === "loading" ? (
          <HomeRaceListSkeleton count={3} />
        ) : raceWindowsStatus === "error" ? (
          <HomeRaceListMessage count={3}>スケジュールを読み込めませんでした。</HomeRaceListMessage>
        ) : scheduledTasks.length > 0 ? (
          <>
            <div className="home-race-list">
              {visibleScheduledTasks.map((task) => (
                <Link href={racePath(task.race)} key={`${task.id}-${task.scheduledAt}`}>
                  <strong>
                    {task.label} / {formatRaceLine(task.race)}
                  </strong>
                  <span>
                    予定 {new Date(task.scheduledAt).toLocaleTimeString("ja-JP")} / 実行まで{" "}
                    {formatTaskCountdown(task.scheduledAt, now)}
                  </span>
                </Link>
              ))}
            </div>
            <div className="home-schedule-pagination" aria-label="スケジュール一覧のページ操作">
              <span>
                {currentSchedulePage} / {totalSchedulePages}ページ（{scheduledTasks.length}件）
              </span>
              <div>
                <button
                  disabled={currentSchedulePage <= 1}
                  onClick={() => setSchedulePage(Math.max(1, currentSchedulePage - 1))}
                  type="button"
                >
                  前へ
                </button>
                <button
                  disabled={currentSchedulePage >= totalSchedulePages}
                  onClick={() =>
                    setSchedulePage(Math.min(totalSchedulePages, currentSchedulePage + 1))
                  }
                  type="button"
                >
                  次へ
                </button>
              </div>
            </div>
          </>
        ) : (
          <HomeRaceListMessage count={3}>
            予定されている実行タスクはありません。
          </HomeRaceListMessage>
        )}
      </section>
    </div>
  );
}
