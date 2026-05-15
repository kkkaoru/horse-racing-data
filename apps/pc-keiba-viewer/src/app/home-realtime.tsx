"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
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
import { buildRealtimeUrl, isRealtimeRacePayload } from "./races/detail/realtime-client";

interface HomeRealtimeProps {
  initialFinished: TopRaceSummary[];
  initialUpcoming: TopRaceSummary[];
  realtimeApiBaseUrl: string;
}

type RaceWindowsPayload = {
  finished: TopRaceSummary[];
  upcoming: TopRaceSummary[];
};

type OddsSchedule = {
  lastFetchedAt: string | null;
  nextFetchAt: string;
  race: TopRaceSummary;
};

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

const formatRaceLine = (race: TopRaceSummary): string =>
  [
    formatKeibajo(race.keibajoCode),
    formatRaceNumber(race.raceBango),
    formatTime(race.hassoJikoku),
    getTrackSurfaceLabel(race.trackCode),
    formatDistance(race.kyori),
  ].join(" / ");

export function HomeRealtime({
  initialFinished,
  initialUpcoming,
  realtimeApiBaseUrl,
}: HomeRealtimeProps) {
  const [now, setNow] = useState(() => Date.now());
  const [raceWindows, setRaceWindows] = useState<RaceWindowsPayload>({
    finished: initialFinished,
    upcoming: initialUpcoming,
  });
  const [oddsSchedules, setOddsSchedules] = useState<OddsSchedule[]>([]);
  const [nextOddsFetchAt, setNextOddsFetchAt] = useState<string | null>(null);
  const races = useMemo(
    () =>
      [...raceWindows.finished, ...raceWindows.upcoming].toSorted(
        (left, right) =>
          new Date(left.raceStartAt).getTime() - new Date(right.raceStartAt).getTime(),
      ),
    [raceWindows],
  );
  const upcoming = races.filter((race) => new Date(race.raceStartAt).getTime() >= now).slice(0, 5);
  const finished = races
    .filter((race) => new Date(race.raceStartAt).getTime() < now)
    .slice(-5)
    .toReversed();
  const upcomingOddsRaces = useMemo(
    () => raceWindows.upcoming.filter((race) => race.source === "nar" || race.source === "jra"),
    [raceWindows.upcoming],
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
        }
      } catch {
        // Keep the initial server-rendered race windows if refresh fails.
      }
    };
    const timer = window.setInterval(() => void load(), 60_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const load = async () => {
      const payloads = await Promise.all(
        upcomingOddsRaces.map(
          async (race): Promise<[TopRaceSummary, RealtimeRacePayload] | null> => {
            const url = buildRealtimeUrl({
              apiBaseUrl: realtimeApiBaseUrl,
              day: race.kaisaiTsukihi.slice(2, 4),
              keibajoCode: race.keibajoCode,
              month: race.kaisaiTsukihi.slice(0, 2),
              raceNumber: race.raceBango,
              source: race.source,
              year: race.kaisaiNen,
            });
            if (!url) {
              return null;
            }
            try {
              const response = await fetch(url, { cache: "no-store" });
              const data: unknown = await response.json();
              return isRealtimeRacePayload(data) ? [race, data] : null;
            } catch {
              return null;
            }
          },
        ),
      );
      const validPayloads = payloads.filter(
        (payload): payload is [TopRaceSummary, RealtimeRacePayload] => payload !== null,
      );
      const payloadsByRace = new Map(
        validPayloads.map(([race, payload]) => [
          `${race.source}-${race.keibajoCode}-${race.raceBango}`,
          payload,
        ]),
      );
      const schedules = upcomingOddsRaces
        .flatMap((race): OddsSchedule[] => {
          const nextFetchAt = getNextOddsFetchAt(race.raceStartAt, Date.now(), race.source);
          if (!nextFetchAt) {
            return [];
          }
          const payload = payloadsByRace.get(
            `${race.source}-${race.keibajoCode}-${race.raceBango}`,
          );
          return [
            {
              lastFetchedAt: payload?.odds?.fetchedAt ?? payload?.source?.lastOddsFetchAt ?? null,
              nextFetchAt,
              race,
            },
          ];
        })
        .toSorted(
          (left, right) =>
            new Date(left.nextFetchAt).getTime() - new Date(right.nextFetchAt).getTime(),
        );
      setNextOddsFetchAt(schedules[0]?.nextFetchAt ?? null);
      setOddsSchedules(schedules.slice(0, 5));
    };
    void load();
    const timer = window.setInterval(() => void load(), 30_000);
    return () => window.clearInterval(timer);
  }, [realtimeApiBaseUrl, upcomingOddsRaces]);

  return (
    <div className="home-live-grid">
      <section className="home-panel">
        <div className="section-heading compact">
          <h2>次のレース</h2>
          <span>自動更新</span>
        </div>
        <div className="home-race-list">
          {upcoming.map((race, index) => (
            <Link
              href={racePath(race)}
              key={`${race.source}-${race.keibajoCode}-${race.raceBango}`}
            >
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
      </section>
      <section className="home-panel">
        <div className="section-heading compact">
          <h2>直近の発走済み</h2>
          <span>最大5件</span>
        </div>
        <div className="home-race-list">
          {finished.map((race) => (
            <Link
              href={racePath(race)}
              key={`${race.source}-${race.keibajoCode}-${race.raceBango}`}
            >
              <strong>{formatRaceLine(race)}</strong>
              <span>{race.raceStartAt.slice(5, 16).replace("T", " ")}</span>
            </Link>
          ))}
        </div>
      </section>
      <section className="home-panel home-panel-wide">
        <div className="section-heading compact">
          <h2>オッズ更新</h2>
          <span>
            次回予定 {nextOddsFetchAt ? new Date(nextOddsFetchAt).toLocaleTimeString("ja-JP") : "-"}
          </span>
        </div>
        <div className="home-race-list">
          {oddsSchedules.length > 0 ? (
            oddsSchedules.map((schedule) => (
              <Link
                href={racePath(schedule.race)}
                key={`${schedule.race.keibajoCode}-${schedule.race.raceBango}-${schedule.nextFetchAt}`}
              >
                <strong>{formatRaceLine(schedule.race)}</strong>
                <span>
                  次回 {new Date(schedule.nextFetchAt).toLocaleTimeString("ja-JP")}
                  {schedule.lastFetchedAt
                    ? ` / 更新 ${new Date(schedule.lastFetchedAt).toLocaleTimeString("ja-JP")}`
                    : ""}
                </span>
              </Link>
            ))
          ) : (
            <p className="empty-state">対象のオッズ更新予定はありません。</p>
          )}
        </div>
      </section>
    </div>
  );
}
