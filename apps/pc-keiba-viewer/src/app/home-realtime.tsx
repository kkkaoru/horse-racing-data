"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { formatDistance, formatKeibajo, formatRaceNumber, formatTime } from "../lib/format";
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

type OddsUpdate = {
  fetchedAt: string;
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

const formatCountdown = (target: string, now: number): string => {
  const diff = new Date(target).getTime() - now;
  if (diff <= 0) {
    return "発走済み";
  }
  const totalSeconds = Math.floor(diff / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${hours}時間${String(minutes).padStart(2, "0")}分${String(seconds).padStart(2, "0")}秒`;
};

const formatRaceLine = (race: TopRaceSummary): string =>
  [
    formatKeibajo(race.keibajoCode),
    formatRaceNumber(race.raceBango),
    formatTime(race.hassoJikoku),
    formatDistance(race.kyori),
  ].join(" / ");

const getNextOddsFetchAt = (payload: RealtimeRacePayload): string | null => {
  const fetchedAt = payload.odds?.fetchedAt ?? payload.source?.lastOddsFetchAt;
  if (!fetchedAt || !payload.source?.raceStartAtJst) {
    return null;
  }
  const raceStart = new Date(payload.source.raceStartAtJst).getTime();
  const fetched = new Date(fetchedAt).getTime();
  if (!Number.isFinite(raceStart) || !Number.isFinite(fetched)) {
    return null;
  }
  const minutesToStart = (raceStart - Date.now()) / 60_000;
  const intervalMinutes = minutesToStart <= 30 ? 3 : 10;
  return new Date(fetched + intervalMinutes * 60_000).toISOString();
};

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
  const [updates, setUpdates] = useState<OddsUpdate[]>([]);
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
    () => raceWindows.upcoming.filter((race) => race.source === "nar").slice(0, 5),
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
      const nextFetches = validPayloads
        .map(([, payload]) => getNextOddsFetchAt(payload))
        .filter((value): value is string => value !== null)
        .toSorted((left, right) => new Date(left).getTime() - new Date(right).getTime());
      setNextOddsFetchAt(nextFetches[0] ?? null);
      setUpdates(
        validPayloads
          .flatMap(([race, payload]) =>
            payload.odds?.fetchedAt ? [{ fetchedAt: payload.odds.fetchedAt, race }] : [],
          )
          .toSorted(
            (left, right) =>
              new Date(right.fetchedAt).getTime() - new Date(left.fetchedAt).getTime(),
          )
          .slice(0, 5),
      );
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
          {upcoming.map((race) => (
            <Link
              href={racePath(race)}
              key={`${race.source}-${race.keibajoCode}-${race.raceBango}`}
            >
              <strong>{formatCountdown(race.raceStartAt, now)}</strong>
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
          {updates.length > 0 ? (
            updates.map((update) => (
              <Link
                href={racePath(update.race)}
                key={`${update.race.keibajoCode}-${update.race.raceBango}-${update.fetchedAt}`}
              >
                <strong>{formatRaceLine(update.race)}</strong>
                <span>{new Date(update.fetchedAt).toLocaleString("ja-JP")} 更新</span>
              </Link>
            ))
          ) : (
            <p className="empty-state">取得済みのオッズ更新はまだありません。</p>
          )}
        </div>
      </section>
    </div>
  );
}
