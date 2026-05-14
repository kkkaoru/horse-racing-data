import Link from "next/link";
import { Suspense } from "react";
import type { CSSProperties } from "react";

import { getTopRaceWindows } from "../db/queries";
import { HomeRealtime } from "./home-realtime";

export const revalidate = 15;

const links = [
  { href: "/races", label: "開催日一覧" },
  { href: "/horses", label: "馬一覧" },
  { href: "/jockeys", label: "騎手一覧" },
  { href: "/owners", label: "馬主一覧" },
  { href: "/trainers", label: "調教師一覧" },
];

const homeTrackRunnerCount = 16;
const jockeyRunnerEmojis = ["🏇", "🏇🏻", "🏇🏼", "🏇🏽", "🏇🏾", "🏇🏿"] as const;
const runnerFilters = [
  "none",
  "hue-rotate(24deg) saturate(1.15)",
  "hue-rotate(72deg) saturate(1.25) brightness(1.04)",
  "hue-rotate(142deg) saturate(1.18)",
  "hue-rotate(205deg) saturate(1.22) brightness(1.02)",
  "hue-rotate(300deg) saturate(1.2)",
  "sepia(0.2) saturate(1.35) brightness(0.96)",
] as const;
const runnerSizes = ["18px", "26px", "36px"] as const;
const racePatterns = ["pack", "runaway", "closer", "duel"] as const;
const pacePatterns = [
  "steady",
  "accelerate-early",
  "accelerate-middle",
  "accelerate-late",
  "surge-fade-early",
  "surge-fade-middle",
  "surge-fade-late",
] as const;

type RacePattern = (typeof racePatterns)[number];
type PacePattern = (typeof pacePatterns)[number];
type HomeTrackRunnerStyle = CSSProperties & {
  "--horse-accelerate-position": string;
  "--horse-decelerate-position": string;
  "--horse-end-position": string;
  "--horse-run-filter": string;
  "--horse-pre-accelerate-position": string;
  "--horse-run-lane": string;
  "--horse-run-size": string;
  "--horse-start-position": string;
};

interface HomeTrackRunner {
  emoji: string;
  key: string;
  pacePattern: PacePattern;
  style: HomeTrackRunnerStyle;
}

function getRacePattern() {
  return racePatterns[Math.floor(Math.random() * racePatterns.length)] ?? "pack";
}

function getRunnerRaceOffsets(index: number, pattern: RacePattern) {
  const randomJitter = Math.random() * 12 - 6;

  if (pattern === "runaway" && index === 0) {
    return { start: -18 + randomJitter, finish: -210 + randomJitter };
  }

  if (pattern === "closer" && index === homeTrackRunnerCount - 1) {
    return { start: 104 + randomJitter, finish: -182 + randomJitter };
  }

  if (pattern === "duel" && index < 2) {
    return {
      start: index * 10 + randomJitter,
      finish: -154 + index * 6 + randomJitter,
    };
  }

  if (pattern === "pack") {
    return {
      start: randomJitter,
      finish: randomJitter - 24,
    };
  }

  return {
    start: randomJitter,
    finish: randomJitter - 28,
  };
}

function getPacePattern(index: number, pattern: RacePattern): PacePattern {
  if (pattern === "runaway" && index === 0) {
    return Math.random() > 0.5 ? "accelerate-early" : "surge-fade-early";
  }

  if (pattern === "closer" && index === homeTrackRunnerCount - 1) {
    return Math.random() > 0.5 ? "accelerate-late" : "surge-fade-late";
  }

  if (pattern === "duel" && index < 2) {
    return Math.random() > 0.5 ? "accelerate-middle" : "surge-fade-middle";
  }

  return pacePatterns[Math.floor(Math.random() * pacePatterns.length)] ?? "steady";
}

function getProgressPosition(progress: number, pxOffset: number) {
  return `calc(${-150 * progress}vw + ${(-148 * progress + pxOffset).toFixed(1)}px)`;
}

function getHomeTrackRunners(): HomeTrackRunner[] {
  const horseOnlySlots = new Set<number>();
  const racePattern = getRacePattern();

  if (Math.random() < 0.1) {
    horseOnlySlots.add(3 + Math.floor(Math.random() * (homeTrackRunnerCount - 3)));
  }

  return Array.from({ length: homeTrackRunnerCount }, (_, index) => {
    const isContender = index < 3;
    const startOffset = isContender
      ? index * 0.05
      : 0.08 + (index % 8) * 0.035 + Math.random() * 0.04;
    const lane = isContender ? index : (index + Math.floor(index / 3)) % 6;
    const size = runnerSizes[Math.floor(Math.random() * runnerSizes.length)] ?? "26px";
    const filter = runnerFilters[Math.floor(Math.random() * runnerFilters.length)] ?? "none";
    const scatterOffset = (index - (homeTrackRunnerCount - 1) / 2) * 9 + (Math.random() * 10 - 5);
    const raceOffsets = getRunnerRaceOffsets(index, racePattern);
    const pacePattern = getPacePattern(index, racePattern);
    const startPosition = `${(startOffset * 90 + raceOffsets.start + scatterOffset).toFixed(1)}px`;
    const finishOffset = raceOffsets.finish + Math.random() * 12 - 6;
    const accelerateProgress = 0.16 + Math.random() * 0.5;
    const decelerateProgress = Math.min(0.86, accelerateProgress + 0.18 + Math.random() * 0.18);
    const acceleratePosition = getProgressPosition(
      accelerateProgress * 0.62,
      raceOffsets.start * 0.24 + scatterOffset * 0.36,
    );
    const preAcceleratePosition = getProgressPosition(
      accelerateProgress * 0.34,
      raceOffsets.start * 0.34 + scatterOffset * 0.42,
    );
    const deceleratePosition = getProgressPosition(
      decelerateProgress * 1.08,
      finishOffset * 0.48 + scatterOffset * 0.18,
    );
    const style: HomeTrackRunnerStyle = {
      "--horse-start-position": startPosition,
      "--horse-pre-accelerate-position": preAcceleratePosition,
      "--horse-accelerate-position": acceleratePosition,
      "--horse-decelerate-position": deceleratePosition,
      "--horse-end-position": `calc(-150vw + ${(-148 + finishOffset).toFixed(1)}px)`,
      "--horse-run-filter": filter,
      "--horse-run-lane": `${lane}`,
      "--horse-run-size": size,
    };

    const isHorseOnly = horseOnlySlots.has(index);
    const emoji = isHorseOnly
      ? "🐎"
      : (jockeyRunnerEmojis[Math.floor(Math.random() * jockeyRunnerEmojis.length)] ?? "🏇");

    return {
      emoji,
      key: `${racePattern}-${index}-${isHorseOnly ? "horse" : "jockey"}-${emoji}-${lane}`,
      pacePattern,
      style,
    };
  });
}

async function HomeRealtimePanel() {
  const { finished, upcoming } = await getTopRaceWindows().catch((error: unknown) => {
    console.error("Failed to load top race windows", error);
    return { finished: [], upcoming: [] };
  });
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

  return (
    <HomeRealtime
      initialFinished={finished}
      initialUpcoming={upcoming}
      realtimeApiBaseUrl={realtimeApiBaseUrl}
    />
  );
}

function HomeRealtimeSkeleton() {
  return (
    <div className="home-live-grid" aria-label="トップページのレース情報を読み込み中">
      {["次のレース", "直近の発走済み", "オッズ更新"].map((title, index) => (
        <section className={index === 2 ? "home-panel home-panel-wide" : "home-panel"} key={title}>
          <div className="section-heading compact">
            <h2>{title}</h2>
            <span>読み込み中</span>
          </div>
          <div className="home-race-list home-race-list-skeleton">
            {Array.from({ length: index === 2 ? 3 : 5 }, (_, itemIndex) => (
              <div className="home-race-skeleton-item" key={itemIndex}>
                <span className="skeleton-text short" />
                <span className="skeleton-text medium" />
              </div>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

export default function HomePage() {
  const homeTrackRunners = getHomeTrackRunners();

  return (
    <section className="page-shell home-shell">
      <div className="home-heading">
        <p className="eyebrow">PC-KEIBA Viewer</p>
        <h1 className="home-title">
          <span>レースと成績をすばやく確認</span>
          <span className="home-horse-track" aria-hidden="true">
            <span className="home-finish-gate">GOAL</span>
            {homeTrackRunners.map((runner, index) => (
              <span
                className={`home-running-horse runner-${index + 1} pace-${runner.pacePattern}`}
                key={runner.key}
                style={runner.style}
              >
                {runner.emoji}
              </span>
            ))}
          </span>
        </h1>
      </div>
      <nav className="home-link-grid" aria-label="primary pages">
        {links.map((link) => (
          <Link href={link.href} key={link.href}>
            {link.label}
          </Link>
        ))}
      </nav>
      <Suspense fallback={<HomeRealtimeSkeleton />}>
        <HomeRealtimePanel />
      </Suspense>
    </section>
  );
}
