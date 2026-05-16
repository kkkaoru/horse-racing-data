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
  { href: "/mypage", label: "マイページ" },
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
const racePatterns = [
  "pack",
  "runaway",
  "closer",
  "duel",
  "wide",
  "late-pack",
  "front-pack",
  "staggered",
] as const;
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
  "--horse-run-duration": string;
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
  const randomJitter = Math.random() * 28 - 14;
  const groupRank = index - (homeTrackRunnerCount - 1) / 2;

  if (pattern === "runaway" && index === 0) {
    return { start: -72 + randomJitter, finish: -250 + randomJitter };
  }

  if (pattern === "closer" && index === homeTrackRunnerCount - 1) {
    return { start: 170 + randomJitter, finish: -240 + randomJitter };
  }

  if (pattern === "duel" && index < 2) {
    return {
      start: -24 + index * 18 + randomJitter,
      finish: -210 + index * 10 + randomJitter,
    };
  }

  if (pattern === "wide") {
    return {
      start: groupRank * 18 + randomJitter,
      finish: groupRank * -16 + randomJitter,
    };
  }

  if (pattern === "late-pack") {
    return {
      start: groupRank * 20 + randomJitter,
      finish: randomJitter - 48,
    };
  }

  if (pattern === "front-pack") {
    return {
      start: randomJitter - 24,
      finish: groupRank * 18 + randomJitter,
    };
  }

  if (pattern === "staggered") {
    return {
      start: ((index % 4) - 1.5) * 46 + randomJitter,
      finish: ((index % 5) - 2) * 38 + randomJitter,
    };
  }

  if (pattern === "pack") {
    return {
      start: randomJitter,
      finish: randomJitter - 36,
    };
  }

  return {
    start: randomJitter,
    finish: randomJitter - 44,
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
  const raceDuration = 6.5 + Math.random() * 3.2;
  const laneShift = Math.floor(Math.random() * 6);
  const laneStep = 7 + Math.floor(Math.random() * 4);
  const startSpread = 55 + Math.random() * 90;
  const finishSpread = 16 + Math.random() * 54;

  if (Math.random() < 0.1) {
    horseOnlySlots.add(3 + Math.floor(Math.random() * (homeTrackRunnerCount - 3)));
  }

  return Array.from({ length: homeTrackRunnerCount }, (_, index) => {
    const isContender = index < 3;
    const startOffset = isContender
      ? index * 0.04
      : 0.04 + (index % 8) * 0.028 + Math.random() * 0.08;
    const lane = isContender
      ? (index + laneShift) % 6
      : (index + Math.floor(index / 3) + laneShift) % 6;
    const size = runnerSizes[Math.floor(Math.random() * runnerSizes.length)] ?? "26px";
    const filter = runnerFilters[Math.floor(Math.random() * runnerFilters.length)] ?? "none";
    const scatterOffset =
      (index - (homeTrackRunnerCount - 1) / 2) * (startSpread / homeTrackRunnerCount) +
      (Math.random() * 20 - 10);
    const raceOffsets = getRunnerRaceOffsets(index, racePattern);
    const pacePattern = getPacePattern(index, racePattern);
    const startPosition = `${(startOffset * 90 + raceOffsets.start + scatterOffset).toFixed(1)}px`;
    const finishOffset =
      raceOffsets.finish +
      (index - (homeTrackRunnerCount - 1) / 2) * (finishSpread / homeTrackRunnerCount) +
      Math.random() * 24 -
      12;
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
      "--horse-run-duration": `${raceDuration.toFixed(2)}s`,
      "--horse-run-filter": filter,
      "--horse-run-lane": `${lane * laneStep}`,
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
  let initialLoadFailed = false;
  const { finished, upcoming } = await getTopRaceWindows().catch((error: unknown) => {
    console.error("Failed to load top race windows", error);
    initialLoadFailed = true;
    return { finished: [], upcoming: [] };
  });

  return (
    <HomeRealtime
      initialFinished={finished}
      initialLoadFailed={initialLoadFailed}
      initialNow={Date.now()}
      initialUpcoming={upcoming}
    />
  );
}

const homeRaceListMinHeight = (count: number): string =>
  `${count * 42 + Math.max(0, count - 1) * 8}px`;

function HomeRealtimeSkeleton() {
  return (
    <div className="home-live-grid" aria-label="トップページのレース情報を読み込み中">
      {["次のレース", "直近の発走済み", "オッズ更新"].map((title, index) => (
        <section className={index === 2 ? "home-panel home-panel-wide" : "home-panel"} key={title}>
          <div className="section-heading compact">
            <h2>{title}</h2>
            <span>読み込み中</span>
          </div>
          <div
            className="home-race-list home-race-list-skeleton"
            style={{ minHeight: homeRaceListMinHeight(index === 2 ? 3 : 5) }}
          >
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
