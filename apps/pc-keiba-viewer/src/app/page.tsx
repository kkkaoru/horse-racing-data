import type { Metadata } from "next";
import Link from "next/link";
import { Suspense } from "react";
import type { CSSProperties } from "react";

import { getTopRaceWindows } from "../db/queries";
import { HomeRealtime } from "./home-realtime";

export const revalidate = 15;

export const metadata: Metadata = {
  title: "ホーム",
};

const links = [
  { href: "/races", label: "開催日一覧" },
  { href: "/horses", label: "馬一覧" },
  { href: "/jockeys", label: "騎手一覧" },
  { href: "/owners", label: "馬主一覧" },
  { href: "/trainers", label: "調教師一覧" },
  { href: "/mypage", label: "マイページ" },
];

const minHomeTrackRunnerCount = 4;
const maxHomeTrackRunnerCount = 18;
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
type HomeTrackRunnerStyle = CSSProperties &
  Partial<Record<`--horse-race-position-${number}`, string>> & {
    "--horse-accelerate-position": string;
    "--horse-approach-position": string;
    "--horse-decelerate-position": string;
    "--horse-end-position": string;
    "--horse-gallop-delay": string;
    "--horse-gallop-duration": string;
    "--horse-gallop-forward-tilt": string;
    "--horse-gallop-lift": string;
    "--horse-gallop-land": string;
    "--horse-gallop-rear-tilt": string;
    "--horse-run-opacity": string;
    "--horse-run-duration": string;
    "--horse-run-filter": string;
    "--horse-pre-accelerate-position": string;
    "--horse-run-lane": string;
    "--horse-run-scale": string;
    "--horse-run-size": string;
    "--horse-start-position": string;
    "--horse-y-end": string;
    "--horse-y-mid": string;
    "--horse-y-start": string;
  };

interface HomeTrackRunner {
  emoji: string;
  key: string;
  keyframes: string;
  pacePattern: PacePattern;
  style: HomeTrackRunnerStyle;
}

interface RunnerMotionProfile {
  stops: Array<{
    fraction: number;
    progress: number;
    yProgress: number;
  }>;
}

function getRacePattern() {
  return racePatterns[Math.floor(Math.random() * racePatterns.length)] ?? "pack";
}

function getRunnerRaceOffsets(index: number, pattern: RacePattern, runnerCount: number) {
  const randomJitter = Math.random() * 28 - 14;
  const groupRank = index - (runnerCount - 1) / 2;

  if (pattern === "runaway" && index === 0) {
    return { start: -72 + randomJitter, finish: -250 + randomJitter };
  }

  if (pattern === "closer" && index === runnerCount - 1) {
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

function getPacePattern(index: number, pattern: RacePattern, runnerCount: number): PacePattern {
  if (pattern === "runaway" && index === 0) {
    return Math.random() > 0.5 ? "accelerate-early" : "surge-fade-early";
  }

  if (pattern === "closer" && index === runnerCount - 1) {
    return Math.random() > 0.5 ? "accelerate-late" : "surge-fade-late";
  }

  if (pattern === "duel" && index < 2) {
    return Math.random() > 0.5 ? "accelerate-middle" : "surge-fade-middle";
  }

  return pacePatterns[Math.floor(Math.random() * pacePatterns.length)] ?? "steady";
}

function getTravelPosition(fraction: number) {
  return `calc(${(-18 - 132 * fraction).toFixed(2)}vw + ${(-65 - 83 * fraction).toFixed(1)}px)`;
}

function getMotionYPosition(progress: number) {
  if (progress < 0.34) {
    return "var(--horse-y-start, 0px)";
  }

  if (progress < 0.68) {
    return "var(--horse-y-mid, 0px)";
  }

  return "var(--horse-y-end, 0px)";
}

function getRunnerMotionProfile(): RunnerMotionProfile {
  const progressStops = [
    0.12 + Math.random() * 0.03,
    0.25 + Math.random() * 0.04,
    0.4 + Math.random() * 0.05,
    0.56 + Math.random() * 0.05,
    0.72 + Math.random() * 0.05,
    0.86 + Math.random() * 0.04,
  ].toSorted((left, right) => left - right);
  const segmentDurations = [
    progressStops[0] ?? 0.14,
    (progressStops[1] ?? 0.27) - (progressStops[0] ?? 0.14),
    (progressStops[2] ?? 0.43) - (progressStops[1] ?? 0.27),
    (progressStops[3] ?? 0.6) - (progressStops[2] ?? 0.43),
    (progressStops[4] ?? 0.74) - (progressStops[3] ?? 0.6),
    (progressStops[5] ?? 0.88) - (progressStops[4] ?? 0.74),
    1 - (progressStops[5] ?? 0.88),
  ];
  const baseVelocity = 0.52 + Math.random() * 0.22;
  const peakVelocity = baseVelocity + 0.78 + Math.random() * 0.72;
  const velocities = [
    baseVelocity,
    baseVelocity + (peakVelocity - baseVelocity) * (0.24 + Math.random() * 0.1),
    baseVelocity + (peakVelocity - baseVelocity) * (0.48 + Math.random() * 0.1),
    baseVelocity + (peakVelocity - baseVelocity) * (0.78 + Math.random() * 0.08),
    peakVelocity,
    peakVelocity * (0.44 + Math.random() * 0.14),
    peakVelocity * (0.18 + Math.random() * 0.1),
  ];
  const rawDistances = segmentDurations.map(
    (duration, index) => duration * (velocities[index] ?? 1),
  );
  const totalDistance = rawDistances.reduce((sum, distance) => sum + distance, 0);
  let accumulatedDistance = 0;

  return {
    stops: progressStops.map((progress, index) => {
      accumulatedDistance += rawDistances[index] ?? 0;
      return {
        fraction: accumulatedDistance / totalDistance,
        progress,
        yProgress: progress,
      };
    }),
  };
}

const getRaceKeyframeStops = ({
  approachDuration,
  holdDuration,
  raceDuration,
}: {
  approachDuration: number;
  holdDuration: number;
  raceDuration: number;
}) => {
  const totalDuration = approachDuration + holdDuration + raceDuration;
  const raceStart = ((approachDuration + holdDuration) / totalDuration) * 100;
  const raceRange = 100 - raceStart;
  return {
    approachEnd: (approachDuration / totalDuration) * 100,
    raceStart,
    totalDuration,
    at: (raceProgress: number) => raceStart + raceRange * raceProgress,
  };
};

const buildRunnerKeyframes = ({
  keyframesName,
  motionProfile,
  stops,
}: {
  keyframesName: string;
  motionProfile: RunnerMotionProfile;
  stops: ReturnType<typeof getRaceKeyframeStops>;
}): string => {
  const raceStops = motionProfile.stops.map((stop, index) => ({
    position: `var(--horse-race-position-${index + 1}, ${getTravelPosition(stop.fraction)})`,
    progress: stop.progress,
    yPosition: getMotionYPosition(stop.yProgress),
  }));

  return `
@keyframes ${keyframesName} {
  0% { transform: translateX(var(--horse-approach-position, 160px)) translateY(var(--horse-y-start, 0px)); }
  ${stops.approachEnd.toFixed(3)}% { transform: translateX(var(--horse-start-position, 0px)) translateY(var(--horse-y-start, 0px)); }
  ${stops.raceStart.toFixed(3)}% { transform: translateX(var(--horse-start-position, 0px)) translateY(var(--horse-y-start, 0px)); }
  ${raceStops
    .map(
      (stop) =>
        `${stops.at(stop.progress).toFixed(3)}% { transform: translateX(${stop.position}) translateY(${stop.yPosition}); }`,
    )
    .join("\n  ")}
  100% { transform: translateX(var(--horse-end-position, calc(-150vw - 148px))) translateY(var(--horse-y-end, 0px)); }
}`;
};

function getHomeTrackRunners(): HomeTrackRunner[] {
  const horseOnlySlots = new Set<number>();
  const runnerCount =
    minHomeTrackRunnerCount +
    Math.floor(Math.random() * (maxHomeTrackRunnerCount - minHomeTrackRunnerCount + 1));
  const racePattern = getRacePattern();
  const approachDuration = 1.15 + Math.random() * 0.7;
  const holdDuration = 1 + Math.random() * 4;
  const raceDuration = 7.2 + Math.random() * 2.8;
  const stops = getRaceKeyframeStops({ approachDuration, holdDuration, raceDuration });
  const laneShift = Math.floor(Math.random() * 6);
  const laneStep = 9 + Math.floor(Math.random() * 3);
  const startSpread = 55 + Math.random() * 90;

  if (runnerCount > 4 && Math.random() < 0.16) {
    horseOnlySlots.add(3 + Math.floor(Math.random() * (runnerCount - 3)));
  }

  return Array.from({ length: runnerCount }, (_, index) => {
    const isContender = index < 3;
    const startOffset = isContender
      ? index * 0.04
      : 0.04 + (index % 8) * 0.028 + Math.random() * 0.08;
    const lane = isContender
      ? (index + laneShift) % 6
      : (index + Math.floor(index / 3) + laneShift) % 6;
    const size = runnerSizes[Math.floor(Math.random() * runnerSizes.length)] ?? "26px";
    const filter = runnerFilters[Math.floor(Math.random() * runnerFilters.length)] ?? "none";
    const raceOffsets = getRunnerRaceOffsets(index, racePattern, runnerCount);
    const pacePattern = getPacePattern(index, racePattern, runnerCount);
    const paceSpeedFactor = Math.max(
      0,
      Math.min(
        1,
        (pacePattern === "accelerate-early" || pacePattern === "surge-fade-early" ? 0.18 : 0) +
          (pacePattern === "accelerate-middle" || pacePattern === "surge-fade-middle" ? 0.1 : 0) +
          (pacePattern === "steady" ? 0.04 : 0) +
          Math.random() * 0.78,
      ),
    );
    const gallopDuration = 0.74 - paceSpeedFactor * 0.28 + Math.random() * 0.08;
    const gallopDelay = -Math.random() * gallopDuration;
    const gallopLift = 1.1 + paceSpeedFactor * 2.8 + Math.random() * 0.7;
    const gallopLand = 0.15 + Math.random() * 0.55;
    const gallopForwardTilt = 1.2 + paceSpeedFactor * 3.1 + Math.random() * 1.4;
    const gallopRearTilt = -(0.8 + Math.random() * 2.2);
    const runnerScale = 0.86 + Math.random() * 0.32;
    const opacity = 0.82 + Math.random() * 0.18;
    const scatterOffset =
      (index - (runnerCount - 1) / 2) * (startSpread / runnerCount) + (Math.random() * 52 - 26);
    const startPosition = getTravelPosition(0);
    const approachPosition = `${Math.max(
      18,
      startOffset * 120 + raceOffsets.start + scatterOffset + 96 + Math.random() * 96,
    ).toFixed(1)}px`;
    const motionProfile = getRunnerMotionProfile();
    const racePositionStyle: Partial<Record<`--horse-race-position-${number}`, string>> = {};

    motionProfile.stops.forEach((stop, stopIndex) => {
      racePositionStyle[`--horse-race-position-${stopIndex + 1}`] = getTravelPosition(
        stop.fraction,
      );
    });

    const verticalDirection = Math.random() < 0.5 ? 1 : -1;
    const verticalShift = 4 + Math.random() * 12;
    const verticalMidShift =
      verticalDirection * (Math.random() * verticalShift * 0.4 - verticalShift * 0.2);
    const keyframesName = `home-race-horse-${index}`;
    const style: HomeTrackRunnerStyle = {
      ...racePositionStyle,
      animationName: keyframesName,
      animationTimingFunction: "linear",
      "--horse-approach-position": approachPosition,
      "--horse-start-position": startPosition,
      "--horse-pre-accelerate-position": getTravelPosition(
        motionProfile.stops[1]?.fraction ?? 0.28,
      ),
      "--horse-accelerate-position": getTravelPosition(motionProfile.stops[4]?.fraction ?? 0.78),
      "--horse-decelerate-position": getTravelPosition(motionProfile.stops[5]?.fraction ?? 0.92),
      "--horse-end-position": getTravelPosition(1),
      "--horse-gallop-delay": `${gallopDelay.toFixed(2)}s`,
      "--horse-gallop-duration": `${gallopDuration.toFixed(2)}s`,
      "--horse-gallop-forward-tilt": `${gallopForwardTilt.toFixed(1)}deg`,
      "--horse-gallop-lift": `${gallopLift.toFixed(1)}px`,
      "--horse-gallop-land": `${gallopLand.toFixed(1)}px`,
      "--horse-gallop-rear-tilt": `${gallopRearTilt.toFixed(1)}deg`,
      "--horse-run-duration": `${stops.totalDuration.toFixed(2)}s`,
      "--horse-run-filter": filter,
      "--horse-run-lane": `${lane * laneStep}`,
      "--horse-run-opacity": opacity.toFixed(2),
      "--horse-run-scale": runnerScale.toFixed(2),
      "--horse-run-size": size,
      "--horse-y-end": `${(-verticalDirection * verticalShift).toFixed(1)}px`,
      "--horse-y-mid": `${verticalMidShift.toFixed(1)}px`,
      "--horse-y-start": `${(verticalDirection * verticalShift).toFixed(1)}px`,
    };

    const isHorseOnly = horseOnlySlots.has(index);
    const emoji = isHorseOnly
      ? "🐎"
      : (jockeyRunnerEmojis[Math.floor(Math.random() * jockeyRunnerEmojis.length)] ?? "🏇");

    return {
      emoji,
      key: `${racePattern}-${index}-${isHorseOnly ? "horse" : "jockey"}-${emoji}-${lane}`,
      keyframes: buildRunnerKeyframes({ keyframesName, motionProfile, stops }),
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
            <style
              dangerouslySetInnerHTML={{
                __html: homeTrackRunners.map((runner) => runner.keyframes).join("\n"),
              }}
            />
            <span className="home-start-gate">START</span>
            <span className="home-finish-gate">GOAL</span>
            {homeTrackRunners.map((runner, index) => (
              <span
                className={`home-running-horse runner-${index + 1} pace-${runner.pacePattern}`}
                key={runner.key}
                style={runner.style}
              >
                <span className="home-running-horse-gallop">{runner.emoji}</span>
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
