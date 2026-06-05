"use client";

import { useEffect, useMemo, useState, type CSSProperties } from "react";

type BrandRunnerStyle = CSSProperties & {
  "--brand-runner-filter": string;
};

const colorFilters = [
  "saturate(0.85) contrast(1.05)",
  "hue-rotate(28deg) saturate(1.1) contrast(1.04)",
  "hue-rotate(72deg) saturate(1.2) brightness(1.03)",
  "hue-rotate(138deg) saturate(1.08) contrast(1.06)",
  "hue-rotate(205deg) saturate(1.16) brightness(1.02)",
  "hue-rotate(292deg) saturate(1.12) contrast(1.05)",
  "sepia(0.18) saturate(1.25) brightness(0.98)",
] as const;

const TEN_MINUTES_MS = 10 * 60 * 1000;
const SIN_MULTIPLIER = 9301;
const SIN_OFFSET = 49297;
const SIN_SCALE = 233280;

const getTenMinuteBucket = (): number => Math.floor(Date.now() / TEN_MINUTES_MS);

const seededRandom = (seed: number): number => {
  const value = Math.sin(seed * SIN_MULTIPLIER + SIN_OFFSET) * SIN_SCALE;
  return value - Math.floor(value);
};

const buildStyle = (bucket: number): BrandRunnerStyle => {
  const colorIndex = Math.floor(seededRandom(bucket) * colorFilters.length) % colorFilters.length;
  return {
    "--brand-runner-filter": colorFilters[colorIndex] ?? colorFilters[0],
  };
};

export function BrandRunner() {
  const [bucket, setBucket] = useState<number | null>(null);
  const style = useMemo<BrandRunnerStyle | undefined>(
    () => (bucket === null ? undefined : buildStyle(bucket)),
    [bucket],
  );

  useEffect(() => {
    setBucket(getTenMinuteBucket());
  }, []);

  useEffect(() => {
    if (bucket === null) {
      return undefined;
    }
    const delay = TEN_MINUTES_MS - (Date.now() % TEN_MINUTES_MS);
    const timer = window.setTimeout(() => {
      setBucket(getTenMinuteBucket());
    }, delay);
    return () => window.clearTimeout(timer);
  }, [bucket]);

  return (
    <span className="brand-mark" aria-hidden="true">
      <span className="brand-runner" style={style}>
        🏇
      </span>
      <span className="brand-track" />
    </span>
  );
}
