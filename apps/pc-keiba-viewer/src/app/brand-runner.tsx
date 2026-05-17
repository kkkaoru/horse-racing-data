"use client";

import { useEffect, useMemo, useState, type CSSProperties } from "react";

type BrandRunnerStyle = CSSProperties & {
  "--brand-runner-filter": string;
  "--brand-runner-size": string;
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

const getTenMinuteBucket = (): number => Math.floor(Date.now() / (10 * 60 * 1000));

const seededRandom = (seed: number): number => {
  const value = Math.sin(seed * 9301 + 49297) * 233280;
  return value - Math.floor(value);
};

export function BrandRunner() {
  const [bucket, setBucket] = useState(getTenMinuteBucket);
  const style = useMemo<BrandRunnerStyle>(() => {
    const colorIndex = Math.floor(seededRandom(bucket) * colorFilters.length) % colorFilters.length;
    const size = 20 + seededRandom(bucket + 11) * 4;
    return {
      "--brand-runner-filter": colorFilters[colorIndex] ?? colorFilters[0],
      "--brand-runner-size": `${size.toFixed(1)}px`,
    };
  }, [bucket]);

  useEffect(() => {
    const delay = 10 * 60 * 1000 - (Date.now() % (10 * 60 * 1000));
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
