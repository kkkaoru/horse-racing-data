// Run with bun. Client-side React hook that subscribes to the 馬体重 SSE stream
// and returns the latest snapshot. The initial value is consumed straight from
// the SSR-seeded payload so the first paint does not flicker.

"use client";

import { useEffect, useState } from "react";

export interface HorseWeightEntry {
  changeAmount: number | null;
  changeSign: string | null;
  horseName: string | null;
  horseNumber: string;
  weight: number | null;
}

export interface HorseWeightSnapshot {
  fetchedAt: string;
  horses: HorseWeightEntry[];
}

export interface UseHorseWeightStreamParams {
  day: string;
  initial: HorseWeightSnapshot | null;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: string;
  year: string;
}

const WEIGHTS_EVENT_NAME = "weights";

const isStreamableSource = (source: string): boolean => source === "jra" || source === "nar";

const buildStreamUrl = (params: Omit<UseHorseWeightStreamParams, "initial">): string =>
  `/api/races/${params.year}/${params.month}/${params.day}/${params.keibajoCode}/${params.raceNumber}/horse-weights-stream?source=${encodeURIComponent(params.source)}`;

const resolveStreamUrl = (params: Omit<UseHorseWeightStreamParams, "initial">): string | null =>
  isStreamableSource(params.source) ? buildStreamUrl(params) : null;

const isHorseWeightSnapshot = (value: unknown): value is HorseWeightSnapshot => {
  if (typeof value !== "object" || value === null) return false;
  const fetchedAt: unknown = Reflect.get(value, "fetchedAt");
  const horses: unknown = Reflect.get(value, "horses");
  return typeof fetchedAt === "string" && Array.isArray(horses);
};

const parseEventData = (raw: string): HorseWeightSnapshot | null => {
  try {
    const parsed: unknown = JSON.parse(raw);
    return isHorseWeightSnapshot(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

export const useHorseWeightStream = (
  params: UseHorseWeightStreamParams,
): HorseWeightSnapshot | null => {
  const [snapshot, setSnapshot] = useState<HorseWeightSnapshot | null>(params.initial);
  const url = resolveStreamUrl(params);
  useEffect(() => {
    if (url === null) return undefined;
    const source = new EventSource(url);
    const handleMessage = (event: MessageEvent) => {
      const next = parseEventData(event.data);
      if (next !== null && next.horses.length > 0) setSnapshot(next);
    };
    source.addEventListener(WEIGHTS_EVENT_NAME, handleMessage);
    return () => {
      source.removeEventListener(WEIGHTS_EVENT_NAME, handleMessage);
      source.close();
    };
  }, [url]);
  return snapshot;
};
