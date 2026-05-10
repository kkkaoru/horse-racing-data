"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { useEffect, useState } from "react";

export interface RealtimeRaceRequest {
  apiBaseUrl: string;
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: string;
  year: string;
}

const POLL_INTERVAL_MS = 30_000;

export const buildRealtimeUrl = ({
  apiBaseUrl,
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RealtimeRaceRequest): string | null => {
  if (!apiBaseUrl || source !== "nar") {
    return null;
  }
  return `${apiBaseUrl.replace(/\/$/u, "")}/api/nar/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/realtime`;
};

export const isRealtimeRacePayload = (value: unknown): value is RealtimeRacePayload =>
  typeof value === "object" &&
  value !== null &&
  "raceKey" in value &&
  typeof value.raceKey === "string";

export const useRealtimeRacePayload = (
  request: RealtimeRaceRequest,
  initialPayload: RealtimeRacePayload | null,
): {
  error: string | null;
  payload: RealtimeRacePayload | null;
} => {
  const realtimeUrl = buildRealtimeUrl(request);
  const [payload, setPayload] = useState<RealtimeRacePayload | null>(initialPayload);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!realtimeUrl) {
      return undefined;
    }
    let cancelled = false;
    const load = async () => {
      try {
        const response = await fetch(realtimeUrl, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`realtime api ${response.status}`);
        }
        const data: unknown = await response.json();
        if (!isRealtimeRacePayload(data)) {
          throw new Error("invalid realtime payload");
        }
        if (!cancelled) {
          setPayload(data);
          setError(null);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof Error ? caught.message : String(caught));
        }
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [realtimeUrl]);

  return { error, payload };
};
