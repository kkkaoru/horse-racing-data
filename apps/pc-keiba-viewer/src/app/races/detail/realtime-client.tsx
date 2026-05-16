"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { createContext, useContext, useEffect, useRef, type ReactNode } from "react";
import { useStore } from "zustand";
import { createStore, type StoreApi } from "zustand/vanilla";

import { fetchWithRetry } from "../../../lib/fetch-with-retry";

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

interface RealtimeRaceState {
  error: string | null;
  payload: RealtimeRacePayload | null;
  setError: (error: string | null) => void;
  setPayload: (payload: RealtimeRacePayload | null) => void;
}

type RealtimeRaceStore = StoreApi<RealtimeRaceState>;

interface RealtimeRaceProviderProps {
  children: ReactNode;
  initialPayload: RealtimeRacePayload | null;
  request: RealtimeRaceRequest;
}

const RealtimeRaceContext = createContext<RealtimeRaceStore | null>(null);

const createRealtimeRaceStore = (initialPayload: RealtimeRacePayload | null): RealtimeRaceStore =>
  createStore<RealtimeRaceState>((set) => ({
    error: null,
    payload: initialPayload,
    setError: (error) => set({ error }),
    setPayload: (payload) => set({ error: null, payload }),
  }));

export const buildRealtimeUrl = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RealtimeRaceRequest): string | null => {
  if (source !== "nar" && source !== "jra") {
    return null;
  }
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/realtime?source=${encodeURIComponent(source)}`;
};

export const isRealtimeRacePayload = (value: unknown): value is RealtimeRacePayload =>
  typeof value === "object" &&
  value !== null &&
  "raceKey" in value &&
  typeof value.raceKey === "string";

export function RealtimeRaceProvider({
  children,
  initialPayload,
  request,
}: RealtimeRaceProviderProps) {
  const realtimeUrl = buildRealtimeUrl(request);
  const storeRef = useRef<RealtimeRaceStore | null>(null);
  if (storeRef.current === null) {
    storeRef.current = createRealtimeRaceStore(initialPayload);
  }

  useEffect(() => {
    storeRef.current?.getState().setPayload(initialPayload);
  }, [initialPayload, realtimeUrl]);

  useEffect(() => {
    if (!realtimeUrl || !storeRef.current) {
      return undefined;
    }
    const store = storeRef.current;
    let cancelled = false;
    const load = async () => {
      try {
        const response = await fetchWithRetry(realtimeUrl, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`realtime api ${response.status}`);
        }
        const data: unknown = await response.json();
        if (!isRealtimeRacePayload(data)) {
          throw new Error("invalid realtime payload");
        }
        if (!cancelled) {
          store.getState().setPayload(data);
        }
      } catch (caught) {
        if (!cancelled) {
          store.getState().setError(caught instanceof Error ? caught.message : String(caught));
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

  return (
    <RealtimeRaceContext.Provider value={storeRef.current}>{children}</RealtimeRaceContext.Provider>
  );
}

export const useRealtimeRaceSelector = <T,>(selector: (state: RealtimeRaceState) => T): T => {
  const store = useContext(RealtimeRaceContext);
  if (!store) {
    throw new Error("useRealtimeRaceSelector must be used within RealtimeRaceProvider.");
  }
  return useStore(store, selector);
};

export const useRealtimeRacePayload = (
  request: RealtimeRaceRequest,
  initialPayload: RealtimeRacePayload | null,
): {
  error: string | null;
  payload: RealtimeRacePayload | null;
} => {
  const contextStore = useContext(RealtimeRaceContext);
  const fallbackStoreRef = useRef<RealtimeRaceStore | null>(null);
  if (fallbackStoreRef.current === null) {
    fallbackStoreRef.current = createRealtimeRaceStore(initialPayload);
  }
  const store = contextStore ?? fallbackStoreRef.current;

  const realtimeUrl = buildRealtimeUrl(request);
  const payload = useStore(store, (state) => state.payload);
  const error = useStore(store, (state) => state.error);

  useEffect(() => {
    if (contextStore || !realtimeUrl) {
      return undefined;
    }
    let cancelled = false;
    const load = async () => {
      try {
        const response = await fetchWithRetry(realtimeUrl, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`realtime api ${response.status}`);
        }
        const data: unknown = await response.json();
        if (!isRealtimeRacePayload(data)) {
          throw new Error("invalid realtime payload");
        }
        if (!cancelled) {
          fallbackStoreRef.current?.getState().setPayload(data);
        }
      } catch (caught) {
        if (!cancelled) {
          fallbackStoreRef.current
            ?.getState()
            .setError(caught instanceof Error ? caught.message : String(caught));
        }
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [contextStore, realtimeUrl]);

  return { error, payload };
};
