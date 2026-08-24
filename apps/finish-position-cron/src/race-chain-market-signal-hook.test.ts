// Run with bun. Tests for the bounded best-effort Queue hook.

import { expect, test, vi } from "vitest";

import type {
  MarketSignalFoundationBucket,
  MarketSignalFoundationResult,
  MaterializeMarketSignalFoundationInput,
} from "./race-chain-market-signal-foundation";
import {
  addMarketSignalAttestationToUrl,
  prepareMarketSignalFoundationBestEffort,
  type PrepareMarketSignalFoundationInput,
} from "./race-chain-market-signal-hook";
import type { FetchRaceInput, RealtimeOdds } from "./scoring/rescore-realtime";

const bucket: MarketSignalFoundationBucket = {
  get: async () => null,
  head: async () => null,
  put: async () => undefined,
};

const input = (): PrepareMarketSignalFoundationInput => ({
  category: "jra",
  env: { FEATURES_CACHE: bucket, WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED: "1" },
  fetchImpl: fetch,
  keibajoCode: "05",
  raceBango: "11",
  runYmd: "20260824",
});

const odds = (): Map<number, RealtimeOdds> =>
  new Map([[1, { tanshoNinkijun: 1, tanshoOdds: 2.5 }]]);

test("attestation URL helper forwards the complete stale-artifact guard", () => {
  const value = new URL(
    addMarketSignalAttestationToUrl("http://do/predict?category=jra", {
      baseGenerationId: "base-generation",
      etag: "artifact-etag",
      key: "artifact/key.json",
      oddsSnapshotHash: "odds-hash",
      version: "artifact-version",
    }),
  );

  expect(Object.fromEntries(value.searchParams)).toStrictEqual({
    category: "jra",
    marketSignalBaseGenerationId: "base-generation",
    marketSignalFoundationEtag: "artifact-etag",
    marketSignalFoundationKey: "artifact/key.json",
    marketSignalFoundationVersion: "artifact-version",
    marketSignalOddsSnapshotHash: "odds-hash",
  });
});

test("prepare hook skips disabled and unsupported work without fetching odds", async () => {
  const fetchOdds = vi.fn(async (_value: FetchRaceInput) => odds());
  const materialize = vi.fn(
    async (
      _value: MaterializeMarketSignalFoundationInput,
    ): Promise<MarketSignalFoundationResult> => ({ reason: "disabled", status: "skipped" }),
  );

  await expect(
    prepareMarketSignalFoundationBestEffort(
      { ...input(), env: { FEATURES_CACHE: bucket } },
      { fetchOdds, materialize },
    ),
  ).resolves.toStrictEqual({ reason: "disabled", status: "unavailable" });
  await expect(
    prepareMarketSignalFoundationBestEffort(
      { ...input(), category: "nar" },
      { fetchOdds, materialize },
    ),
  ).resolves.toStrictEqual({ reason: "unsupported-category", status: "unavailable" });
  expect(fetchOdds).not.toHaveBeenCalled();
  expect(materialize).not.toHaveBeenCalled();
});

test("prepare hook returns the exact current R2 identity and emits timing telemetry", async () => {
  const fetchOdds = vi.fn(async (_value: FetchRaceInput) => odds());
  const materialize = vi.fn(
    async (
      _value: MaterializeMarketSignalFoundationInput,
    ): Promise<MarketSignalFoundationResult> => ({
      artifactEtag: "artifact-etag",
      artifactVersion: "artifact-version",
      baseGenerationId: "base-generation",
      cacheHit: false,
      computeMs: 4,
      key: "feat-racechain-market-signal/catalog-v1/jra/20260824/05/11/foundation.json",
      oddsSnapshotHash: "odds-hash",
      rowCount: 16,
      status: "materialized",
      totalMs: 12,
    }),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await expect(
    prepareMarketSignalFoundationBestEffort(input(), { fetchOdds, materialize }),
  ).resolves.toStrictEqual({
    attestation: {
      baseGenerationId: "base-generation",
      etag: "artifact-etag",
      key: "feat-racechain-market-signal/catalog-v1/jra/20260824/05/11/foundation.json",
      oddsSnapshotHash: "odds-hash",
      version: "artifact-version",
    },
    cacheHit: false,
    status: "ready",
  });
  expect(fetchOdds).toHaveBeenCalledWith({
    fetchImpl: fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260824",
    source: "jra",
  });
  expect(materialize).toHaveBeenCalledWith(
    expect.objectContaining({ liveOddsByHorseNumber: odds() }),
  );
  expect(JSON.parse(String(logSpy.mock.calls[0]?.[0]))).toMatchObject({
    cacheHit: false,
    computeMs: 4,
    event: "worker-market-signal-foundation",
    rowCount: 16,
    status: "ready",
    totalMs: 12,
  });
  logSpy.mockRestore();
});

test("prepare hook keeps the Container fallback on a materializer miss", async () => {
  const fetchOdds = vi.fn(async (_value: FetchRaceInput) => odds());
  const materialize = vi.fn(
    async (
      _value: MaterializeMarketSignalFoundationInput,
    ): Promise<MarketSignalFoundationResult> => ({
      reason: "market-signal-incomplete-live-board",
      status: "fallback",
    }),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await expect(
    prepareMarketSignalFoundationBestEffort(input(), { fetchOdds, materialize }),
  ).resolves.toStrictEqual({
    reason: "market-signal-incomplete-live-board",
    status: "unavailable",
  });
  expect(JSON.parse(String(logSpy.mock.calls[0]?.[0]))).toMatchObject({
    reason: "market-signal-incomplete-live-board",
    status: "fallback",
  });
  logSpy.mockRestore();
});

test("prepare hook catches fetch and R2 failures so prediction still reaches the Container", async () => {
  const fetchOdds = vi.fn(async (_value: FetchRaceInput): Promise<Map<number, RealtimeOdds>> => {
    throw new Error("odds unavailable");
  });
  const materialize = vi.fn(
    async (
      _value: MaterializeMarketSignalFoundationInput,
    ): Promise<MarketSignalFoundationResult> => ({ reason: "unused", status: "fallback" }),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await expect(
    prepareMarketSignalFoundationBestEffort(input(), { fetchOdds, materialize }),
  ).resolves.toStrictEqual({ reason: "hook-error", status: "unavailable" });
  expect(materialize).not.toHaveBeenCalled();
  expect(JSON.parse(String(warnSpy.mock.calls[0]?.[0]))).toMatchObject({
    error: "Error: odds unavailable",
    status: "fallback",
  });
  warnSpy.mockRestore();
});
