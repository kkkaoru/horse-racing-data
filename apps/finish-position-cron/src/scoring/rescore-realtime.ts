// Run with bun. Worker-native realtime odds + bataiju fetcher for the Stage-2
// per-race rescore, a faithful TS port of
// apps/finish-position-predict-container/src/realtime_odds_fetcher.py
// (build_race_key / encode_race_key / extract_rows / extract_weight_map /
// fetch_odds_for_race / fetch_weight_for_race / fetch_with_retry).
//
// Both endpoints sit behind the Cloudflare WAF, which rejects the runtime's
// default empty User-Agent with HTTP 403, so every request carries an explicit
// non-empty UA + Accept header. Individual fetch failures are swallowed and
// surfaced as an empty Map so the late-binding recompute falls back to the
// cached odds/weight (same graceful degradation the Python container takes).
//
// Units (matching the Python fetcher): D1 odds is a direct multiplier (no
// divide-by-10), ninkijun is 1-based ascending, weight is an integer kg.

import type { PredictCategory } from "../types";

const HOT_ODDS_BASE_URL = "https://sync-realtime-data-hot.kkk4oru.com/api/odds";
const WEIGHT_BASE_URL = "https://sync-realtime-data.kkk4oru.com/api/horse-weight";
const REQUEST_HEADERS: Record<string, string> = {
  Accept: "application/json",
  "User-Agent": "horse-racing-data-predict/1.0",
};
const FETCH_TIMEOUT_MS = 5000;
const FETCH_MAX_RETRIES = 2;
const FETCH_BACKOFF_BASE_MS = 500;
const BACKOFF_FACTOR = 2;
const RACE_YEAR_END = 4;
const ODDS_FLOOR = 0;
const WEIGHT_FLOOR = 0;

// Map the predict category to the raceKey source segment. Ban-ei rides on the
// NAR feed (Obihiro), so both fetch under the "nar" source.
const SOURCE_BY_CATEGORY: Record<PredictCategory, string> = {
  "ban-ei": "nar",
  jra: "jra",
  nar: "nar",
};

export interface RealtimeOdds {
  tanshoOdds: number;
  tanshoNinkijun: number;
}

export interface BuildRaceKeyInput {
  source: string;
  // YYYYMMDD run date; split into the {YYYY}:{MMDD} raceKey segments.
  runYmd: string;
  // 2-digit zero-padded keibajo_code / race_bango from the rescore message.
  keibajoCode: string;
  raceBango: string;
}

export interface FetchRaceInput {
  source: string;
  runYmd: string;
  keibajoCode: string;
  raceBango: string;
  // Injectable fetch so tests can stub the network without patching globals.
  fetchImpl: typeof fetch;
}

interface TanshoEntry {
  combination: unknown;
  odds: unknown;
  rank: unknown;
}

interface HorseWeightEntry {
  horseNumber: unknown;
  weight: unknown;
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// {source}:{YYYY}:{MMDD}:{KK}:{RR} (realtime_odds_fetcher.build_race_key).
export const buildRaceKey = (input: BuildRaceKeyInput): string => {
  const year = input.runYmd.slice(0, RACE_YEAR_END);
  const mmdd = input.runYmd.slice(RACE_YEAR_END);
  return `${input.source}:${year}:${mmdd}:${input.keibajoCode}:${input.raceBango}`;
};

export const sourceForCategory = (category: PredictCategory): string =>
  SOURCE_BY_CATEGORY[category];

// Percent-encode the raceKey for use as a single URL path segment (the colons
// must be escaped). Mirrors urllib.parse.quote(race_key, safe="").
export const encodeRaceKey = (raceKey: string): string => encodeURIComponent(raceKey);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

// JSON.parse only yields finite numbers (NaN / Infinity decode to null), so the
// number branch needs no isFinite guard; the string-parse branch does, because
// "Infinity" / "1e999" parse to non-finite values that must be rejected.
const toInt = (value: unknown): number | null => {
  if (typeof value === "number") return Math.trunc(value);
  if (typeof value !== "string") return null;
  const parsed = Number(value.trim());
  return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
};

const toFloat = (value: unknown): number | null => {
  if (typeof value === "number") return value;
  if (typeof value !== "string") return null;
  const parsed = Number(value.trim());
  return Number.isFinite(parsed) ? parsed : null;
};

const fetchJsonOnce = async (url: string, fetchImpl: typeof fetch): Promise<unknown> => {
  const response = await fetchImpl(url, {
    headers: REQUEST_HEADERS,
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
};

interface FetchWithRetryInput {
  url: string;
  fetchImpl: typeof fetch;
  attempt: number;
}

const fetchWithRetry = async (input: FetchWithRetryInput): Promise<unknown> => {
  try {
    return await fetchJsonOnce(input.url, input.fetchImpl);
  } catch (error) {
    if (input.attempt >= FETCH_MAX_RETRIES) throw error;
    const backoffMs = FETCH_BACKOFF_BASE_MS * BACKOFF_FACTOR ** input.attempt;
    console.warn(`realtime fetch attempt ${input.attempt + 1} failed, retrying`, String(error));
    await sleep(backoffMs);
    return fetchWithRetry({
      attempt: input.attempt + 1,
      fetchImpl: input.fetchImpl,
      url: input.url,
    });
  }
};

const tanshoEntriesFrom = (response: unknown): TanshoEntry[] => {
  if (!isRecord(response)) return [];
  const latest = response.latest;
  if (!isRecord(latest)) return [];
  const tansho = latest.tansho;
  if (!Array.isArray(tansho)) return [];
  return tansho.filter(isRecord).map((entry) => ({
    combination: entry.combination,
    odds: entry.odds,
    rank: entry.rank,
  }));
};

const addTanshoEntry = (acc: Map<number, RealtimeOdds>, entry: TanshoEntry): void => {
  const umaban = toInt(entry.combination);
  const odds = toFloat(entry.odds);
  const ninkijun = toInt(entry.rank);
  if (umaban === null || odds === null || ninkijun === null) return;
  if (odds <= ODDS_FLOOR) return;
  acc.set(umaban, { tanshoNinkijun: ninkijun, tanshoOdds: odds });
};

const extractOddsMap = (response: unknown): Map<number, RealtimeOdds> => {
  const result = new Map<number, RealtimeOdds>();
  tanshoEntriesFrom(response).forEach((entry) => addTanshoEntry(result, entry));
  return result;
};

const weightEntriesFrom = (response: unknown): HorseWeightEntry[] => {
  if (!isRecord(response)) return [];
  const horses = response.horses;
  if (!Array.isArray(horses)) return [];
  return horses
    .filter(isRecord)
    .map((entry) => ({ horseNumber: entry.horseNumber, weight: entry.weight }));
};

const addWeightEntry = (acc: Map<number, number>, entry: HorseWeightEntry): void => {
  const umaban = toInt(entry.horseNumber);
  const bataiju = toInt(entry.weight);
  if (umaban === null || bataiju === null) return;
  if (bataiju <= WEIGHT_FLOOR) return;
  acc.set(umaban, bataiju);
};

const extractWeightMap = (response: unknown): Map<number, number> => {
  const result = new Map<number, number>();
  weightEntriesFrom(response).forEach((entry) => addWeightEntry(result, entry));
  return result;
};

// Fetch {umaban -> {tanshoOdds, tanshoNinkijun}} for one race; empty Map on any
// error (the late-binding recompute then falls back to the cached odds).
export const fetchOddsForRace = async (
  input: FetchRaceInput,
): Promise<Map<number, RealtimeOdds>> => {
  const raceKey = buildRaceKey(input);
  const url = `${HOT_ODDS_BASE_URL}/${encodeRaceKey(raceKey)}`;
  try {
    const response = await fetchWithRetry({ attempt: 0, fetchImpl: input.fetchImpl, url });
    return extractOddsMap(response);
  } catch (error) {
    console.warn(`realtime odds fetch failed race_key=${raceKey}`, String(error));
    return new Map<number, RealtimeOdds>();
  }
};

// Fetch {umaban -> bataiju_kg} for one race; empty Map on any error.
export const fetchWeightForRace = async (input: FetchRaceInput): Promise<Map<number, number>> => {
  const raceKey = buildRaceKey(input);
  const url = `${WEIGHT_BASE_URL}/${encodeRaceKey(raceKey)}`;
  try {
    const response = await fetchWithRetry({ attempt: 0, fetchImpl: input.fetchImpl, url });
    return extractWeightMap(response);
  } catch (error) {
    console.warn(`realtime weight fetch failed race_key=${raceKey}`, String(error));
    return new Map<number, number>();
  }
};
