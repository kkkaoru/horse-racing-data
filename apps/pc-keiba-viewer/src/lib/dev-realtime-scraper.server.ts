// Run with bun. dev-only NAR realtime odds scraper used by `next dev` to
// bypass the upstream Cloudflare Worker. Behind the
// `PC_KEIBA_DEV_REALTIME_SCRAPER=1` env flag and NODE_ENV=development guard.
import "server-only";
import { buildNarRaceKey, LOCAL_KEIBAJO_TO_NAR_BABA_CODE } from "horse-racing-realtime/nar";
import type {
  RealtimeOddsType,
  RealtimeRaceEntry,
  RealtimeRacePayload,
  RealtimeRaceSource,
} from "horse-racing-realtime/types";
import {
  buildRaceListUrl,
  extractOddsLinks,
  fetchOdds,
  fetchRaceLinksFromRaceList,
  fetchRacePage,
  parseRaceEntries,
  parseRaceMetadata,
} from "sync-realtime-data-hot/keiba-go";
import type { OddsData, OddsType, RaceEntry } from "sync-realtime-data-hot/types";

import {
  appendSnapshot,
  buildHistoryByType,
  buildHorseTrends,
  buildTanshoHistoryPoints,
  buildTrendsByType,
  readHistory,
} from "./dev-realtime-history-store.server";

export interface DevRealtimeRequest {
  day: string; // "29"
  keibajoCode: string; // "47"
  month: string; // "05"
  raceNumber: string; // "01"
  source: "jra" | "nar";
  year: string; // "2026"
}

interface NarScrapeContext {
  babaCode: string;
  debaUrl: string;
  raceNumber: string;
  request: DevRealtimeRequest;
}

const DEV_FLAG_ENABLED = "1";
const DEV_NODE_ENV = "development";
const PHASE1_ODDS_TYPES: OddsType[] = ["tansho", "fukusho"];
const NAR_SOURCE = "nar";
const JRA_SOURCE = "jra";

const NAR_BABA_CODE_LOOKUP: Record<string, string> = LOCAL_KEIBAJO_TO_NAR_BABA_CODE;

const resolveNarBabaCode = (keibajoCode: string): string | undefined =>
  NAR_BABA_CODE_LOOKUP[keibajoCode];

const padRaceNumber = (raceNumber: string): string => raceNumber.padStart(2, "0");

const buildTargetDate = (request: DevRealtimeRequest): string =>
  `${request.year}${request.month}${request.day}`;

const buildRaceKey = (request: DevRealtimeRequest): string => {
  const monthDay = `${request.month}${request.day}`;
  if (request.source === NAR_SOURCE) {
    return buildNarRaceKey(request.year, monthDay, request.keibajoCode, request.raceNumber);
  }
  // JRA shares the same 5-segment key shape with the `jra:` prefix.
  return `${JRA_SOURCE}:${request.year}:${monthDay}:${request.keibajoCode}:${padRaceNumber(request.raceNumber)}`;
};

const buildEmptyPayload = (request: DevRealtimeRequest): RealtimeRacePayload => ({
  horseWeights: null,
  odds: null,
  raceEntries: null,
  raceKey: buildRaceKey(request),
  raceResults: null,
  source: null,
  trackCondition: null,
});

const pickPhase1OddsLinks = (
  oddsLinks: Partial<Record<OddsType, string>>,
): Partial<Record<OddsType, string>> => {
  const picked: Partial<Record<OddsType, string>> = {};
  PHASE1_ODDS_TYPES.forEach((type) => {
    const url = oddsLinks[type];
    if (url) {
      picked[type] = url;
    }
  });
  return picked;
};

const toRealtimeRaceEntry = (
  entry: Omit<RaceEntry, "fetchedAt">,
  fetchedAt: string,
): RealtimeRaceEntry => ({
  fetchedAt,
  horseName: entry.horseName,
  horseNumber: entry.horseNumber,
  jockeyName: entry.jockeyName,
  status: entry.status,
});

const toRealtimeOddsLatest = (
  odds: Partial<Record<OddsType, OddsData[]>>,
): Partial<Record<RealtimeOddsType, OddsData[]>> => odds;

const buildSource = (
  context: NarScrapeContext,
  params: {
    oddsLinks: Partial<Record<OddsType, string>>;
    raceName: string | null;
    raceStartAtJst: string;
  },
): RealtimeRaceSource => ({
  babaCode: context.babaCode,
  debaUrl: context.debaUrl,
  kaisaiNen: context.request.year,
  kaisaiTsukihi: `${context.request.month}${context.request.day}`,
  keibajoCode: context.request.keibajoCode,
  lastOddsFetchAt: null,
  lastWeightFetchAt: null,
  oddsLinks: params.oddsLinks,
  raceBango: padRaceNumber(context.raceNumber),
  raceKey: buildNarRaceKey(
    context.request.year,
    `${context.request.month}${context.request.day}`,
    context.request.keibajoCode,
    context.raceNumber,
  ),
  raceName: params.raceName,
  raceStartAtJst: params.raceStartAtJst,
  source: NAR_SOURCE,
});

const buildJstStartIso = (request: DevRealtimeRequest, startTime: string | null): string => {
  // startTime is "HHMM" (e.g. "1630"); without it fall back to noon JST so the
  // payload remains a valid ISO without leaking a dead `?? null` arm.
  const hourMinute = startTime ?? "1200";
  const hour = hourMinute.slice(0, 2);
  const minute = hourMinute.slice(2, 4);
  return `${request.year}-${request.month}-${request.day}T${hour}:${minute}:00+09:00`;
};

const scrapeNarPayload = async (context: NarScrapeContext): Promise<RealtimeRacePayload> => {
  const html = await fetchRacePage(context.debaUrl);
  const oddsLinks = pickPhase1OddsLinks(extractOddsLinks(html, context.debaUrl));
  const metadata = parseRaceMetadata(html);
  const entries = parseRaceEntries(html);
  const fetchedAt = new Date().toISOString();
  const latest = toRealtimeOddsLatest(await fetchOdds(context.debaUrl, oddsLinks));
  const source = buildSource(context, {
    oddsLinks,
    raceName: metadata.raceName,
    raceStartAtJst: buildJstStartIso(context.request, metadata.startTime),
  });
  appendSnapshot(source.raceKey, { byType: latest, fetchedAt });
  const snapshots = readHistory(source.raceKey);
  const history = buildTanshoHistoryPoints(snapshots);
  const historyByType = buildHistoryByType(snapshots);
  return {
    horseWeights: null,
    odds: {
      fetchedAt,
      historyByType,
      horseTrends: buildHorseTrends(history),
      history,
      latest,
      trendsByType: buildTrendsByType(historyByType),
    },
    raceEntries: {
      fetchedAt,
      horses: entries.map((entry) => toRealtimeRaceEntry(entry, fetchedAt)),
    },
    raceKey: source.raceKey,
    raceResults: null,
    source,
    trackCondition: null,
  };
};

const findNarRaceLink = async (
  raceListUrl: string,
  raceNumber: string,
): Promise<{ raceNumber: string; url: string } | null> => {
  const padded = padRaceNumber(raceNumber);
  const links = await fetchRaceLinksFromRaceList(raceListUrl);
  return links.find((link) => link.raceNumber === padded) ?? null;
};

export const isDevScraperEnabled = (): boolean =>
  process.env.NODE_ENV === DEV_NODE_ENV &&
  process.env.PC_KEIBA_DEV_REALTIME_SCRAPER === DEV_FLAG_ENABLED;

export const buildDevRealtimePayload = async (
  request: DevRealtimeRequest,
): Promise<RealtimeRacePayload> => {
  // JRA scraping needs Playwright; Phase 1 leaves it empty so dev can still
  // exercise the route without spinning up a headless browser.
  if (request.source === JRA_SOURCE) {
    return buildEmptyPayload(request);
  }
  const babaCode = resolveNarBabaCode(request.keibajoCode);
  if (!babaCode) {
    return buildEmptyPayload(request);
  }
  const raceListUrl = buildRaceListUrl(buildTargetDate(request), babaCode).url;
  try {
    const link = await findNarRaceLink(raceListUrl, request.raceNumber);
    if (!link) {
      return buildEmptyPayload(request);
    }
    return await scrapeNarPayload({
      babaCode,
      debaUrl: link.url,
      raceNumber: link.raceNumber,
      request,
    });
  } catch (error) {
    console.warn("dev-realtime-scraper: NAR scrape failed", error);
    return buildEmptyPayload(request);
  }
};
