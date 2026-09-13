// Run with bun (bunx vitest).
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type { WinRateHeatmapPartnershipRow } from "./win-rate-heatmap";
import {
  buildWinRateHeatmapCatalogUrl,
  type WinRateHeatmapCatalogQuery,
} from "./win-rate-heatmap-catalog.server";

const KINDS: readonly WinRateHeatmapPartnershipRow["category"][] = [
  "horseJockey",
  "jockeyVenue",
  "jockeyTrainerVenue",
];
const RATE_SCALE: number = 1000;
const RATE_PRECISION: number = 10;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isKind = (value: unknown): value is WinRateHeatmapPartnershipRow["category"] =>
  KINDS.some((kind) => kind === value);
const count = (value: unknown): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    throw new Error("Partnership count is malformed");
  return value;
};
const rate = (value: number, starts: number): number =>
  starts === 0 ? 0 : Math.round((value * RATE_SCALE) / starts) / RATE_PRECISION;
const parseRow = (value: unknown): WinRateHeatmapPartnershipRow => {
  if (!isRecord(value) || !isKind(value.kind) || typeof value.name !== "string")
    throw new Error("Partnership row is malformed");
  const starts: number = count(value.starts);
  const winCount: number = count(value.wins);
  const quinellaCount: number = count(value.places);
  const showCount: number = count(value.shows);
  const horseNumber: number = count(value.umaban);
  if (
    horseNumber < 1 ||
    horseNumber > 99 ||
    winCount > quinellaCount ||
    quinellaCount > showCount ||
    showCount > starts
  )
    throw new Error("Partnership counts are inconsistent");
  return {
    category: value.kind,
    currentHorseNumbers: String(horseNumber),
    name: value.name,
    details: [],
    horseCount: 0,
    starts,
    winCount,
    quinellaCount,
    showCount,
    winRate: rate(winCount, starts),
    quinellaRate: rate(quinellaCount, starts),
    showRate: rate(showCount, starts),
  };
};

// Only the warm producer calls this endpoint. UI and MCP consume the published
// presentation, and never request new history scans during rendering.
export const fetchHeatmapPartnershipRows = async (
  query: WinRateHeatmapCatalogQuery,
): Promise<WinRateHeatmapPartnershipRow[] | null> => {
  const env = await safeGetCloudflareEnv();
  if (!env?.R2_CATALOG) return null;
  const url: URL = buildWinRateHeatmapCatalogUrl(query);
  url.pathname = "/v1/heatmap-partnership-stats";
  url.searchParams.set("warm", "1");
  const response: Response = await env.R2_CATALOG.fetch(url.href);
  if (!response.ok) throw new Error(`Partnership catalog request failed: ${response.status}`);
  const payload: unknown = await response.json();
  if (!isRecord(payload) || !Array.isArray(payload.partnershipRows))
    throw new Error("Partnership payload is malformed");
  return payload.partnershipRows.map(parseRow);
};
