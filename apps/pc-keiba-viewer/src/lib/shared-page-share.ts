// Run with bun. Pure helpers for src/scripts/share-race-page.ts: which paths a
// shared race page needs bypassed in Cloudflare Access, and how the share is
// named so it can be found again for revoke and prune.

import { SHARED_PAGE_HOST } from "./shared-page-access";

export const SHARE_APP_NAME_PREFIX: string = "PC Keiba Viewer share ";
export const SHARE_ASSETS_APP_NAME: string = "PC Keiba Viewer shared page assets";
// The page's JS/CSS chunks and icon, shared by every shared race page.
export const SHARE_ASSET_PATHS: readonly string[] = [
  "/_next/static/*",
  "/icon.svg",
  "/favicon.ico",
];
const PAGE_PATH_PATTERN: RegExp =
  /^\/races\/(?<year>\d{4})\/(?<month>\d{2})\/(?<day>\d{2})\/(?<keibajoCode>[0-9A-Z]{2})\/(?<raceNumber>\d{2})$/u;
const APP_NAME_PATTERN: RegExp =
  /^PC Keiba Viewer share (?<pagePath>\S+) until (?<expiresAt>\S+)$/u;
const SECONDS_PER_DAY: number = 86_400;
// The entry must outlive the deadline: while it exists an expired share is
// refused even if its Access app was not pruned yet.
const ENTRY_RETENTION_DAYS: number = 30;

export interface ShareAppName {
  expiresAt: string;
  pagePath: string;
}

export const parseSharedPagePath = (pageUrl: string): string => {
  const url = new URL(pageUrl);
  if (url.hostname !== SHARED_PAGE_HOST || !PAGE_PATH_PATTERN.test(url.pathname)) {
    throw new Error(`Not a ${SHARED_PAGE_HOST} race page URL: ${pageUrl}`);
  }
  return url.pathname;
};

export const parseShareExpiry = (value: string, nowMs: number): string => {
  const expiresMs = Date.parse(value);
  if (!/(?:Z|[+-]\d{2}:\d{2})$/u.test(value) || Number.isNaN(expiresMs)) {
    throw new Error(`Expiry must be an ISO date-time with an offset: ${value}`);
  }
  if (expiresMs <= nowMs) throw new Error(`Expiry is in the past: ${value}`);
  return value;
};

export const buildShareDestinations = (pagePath: string): string[] => {
  const groups = PAGE_PATH_PATTERN.exec(pagePath)?.groups;
  if (groups === undefined) throw new Error(`Not a race page path: ${pagePath}`);
  const { day, keibajoCode, month, raceNumber, year } = groups;
  return [
    pagePath,
    `/api${pagePath}/*`,
    // Served by the finish-position-clef-rerank Worker, which has no share gate.
    `/api/clef-rerank/bets/${year}${month}${day}/${keibajoCode}/${raceNumber}`,
  ].map((path) => `${SHARED_PAGE_HOST}${path}`);
};

export const buildShareAssetDestinations = (): string[] =>
  SHARE_ASSET_PATHS.map((path) => `${SHARED_PAGE_HOST}${path}`);

export const buildShareAppName = ({ expiresAt, pagePath }: ShareAppName): string =>
  `${SHARE_APP_NAME_PREFIX}${pagePath} until ${expiresAt}`;

export const parseShareAppName = (name: string): ShareAppName | null => {
  const groups = APP_NAME_PATTERN.exec(name)?.groups;
  return groups === undefined
    ? null
    : { expiresAt: String(groups.expiresAt), pagePath: String(groups.pagePath) };
};

export const buildShareEntryTtlSeconds = (expiresAt: string, nowMs: number): number =>
  Math.ceil((Date.parse(expiresAt) - nowMs) / 1000) + ENTRY_RETENTION_DAYS * SECONDS_PER_DAY;
