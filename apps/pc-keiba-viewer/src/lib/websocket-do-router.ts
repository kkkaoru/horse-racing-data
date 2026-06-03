// Run with bun. WebSocket upgrade interceptor that forwards directly to the matching Durable Object,
// bypassing the OpenNext Next.js Route Handler pipeline (which strips the Cloudflare-specific
// `webSocket` property from `Response(null, { status: 101, webSocket: client })`, causing the
// parent fetch to hang until the 30s wall-time limit).

const TRENDS_LIVE_PATH_REGEX =
  /^\/api\/races\/(?<year>\d{4})\/(?<month>\d{2})\/(?<day>\d{2})\/(?<keibajoCode>[0-9A-Z]{2})\/(?<raceNumber>\d{2})\/trends\/live$/u;
const PADDOCK_LIVE_PATH_REGEX =
  /^\/api\/races\/(?<year>\d{4})\/(?<month>\d{2})\/(?<day>\d{2})\/(?<keibajoCode>[0-9A-Z]{2})\/(?<raceNumber>\d{2})\/paddock\/live$/u;
const WS_UPGRADE_HEADER_VALUE = "websocket";
const RACE_TREND_ROOM_INTERNAL_URL = "https://race-trend-room/ws";
const PADDOCK_ROOM_INTERNAL_URL = "https://paddock-room/ws";
const RACE_SOURCES: ReadonlySet<string> = new Set(["jra", "nar"]);

interface RaceTrendsLiveMatch {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}

const isWebSocketUpgrade = (request: Request): boolean =>
  request.headers.get("upgrade")?.toLowerCase() === WS_UPGRADE_HEADER_VALUE;

const matchPath = (regex: RegExp, pathname: string): RaceTrendsLiveMatch | null => {
  const groups = regex.exec(pathname)?.groups;
  if (!groups) {
    return null;
  }
  return {
    day: String(groups.day),
    keibajoCode: String(groups.keibajoCode),
    month: String(groups.month),
    raceNumber: String(groups.raceNumber),
    year: String(groups.year),
  };
};

const isRaceSource = (value: string | null): value is "jra" | "nar" =>
  value !== null && RACE_SOURCES.has(value);

const buildRaceTrendRoomKey = (params: RaceTrendsLiveMatch, source: "jra" | "nar"): string =>
  `${source}:${params.year}${params.month}${params.day}:${params.keibajoCode}:${params.raceNumber}`;

const buildPaddockRoomKey = (params: RaceTrendsLiveMatch): string =>
  `${params.year}${params.month}${params.day}:${params.keibajoCode}:${params.raceNumber}`;

const forwardToDurableObject = (
  namespace: PcKeibaDurableObjectNamespace,
  raceKey: string,
  internalUrl: string,
  request: Request,
): Promise<Response> => {
  const stub = namespace.get(namespace.idFromName(raceKey));
  const encodedKey = encodeURIComponent(raceKey);
  return stub.fetch(new Request(`${internalUrl}?raceKey=${encodedKey}`, request));
};

const tryRouteTrendsLive = (
  request: Request,
  url: URL,
  env: CloudflareEnv,
): Promise<Response> | null => {
  const match = matchPath(TRENDS_LIVE_PATH_REGEX, url.pathname);
  if (!match) {
    return null;
  }
  const source = url.searchParams.get("source");
  if (!isRaceSource(source)) {
    return null;
  }
  const namespace = env.RACE_TREND_ROOM;
  if (!namespace) {
    return null;
  }
  return forwardToDurableObject(
    namespace,
    buildRaceTrendRoomKey(match, source),
    RACE_TREND_ROOM_INTERNAL_URL,
    request,
  );
};

const tryRoutePaddockLive = (
  request: Request,
  url: URL,
  env: CloudflareEnv,
): Promise<Response> | null => {
  const match = matchPath(PADDOCK_LIVE_PATH_REGEX, url.pathname);
  if (!match) {
    return null;
  }
  const namespace = env.PADDOCK_ROOM;
  if (!namespace) {
    return null;
  }
  return forwardToDurableObject(
    namespace,
    buildPaddockRoomKey(match),
    PADDOCK_ROOM_INTERNAL_URL,
    request,
  );
};

export const routeWebSocketUpgradeToDurableObject = (
  request: Request,
  env: CloudflareEnv,
): Promise<Response> | null => {
  if (!isWebSocketUpgrade(request)) {
    return null;
  }
  const url = new URL(request.url);
  return tryRouteTrendsLive(request, url, env) ?? tryRoutePaddockLive(request, url, env);
};
