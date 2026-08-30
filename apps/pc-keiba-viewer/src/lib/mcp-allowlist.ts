// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

const API_SPEC_PATH: string = "/api/spec";
const API_TOP_RACES_PATH: string = "/api/top-races";
const DAILY_FINISH_PREDICTIONS_PATH: string = "/api/finish-predictions/daily";
const FAVORITES_SEARCH_PATH: string = "/api/mypage/favorites/search";
const FAVORITES_PATH: string = "/api/mypage/favorites";

const RACE_API_ROOT: RegExp =
  /^\/api\/races\/\d{4}\/\d{2}\/\d{2}\/[0-9A-Z]{2}\/\d{2}\/(?:entity-recent-results|paddock|premium|realtime|recent-results|running-styles|trends|sections\/[a-z0-9-]+)$/;
const PADDOCK_STATE_API: RegExp =
  /^\/api\/races\/\d{4}\/\d{2}\/\d{2}\/[0-9A-Z]{2}\/\d{2}\/paddock$/;
const HORSE_RUNNING_STYLES_API: RegExp = /^\/api\/horses\/[0-9]{6,16}\/running-styles$/;

const parseAbsoluteApiPath = (pathWithQuery: string): URL | null => {
  if (!pathWithQuery.startsWith("/")) {
    return null;
  }
  try {
    return new URL(pathWithQuery, "https://mcp.invalid");
  } catch {
    return null;
  }
};

export const isMcpAllowedApiPath = (pathname: string): boolean => {
  if (pathname === API_SPEC_PATH) {
    return true;
  }
  if (pathname === API_TOP_RACES_PATH) {
    return true;
  }
  if (pathname === DAILY_FINISH_PREDICTIONS_PATH) {
    return true;
  }
  if (pathname === FAVORITES_SEARCH_PATH) {
    return true;
  }
  if (pathname === FAVORITES_PATH) {
    return true;
  }
  if (RACE_API_ROOT.test(pathname)) {
    return true;
  }
  return HORSE_RUNNING_STYLES_API.test(pathname);
};

export const resolveMcpApiPath = (pathWithQuery: string): string | null => {
  const resolved = parseAbsoluteApiPath(pathWithQuery);
  if (resolved === null || !isMcpAllowedApiPath(resolved.pathname)) {
    return null;
  }
  return `${resolved.pathname}${resolved.search}`;
};

export const resolveMcpPaddockWritePath = (pathWithQuery: string): string | null => {
  const resolved = parseAbsoluteApiPath(pathWithQuery);
  if (resolved === null || !PADDOCK_STATE_API.test(resolved.pathname)) {
    return null;
  }
  return `${resolved.pathname}${resolved.search}`;
};
