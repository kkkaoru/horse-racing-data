// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

const API_SPEC_PATH: string = "/api/spec";
const API_TOP_RACES_PATH: string = "/api/top-races";
const FAVORITES_SEARCH_PATH: string = "/api/mypage/favorites/search";
const FAVORITES_PATH: string = "/api/mypage/favorites";

const RACE_API_ROOT: RegExp =
  /^\/api\/races\/\d{4}\/\d{2}\/\d{2}\/[0-9A-Z]{2}\/\d{2}\/(?:paddock|premium|realtime|recent-results|running-styles|trends|sections\/[a-z0-9-]+)$/;
const HORSE_RUNNING_STYLES_API: RegExp = /^\/api\/horses\/[0-9]{6,16}\/running-styles$/;

export const isMcpAllowedApiPath = (pathname: string): boolean => {
  if (pathname === API_SPEC_PATH) {
    return true;
  }
  if (pathname === API_TOP_RACES_PATH) {
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
  if (!pathWithQuery.startsWith("/")) {
    return null;
  }
  try {
    const resolved = new URL(pathWithQuery, "https://mcp.invalid");
    if (!isMcpAllowedApiPath(resolved.pathname)) {
      return null;
    }
    return `${resolved.pathname}${resolved.search}`;
  } catch {
    return null;
  }
};
