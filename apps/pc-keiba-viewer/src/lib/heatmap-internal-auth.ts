// Run with bun (vitest) / Cloudflare Workers runtime.
import { secretsEqual } from "./mcp-auth";

export const isHeatmapInternalRequest = (request: Request, token: string | undefined): boolean => {
  const provided = request.headers.get("x-pc-keiba-internal-token");
  return (
    typeof token === "string" &&
    token.length > 0 &&
    provided !== null &&
    secretsEqual(provided, token)
  );
};
