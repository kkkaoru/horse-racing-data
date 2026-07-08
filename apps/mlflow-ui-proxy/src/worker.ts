// Run with bun.
import { isAuthorized, unauthorizedResponse } from "./auth";
import { proxyRequest } from "./proxy";
import type { Env } from "./types";

export default {
  fetch: async (request: Request, env: Env): Promise<Response> => {
    const authorized = await isAuthorized(request, env);
    return authorized ? proxyRequest(request, env) : unauthorizedResponse();
  },
};
