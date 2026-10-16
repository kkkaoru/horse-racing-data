import { getDayBaseManifest } from "./day-base-manifest";
import { isAuthorized } from "./trigger";
import type { Env } from "./types";

export const DAY_BASE_MANIFEST_PATH = "/api/internal/day-base-manifest";

export const handleDayBaseManifest = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    return new Response("Unauthorized", { status: 401 });
  }
  const { searchParams } = new URL(request.url);
  const category = searchParams.get("category");
  const runYmd = searchParams.get("runYmd");
  if (
    (category !== "jra" && category !== "nar" && category !== "ban-ei") ||
    runYmd === null ||
    !/^\d{8}$/.test(runYmd)
  ) {
    return new Response("Invalid scope", { status: 400 });
  }
  const manifest = await getDayBaseManifest({
    category,
    env,
    runYmd,
  });
  return Response.json(manifest, { headers: { "Cache-Control": "no-store" } });
};
