// Run with bun (vitest) / Cloudflare Workers runtime.
// Starts HeatmapWarmWorkflow instances (one per venue) for a race day.
import { NextResponse } from "next/server";

import { getRacesByDate } from "../../../../db/queries";
import { safeGetCloudflareEnv } from "../../../../lib/cloudflare-context.server";
import { getJstDateParts, parseIsoDateParts } from "../../../../lib/race-detail-section-cache";
import { startHeatmapWarmWorkflows } from "../../../../lib/win-rate-heatmap-warm-workflow";

export const dynamic = "force-dynamic";

const HTTP_STATUS_NOT_FOUND = 404;
const HTTP_STATUS_SERVICE_UNAVAILABLE = 503;

export async function POST(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  if (request.headers.get("X-PC-Keiba-Cache-Warm") !== "scheduled") {
    return NextResponse.json({ error: "not_found" }, { status: HTTP_STATUS_NOT_FOUND });
  }
  const now = new Date();
  const target = parseIsoDateParts(searchParams.get("date")) ?? getJstDateParts(now);
  const date = `${target.year}-${target.month}-${target.day}`;
  const workflow = (await safeGetCloudflareEnv())?.HEATMAP_WARM_WORKFLOW;
  if (workflow === undefined) {
    return NextResponse.json(
      { date, error: "HEATMAP_WARM_WORKFLOW binding is unavailable" },
      { status: HTTP_STATUS_SERVICE_UNAVAILABLE },
    );
  }
  const races = await getRacesByDate(target.year, target.month, target.day);
  const started = await startHeatmapWarmWorkflows({
    date: target,
    nowMs: now.getTime(),
    races,
    workflow,
  });
  return NextResponse.json({ date, ...started });
}
