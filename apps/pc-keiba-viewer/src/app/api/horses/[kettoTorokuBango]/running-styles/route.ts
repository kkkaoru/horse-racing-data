// Run with bun. Read-only D1 query endpoint for horse-detail running-style
// history.

import { NextResponse } from "next/server";

import { getHorseRecentRunningStylesWithCache } from "../../../../../lib/running-style-cache.server";

export const dynamic = "force-dynamic";

interface RouteContext {
  params: Promise<{ kettoTorokuBango: string }>;
}

const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 100;

const resolveLimit = (request: Request): number => {
  const url = new URL(request.url);
  const raw = url.searchParams.get("limit");
  if (raw === null) return DEFAULT_LIMIT;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_LIMIT;
  return Math.min(parsed, MAX_LIMIT);
};

export async function GET(request: Request, context: RouteContext) {
  const { kettoTorokuBango } = await context.params;
  if (!/^\d{10}$/u.test(kettoTorokuBango)) {
    return NextResponse.json({ error: "invalid_ketto" }, { status: 400 });
  }
  const rows = await getHorseRecentRunningStylesWithCache(kettoTorokuBango, resolveLimit(request));
  return NextResponse.json(rows, {
    headers: { "cache-control": "public, max-age=30, s-maxage=30" },
  });
}
