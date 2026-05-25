// Warm the KV-backed top race windows snapshot. Hit by the `*/5 * * * *`
// cron so the home page always has a sub-second cache hit and Neon never
// suspends from inactivity.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import { NextResponse } from "next/server";

import { refreshTopRaceWindowsCache } from "../../../../db/queries";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const allowed =
    request.headers.get("X-PC-Keiba-Cache-Warm") === "scheduled" ||
    new URL(request.url).searchParams.get("debug") === "1";
  if (!allowed) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }
  const payload = await refreshTopRaceWindowsCache();
  return NextResponse.json({
    finishedCount: payload.finished.length,
    upcomingCount: payload.upcoming.length,
  });
}
