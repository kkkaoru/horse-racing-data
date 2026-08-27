// Run with bun. Proxies a bounded cursor page from the R2 Catalog service.
import { NextResponse } from "next/server";

import { fetchRaceEntityRecentResultsCatalog } from "../../../../../../../../../lib/race-entity-recent-results-catalog.server";

export const dynamic = "force-dynamic";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const validRoute = (input: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}): boolean =>
  /^\d{4}$/u.test(input.year) &&
  /^\d{2}$/u.test(input.month) &&
  /^\d{2}$/u.test(input.day) &&
  /^[0-9A-Z]{2}$/u.test(input.keibajoCode) &&
  /^\d{2}$/u.test(input.raceNumber);

export async function GET(request: Request, context: RouteContext): Promise<Response> {
  const route = await context.params;
  if (!validRoute(route)) {
    return NextResponse.json(
      { error: { code: "RACE_NOT_FOUND", message: "The target race route is invalid." } },
      { status: 400 },
    );
  }
  const search = new URL(request.url).searchParams;
  const result = await fetchRaceEntityRecentResultsCatalog({
    cursor: search.get("cursor"),
    date: `${route.year}${route.month}${route.day}`,
    entityType: search.get("entityType") ?? "",
    horseNumber: search.get("horseNumber") ?? "",
    keibajoCode: route.keibajoCode,
    limit: search.get("limit"),
    raceNumber: route.raceNumber,
    source: search.get("source") ?? "",
  });
  return NextResponse.json(result.value, { status: result.status });
}
