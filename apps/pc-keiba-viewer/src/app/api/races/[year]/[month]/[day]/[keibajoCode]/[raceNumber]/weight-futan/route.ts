// Run with bun. Compact horse-weight minus carried-weight rows for one race.
import { NextResponse } from "next/server";

import { getRaceRunners } from "../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import { buildWeightFutanDiffRows } from "../../../../../../../../../lib/weight-futan-diff";

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

const NO_STORE_HEADERS = { "cache-control": "no-store" };

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const isValidRouteParams = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): boolean =>
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/.test(keibajoCode) &&
  /^\d{2}$/.test(raceNumber);

export async function GET(request: Request, context: RouteContext) {
  const sourceParam = new URL(request.url).searchParams.get("source");
  if (!isRaceSource(sourceParam)) {
    return NextResponse.json({ error: "invalid source" }, { status: 400 });
  }
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  if (!isValidRouteParams(year, month, day, keibajoCode, raceNumber)) {
    return NextResponse.json({ error: "invalid_params" }, { status: 400 });
  }
  const runners = await getRaceRunners(sourceParam, year, month, day, keibajoCode, raceNumber);
  return NextResponse.json(
    { r: buildWeightFutanDiffRows({ keibajoCode, runners }) },
    { headers: NO_STORE_HEADERS },
  );
}
