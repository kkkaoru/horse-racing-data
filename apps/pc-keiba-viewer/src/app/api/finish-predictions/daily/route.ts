// Run with bun. Serves per-day finish predictions for the authenticated MCP endpoint.

import { NextResponse } from "next/server";

import type { RaceSource } from "../../../../lib/codes";
import { getDailyFinishPredictions } from "../../../../lib/daily-finish-predictions.server";

const YEAR_PATTERN: RegExp = /^\d{4}$/u;
const MONTH_DAY_PATTERN: RegExp = /^\d{2}$/u;
const KEIBAJO_PATTERN: RegExp = /^[0-9A-Z]{2}$/u;

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const invalidRequest = (): NextResponse =>
  NextResponse.json(
    {
      error:
        "year, month, day, and source=jra|nar are required; optional keibajoCode and raceNumber must be supplied together in two-character format",
    },
    { status: 400 },
  );

export async function GET(request: Request): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const year = searchParams.get("year");
  const month = searchParams.get("month");
  const day = searchParams.get("day");
  const source = searchParams.get("source");
  const keibajoCode = searchParams.get("keibajoCode");
  const raceNumber = searchParams.get("raceNumber");
  if (
    year === null ||
    !YEAR_PATTERN.test(year) ||
    month === null ||
    !MONTH_DAY_PATTERN.test(month) ||
    day === null ||
    !MONTH_DAY_PATTERN.test(day) ||
    !isRaceSource(source) ||
    (keibajoCode === null) !== (raceNumber === null) ||
    (keibajoCode !== null && !KEIBAJO_PATTERN.test(keibajoCode)) ||
    (raceNumber !== null && !MONTH_DAY_PATTERN.test(raceNumber))
  ) {
    return invalidRequest();
  }
  const race =
    keibajoCode === null || raceNumber === null ? undefined : { keibajoCode, raceNumber };
  const payload = await getDailyFinishPredictions({ day, month, race, source, year });
  return NextResponse.json(payload, {
    headers: { "Cache-Control": "private, max-age=0, no-store" },
  });
}
