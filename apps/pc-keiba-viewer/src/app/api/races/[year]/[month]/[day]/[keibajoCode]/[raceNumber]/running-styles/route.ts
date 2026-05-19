// Run with bun. Read-only D1 query endpoint for race-by-race
// running-style predictions.

import { NextResponse } from "next/server";

import {
  buildRaceKey,
  getRaceRunningStylesFromD1,
} from "../../../../../../../../../db/corner-running-style-queries";

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

const SOURCE_PREFIXES = ["jra", "nar"] as const;

const ALLOWED_CODE = /^[0-9A-Z]{1,2}$/u;

const requireSourceFromHost = (request: Request): "jra" | "nar" => {
  const url = new URL(request.url);
  const value = url.searchParams.get("source");
  if (value === "jra" || value === "nar") return value;
  return "nar";
};

const isValidParams = (params: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}): boolean =>
  /^\d{4}$/u.test(params.year) &&
  /^\d{1,2}$/u.test(params.month) &&
  /^\d{1,2}$/u.test(params.day) &&
  ALLOWED_CODE.test(params.keibajoCode) &&
  /^\d{1,2}$/u.test(params.raceNumber);

const buildKaisaiTsukihi = (month: string, day: string): string =>
  `${month.padStart(2, "0")}${day.padStart(2, "0")}`;

const collectRowsForSources = async (
  raceParams: { kaisaiNen: string; kaisaiTsukihi: string; keibajoCode: string; raceBango: string },
  preferredSource: "jra" | "nar",
): Promise<unknown[]> => {
  const orderedSources = SOURCE_PREFIXES.toSorted((a, b) =>
    a === preferredSource ? -1 : b === preferredSource ? 1 : 0,
  );
  const lookups = await Promise.all(
    orderedSources.map((source) =>
      getRaceRunningStylesFromD1(buildRaceKey({ ...raceParams, source })),
    ),
  );
  return lookups.find((rows) => rows.length > 0) ?? [];
};

export async function GET(request: Request, context: RouteContext) {
  const params = await context.params;
  if (!isValidParams(params)) {
    return NextResponse.json({ error: "invalid_params" }, { status: 400 });
  }
  const preferredSource = requireSourceFromHost(request);
  const rows = await collectRowsForSources(
    {
      kaisaiNen: params.year,
      kaisaiTsukihi: buildKaisaiTsukihi(params.month, params.day),
      keibajoCode: params.keibajoCode,
      raceBango: params.raceNumber,
    },
    preferredSource,
  );
  return NextResponse.json(rows, {
    headers: { "cache-control": "public, max-age=30, s-maxage=30" },
  });
}
