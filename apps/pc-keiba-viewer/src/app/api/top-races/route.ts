import { NextResponse } from "next/server";

import { getTopRaceWindows } from "../../../db/queries";

export const dynamic = "force-dynamic";

export async function GET() {
  const raceWindows = await getTopRaceWindows();
  return NextResponse.json(raceWindows, {
    headers: {
      "Cache-Control": "public, s-maxage=15, stale-while-revalidate=45",
    },
  });
}
