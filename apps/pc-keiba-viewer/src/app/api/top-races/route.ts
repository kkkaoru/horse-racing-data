import { NextResponse } from "next/server";

import { getTopRaceWindows } from "../../../db/queries";

export const dynamic = "force-dynamic";

export async function GET() {
  const raceWindows = await getTopRaceWindows();
  return NextResponse.json(raceWindows, {
    headers: {
      "Cache-Control": "private, max-age=0, no-store",
    },
  });
}
