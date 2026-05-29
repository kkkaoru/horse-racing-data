import { NextResponse } from "next/server";

import type { RaceSource } from "../../../../../../../../../../lib/codes";
import {
  connectRaceTrendRoom,
  isRaceTrendRoomParams,
} from "../../../../../../../../../../lib/race-trend-room.server";

export const dynamic = "force-dynamic";

interface RaceTrendLiveRouteProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

export async function GET(request: Request, { params }: RaceTrendLiveRouteProps) {
  const routeParams = await params;
  const source = new URL(request.url).searchParams.get("source");
  if (!isRaceSource(source)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const roomParams = { ...routeParams, source };
  if (!isRaceTrendRoomParams(roomParams)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const response = await connectRaceTrendRoom(roomParams, request);
  if (!response) {
    return NextResponse.json({ error: "race_trend_room_unavailable" }, { status: 501 });
  }
  return response;
}
