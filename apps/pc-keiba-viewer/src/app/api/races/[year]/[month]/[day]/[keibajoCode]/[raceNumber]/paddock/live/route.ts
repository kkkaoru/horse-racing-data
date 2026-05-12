import { NextResponse } from "next/server";

import {
  connectPaddockRoom,
  isPaddockRaceParams,
} from "../../../../../../../../../../lib/paddock-server";

export const dynamic = "force-dynamic";

interface PaddockLiveRouteProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

export async function GET(request: Request, { params }: PaddockLiveRouteProps) {
  const raceParams = await params;
  if (!isPaddockRaceParams(raceParams)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const response = connectPaddockRoom(raceParams, request);
  if (!response) {
    return NextResponse.json({ error: "paddock_room_unavailable" }, { status: 501 });
  }
  return response;
}
