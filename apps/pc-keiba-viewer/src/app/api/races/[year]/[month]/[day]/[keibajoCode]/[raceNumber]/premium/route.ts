import { NextResponse } from "next/server";

import { getRaceDetail } from "../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import { fetchWithRetry } from "../../../../../../../../../lib/fetch-with-retry";
import {
  buildNetkeibaRaceId,
  parseNetkeibaTrainingReviews,
} from "../../../../../../../../../lib/netkeiba-training";

interface RouteContext {
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

const isObjectRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hasTrainingReviews = (value: unknown): boolean =>
  isObjectRecord(value) && Array.isArray(value.trainingReviews) && value.trainingReviews.length > 0;

const fetchNetkeibaTrainingReviews = async ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}): Promise<unknown[]> => {
  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return [];
  }
  const raceId = buildNetkeibaRaceId({
    kaisaiKai: race.kaisaiKai,
    kaisaiNen: race.kaisaiNen,
    kaisaiNichime: race.kaisaiNichime,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
  });
  if (!raceId) {
    return [];
  }
  const response = await fetch(`https://race.netkeiba.com/race/oikiri.html?race_id=${raceId}`, {
    cache: "no-store",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    },
  });
  if (!response.ok) {
    return [];
  }
  const html = await response.text();
  return parseNetkeibaTrainingReviews(html);
};

export async function GET(request: Request, context: RouteContext) {
  const sourceParam = new URL(request.url).searchParams.get("source");
  const source = isRaceSource(sourceParam) ? sourceParam : "jra";
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";
  const upstreamUrl = `${realtimeApiBaseUrl}/api/${source}/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/premium`;

  try {
    const response = await fetchWithRetry(upstreamUrl, { cache: "no-store" });
    const body = await response.text();
    if (source === "jra" && response.ok) {
      const payload: unknown = JSON.parse(body);
      if (isObjectRecord(payload) && !hasTrainingReviews(payload)) {
        const trainingReviews = await fetchNetkeibaTrainingReviews({
          day,
          keibajoCode,
          month,
          raceNumber,
          source,
          year,
        });
        if (trainingReviews.length > 0) {
          return NextResponse.json(
            { ...payload, trainingReviews },
            {
              headers: {
                "cache-control": "no-store",
              },
            },
          );
        }
      }
    }
    return new Response(body, {
      headers: {
        "cache-control": "no-store",
        "content-type": response.headers.get("content-type") ?? "application/json; charset=utf-8",
      },
      status: response.status,
      statusText: response.statusText,
    });
  } catch {
    const trainingReviews =
      source === "jra"
        ? await fetchNetkeibaTrainingReviews({
            day,
            keibajoCode,
            month,
            raceNumber,
            source,
            year,
          }).catch(() => [])
        : [];
    return NextResponse.json({ paddockBulletins: [], trainingReviews }, { status: 200 });
  }
}
