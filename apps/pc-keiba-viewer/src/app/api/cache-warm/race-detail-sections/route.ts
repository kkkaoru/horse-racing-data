import { getCloudflareContext } from "@opennextjs/cloudflare";
import { NextResponse } from "next/server";

import { getRacesByDate } from "../../../../db/queries";
import { isCornerPacePredictionSupported } from "../../../../lib/race-pace-prediction";
import {
  DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS,
  getTomorrowJstDateParts,
  parseIsoDateParts,
  type DetailSectionCacheWarmMessage,
  type DetailSectionCacheableSection,
} from "../../../../lib/race-detail-section-cache";

export const dynamic = "force-dynamic";

const QUEUE_BATCH_SIZE = 50;

const getCloudflareEnv = async (): Promise<CloudflareEnv | null> => {
  try {
    return (await getCloudflareContext({ async: true })).env;
  } catch {
    return null;
  }
};

const chunkArray = <T>(items: readonly T[], size: number): T[][] => {
  const chunks: T[][] = [];
  items.forEach((item, index) => {
    if (index % size === 0) {
      chunks.push([]);
    }
    chunks[chunks.length - 1]?.push(item);
  });
  return chunks;
};

const getTargetDateParts = (searchParams: URLSearchParams) =>
  parseIsoDateParts(searchParams.get("date")) ?? getTomorrowJstDateParts();

const getRaceSections = (race: {
  distance?: string | null;
  keibajoCode: string;
  source: "jra" | "nar";
}): DetailSectionCacheableSection[] => {
  const sections: DetailSectionCacheableSection[] = [...DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS];
  if (race.source === "nar") {
    sections.push("ability");
  }
  if (
    isCornerPacePredictionSupported({
      distance: race.distance ?? null,
      keibajoCode: race.keibajoCode,
      source: race.source,
    })
  ) {
    sections.push("pace-prediction");
  }
  return sections;
};

const sendBatches = async (
  queue: PcKeibaQueue<DetailSectionCacheWarmMessage>,
  messages: DetailSectionCacheWarmMessage[],
): Promise<void> => {
  await Promise.all(
    chunkArray(messages, QUEUE_BATCH_SIZE).map((chunk) =>
      queue.sendBatch(chunk.map((body) => ({ body }))),
    ),
  );
};

export async function POST(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  const allowed =
    request.headers.get("X-PC-Keiba-Cache-Warm") === "scheduled" ||
    searchParams.get("debug") === "1";
  if (!allowed) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const target = getTargetDateParts(searchParams);
  const races = await getRacesByDate(target.year, target.month, target.day);
  const messages = races.flatMap((race): DetailSectionCacheWarmMessage[] =>
    getRaceSections({
      distance: race.kyori,
      keibajoCode: race.keibajoCode,
      source: race.source,
    }).map((section) => ({
      day: target.day,
      keibajoCode: race.keibajoCode,
      month: target.month,
      raceNumber: race.raceBango,
      section,
      source: race.source,
      year: target.year,
    })),
  );
  const env = await getCloudflareEnv();
  const queue = env?.DETAIL_SECTION_CACHE_QUEUE;
  if (!queue) {
    return NextResponse.json(
      {
        date: `${target.year}-${target.month}-${target.day}`,
        enqueued: 0,
        error: "DETAIL_SECTION_CACHE_QUEUE binding is unavailable",
        raceCount: races.length,
      },
      { status: 503 },
    );
  }

  await sendBatches(queue, messages);

  return NextResponse.json({
    date: `${target.year}-${target.month}-${target.day}`,
    enqueued: messages.length,
    raceCount: races.length,
  });
}
