import { getCloudflareContext } from "@opennextjs/cloudflare";
import { NextResponse } from "next/server";

import { isPaddockRaceParams } from "../../../../../../../../../../lib/paddock-server";

export const dynamic = "force-dynamic";

interface DiscordPaddockRouteProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

interface DiscordPaddockHorsePayload {
  attention: number;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  kaeshi: number;
  odds: string;
  officialRank: string;
  paddock: number;
  popularity: string;
  preference: number;
  sexAge: string;
  total: string;
  weight: string;
}

interface DiscordPaddockPayload {
  detailUrl: string;
  horses: DiscordPaddockHorsePayload[];
  raceMeta: string;
  raceNumberLabel: string;
  racePlace: string;
  raceStartsAtLabel: string;
  raceTitle: string;
}

const getCorsHeaders = (request: Request): Record<string, string> => {
  const origin = request.headers.get("origin");
  if (!origin) {
    return {};
  }
  const { hostname } = new URL(origin);
  if (hostname !== "localhost" && hostname !== "127.0.0.1" && hostname !== "0.0.0.0") {
    return {};
  }
  return {
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "POST,OPTIONS",
    "Access-Control-Allow-Origin": origin,
    Vary: "Origin",
  };
};

const getDiscordWebhookUrl = (): string | null => {
  if (process.env.PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL) {
    return process.env.PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL;
  }

  try {
    return getCloudflareContext().env.PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL ?? null;
  } catch {
    return null;
  }
};

const getDiscordBotName = (): string => {
  if (process.env.PC_KEIBA_PADDOCK_DISCORD_BOT_NAME) {
    return process.env.PC_KEIBA_PADDOCK_DISCORD_BOT_NAME;
  }

  try {
    return getCloudflareContext().env.PC_KEIBA_PADDOCK_DISCORD_BOT_NAME ?? "PC-KEIBA Paddock";
  } catch {
    return "PC-KEIBA Paddock";
  }
};

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isString = (value: unknown): value is string => typeof value === "string";

const isDiscordHorsePayload = (value: unknown): value is DiscordPaddockHorsePayload => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "horseNumber" in value &&
    isString(value.horseNumber) &&
    "horseName" in value &&
    isString(value.horseName) &&
    "sexAge" in value &&
    isString(value.sexAge) &&
    "weight" in value &&
    isString(value.weight) &&
    "jockeyName" in value &&
    isString(value.jockeyName) &&
    "popularity" in value &&
    isString(value.popularity) &&
    "odds" in value &&
    isString(value.odds) &&
    "officialRank" in value &&
    isString(value.officialRank) &&
    "total" in value &&
    isString(value.total) &&
    "paddock" in value &&
    isFiniteNumber(value.paddock) &&
    "kaeshi" in value &&
    isFiniteNumber(value.kaeshi) &&
    "attention" in value &&
    isFiniteNumber(value.attention) &&
    "preference" in value &&
    isFiniteNumber(value.preference)
  );
};

const isDiscordPayload = (value: unknown): value is DiscordPaddockPayload => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "detailUrl" in value &&
    isString(value.detailUrl) &&
    value.detailUrl.startsWith("https://pc-keiba-viewer.kkk4oru.com/") &&
    "raceMeta" in value &&
    isString(value.raceMeta) &&
    "raceNumberLabel" in value &&
    isString(value.raceNumberLabel) &&
    "racePlace" in value &&
    isString(value.racePlace) &&
    "raceStartsAtLabel" in value &&
    isString(value.raceStartsAtLabel) &&
    "raceTitle" in value &&
    isString(value.raceTitle) &&
    "horses" in value &&
    Array.isArray(value.horses) &&
    value.horses.length > 0 &&
    value.horses.length <= 18 &&
    value.horses.every(isDiscordHorsePayload)
  );
};

const truncate = (value: string, maxLength: number): string =>
  value.length <= maxLength ? value : `${value.slice(0, Math.max(0, maxLength - 1))}…`;

const getScoreNumber = (horse: DiscordPaddockHorsePayload): number => {
  const score = Number(horse.total);
  return Number.isFinite(score) ? score : 0;
};

const sortByPaddockScore = (
  left: DiscordPaddockHorsePayload,
  right: DiscordPaddockHorsePayload,
): number => {
  const scoreDiff = getScoreNumber(right) - getScoreNumber(left);
  if (scoreDiff !== 0) {
    return scoreDiff;
  }
  const leftRank = Number(left.officialRank);
  const rightRank = Number(right.officialRank);
  if (Number.isFinite(leftRank) && Number.isFinite(rightRank) && leftRank !== rightRank) {
    return leftRank - rightRank;
  }
  return Number(left.horseNumber) - Number(right.horseNumber);
};

const formatHorseLine = (horse: DiscordPaddockHorsePayload, index: number): string => {
  const rankIcon = index === 0 ? "🥇" : index === 1 ? "🥈" : index === 2 ? "🥉" : "▫️";
  return [
    `${rankIcon} **${horse.horseNumber} ${horse.horseName}**（${horse.sexAge || "-"}）`,
    `　👤 ${horse.jockeyName || "-"}　⚖️ ${horse.weight || "-"}　📈 ${horse.popularity}人気　💴 ${horse.odds}`,
    `　⭐ **${horse.total}**　🏅 公式${horse.officialRank}　👀 気配${horse.paddock} 返し${horse.kaeshi} 注目${horse.attention} 好み${horse.preference}`,
  ].join("\n");
};

const buildDiscordEmbed = (payload: DiscordPaddockPayload) => {
  const sortedHorses = payload.horses.toSorted(sortByPaddockScore);
  const topHorses = sortedHorses.slice(0, 3);
  const horseLines = topHorses.map((horse, index) => formatHorseLine(horse, index));
  const officialRankLines = payload.horses
    .filter((horse) => Number.isFinite(Number(horse.officialRank)))
    .toSorted((left, right) => Number(left.officialRank) - Number(right.officialRank))
    .map(
      (horse) =>
        `${horse.officialRank}. **${horse.horseNumber} ${horse.horseName}** / ⭐ ${horse.total} / ⚖️ ${horse.weight || "-"}`,
    );
  const topHorse = sortedHorses[0];

  return {
    author: {
      name: "PC-KEIBA Viewer",
    },
    color: 0x5865f2,
    description: [
      `**${truncate(payload.raceTitle, 120)}**`,
      `🏟️ ${payload.racePlace || "-"}　#️⃣ ${payload.raceNumberLabel || "-"}　🕒 ${payload.raceStartsAtLabel || "-"}`,
      topHorse
        ? `✅ 最上位: **${topHorse.horseNumber} ${topHorse.horseName}** / ⭐ **${topHorse.total}**`
        : "",
      `[レース詳細を開く](${payload.detailUrl})`,
    ]
      .filter(Boolean)
      .join("\n"),
    fields: [
      {
        inline: false,
        name: "上位3頭",
        value: truncate(horseLines.join("\n\n"), 1024),
      },
      ...(officialRankLines.length > 0
        ? [
            {
              inline: false,
              name: "公式評価順",
              value: truncate(officialRankLines.join("\n"), 1024),
            },
          ]
        : []),
    ],
    footer: {
      text: `全${payload.horses.length}頭から総合順で上位3頭を表示`,
    },
    timestamp: new Date().toISOString(),
    title: "🏇 パドック評価通知",
  };
};

export async function OPTIONS(request: Request) {
  return new Response(null, { headers: getCorsHeaders(request), status: 204 });
}

export async function POST(request: Request, { params }: DiscordPaddockRouteProps) {
  const raceParams = await params;
  if (!isPaddockRaceParams(raceParams)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const payload: unknown = await request.json().catch(() => null);
  if (!isDiscordPayload(payload)) {
    return NextResponse.json({ error: "invalid_payload" }, { status: 400 });
  }

  const webhookUrl = getDiscordWebhookUrl();
  if (!webhookUrl) {
    return NextResponse.json({ error: "discord_webhook_not_configured" }, { status: 503 });
  }

  const response = await fetch(webhookUrl, {
    body: JSON.stringify({
      embeds: [buildDiscordEmbed(payload)],
      username: getDiscordBotName(),
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });

  if (!response.ok) {
    return NextResponse.json(
      { error: "discord_webhook_failed", status: response.status },
      { status: 502 },
    );
  }

  return NextResponse.json(
    { ok: true },
    {
      headers: {
        ...getCorsHeaders(request),
        "Cache-Control": "private, max-age=0, no-store",
      },
    },
  );
}
