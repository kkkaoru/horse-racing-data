import { NextResponse } from "next/server";

import { safeGetCloudflareEnv } from "../../../../../../../../../../lib/cloudflare-context.server";
import {
  buildPaddockDiscordOfficialRankLines,
  chunkPaddockDiscordOfficialRankFields,
  formatPaddockDiscordHorseLine,
  type DiscordPaddockHorsePayload,
} from "../../../../../../../../../../lib/paddock-discord";
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

interface DiscordPaddockPayload {
  detailUrl: string;
  horses: DiscordPaddockHorsePayload[];
  raceMeta: string;
  raceNumberLabel: string;
  racePlace: string;
  raceStartsAtLabel: string;
  raceTitle: string;
  type?: "user-paddock";
}

interface DiscordExternalPaddockBulletinPayload {
  commentText: string;
  evaluationText: string;
  frameNumber: string;
  groupLabel: string;
  horseName: string;
  horseNumber: string;
}

interface DiscordExternalPaddockPayload {
  bulletins: DiscordExternalPaddockBulletinPayload[];
  detailUrl: string;
  raceMeta: string;
  raceNumberLabel: string;
  racePlace: string;
  raceStartsAtLabel: string;
  raceTitle: string;
  type: "external-paddock";
}

type DiscordPayload = DiscordPaddockPayload | DiscordExternalPaddockPayload;

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

const getEnvValue = async (key: string): Promise<string | null> => {
  const value = process.env[key];
  if (value) {
    return value;
  }

  const env = await safeGetCloudflareEnv();
  if (!env) {
    return null;
  }
  if (key === "PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_BOT_NAME") {
    return env.PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_BOT_NAME ?? null;
  }
  if (key === "PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_WEBHOOK_URL") {
    return env.PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_WEBHOOK_URL ?? null;
  }
  if (key === "PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL") {
    return env.PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL ?? null;
  }
  return null;
};

const getDiscordWebhookUrl = async (payload: DiscordPayload): Promise<string | null> =>
  "type" in payload && payload.type === "external-paddock"
    ? getEnvValue("PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_WEBHOOK_URL")
    : getEnvValue("PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL");

const getDiscordBotName = async (payload: DiscordPayload): Promise<string> => {
  if ("type" in payload && payload.type === "external-paddock") {
    return (await getEnvValue("PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_BOT_NAME")) ?? "外部パドック速報";
  }

  if (process.env.PC_KEIBA_PADDOCK_DISCORD_BOT_NAME) {
    return process.env.PC_KEIBA_PADDOCK_DISCORD_BOT_NAME;
  }

  const env = await safeGetCloudflareEnv();
  return env?.PC_KEIBA_PADDOCK_DISCORD_BOT_NAME ?? "PC-KEIBA Paddock";
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

const isDiscordExternalBulletinPayload = (
  value: unknown,
): value is DiscordExternalPaddockBulletinPayload => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "groupLabel" in value &&
    isString(value.groupLabel) &&
    "frameNumber" in value &&
    isString(value.frameNumber) &&
    "horseNumber" in value &&
    isString(value.horseNumber) &&
    "horseName" in value &&
    isString(value.horseName) &&
    "evaluationText" in value &&
    isString(value.evaluationText) &&
    "commentText" in value &&
    isString(value.commentText)
  );
};

const isDiscordExternalPayload = (value: unknown): value is DiscordExternalPaddockPayload => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "type" in value &&
    value.type === "external-paddock" &&
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
    "bulletins" in value &&
    Array.isArray(value.bulletins) &&
    value.bulletins.length > 0 &&
    value.bulletins.length <= 18 &&
    value.bulletins.every(isDiscordExternalBulletinPayload)
  );
};

const isKnownDiscordPayload = (value: unknown): value is DiscordPayload =>
  isDiscordPayload(value) || isDiscordExternalPayload(value);

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

const buildOfficialRankFields = (horses: DiscordPaddockHorsePayload[]) => {
  const chunks = chunkPaddockDiscordOfficialRankFields(
    buildPaddockDiscordOfficialRankLines(horses),
  );
  return chunks.map((value, index) => ({
    inline: false,
    name: index === 0 ? "公式評価順" : `公式評価順 (続き ${index + 1})`,
    value,
  }));
};

const buildDiscordEmbed = (payload: DiscordPaddockPayload) => {
  const sortedHorses = payload.horses.toSorted(sortByPaddockScore);
  const topHorses = sortedHorses.slice(0, 3);
  const horseLines = topHorses.map((horse, index) => formatPaddockDiscordHorseLine(horse, index));
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
      ...buildOfficialRankFields(payload.horses),
    ],
    footer: {
      text: `全${payload.horses.length}頭から総合順で上位3頭を表示`,
    },
    timestamp: new Date().toISOString(),
    title: "🏇 パドック評価通知",
  };
};

const formatExternalBulletinLine = (row: DiscordExternalPaddockBulletinPayload): string =>
  [
    `**${row.horseNumber} 番 ${truncate(row.horseName || "-", 32)}**　${row.groupLabel || "-"} / ${row.evaluationText || "-"}`,
    row.commentText ? `> ${truncate(row.commentText, 140)}` : "> コメントなし",
  ].join("\n");

const formatExternalRaceStart = (value: string): string =>
  value.includes("JST") ? value : `${value || "-"}（JST）`;

const buildExternalDiscordEmbed = (payload: DiscordExternalPaddockPayload) => ({
  author: {
    name: "External Paddock Feed",
  },
  color: 0xf97316,
  description: [
    `🏟️ **${payload.racePlace || "-"} ${payload.raceNumberLabel || "-"}**`,
    `🏷️ **${truncate(payload.raceTitle, 120)}**`,
    `🕒 ${formatExternalRaceStart(payload.raceStartsAtLabel)}`,
    `[レース詳細を開く](${payload.detailUrl})`,
    "",
    truncate(payload.bulletins.map(formatExternalBulletinLine).join("\n────────────\n"), 1400),
  ].join("\n"),
  footer: {
    text: `外部速報 ${payload.bulletins.length}件`,
  },
  timestamp: new Date().toISOString(),
  title: "🚨 外部パドック速報",
});

const buildEmbed = (payload: DiscordPayload) =>
  "type" in payload && payload.type === "external-paddock"
    ? buildExternalDiscordEmbed(payload)
    : buildDiscordEmbed(payload);

export async function OPTIONS(request: Request) {
  return new Response(null, { headers: getCorsHeaders(request), status: 204 });
}

export async function POST(request: Request, { params }: DiscordPaddockRouteProps) {
  const raceParams = await params;
  if (!isPaddockRaceParams(raceParams)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const payload: unknown = await request.json().catch(() => null);
  if (!isKnownDiscordPayload(payload)) {
    return NextResponse.json({ error: "invalid_payload" }, { status: 400 });
  }

  const [webhookUrl, botName] = await Promise.all([
    getDiscordWebhookUrl(payload),
    getDiscordBotName(payload),
  ]);
  if (!webhookUrl) {
    return NextResponse.json({ error: "discord_webhook_not_configured" }, { status: 503 });
  }

  const response = await fetch(webhookUrl, {
    body: JSON.stringify({
      embeds: [buildEmbed(payload)],
      username: botName,
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
