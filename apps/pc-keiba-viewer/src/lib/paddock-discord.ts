export interface DiscordPaddockHorsePayload {
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

const formatNonZeroMetric = (label: string, value: number): string | null =>
  value === 0 ? null : `${label}${value}`;

export const formatPaddockDiscordHorseLine = (
  horse: DiscordPaddockHorsePayload,
  index: number,
): string => {
  const rankIcon = index === 0 ? "🥇" : index === 1 ? "🥈" : index === 2 ? "🥉" : "▫️";
  const metricLine = [
    formatNonZeroMetric("気配", horse.paddock),
    formatNonZeroMetric("返し", horse.kaeshi),
    formatNonZeroMetric("注目", horse.attention),
    formatNonZeroMetric("好み", horse.preference),
  ]
    .filter((value): value is string => value !== null)
    .join(" ");
  return [
    `${rankIcon} **${horse.horseNumber} ${horse.horseName}**（${horse.sexAge || "-"}）`,
    `　👤 ${horse.jockeyName || "-"}　⚖️ ${horse.weight || "-"}　📈 ${horse.popularity}人気　💴 ${horse.odds}`,
    `　⭐ **${horse.total}**　公式${horse.officialRank}${metricLine ? `　👀 ${metricLine}` : ""}`,
  ].join("\n");
};
