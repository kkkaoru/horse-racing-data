// Run with bun.
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

interface OfficialRankFieldAccumulator {
  finalized: string[][];
  pending: string[];
  pendingLength: number;
}

interface ChunkAppendInput {
  accumulator: OfficialRankFieldAccumulator;
  line: string;
  nextLength: number;
}

// Discord embed field value hard cap (1024 chars). We pack as many full
// official-rank lines as possible per field and overflow into additional
// fields so every ranked horse is included without truncation.
const DISCORD_EMBED_FIELD_MAX_CHARS = 1024;
const OFFICIAL_RANK_LINE_SEPARATOR = "\n";
const EMPTY_OFFICIAL_RANK_ACCUMULATOR: OfficialRankFieldAccumulator = {
  finalized: [],
  pending: [],
  pendingLength: 0,
};

const formatNonZeroMetric = (label: string, value: number): string | null =>
  value === 0 ? null : `${label}${value}`;

const hasOfficialRank = (officialRank: string): boolean =>
  officialRank.length > 0 && officialRank !== "-" && Number.isFinite(Number(officialRank));

const compareByOfficialRankAsc = (
  left: DiscordPaddockHorsePayload,
  right: DiscordPaddockHorsePayload,
): number => Number(left.officialRank) - Number(right.officialRank);

const computeAppendedLength = (currentLength: number, line: string): number =>
  currentLength === 0 ? line.length : currentLength + line.length + 1;

const startNewChunk = (
  accumulator: OfficialRankFieldAccumulator,
  line: string,
): OfficialRankFieldAccumulator => ({
  finalized: [...accumulator.finalized, accumulator.pending],
  pending: [line],
  pendingLength: line.length,
});

const appendToCurrentChunk = ({
  accumulator,
  line,
  nextLength,
}: ChunkAppendInput): OfficialRankFieldAccumulator => ({
  finalized: accumulator.finalized,
  pending: [...accumulator.pending, line],
  pendingLength: nextLength,
});

const reduceOfficialRankLine = (
  accumulator: OfficialRankFieldAccumulator,
  line: string,
): OfficialRankFieldAccumulator => {
  const projectedLength = computeAppendedLength(accumulator.pendingLength, line);
  const shouldStartNewChunk =
    accumulator.pending.length > 0 && projectedLength > DISCORD_EMBED_FIELD_MAX_CHARS;
  return shouldStartNewChunk
    ? startNewChunk(accumulator, line)
    : appendToCurrentChunk({ accumulator, line, nextLength: projectedLength });
};

const flattenOfficialRankAccumulator = (accumulator: OfficialRankFieldAccumulator): string[][] =>
  accumulator.pending.length === 0
    ? accumulator.finalized
    : [...accumulator.finalized, accumulator.pending];

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
  const summaryParts = [`⭐ **${horse.total}**`];
  if (hasOfficialRank(horse.officialRank)) {
    summaryParts.push(`公式${horse.officialRank}`);
  }
  if (metricLine) {
    summaryParts.push(`👀 ${metricLine}`);
  }
  return [
    `${rankIcon} **${horse.horseNumber} ${horse.horseName}**（${horse.sexAge || "-"}）`,
    `　👤 ${horse.jockeyName || "-"}　⚖️ ${horse.weight || "-"}　📈 ${horse.popularity}人気　💴 ${horse.odds}`,
    `　${summaryParts.join("　")}`,
  ].join("\n");
};

export const formatPaddockDiscordOfficialRankLine = (horse: DiscordPaddockHorsePayload): string =>
  `${horse.officialRank}. **${horse.horseNumber} ${horse.horseName}** / ⭐ ${horse.total} / ⚖️ ${horse.weight || "-"}`;

export const buildPaddockDiscordOfficialRankLines = (
  horses: DiscordPaddockHorsePayload[],
): string[] =>
  horses
    .filter((horse) => hasOfficialRank(horse.officialRank))
    .toSorted(compareByOfficialRankAsc)
    .map(formatPaddockDiscordOfficialRankLine);

export const chunkPaddockDiscordOfficialRankFields = (lines: string[]): string[] =>
  flattenOfficialRankAccumulator(
    lines.reduce<OfficialRankFieldAccumulator>(
      reduceOfficialRankLine,
      EMPTY_OFFICIAL_RANK_ACCUMULATOR,
    ),
  ).map((chunkLines) => chunkLines.join(OFFICIAL_RANK_LINE_SEPARATOR));
