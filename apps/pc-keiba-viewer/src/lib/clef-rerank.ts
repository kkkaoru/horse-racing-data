// Run with bun (bunx vitest). Decoder for the finish-position-clef-rerank Worker payload.

export interface ClefRerankRow {
  horseNumber: number;
  rank: number;
  baseRank: number;
  clefWin: number;
  clefTop3: number;
}

export interface ClefRerankCard {
  modelVersion: string;
  label: string;
  baseModelVersion: string;
  baseGeneratedAt: string;
  predictions: ClefRerankRow[];
}

export interface ClefRerankRaceAddress {
  year: string;
  month: string;
  day: string;
  keibajoCode: string;
  raceNumber: string;
}

const CLEF_RERANK_ENDPOINT: string = "/api/clef-rerank";
const TWO_DIGITS: number = 2;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isRow = (value: unknown): value is ClefRerankRow =>
  isRecord(value) &&
  isFiniteNumber(value.horseNumber) &&
  isFiniteNumber(value.rank) &&
  isFiniteNumber(value.baseRank) &&
  isFiniteNumber(value.clefWin) &&
  isFiniteNumber(value.clefTop3);

const pad = (value: string): string => value.padStart(TWO_DIGITS, "0");

export const buildClefRerankUrl = ({
  year,
  month,
  day,
  keibajoCode,
  raceNumber,
}: ClefRerankRaceAddress): string =>
  `${CLEF_RERANK_ENDPOINT}/${year}${pad(month)}${pad(day)}/${keibajoCode}/${pad(raceNumber)}`;

export const readClefRerank = (value: unknown): ClefRerankCard | null =>
  isRecord(value) &&
  typeof value.modelVersion === "string" &&
  typeof value.label === "string" &&
  typeof value.baseModelVersion === "string" &&
  typeof value.baseGeneratedAt === "string" &&
  Array.isArray(value.predictions) &&
  value.predictions.length > 0 &&
  value.predictions.every(isRow)
    ? {
        modelVersion: value.modelVersion,
        label: value.label,
        baseModelVersion: value.baseModelVersion,
        baseGeneratedAt: value.baseGeneratedAt,
        predictions: value.predictions.map((row) => ({
          horseNumber: row.horseNumber,
          rank: row.rank,
          baseRank: row.baseRank,
          clefWin: row.clefWin,
          clefTop3: row.clefTop3,
        })),
      }
    : null;

export const formatRankChange = (row: Pick<ClefRerankRow, "rank" | "baseRank">): string =>
  row.baseRank === row.rank
    ? "→"
    : `${row.baseRank > row.rank ? "↑" : "↓"}${String(Math.abs(row.baseRank - row.rank))}`;
