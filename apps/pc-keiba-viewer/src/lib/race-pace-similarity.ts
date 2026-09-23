// Run with bun (vitest) / Cloudflare Workers runtime.
// Nearest-neighbour estimates for pace and finish-position prediction,
// computed in memory.
//
// getRacePaceSimilarityFeatures and getFinishPositionSimilarityFeatures used
// to run one PostgreSQL query per runner. Every query re-read the same
// race-only filtered candidate rows (2,500 / 8,000) and ranked them with
// pgvector; pace similarity took ~12s per race on 2026-09-23. The candidates
// are now read once and each runner is ranked here with the same maths as the
// SQL: nearest by L2 distance, weight 1 / (1 + distance), weighted means of
// sum(value * weight) over non-null values divided by sum(weight), and plain
// means for win / show rates.

import type { FinishPositionSimilarityFeature, RacePaceSimilarityFeature } from "./race-types";

export interface VectorCandidate {
  vector: readonly number[];
}

export interface RacePaceCornerCandidate extends VectorCandidate {
  corners: readonly [number | null, number | null, number | null, number | null];
}

export interface FinishPositionCandidate extends VectorCandidate {
  finishNorm: number;
  finishPosition: number | null;
}

export interface SimilarityParams<T extends VectorCandidate> {
  candidates: readonly T[];
  horseNumber: string;
  runnerCount: number;
  vector: readonly number[];
}

export interface WeightedNeighbor<T> {
  candidate: T;
  weight: number;
}

interface DistancedCandidate<T> {
  candidate: T;
  distance: number;
}

const PACE_NEIGHBOR_LIMIT = 40;
const FINISH_NEIGHBOR_LIMIT = 80;
const SHOW_POSITION_LIMIT = 3;

export const parseVectorText = (text: unknown): number[] | null => {
  if (typeof text !== "string") return null;
  const trimmed = text.trim();
  if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) return null;
  const values = trimmed
    .slice(1, -1)
    .split(",")
    .map((part) => Number(part));
  return values.length > 0 && values.every(Number.isFinite) ? values : null;
};

const l2Distance = (left: readonly number[], right: readonly number[]): number =>
  Math.sqrt(left.reduce((sum, value, index) => sum + (value - (right[index] ?? 0)) ** 2, 0));

const compareByDistance = <T>(left: DistancedCandidate<T>, right: DistancedCandidate<T>): number =>
  left.distance - right.distance;

export const nearestWeighted = <T extends VectorCandidate>(
  candidates: readonly T[],
  vector: readonly number[],
  limit: number,
): WeightedNeighbor<T>[] =>
  candidates
    .map((candidate) => ({ candidate, distance: l2Distance(candidate.vector, vector) }))
    .toSorted(compareByDistance)
    .slice(0, limit)
    .map(({ candidate, distance }) => ({ candidate, weight: 1 / (1 + distance) }));

const totalWeightOf = <T>(neighbors: readonly WeightedNeighbor<T>[]): number =>
  neighbors.reduce((sum, neighbor) => sum + neighbor.weight, 0);

const weightedMean = <T>(
  neighbors: readonly WeightedNeighbor<T>[],
  valueOf: (candidate: T) => number | null,
  totalWeight: number,
): number | null => {
  const weighted = neighbors.flatMap((neighbor) => {
    const value = valueOf(neighbor.candidate);
    return value === null ? [] : [value * neighbor.weight];
  });
  return weighted.length === 0
    ? null
    : weighted.reduce((sum, value) => sum + value, 0) / totalWeight;
};

const shareOf = <T>(
  neighbors: readonly WeightedNeighbor<T>[],
  matches: (candidate: T) => boolean,
): number => neighbors.filter((neighbor) => matches(neighbor.candidate)).length / neighbors.length;

const scaleToField = (value: number | null, runnerCount: number): number | null =>
  value === null ? null : value * (runnerCount - 1) + 1;

export const computeRacePaceSimilarity = ({
  candidates,
  horseNumber,
  runnerCount,
  vector,
}: SimilarityParams<RacePaceCornerCandidate>): RacePaceSimilarityFeature | null => {
  const nearest = nearestWeighted(candidates, vector, PACE_NEIGHBOR_LIMIT);
  if (nearest.length === 0) return null;
  const totalWeight = totalWeightOf(nearest);
  const corner = (index: 0 | 1 | 2 | 3): number | null =>
    scaleToField(
      weightedMean(nearest, (candidate) => candidate.corners[index], totalWeight),
      runnerCount,
    );
  return {
    corner1: corner(0),
    corner2: corner(1),
    corner3: corner(2),
    corner4: corner(3),
    horseNumber,
    neighborCount: nearest.length,
    similarityScore: totalWeight / nearest.length,
  };
};

export const computeFinishPositionSimilarity = ({
  candidates,
  horseNumber,
  runnerCount,
  vector,
}: SimilarityParams<FinishPositionCandidate>): FinishPositionSimilarityFeature | null => {
  const nearest = nearestWeighted(candidates, vector, FINISH_NEIGHBOR_LIMIT);
  if (nearest.length === 0) return null;
  const totalWeight = totalWeightOf(nearest);
  return {
    averageFinishPosition: scaleToField(
      weightedMean(nearest, (candidate) => candidate.finishNorm, totalWeight),
      runnerCount,
    ),
    horseNumber,
    neighborCount: nearest.length,
    showRate: shareOf(
      nearest,
      (candidate) =>
        candidate.finishPosition !== null &&
        candidate.finishPosition >= 1 &&
        candidate.finishPosition <= SHOW_POSITION_LIMIT,
    ),
    similarityScore: totalWeight / nearest.length,
    winRate: shareOf(nearest, (candidate) => candidate.finishPosition === 1),
  };
};
