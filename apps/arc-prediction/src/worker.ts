// Run checks with bun run check; deployed as a Cloudflare Worker behind existing Access.
import {
  parseXgboostJsonModel,
  scoreXgboostModel,
  type XgboostModel,
} from "../../../packages/xgboost-json-tree/src/index";
import modelJson from "./artifacts/model.json";
import card from "./artifacts/card.json";
import winModelJson from "./artifacts/win-model.json";
import evaluation from "./artifacts/evaluation.json";

export interface ArcRunner {
  horseId: string;
  umaban: number;
  name: string;
  features: (number | null)[];
}

export interface ArcCard {
  raceDate: string;
  raceName: string;
  modelVersion: string;
  modelSha256: string;
  baseModelSha256: string;
  winModelSha256: string;
  trainingMaxDate: string;
  sourceSha256: string;
  featureSnapshotAt: string;
  featureNames: string[];
  runners: ArcRunner[];
}

interface ScoreRow {
  horseId: string;
  umaban: number;
  name: string;
  baseScore: number;
  winScore: number;
}

const MODEL: XgboostModel = parseXgboostJsonModel(modelJson);
const WIN_MODEL: XgboostModel = parseXgboostJsonModel(winModelJson);
const WIN_GATE: number = 2;
const MIN_FIELD: number = 5;
const MAX_FIELD: number = 24;
const ROUTE: string = "/api/arc-prediction/";
const RESPONSE_HEADERS: Record<string, string> = { "Cache-Control": "no-store" };

const scoreOrder = (a: ScoreRow, b: ScoreRow): number =>
  b.baseScore - a.baseScore || a.horseId.localeCompare(b.horseId);

const winOrder = (a: ScoreRow, b: ScoreRow): number =>
  b.winScore - a.winScore || a.horseId.localeCompare(b.horseId);

const scoreRunner = (runner: ArcRunner): ScoreRow => {
  const features: number[] = runner.features.map((value) => (value === null ? Number.NaN : value));
  return {
    horseId: runner.horseId,
    umaban: runner.umaban,
    name: runner.name,
    baseScore: scoreXgboostModel({ model: MODEL, features }),
    winScore: scoreXgboostModel({ model: WIN_MODEL, features }),
  };
};

const validRunner = (runner: ArcRunner): boolean =>
  runner.horseId.length > 0 &&
  Number.isInteger(runner.umaban) &&
  runner.umaban > 0 &&
  runner.umaban <= MAX_FIELD &&
  runner.features.length === MODEL.num_feature &&
  runner.features.every((value) => value === null || Number.isFinite(value));

export const scoreArcCard = (input: ArcCard) => {
  if (
    input.raceDate <= input.trainingMaxDate ||
    input.featureNames.join(",") !== MODEL.feature_names.join(",") ||
    input.runners.length < MIN_FIELD ||
    input.runners.length > MAX_FIELD ||
    !input.runners.every(validRunner) ||
    new Set(input.runners.map((row) => row.horseId)).size !== input.runners.length ||
    new Set(input.runners.map((row) => row.umaban)).size !== input.runners.length
  ) {
    throw new Error("Invalid Arc field, feature schema, or temporal cutoff");
  }
  const scored: ScoreRow[] = input.runners.map(scoreRunner).sort(scoreOrder);
  // Only the first two can swap: every unordered Top2+ set is invariant.
  const ordered: ScoreRow[] = [
    ...scored.slice(0, WIN_GATE).sort(winOrder),
    ...scored.slice(WIN_GATE),
  ];
  return {
    raceDate: input.raceDate,
    raceName: input.raceName,
    modelVersion: input.modelVersion,
    modelSha256: input.modelSha256,
    baseModelSha256: input.baseModelSha256,
    winModelSha256: input.winModelSha256,
    policy: "top2-win-rerank",
    evaluationWasTuned: true,
    sourceSha256: input.sourceSha256,
    trainingMaxDate: input.trainingMaxDate,
    featureSnapshotAt: input.featureSnapshotAt,
    featureCutoffExclusive: input.raceDate,
    liveFeatures: false,
    predictions: ordered.map((row, index) => ({ ...row, predictedRank: index + 1 })),
  };
};

export default {
  fetch(request: Request): Response {
    if (request.method !== "GET") {
      return Response.json(
        { error: "Method not allowed" },
        { status: 405, headers: { Allow: "GET" } },
      );
    }
    const pathname: string = new URL(request.url).pathname;
    if (pathname === `${ROUTE}health`) {
      return Response.json(
        { modelVersion: card.modelVersion, modelSha256: card.modelSha256 },
        { headers: RESPONSE_HEADERS },
      );
    }
    if (pathname === `${ROUTE}evaluation`) {
      return Response.json(evaluation, { headers: RESPONSE_HEADERS });
    }
    if (pathname !== `${ROUTE}${card.raceDate}`) {
      return Response.json(
        { error: "Arc race not available" },
        { status: 404, headers: RESPONSE_HEADERS },
      );
    }
    return Response.json(scoreArcCard(card), { headers: RESPONSE_HEADERS });
  },
};
