// Run with bun run test:coverage.
import { expect, it } from "vitest";
import worker, { scoreArcCard, type ArcCard } from "./worker";
import card from "./artifacts/card.json";

it("matches all sixteen native XGBoost scores and ordering", () => {
  const result = scoreArcCard(card);
  const native = new Map(card.runners.map((row) => [row.horseId, row.nativeScore]));
  const errors = result.predictions.map((row) =>
    Math.abs(row.baseScore - (native.get(row.horseId) ?? Number.NaN)),
  );
  expect(Math.max(...errors)).toBeLessThan(1e-7);
  expect(result.predictions.map((row) => row.umaban)).toStrictEqual([
    16, 1, 9, 10, 7, 13, 12, 15, 3, 14, 2, 8, 11, 6, 4, 5,
  ]);
  const nativeWins = new Map(card.runners.map((row) => [row.horseId, row.nativeWinScore]));
  expect(
    Math.max(
      ...result.predictions.map((row) =>
        Math.abs(row.winScore - (nativeWins.get(row.horseId) ?? Number.NaN)),
      ),
    ),
  ).toBeLessThan(1e-7);
  expect(result.liveFeatures).toBe(false);
});

it("uses horse identity for native-compatible tied-score ordering", () => {
  const result = scoreArcCard({
    ...card,
    runners: card.runners.map((row) => ({ ...row, features: Array<number | null>(18).fill(null) })),
  });
  expect(result.predictions.slice(0, 2).map((row) => row.horseId)).toStrictEqual([
    "000a024841",
    "000a024a0d",
  ]);
});

it.each<ArcCard>([
  { ...card, raceDate: "2025-10-05" },
  { ...card, featureNames: ["wrong"] },
  { ...card, runners: [] },
  { ...card, runners: [...card.runners, ...card.runners] },
  { ...card, runners: card.runners.map((row) => ({ ...row, horseId: "" })) },
  { ...card, runners: card.runners.map((row) => ({ ...row, horseId: "duplicate" })) },
  { ...card, runners: card.runners.map((row) => ({ ...row, umaban: 1 })) },
  { ...card, runners: card.runners.map((row) => ({ ...row, umaban: 0 })) },
  { ...card, runners: card.runners.map((row) => ({ ...row, umaban: 25 })) },
  { ...card, runners: card.runners.map((row) => ({ ...row, umaban: 1.5 })) },
  { ...card, runners: card.runners.map((row) => ({ ...row, features: [] })) },
  {
    ...card,
    runners: card.runners.map((row) => ({ ...row, features: Array<number>(18).fill(Infinity) })),
  },
])("rejects invalid fields, features and temporal cutoffs %#", (invalid) => {
  expect(() => scoreArcCard(invalid)).toThrow(
    "Invalid Arc field, feature schema, or temporal cutoff",
  );
});

it("generates predictions on each production route invocation", async () => {
  const response = worker.fetch(new Request("https://example.com/api/arc-prediction/2026-10-04"));
  const payload: unknown = await response.json();
  expect(response.status).toBe(200);
  expect(response.headers.get("Cache-Control")).toBe("no-store");
  expect(payload).toMatchObject({
    modelVersion: "arc-xgb-top1-v2-20261004",
    policy: "top2-win-rerank",
    evaluationWasTuned: true,
    trainingMaxDate: "2025-10-05",
    featureCutoffExclusive: "2026-10-04",
    liveFeatures: false,
  });
});

it("exposes a model health identity", async () => {
  const response = worker.fetch(new Request("https://example.com/api/arc-prediction/health"));
  expect(await response.json()).toStrictEqual({
    modelVersion: "arc-xgb-top1-v2-20261004",
    modelSha256: "f623eb7bd7c95434d8535d37b1762614c3857a330644a595d7085a1ae12b46f1",
  });
});

it("rejects unsupported races rather than applying an Arc model globally", () => {
  expect(
    worker.fetch(new Request("https://example.com/api/arc-prediction/2023-10-01")).status,
  ).toBe(404);
});

it("reports the retrospective tuning limitation", async () => {
  const response = worker.fetch(new Request("https://example.com/api/arc-prediction/evaluation"));
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({
    evaluationWasTuned: true,
    preservesBaselinePrecisionPerYear: true,
    trainingRaces: 17,
  });
});

it("does not accept writes", () => {
  expect(
    worker.fetch(
      new Request("https://example.com/api/arc-prediction/2026-10-04", { method: "POST" }),
    ).status,
  ).toBe(405);
});
