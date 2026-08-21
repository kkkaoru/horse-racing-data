import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { safeGetCloudflareEnvMock } = vi.hoisted(() => ({
  safeGetCloudflareEnvMock: vi.fn<() => Promise<CloudflareEnv | null>>(),
}));

vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareEnv: safeGetCloudflareEnvMock,
}));

import {
  buildRaceTrainingCatalogUrl,
  fetchRaceTrainingsFromCatalog,
} from "./race-training-catalog.server";

const query = {
  day: "3",
  keibajoCode: "5",
  month: "8",
  raceBango: "1",
  year: "2026",
};

const catalogTraining = {
  babamawari: null,
  bamei: "カタログ馬",
  chokyoJikoku: "0600",
  chokyoNengappi: "20260802",
  course: "札幌ダート",
  currentJockeyName: "騎手",
  lapTime10f: null,
  lapTime1f: "123",
  lapTime2f: null,
  lapTime3f: null,
  lapTime4f: null,
  lapTime5f: null,
  lapTime6f: null,
  lapTime7f: null,
  lapTime8f: null,
  lapTime9f: null,
  premiumCommentText: "良い動き",
  premiumEvaluationGrade: "A",
  premiumEvaluationText: "好気配",
  premiumWorkoutIndex: 1,
  timeGokei10f: null,
  timeGokei2f: "247",
  timeGokei3f: "372",
  timeGokei4f: "498",
  timeGokei5f: null,
  timeGokei6f: null,
  timeGokei7f: null,
  timeGokei8f: null,
  timeGokei9f: null,
  tracenKubun: "札幌",
  trainerName: "調教師",
  trainingDataSource: "netkeiba",
  trainingRiderName: "助手",
  trainingType: "ダート",
  umaban: "5",
};

beforeEach(() => {
  safeGetCloudflareEnvMock.mockReset();
});

it("builds the race-training Catalog URL with zero-padded filters", () => {
  expect(buildRaceTrainingCatalogUrl(query).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-trainings?date=20260803&keibajoCode=05&raceBango=01",
  );
});

it("returns Catalog training rows from the service binding", async () => {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValue(Response.json({ rows: [catalogTraining] }));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchRaceTrainingsFromCatalog(query)).resolves.toStrictEqual([catalogTraining]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const request = fetchMock.mock.calls[0]?.[0];
  expect(request).toBeInstanceOf(Request);
  if (!(request instanceof Request)) throw new Error("Catalog Request expected");
  expect(request.url).toBe(buildRaceTrainingCatalogUrl(query).toString());
});

it("returns null when the Catalog binding is unavailable", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  await expect(fetchRaceTrainingsFromCatalog(query)).resolves.toBeNull();
});

it("returns null for non-success, malformed payload, or malformed rows", async () => {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("unavailable", { status: 503 }))
    .mockResolvedValueOnce(Response.json({ rows: "invalid" }))
    .mockResolvedValueOnce(
      Response.json({ rows: [{ ...catalogTraining, premiumWorkoutIndex: 1.5 }] }),
    );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchRaceTrainingsFromCatalog(query)).resolves.toBeNull();
  await expect(fetchRaceTrainingsFromCatalog(query)).resolves.toBeNull();
  await expect(fetchRaceTrainingsFromCatalog(query)).resolves.toBeNull();
});

it("returns null when Catalog fetch throws", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error("catalog timeout"));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchRaceTrainingsFromCatalog(query)).resolves.toBeNull();
});
