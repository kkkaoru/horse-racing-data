// run with: bun run test
//
// Tests for the event-driven per-race rescore trigger fired right after a
// horse-weight write. Exercises parseRescoreTriggerRequest (pure raceKey ->
// {category, keibajoCode, raceBango, runYmd}) and triggerRescoreAfterWeights
// (the fire-and-forget service-binding POST to finish-position-cron).

import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

const logFetchMock = vi.fn(async () => undefined);

vi.mock("./storage", () => ({
  logFetch: logFetchMock,
  upsertNarRaceSource: vi.fn(async () => {}),
  upsertJraRaceSource: vi.fn(async () => {}),
  listRaceSourceKeibajoCodesByDate: vi.fn(async () => []),
  getRaceSource: vi.fn(async () => null),
  listSchedulableRaceSourcesByDate: vi.fn(async () => []),
  countRaceSourcesByDate: vi.fn(async () => 0),
  countJraRaceSourcesMissingRaceDateFieldsByDate: vi.fn(async () => 0),
  listJraVenueTrackConditionSchedulesByDate: vi.fn(async () => []),
  markTrackConditionQueued: vi.fn(async () => {}),
  claimTrackConditionFetch: vi.fn(async () => false),
  failTrackConditionFetch: vi.fn(async () => {}),
  completeTrackConditionFetch: vi.fn(async () => {}),
  updateLastFetch: vi.fn(async () => {}),
  markResultFetchQueued: vi.fn(async () => {}),
  claimResultFetch: vi.fn(async () => false),
  completeResultFetch: vi.fn(async () => {}),
  recordPartialResultFetch: vi.fn(async () => {}),
  failResultFetch: vi.fn(async () => {}),
  insertHorseWeightSnapshot: vi.fn(async () => {}),
  insertRaceEntrySnapshot: vi.fn(async () => 0),
  insertRaceResultSnapshot: vi.fn(async () => 0),
  runD1Retention: vi.fn(async () => ({ fetchLogsDeleted: 0, oddsSnapshotsDeleted: 0 })),
  upsertPremiumRaceLink: vi.fn(async () => {}),
  getPremiumRaceLink: vi.fn(async () => null),
  replacePremiumRaceData: vi.fn(async () => {}),
  getPremiumRacePayload: vi.fn(async () => null),
  listPremiumRaceDataFetchCandidatesByDate: vi.fn(async () => []),
  markPremiumRaceDataQueued: vi.fn(async () => {}),
  getPremiumRaceDataFetchState: vi.fn(async () => null),
  updatePremiumRaceDataFetchState: vi.fn(async () => {}),
  markPremiumPaddockQueued: vi.fn(async () => {}),
  getPremiumPaddockFetchState: vi.fn(async () => null),
  updatePremiumPaddockFetchState: vi.fn(async () => {}),
  getPremiumPaddockNotificationState: vi.fn(async () => null),
  updatePremiumPaddockNotificationState: vi.fn(async () => {}),
  claimPremiumPaddockNotificationSend: vi.fn(async () => true),
  recordPremiumPaddockNotificationEvent: vi.fn(async () => {}),
  toHorseTrends: vi.fn(() => []),
  toOddsTrendsByType: vi.fn(() => ({})),
  getLatestTrackConditionForRace: vi.fn(async () => null),
  insertJraTrackConditionSnapshot: vi.fn(async () => []),
  getSameDayVenueJockeyWins: vi.fn(async () => []),
  buildRealtimePayload: vi.fn(async () => ({}) as never),
  listRaceSourcesForSeed: vi.fn(async () => []),
  listRaceKeysByDateFromHyperdrive: vi.fn(async () => []),
  deleteDailyRaceEntriesChunk: vi.fn(async () => 0),
  deleteOddsSnapshotsChunk: vi.fn(async () => 0),
  deleteRaceRunningStylesChunk: vi.fn(async () => 0),
  listOddsSnapshotsForExport: vi.fn(async () => []),
}));

interface RescoreEnvOverrides {
  fetchImpl?: typeof fetch;
  omitBinding?: boolean;
  omitToken?: boolean;
}

const buildRescoreEnv = (overrides: RescoreEnvOverrides = {}): Env => {
  const defaultFetch: typeof fetch = async () => new Response("{}", { status: 202 });
  const binding = overrides.omitBinding
    ? undefined
    : { fetch: overrides.fetchImpl ?? defaultFetch };
  const token = overrides.omitToken ? undefined : "secret-token";
  return {
    FINISH_POSITION_CRON: binding,
    REALTIME_DB: {} as unknown as D1Database,
    TRIGGER_TOKEN: token,
  } as unknown as Env;
};

beforeEach(() => {
  logFetchMock.mockClear();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("parseRescoreTriggerRequest maps a JRA race key to category jra", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("jra:2026:0512:05:11")).toStrictEqual({
    category: "jra",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260512",
  });
});

it("parseRescoreTriggerRequest maps a NAR mainland race key to category nar", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0619:45:12")).toStrictEqual({
    category: "nar",
    keibajoCode: "45",
    raceBango: "12",
    runYmd: "20260619",
  });
});

it("parseRescoreTriggerRequest maps NAR keibajoCode 65 to category ban-ei", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0623:65:10")).toStrictEqual({
    category: "ban-ei",
    keibajoCode: "65",
    raceBango: "10",
    runYmd: "20260623",
  });
});

it("parseRescoreTriggerRequest maps NAR keibajoCode 83 to category ban-ei", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0624:83:05")).toStrictEqual({
    category: "ban-ei",
    keibajoCode: "83",
    raceBango: "05",
    runYmd: "20260624",
  });
});

it("parseRescoreTriggerRequest returns null for a malformed race key (too few parts)", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0612:55")).toBe(null);
});

it("parseRescoreTriggerRequest returns null when an unknown source prefix is used", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("xyz:2026:0612:55:01")).toBe(null);
});

it("parseRescoreTriggerRequest returns null when a race key segment is empty", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0612::01")).toBe(null);
});

it("triggerRescoreAfterWeights posts a per-race rescore body and logs ok", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ claimed: true, ok: true }), { status: 202 }),
  );
  await triggerRescoreAfterWeights(
    buildRescoreEnv({ fetchImpl: fetchStub }),
    "jra:2026:0512:05:11",
  );
  expect(fetchStub).toHaveBeenCalledTimes(1);
  const call = fetchStub.mock.calls[0]!;
  const request = call[0] as Request;
  expect(request.url).toBe("https://finish-position-cron.internal/api/internal/rescore-race");
  expect(request.method).toBe("POST");
  expect(request.headers.get("Authorization")).toBe("Bearer secret-token");
  expect(request.headers.get("Content-Type")).toBe("application/json");
  const parsedBody = (await request.json()) as Record<string, string>;
  expect(parsedBody).toStrictEqual({
    category: "jra",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260512",
  });
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "ok",
    "jra:2026:0512:05:11",
    null,
  );
});

it("triggerRescoreAfterWeights swallows a fetch reject and logs an error", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> => {
      throw new Error("boom");
    },
  );
  await triggerRescoreAfterWeights(
    buildRescoreEnv({ fetchImpl: fetchStub }),
    "jra:2026:0512:05:11",
  );
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "boom",
  );
});

it("triggerRescoreAfterWeights is a no-op when the FINISH_POSITION_CRON binding is missing", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  await triggerRescoreAfterWeights(buildRescoreEnv({ omitBinding: true }), "jra:2026:0512:05:11");
  expect(logFetchMock).not.toHaveBeenCalled();
});

it("triggerRescoreAfterWeights is a no-op when TRIGGER_TOKEN is missing", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  await triggerRescoreAfterWeights(buildRescoreEnv({ omitToken: true }), "jra:2026:0512:05:11");
  expect(logFetchMock).not.toHaveBeenCalled();
});

it("triggerRescoreAfterWeights logs an invalid race key shape error without posting", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(async () => new Response("{}", { status: 202 }));
  await triggerRescoreAfterWeights(buildRescoreEnv({ fetchImpl: fetchStub }), "bad-race-key");
  expect(fetchStub).not.toHaveBeenCalled();
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "bad-race-key",
    "invalid race key shape",
  );
});

it("triggerRescoreAfterWeights forwards a NAR race key payload when binding is wired", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ claimed: true, ok: true }), { status: 202 }),
  );
  await triggerRescoreAfterWeights(
    buildRescoreEnv({ fetchImpl: fetchStub }),
    "nar:2026:0619:45:12",
  );
  const request = fetchStub.mock.calls[0]![0] as Request;
  const body = (await request.json()) as Record<string, string>;
  expect(body).toStrictEqual({
    category: "nar",
    keibajoCode: "45",
    raceBango: "12",
    runYmd: "20260619",
  });
});

it("triggerRescoreAfterWeights forwards a ban-ei race key payload when keibajoCode is 83", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ claimed: true, ok: true }), { status: 202 }),
  );
  await triggerRescoreAfterWeights(
    buildRescoreEnv({ fetchImpl: fetchStub }),
    "nar:2026:0624:83:05",
  );
  const request = fetchStub.mock.calls[0]![0] as Request;
  const body = (await request.json()) as Record<string, string>;
  expect(body).toStrictEqual({
    category: "ban-ei",
    keibajoCode: "83",
    raceBango: "05",
    runYmd: "20260624",
  });
});
