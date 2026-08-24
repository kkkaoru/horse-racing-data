// run with: bun run test
//
// Tests for the event-driven per-race rescore trigger fired right after a
// horse-weight write. Exercises parseRescoreTriggerRequest (pure raceKey ->
// {category, keibajoCode, raceBango, runYmd}) and triggerRescoreAfterWeights
// (the fail-closed service-binding POST to finish-position-cron).

import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

const logFetchMock = vi.fn(async () => undefined);
const WEIGHT_GENERATION = {
  weightSnapshotCount: 2,
  weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
  weightSnapshotHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
};

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
  claimResultCacheBust: vi.fn(async () => null),
  claimWeightFetch: vi.fn(async () => true),
  completeResultFetch: vi.fn(async () => {}),
  completeResultCacheBust: vi.fn(async () => {}),
  recordPartialResultFetch: vi.fn(async () => {}),
  failResultFetch: vi.fn(async () => {}),
  incrementEmptyResultAttempts: vi.fn(async () => 0),
  markEmptyResultGiveUp: vi.fn(async () => {}),
  resetEmptyResultAttempts: vi.fn(async () => {}),
  insertHorseWeightSnapshot: vi.fn(async () => {}),
  insertRaceEntrySnapshot: vi.fn(async () => 0),
  insertRaceResultSnapshot: vi.fn(async () => 0),
  listPendingResultCacheBustRaceKeys: vi.fn(async () => []),
  registerResultCacheBust: vi.fn(async () => {}),
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

it("buildWeightSnapshotGeneration canonicalizes horse order into one stable generation", async () => {
  const { buildWeightSnapshotGeneration } = await import("./worker");
  await expect(
    buildWeightSnapshotGeneration("2026-05-12T11:00:00+09:00", [
      {
        changeAmount: null,
        changeSign: null,
        horseName: null,
        horseNumber: "2",
        weight: 490,
      },
      {
        changeAmount: null,
        changeSign: null,
        horseName: null,
        horseNumber: "1",
        weight: 480,
      },
    ]),
  ).resolves.toStrictEqual({
    weightSnapshotCount: 2,
    weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
    weightSnapshotHash: "c74635426c9344e2fc1681f2e5c0e06a1f71d61d2dfa7d72324b24ad32c5c73b",
  });
});

it("buildEntrySnapshotGeneration binds active and excluded NAR runners canonically", async () => {
  const { buildEntrySnapshotGeneration } = await import("./worker");
  await expect(
    buildEntrySnapshotGeneration({
      entries: [
        { horseName: "h3", horseNumber: "3", jockeyName: "j3", status: null },
        { horseName: "h2", horseNumber: "2", jockeyName: "j2", status: "出走取消" },
        { horseName: "h1", horseNumber: "1", jockeyName: "j1", status: null },
      ],
      fetchedAt: "2026-08-24T12:03:41+09:00",
      source: "nar",
    }),
  ).resolves.toStrictEqual({
    activeHorseNumbers: [1, 3],
    entrySnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
    entrySnapshotHash: "00574dee8f89ae6c93ded1fc8187aa58e1a9d460db315e00c1d782296997e5d7",
    excludedHorseNumbers: [2],
  });
});

it("buildEntrySnapshotGeneration keeps a JRA jockey change active and excludes a scratch", async () => {
  const { buildEntrySnapshotGeneration } = await import("./worker");
  await expect(
    buildEntrySnapshotGeneration({
      entries: [
        { horseName: "h1", horseNumber: "1", jockeyName: "j1", status: "騎手変更" },
        { horseName: "h2", horseNumber: "2", jockeyName: "j2", status: "競走除外" },
        { horseName: "h2", horseNumber: "2", jockeyName: "j2", status: null },
      ],
      fetchedAt: "2026-08-24T12:03:41+09:00",
      source: "jra",
    }),
  ).resolves.toMatchObject({ activeHorseNumbers: [1], excludedHorseNumbers: [2] });
});

it("buildEntrySnapshotGeneration rejects an invalid horse number", async () => {
  const { buildEntrySnapshotGeneration } = await import("./worker");
  await expect(
    buildEntrySnapshotGeneration({
      entries: [{ horseName: "h", horseNumber: "0", jockeyName: "j", status: null }],
      fetchedAt: "2026-08-24T12:03:41+09:00",
      source: "nar",
    }),
  ).rejects.toThrow("invalid entry snapshot horse number: 0");
});

it("pickWeightGeneration accepts a canonical runner-bound generation", async () => {
  const { pickWeightGeneration } = await import("./worker");
  expect(
    pickWeightGeneration({
      activeHorseNumbers: [1, 3],
      entrySnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
      entrySnapshotHash: "entry-hash",
      excludedHorseNumbers: [2],
      weightSnapshotCount: 2,
      weightSnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
      weightSnapshotHash: "weight-hash",
    }),
  ).toStrictEqual({
    activeHorseNumbers: [1, 3],
    entrySnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
    entrySnapshotHash: "entry-hash",
    excludedHorseNumbers: [2],
    weightSnapshotCount: 2,
    weightSnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
    weightSnapshotHash: "weight-hash",
  });
});

it("pickWeightGeneration keeps a legacy weight-only generation compatible", async () => {
  const { pickWeightGeneration } = await import("./worker");
  expect(pickWeightGeneration(WEIGHT_GENERATION)).toStrictEqual(WEIGHT_GENERATION);
});

it("pickWeightGeneration rejects malformed weight metadata", async () => {
  const { pickWeightGeneration } = await import("./worker");
  expect(pickWeightGeneration(null)).toBe(null);
  expect(pickWeightGeneration({ weightSnapshotCount: 0 })).toBe(null);
  expect(
    pickWeightGeneration({
      weightSnapshotCount: 1,
      weightSnapshotFetchedAt: 1,
      weightSnapshotHash: "hash",
    }),
  ).toBe(null);
  expect(
    pickWeightGeneration({
      weightSnapshotCount: 1,
      weightSnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
      weightSnapshotHash: 1,
    }),
  ).toBe(null);
});

it("pickWeightGeneration rejects partial and noncanonical runner metadata", async () => {
  const { pickWeightGeneration } = await import("./worker");
  expect(pickWeightGeneration({ ...WEIGHT_GENERATION, activeHorseNumbers: [] })).toBe(null);
  expect(
    pickWeightGeneration({
      ...WEIGHT_GENERATION,
      activeHorseNumbers: [2, 1],
      entrySnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
      entrySnapshotHash: "hash",
      excludedHorseNumbers: [],
    }),
  ).toBe(null);
  expect(
    pickWeightGeneration({
      ...WEIGHT_GENERATION,
      activeHorseNumbers: [1],
      entrySnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
      entrySnapshotHash: "hash",
      excludedHorseNumbers: "2",
    }),
  ).toBe(null);
});

it("pickWeightGeneration rejects missing entry metadata and overlapping runner sets", async () => {
  const { pickWeightGeneration } = await import("./worker");
  expect(
    pickWeightGeneration({
      ...WEIGHT_GENERATION,
      activeHorseNumbers: [1],
      entrySnapshotHash: "hash",
      excludedHorseNumbers: [],
    }),
  ).toBe(null);
  expect(
    pickWeightGeneration({
      ...WEIGHT_GENERATION,
      activeHorseNumbers: [1],
      entrySnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
      excludedHorseNumbers: [],
    }),
  ).toBe(null);
  expect(
    pickWeightGeneration({
      ...WEIGHT_GENERATION,
      activeHorseNumbers: [1],
      entrySnapshotFetchedAt: "2026-08-24T12:03:41+09:00",
      entrySnapshotHash: "hash",
      excludedHorseNumbers: [1],
    }),
  ).toBe(null);
});

it("parseRescoreTriggerRequest maps a JRA race key to category jra", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("jra:2026:0512:05:11", WEIGHT_GENERATION)).toStrictEqual({
    category: "jra",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260512",
    ...WEIGHT_GENERATION,
  });
});

it("parseRescoreTriggerRequest maps a NAR mainland race key to category nar", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0619:45:12", WEIGHT_GENERATION)).toStrictEqual({
    category: "nar",
    keibajoCode: "45",
    raceBango: "12",
    runYmd: "20260619",
    ...WEIGHT_GENERATION,
  });
});

it("parseRescoreTriggerRequest maps NAR keibajoCode 65 to category ban-ei", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0623:65:10", WEIGHT_GENERATION)).toStrictEqual({
    category: "ban-ei",
    keibajoCode: "65",
    raceBango: "10",
    runYmd: "20260623",
    ...WEIGHT_GENERATION,
  });
});

it("parseRescoreTriggerRequest maps NAR keibajoCode 83 to category ban-ei", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0624:83:05", WEIGHT_GENERATION)).toStrictEqual({
    category: "ban-ei",
    keibajoCode: "83",
    raceBango: "05",
    runYmd: "20260624",
    ...WEIGHT_GENERATION,
  });
});

it("parseRescoreTriggerRequest returns null for a malformed race key (too few parts)", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0612:55", WEIGHT_GENERATION)).toBe(null);
});

it("parseRescoreTriggerRequest returns null when an unknown source prefix is used", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("xyz:2026:0612:55:01", WEIGHT_GENERATION)).toBe(null);
});

it("parseRescoreTriggerRequest returns null when a race key segment is empty", async () => {
  const { parseRescoreTriggerRequest } = await import("./worker");
  expect(parseRescoreTriggerRequest("nar:2026:0612::01", WEIGHT_GENERATION)).toBe(null);
});

it("triggerRescoreAfterWeights posts a per-race rescore body and logs ok", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ claimed: true, ok: true }), { status: 202 }),
  );
  await triggerRescoreAfterWeights({
    env: buildRescoreEnv({ fetchImpl: fetchStub }),
    generation: WEIGHT_GENERATION,
    raceKey: "jra:2026:0512:05:11",
    raceStartAtJst: "2026-05-12T15:40:00+09:00",
  });
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
    raceStartAtJst: "2026-05-12T15:40:00+09:00",
    runYmd: "20260512",
    ...WEIGHT_GENERATION,
  });
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "ok",
    "jra:2026:0512:05:11",
    null,
  );
});

it("triggerRescoreAfterWeights rethrows a fetch reject after logging an error", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> => {
      throw new Error("boom");
    },
  );
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ fetchImpl: fetchStub }),
      generation: WEIGHT_GENERATION,
      raceKey: "jra:2026:0512:05:11",
    }),
  ).rejects.toThrow("weight rescore trigger failed: boom");
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "boom",
  );
});

it("triggerRescoreAfterWeights fails closed when the FINISH_POSITION_CRON binding is missing", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ omitBinding: true }),
      generation: WEIGHT_GENERATION,
      raceKey: "jra:2026:0512:05:11",
    }),
  ).rejects.toThrow("weight rescore trigger failed: missing FINISH_POSITION_CRON binding");
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "missing FINISH_POSITION_CRON binding",
  );
});

it("triggerRescoreAfterWeights fails closed when TRIGGER_TOKEN is missing", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ omitToken: true }),
      generation: WEIGHT_GENERATION,
      raceKey: "jra:2026:0512:05:11",
    }),
  ).rejects.toThrow("weight rescore trigger failed: missing TRIGGER_TOKEN");
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "missing TRIGGER_TOKEN",
  );
});

it("triggerRescoreAfterWeights logs an invalid race key shape error without posting", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(async () => new Response("{}", { status: 202 }));
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ fetchImpl: fetchStub }),
      generation: WEIGHT_GENERATION,
      raceKey: "bad-race-key",
    }),
  ).rejects.toThrow("weight rescore trigger failed: invalid race key shape");
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
  await triggerRescoreAfterWeights({
    env: buildRescoreEnv({ fetchImpl: fetchStub }),
    generation: WEIGHT_GENERATION,
    raceKey: "nar:2026:0619:45:12",
  });
  const request = fetchStub.mock.calls[0]![0] as Request;
  const body = (await request.json()) as Record<string, string>;
  expect(body).toStrictEqual({
    category: "nar",
    keibajoCode: "45",
    raceBango: "12",
    runYmd: "20260619",
    ...WEIGHT_GENERATION,
  });
});

it("triggerRescoreAfterWeights fails when the cron reports rescoreEnabled false", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ claimed: false, ok: true, rescoreEnabled: false }), {
        status: 200,
      }),
  );
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ fetchImpl: fetchStub }),
      generation: WEIGHT_GENERATION,
      raceKey: "jra:2026:0512:05:11",
    }),
  ).rejects.toThrow("weight rescore trigger failed: rescore disabled");
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "rescore disabled",
  );
});

it("triggerRescoreAfterWeights logs skip:not-claimed on a claim collision", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ claimed: false, ok: true }), { status: 200 }),
  );
  await triggerRescoreAfterWeights({
    env: buildRescoreEnv({ fetchImpl: fetchStub }),
    generation: WEIGHT_GENERATION,
    raceKey: "jra:2026:0512:05:11",
  });
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "skip:not-claimed",
    "jra:2026:0512:05:11",
    null,
  );
});

it("triggerRescoreAfterWeights logs an error with the status code when the cron rejects the token", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ error: "unauthorized", ok: false }), { status: 401 }),
  );
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ fetchImpl: fetchStub }),
      generation: WEIGHT_GENERATION,
      raceKey: "jra:2026:0512:05:11",
    }),
  ).rejects.toThrow("weight rescore trigger failed: http 401");
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "http 401",
  );
});

it("triggerRescoreAfterWeights logs an error when the cron response body is not an object", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response("null", { status: 200 }),
  );
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ fetchImpl: fetchStub }),
      generation: WEIGHT_GENERATION,
      raceKey: "jra:2026:0512:05:11",
    }),
  ).rejects.toThrow("weight rescore trigger failed: unparsable response body");
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "unparsable response body",
  );
});

it("triggerRescoreAfterWeights logs an error when the cron response JSON cannot be parsed", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response("<html>bad gateway</html>", { status: 200 }),
  );
  await expect(
    triggerRescoreAfterWeights({
      env: buildRescoreEnv({ fetchImpl: fetchStub }),
      generation: WEIGHT_GENERATION,
      raceKey: "jra:2026:0512:05:11",
    }),
  ).rejects.toThrow("weight rescore trigger failed: unparsable response body");
  expect(logFetchMock).toHaveBeenCalledWith(
    expect.anything(),
    "weight-rescore-trigger",
    "error",
    "jra:2026:0512:05:11",
    "unparsable response body",
  );
});

it("triggerRescoreAfterWeights forwards a ban-ei race key payload when keibajoCode is 83", async () => {
  const { triggerRescoreAfterWeights } = await import("./worker");
  const fetchStub = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ claimed: true, ok: true }), { status: 202 }),
  );
  await triggerRescoreAfterWeights({
    env: buildRescoreEnv({ fetchImpl: fetchStub }),
    generation: WEIGHT_GENERATION,
    raceKey: "nar:2026:0624:83:05",
  });
  const request = fetchStub.mock.calls[0]![0] as Request;
  const body = (await request.json()) as Record<string, string>;
  expect(body).toStrictEqual({
    category: "ban-ei",
    keibajoCode: "83",
    raceBango: "05",
    runYmd: "20260624",
    ...WEIGHT_GENERATION,
  });
});
