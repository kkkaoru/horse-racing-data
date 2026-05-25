// run with: bun run test
import { expect, it, vi } from "vitest";
import type { NarRaceSource } from "./types";
import {
  addDaysToYyyymmdd,
  assertJraHorseWeightsComplete,
  buildDetailUrl,
  buildFallbackRaceRow,
  buildPremiumPaddockSignature,
  enqueueJobs,
  floorToHalfHourJstSlot,
  formatMinutesUntilRace,
  formatPremiumPaddockBulletinLine,
  formatRaceStartForDiscord,
  getCronJob,
  getCurrentOddsSlotAt,
  getJstDayStart,
  getNarOddsSaleStartForRace,
  getNarVenueLastRaceStartAtMap,
  getNarVenueMeetingKey,
  getPremiumPaddockRetryAfter,
  getPremiumPaddockRetryDelaySeconds,
  getRaceStart,
  isDue,
  isPremiumRaceDiscoveryTick,
  isPremiumRaceJob,
  isRaceFinished,
  isSlotDue,
  isThreeMinuteTick,
  isTrackConditionDue,
  latestTimestamp,
  minutesUntilRace,
  premiumRaceKeyFromRequest,
  raceKeyFromRequest,
  sameDayVenueJockeyWinsFromRequest,
  toJstSlotIso,
  truncate,
} from "./worker";
import type { Env, Job } from "./types";

const RACE: NarRaceSource = {
  babaCode: "22",
  debaUrl: "https://x.test/race",
  discoveredAt: "2026-05-12T00:00:00+09:00",
  kaisaiKai: "02",
  kaisaiNen: "2026",
  kaisaiNichime: "06",
  kaisaiTsukihi: "0512",
  keibajoCode: "55",
  lastOddsFetchAt: null,
  lastOddsQueuedAt: null,
  lastResultFetchAt: null,
  lastResultQueuedAt: null,
  lastWeightFetchAt: null,
  oddsFetchLockUntil: null,
  oddsLinks: {},
  raceBango: "01",
  raceKey: "nar:2026:0512:55:01",
  raceName: "サンプル",
  raceStartAtJst: "2026-05-12T13:00:00+09:00",
  resultCompleteAt: null,
  resultExpectedHorseCount: null,
  resultFetchLockUntil: null,
  resultSavedHorseCount: null,
  source: "nar",
  updatedAt: "2026-05-12T00:00:00+09:00",
};

it("addDaysToYyyymmdd adds positive days across month boundaries", () => {
  expect(addDaysToYyyymmdd("20260228", 1)).toBe("20260301");
});

it("addDaysToYyyymmdd subtracts days correctly", () => {
  expect(addDaysToYyyymmdd("20260501", -1)).toBe("20260430");
});

it("getCronJob returns discover-premium-race-links on Friday 04:00", () => {
  expect(getCronJob("0 4 * * 5", new Date("2026-05-08T19:00:00Z"))).toStrictEqual({
    date: "20260510",
    type: "discover-premium-race-links",
  });
});

it("getCronJob returns plan-premium-race-data-fetches on Saturday 05:00", () => {
  expect(getCronJob("0 5 * * 6", new Date("2026-05-09T20:00:00Z"))).toStrictEqual({
    date: "20260511",
    type: "plan-premium-race-data-fetches",
  });
});

it("getCronJob returns discover-urls at 00:05 JST", () => {
  expect(getCronJob("5 0 * * *", new Date("2026-05-11T15:05:00Z"))).toStrictEqual({
    date: "20260512",
    type: "discover-urls",
  });
});

it("getCronJob defaults to plan-realtime-fetches for unknown cron", () => {
  expect(getCronJob("* * * * *", new Date("2026-05-12T03:00:00Z"))).toStrictEqual({
    date: "20260512",
    type: "plan-realtime-fetches",
  });
});

it("getRaceStart returns a Date for valid race", () => {
  const start = getRaceStart(RACE);
  expect(start).toBeInstanceOf(Date);
});

it("minutesUntilRace returns positive minutes for upcoming race", () => {
  const minutes = minutesUntilRace(RACE, new Date("2026-05-12T03:00:00Z"));
  expect(minutes).toBeGreaterThan(0);
});

it("minutesUntilRace returns negative minutes for past race", () => {
  const minutes = minutesUntilRace(RACE, new Date("2026-05-13T00:00:00Z"));
  expect(minutes).toBeLessThan(0);
});

it("getNarVenueMeetingKey concatenates source, date, keibajoCode", () => {
  expect(getNarVenueMeetingKey(RACE)).toBe("nar:20260512:55");
});

it("getNarVenueLastRaceStartAtMap returns latest race per nar venue", () => {
  const map = getNarVenueLastRaceStartAtMap([
    { ...RACE, raceStartAtJst: "2026-05-12T13:00:00+09:00" },
    { ...RACE, raceBango: "12", raceStartAtJst: "2026-05-12T16:30:00+09:00" },
  ]);
  expect(map.get("nar:20260512:55")).toBe("2026-05-12T16:30:00+09:00");
});

it("getNarVenueLastRaceStartAtMap excludes jra races", () => {
  const map = getNarVenueLastRaceStartAtMap([
    { ...RACE, source: "jra" },
  ]);
  expect(map.size).toBe(0);
});

it("getCurrentOddsSlotAt returns null for jra race outside window", () => {
  const jraRace: NarRaceSource = { ...RACE, source: "jra", raceStartAtJst: "2026-05-12T13:00:00+09:00" };
  expect(getCurrentOddsSlotAt(jraRace, new Date("2026-05-12T00:00:00Z"))).toBeDefined();
});

it("isDue returns true when lastFetchedAt is null", () => {
  expect(isDue(null, 5, new Date("2026-05-12T12:00:00Z"))).toBe(true);
});

it("isDue returns true when interval has passed", () => {
  expect(
    isDue("2026-05-12T11:00:00.000Z", 5, new Date("2026-05-12T11:10:00.000Z")),
  ).toBe(true);
});

it("isDue returns false when within interval", () => {
  expect(
    isDue("2026-05-12T11:00:00.000Z", 30, new Date("2026-05-12T11:10:00.000Z")),
  ).toBe(false);
});

it("isDue returns true when lastFetchedAt is unparseable", () => {
  expect(isDue("not-a-date", 5, new Date("2026-05-12T12:00:00Z"))).toBe(true);
});

it("isSlotDue returns true when no lastActivity", () => {
  expect(isSlotDue(null, "2026-05-12T12:00:00+09:00")).toBe(true);
});

it("isSlotDue returns true when lastActivity is before slot", () => {
  expect(isSlotDue("2026-05-12T11:30:00+09:00", "2026-05-12T12:00:00+09:00")).toBe(true);
});

it("isSlotDue returns false when lastActivity is after slot", () => {
  expect(isSlotDue("2026-05-12T12:30:00+09:00", "2026-05-12T12:00:00+09:00")).toBe(false);
});

it("latestTimestamp returns null when all inputs are null", () => {
  expect(latestTimestamp(null, null)).toBeNull();
});

it("latestTimestamp returns the most recent timestamp", () => {
  expect(
    latestTimestamp(
      "2026-05-12T10:00:00.000Z",
      null,
      "2026-05-12T12:00:00.000Z",
      "2026-05-12T11:00:00.000Z",
    ),
  ).toBe("2026-05-12T12:00:00.000Z");
});

it("isThreeMinuteTick returns true at 12:00 UTC", () => {
  expect(isThreeMinuteTick(new Date("2026-05-12T12:00:00Z"))).toBe(true);
});

it("isThreeMinuteTick returns false at 12:01 UTC", () => {
  expect(isThreeMinuteTick(new Date("2026-05-12T12:01:00Z"))).toBe(false);
});

it("isPremiumRaceDiscoveryTick returns true at 20:00 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-12T11:00:00Z"))).toBe(true);
});

it("isPremiumRaceDiscoveryTick returns false at 19:00 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-12T10:00:00Z"))).toBe(false);
});

it("getJstDayStart builds a Date for the JST midnight", () => {
  const start = getJstDayStart("20260512");
  expect(start.toISOString()).toBe("2026-05-11T15:00:00.000Z");
});

it("toJstSlotIso builds ISO string with JST offset", () => {
  expect(toJstSlotIso("20260512", "1230")).toBe("2026-05-12T12:30:00+09:00");
});

it("floorToHalfHourJstSlot floors to :30 when minute >= 30", () => {
  expect(floorToHalfHourJstSlot(new Date("2026-05-12T03:45:00.000Z"))).toBe(
    "2026-05-12T12:30:00+09:00",
  );
});

it("floorToHalfHourJstSlot floors to :00 when minute < 30", () => {
  expect(floorToHalfHourJstSlot(new Date("2026-05-12T03:15:00.000Z"))).toBe(
    "2026-05-12T12:00:00+09:00",
  );
});

it("isRaceFinished returns true when race has passed", () => {
  expect(isRaceFinished(RACE, new Date("2026-05-13T00:00:00Z"))).toBe(true);
});

it("isRaceFinished returns false when race is in the future", () => {
  expect(isRaceFinished(RACE, new Date("2026-05-12T00:00:00Z"))).toBe(false);
});

it("isPremiumRaceJob returns true for discover-premium-race-links", () => {
  expect(isPremiumRaceJob({ date: "x", type: "discover-premium-race-links" })).toBe(true);
});

it("isPremiumRaceJob returns true for fetch-premium-paddock", () => {
  expect(isPremiumRaceJob({ raceKey: "x", type: "fetch-premium-paddock" })).toBe(true);
});

it("isPremiumRaceJob returns false for fetch-odds", () => {
  expect(isPremiumRaceJob({ raceKey: "x", type: "fetch-odds" })).toBe(false);
});

it("isTrackConditionDue returns { due: false, slotAt: null } when today doesn't match", () => {
  const result = isTrackConditionDue(
    {
      firstRaceStartAtJst: "2026-05-12T10:00:00+09:00",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T16:30:00+09:00",
    },
    "20260512",
    new Date("2026-05-10T03:00:00Z"),
  );
  expect(result).toStrictEqual({ due: false, slotAt: null });
});

it("isTrackConditionDue returns { due: false, slotAt: null } when after last race", () => {
  const result = isTrackConditionDue(
    {
      firstRaceStartAtJst: "2026-05-12T10:00:00+09:00",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T16:30:00+09:00",
    },
    "20260512",
    new Date("2026-05-12T08:00:00Z"),
  );
  expect(result).toStrictEqual({ due: false, slotAt: null });
});

it("isTrackConditionDue returns due slot during the racing window", () => {
  const result = isTrackConditionDue(
    {
      firstRaceStartAtJst: "2026-05-12T10:00:00+09:00",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T16:30:00+09:00",
    },
    "20260512",
    new Date("2026-05-12T03:30:00Z"),
  );
  expect(result.due).toBe(true);
  expect(result.slotAt).toBe("2026-05-12T12:30:00+09:00");
});

it("buildFallbackRaceRow returns null when babaCode is not in BABA_CODE_TO_LOCAL_KEIBAJO", () => {
  const result = buildFallbackRaceRow(
    "20260512",
    { babaCode: "ZZ", raceNumber: "01", url: "https://x.test" },
    "<html></html>",
  );
  expect(result).toBeNull();
});

it("truncate returns input when below maxLength", () => {
  expect(truncate("abc", 10)).toBe("abc");
});

it("truncate appends ellipsis when value exceeds maxLength", () => {
  expect(truncate("abcdefghij", 6)).toBe("abcde…");
});

it("truncate handles zero maxLength", () => {
  expect(truncate("abc", 0)).toBe("…");
});

it("buildDetailUrl constructs the viewer detail URL with default origin", () => {
  expect(buildDetailUrl(RACE)).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/races/2026/05/12/55/01",
  );
});

it("formatRaceStartForDiscord renders a JST formatted date string", () => {
  const formatted = formatRaceStartForDiscord("2026-05-12T13:30:00+09:00");
  expect(formatted.length).toBeGreaterThan(0);
});

it("formatMinutesUntilRace returns the remaining minutes phrase for future race", () => {
  expect(
    formatMinutesUntilRace("2026-05-12T13:00:00+09:00", new Date("2026-05-12T03:30:00Z")),
  ).toBe("発走まで残り30分");
});

it("formatMinutesUntilRace returns the imminent phrase when seconds away (rounds up to 0)", () => {
  expect(
    formatMinutesUntilRace("2026-05-12T04:00:00.000Z", new Date("2026-05-12T04:00:00.000Z")),
  ).toBe("まもなく発走");
});

it("formatMinutesUntilRace returns elapsed-minutes phrase for past race", () => {
  expect(
    formatMinutesUntilRace("2026-05-12T13:00:00+09:00", new Date("2026-05-12T04:15:00.000Z")),
  ).toBe("発走から15分経過");
});

it("formatPremiumPaddockBulletinLine includes 穴馬 label when groupKey is value", () => {
  const line = formatPremiumPaddockBulletinLine({
    bulletinType: "value",
    commentText: "良いコメント",
    evaluationText: "◎",
    frameNumber: "1",
    groupKey: "value",
    horseName: "サンプル",
    horseNumber: "1",
  });
  expect(line.includes("穴馬")).toBe(true);
});

it("formatPremiumPaddockBulletinLine includes 人気馬 label for non-value groupKey", () => {
  const line = formatPremiumPaddockBulletinLine({
    bulletinType: "popular",
    commentText: null,
    evaluationText: "◯",
    frameNumber: "2",
    groupKey: "popular",
    horseName: "サンプル",
    horseNumber: "2",
  });
  expect(line.includes("人気馬")).toBe(true);
});

it("formatPremiumPaddockBulletinLine renders コメントなし when commentText is empty", () => {
  const line = formatPremiumPaddockBulletinLine({
    bulletinType: "value",
    commentText: null,
    evaluationText: "◎",
    frameNumber: "1",
    groupKey: "value",
    horseName: "サンプル",
    horseNumber: "1",
  });
  expect(line.includes("コメントなし")).toBe(true);
});

it("buildPremiumPaddockSignature produces a 64-char hex sha-256 digest", async () => {
  const signature = await buildPremiumPaddockSignature([
    {
      bulletinType: "value",
      commentText: "コメント",
      evaluationText: "◎",
      frameNumber: "1",
      groupKey: "value",
      horseName: "サンプル",
      horseNumber: "1",
    },
  ]);
  expect(signature.length).toBe(64);
  expect(/^[0-9a-f]{64}$/u.test(signature)).toBe(true);
});

it("buildPremiumPaddockSignature is stable across reorderings of the same bulletins", async () => {
  const left = await buildPremiumPaddockSignature([
    {
      bulletinType: "value",
      commentText: "A",
      evaluationText: "◎",
      frameNumber: "1",
      groupKey: "value",
      horseName: "Aホース",
      horseNumber: "1",
    },
    {
      bulletinType: "popular",
      commentText: "B",
      evaluationText: "◯",
      frameNumber: "2",
      groupKey: "popular",
      horseName: "Bホース",
      horseNumber: "2",
    },
  ]);
  const right = await buildPremiumPaddockSignature([
    {
      bulletinType: "popular",
      commentText: "B",
      evaluationText: "◯",
      frameNumber: "2",
      groupKey: "popular",
      horseName: "Bホース",
      horseNumber: "2",
    },
    {
      bulletinType: "value",
      commentText: "A",
      evaluationText: "◎",
      frameNumber: "1",
      groupKey: "value",
      horseName: "Aホース",
      horseNumber: "1",
    },
  ]);
  expect(left).toBe(right);
});

it("getPremiumPaddockRetryDelaySeconds returns 30 seconds inside the close-to-race window", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T03:55:00Z"));
  expect(delay).toBe(30);
});

it("getPremiumPaddockRetryDelaySeconds returns the default delay outside the window", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-11T00:00:00Z"));
  expect(delay).toBe(120);
});

it("raceKeyFromRequest delegates to raceKeyFromRealtimePath", () => {
  expect(raceKeyFromRequest(new URL("https://x.test/api/jra/races/2026/05/12/08/01/realtime"))).toBe(
    "jra:2026:0512:08:01",
  );
});

it("raceKeyFromRequest returns null for non-matching paths", () => {
  expect(raceKeyFromRequest(new URL("https://x.test/api/other"))).toBeNull();
});

it("premiumRaceKeyFromRequest parses /premium API path", () => {
  expect(
    premiumRaceKeyFromRequest(new URL("https://x.test/api/jra/races/2026/05/12/08/01/premium")),
  ).toBe("jra:2026:0512:08:01");
});

it("premiumRaceKeyFromRequest returns null for non-matching path", () => {
  expect(premiumRaceKeyFromRequest(new URL("https://x.test/api/jra/races"))).toBeNull();
});

it("sameDayVenueJockeyWinsFromRequest parses jockey-wins API path", () => {
  expect(
    sameDayVenueJockeyWinsFromRequest(
      new URL("https://x.test/api/nar/races/2026/05/12/55/03/jockey-wins"),
    ),
  ).toStrictEqual({
    day: "12",
    keibajoCode: "55",
    month: "05",
    raceNumber: "03",
    year: "2026",
  });
});

it("sameDayVenueJockeyWinsFromRequest returns null for non-matching path", () => {
  expect(
    sameDayVenueJockeyWinsFromRequest(new URL("https://x.test/api/jra/races/jockey-wins")),
  ).toBeNull();
});

it("enqueueJobs returns immediately when jobs is empty", async () => {
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  await enqueueJobs(env, []);
  expect(send).not.toHaveBeenCalled();
  expect(sendBatch).not.toHaveBeenCalled();
});

it("enqueueJobs sends a single non-premium job via send", async () => {
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  const job: Job = { raceKey: "k", type: "fetch-odds" };
  await enqueueJobs(env, [job]);
  expect(send).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledWith(job);
});

it("enqueueJobs batches multiple non-premium jobs via sendBatch", async () => {
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  const jobs: Job[] = [
    { raceKey: "k1", type: "fetch-odds" },
    { raceKey: "k2", type: "fetch-odds" },
  ];
  await enqueueJobs(env, jobs);
  expect(sendBatch).toHaveBeenCalledTimes(1);
});

it("enqueueJobs routes premium jobs to PREMIUM_RACE_JOBS with incremental delays", async () => {
  const realtimeSend = vi.fn(async () => {});
  const premiumSend = vi.fn(async () => {});
  const env = {
    PREMIUM_RACE_JOBS: { send: premiumSend, sendBatch: vi.fn() },
    PREMIUM_RACE_QUEUE_DELAY_SECONDS: "10",
    REALTIME_JOBS: { send: realtimeSend, sendBatch: vi.fn() },
  } as unknown as Env;
  const jobs: Job[] = [
    { date: "20260512", type: "discover-premium-race-links" },
    { date: "20260512", type: "plan-premium-race-data-fetches" },
  ];
  await enqueueJobs(env, jobs);
  expect(premiumSend).toHaveBeenCalledTimes(2);
});

it("enqueueJobs falls back to REALTIME_JOBS when PREMIUM_RACE_JOBS unset", async () => {
  const send = vi.fn(async () => {});
  const env = {
    REALTIME_JOBS: { send, sendBatch: vi.fn() },
  } as unknown as Env;
  await enqueueJobs(env, [{ date: "20260512", type: "discover-premium-race-links" }]);
  expect(send).toHaveBeenCalledTimes(1);
});

it("assertJraHorseWeightsComplete returns silently when weights array is empty", () => {
  assertJraHorseWeightsComplete("k", [{ horseName: "h", horseNumber: "1", jockeyName: "j", status: null }], []);
});

it("assertJraHorseWeightsComplete returns silently when all active entries have weights", () => {
  assertJraHorseWeightsComplete(
    "k",
    [
      { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
      { horseName: "h2", horseNumber: "2", jockeyName: "j", status: null },
    ],
    [
      { changeAmount: 0, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 },
      { changeAmount: 0, changeSign: null, horseName: "h2", horseNumber: "2", weight: 510 },
    ],
  );
});

it("assertJraHorseWeightsComplete skips scratched entries when checking completeness", () => {
  assertJraHorseWeightsComplete(
    "k",
    [
      { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
      { horseName: "h2", horseNumber: "2", jockeyName: "j", status: "出走取消" },
    ],
    [{ changeAmount: 0, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 }],
  );
});

it("assertJraHorseWeightsComplete throws when an active entry has no weight row", () => {
  expect(() =>
    assertJraHorseWeightsComplete(
      "k",
      [
        { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
        { horseName: "h2", horseNumber: "2", jockeyName: "j", status: null },
      ],
      [{ changeAmount: 0, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 }],
    ),
  ).toThrow("JRA horse weight rows are sparse: k missing=2");
});

it("getPremiumPaddockRetryAfter returns an ISO string at now + retry delay (default)", () => {
  const env = { REALTIME_TEST_NOW: "2026-05-12T00:00:00.000Z" } as unknown as Env;
  expect(getPremiumPaddockRetryAfter(env, RACE)).toBe("2026-05-12T09:02:00+09:00");
});

it("getNarOddsSaleStartForRace returns null for JRA races", () => {
  const jraRace: NarRaceSource = { ...RACE, source: "jra" };
  expect(getNarOddsSaleStartForRace(jraRace, null)).toBeNull();
});

it("getNarOddsSaleStartForRace returns a Date for NAR races", () => {
  const result = getNarOddsSaleStartForRace(RACE, "2026-05-12T16:30:00+09:00");
  expect(result).toBeInstanceOf(Date);
});
