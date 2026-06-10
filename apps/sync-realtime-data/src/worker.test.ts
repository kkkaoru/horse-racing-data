// run with: bun run test
import { expect, it, vi } from "vitest";
import type { NarRaceSource } from "./types";
import {
  addDaysToYyyymmdd,
  assertJraHorseWeightsComplete,
  assertNarHorseWeightsComplete,
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
  horseWeightRaceKeyFromRequest,
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
  raceTrendDailyTrackQueryFromRequest,
  resolveResultFetchOutcome,
  resolveRetryLockMinutes,
  RESULT_POLL_CRON,
  sameDayVenueJockeyWinsFromRequest,
  toJstSlotIso,
  truncate,
} from "./worker";
import type { Env, Job } from "./types";

const RACE: NarRaceSource = {
  babaCode: "22",
  debaUrl: "https://x.test/race",
  kaisaiKai: "02",
  kaisaiNen: "2026",
  kaisaiNichime: "06",
  kaisaiTsukihi: "0512",
  keibajoCode: "55",
  lastOddsFetchAt: null,
  lastWeightFetchAt: null,
  oddsLinks: {},
  raceBango: "01",
  raceKey: "nar:2026:0512:55:01",
  raceName: "サンプル",
  raceStartAtJst: "2026-05-12T13:00:00+09:00",
  source: "nar",
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

it("RESULT_POLL_CRON is */2 0-13 * * *", () => {
  expect(RESULT_POLL_CRON).toBe("*/2 0-13 * * *");
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
  const map = getNarVenueLastRaceStartAtMap([{ ...RACE, source: "jra" }]);
  expect(map.size).toBe(0);
});

it("getCurrentOddsSlotAt returns null for jra race outside window", () => {
  const jraRace: NarRaceSource = {
    ...RACE,
    source: "jra",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
  };
  expect(getCurrentOddsSlotAt(jraRace, new Date("2026-05-12T00:00:00Z"))).toBeDefined();
});

it("isDue returns true when lastFetchedAt is null", () => {
  expect(isDue(null, 5, new Date("2026-05-12T12:00:00Z"))).toBe(true);
});

it("isDue returns true when interval has passed", () => {
  expect(isDue("2026-05-12T11:00:00.000Z", 5, new Date("2026-05-12T11:10:00.000Z"))).toBe(true);
});

it("isDue returns false when within interval", () => {
  expect(isDue("2026-05-12T11:00:00.000Z", 30, new Date("2026-05-12T11:10:00.000Z"))).toBe(false);
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

it("isPremiumRaceDiscoveryTick returns true at 09:00 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-12T00:00:00Z"))).toBe(true);
});

it("isPremiumRaceDiscoveryTick returns true mid-hour at 09:30 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-12T00:30:00Z"))).toBe(true);
});

it("isPremiumRaceDiscoveryTick returns true mid-hour at 20:45 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-12T11:45:00Z"))).toBe(true);
});

it("isPremiumRaceDiscoveryTick returns false at 08:00 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-11T23:00:00Z"))).toBe(false);
});

it("isPremiumRaceDiscoveryTick returns false at 19:00 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-12T10:00:00Z"))).toBe(false);
});

it("isPremiumRaceDiscoveryTick returns false at 21:00 JST", () => {
  expect(isPremiumRaceDiscoveryTick(new Date("2026-05-12T12:00:00Z"))).toBe(false);
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

it("isPremiumRaceJob returns false for fetch-weights", () => {
  expect(isPremiumRaceJob({ raceKey: "x", type: "fetch-weights" })).toBe(false);
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

it("isTrackConditionDue uses day-before 10:00 slot when today is the day before target", () => {
  const result = isTrackConditionDue(
    {
      firstRaceStartAtJst: "2026-05-13T10:00:00+09:00",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-13T16:30:00+09:00",
    },
    "20260513",
    new Date("2026-05-12T03:00:00Z"),
  );
  expect(result.slotAt).toBe("2026-05-12T10:00:00+09:00");
  expect(result.due).toBe(true);
});

it("isTrackConditionDue returns null slot when now is before 06:00 JST and no morning slot ready", () => {
  const result = isTrackConditionDue(
    {
      firstRaceStartAtJst: "2026-05-12T13:00:00+09:00",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T16:30:00+09:00",
    },
    "20260512",
    new Date("2026-05-11T20:00:00.000Z"),
  );
  expect(result).toStrictEqual({ due: false, slotAt: null });
});

it("isTrackConditionDue uses pre-race morning slot when between 09:00 and first race", () => {
  const result = isTrackConditionDue(
    {
      firstRaceStartAtJst: "2026-05-12T13:00:00+09:00",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T16:30:00+09:00",
    },
    "20260512",
    new Date("2026-05-12T00:30:00Z"),
  );
  expect(result.slotAt).toBe("2026-05-12T09:00:00+09:00");
  expect(result.due).toBe(true);
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

it("minutesUntilRace returns null when raceStart cannot be parsed", async () => {
  const { minutesUntilRace } = await import("./worker");
  expect(
    minutesUntilRace(
      {
        babaCode: "55",
        debaUrl: "u",
        kaisaiKai: null,
        kaisaiNen: "2026",
        kaisaiNichime: null,
        kaisaiTsukihi: "0512",
        keibajoCode: "55",
        lastOddsFetchAt: null,
        lastWeightFetchAt: null,
        oddsLinks: {},
        raceBango: "01",
        raceKey: "nar:2026:0512:55:01",
        raceName: null,
        raceStartAtJst: "invalid-date",
        source: "nar",
      },
      new Date("2026-05-12T05:00:00.000Z"),
    ),
  ).toBeNull();
});

it("getCurrentOddsSlotAt returns null when raceStartAtJst is malformed", async () => {
  const { getCurrentOddsSlotAt } = await import("./worker");
  expect(
    getCurrentOddsSlotAt(
      {
        babaCode: "55",
        debaUrl: "u",
        kaisaiKai: null,
        kaisaiNen: "2026",
        kaisaiNichime: null,
        kaisaiTsukihi: "0512",
        keibajoCode: "55",
        lastOddsFetchAt: null,
        lastWeightFetchAt: null,
        oddsLinks: {},
        raceBango: "01",
        raceKey: "nar:2026:0512:55:01",
        raceName: null,
        raceStartAtJst: "invalid-date",
        source: "nar",
      },
      new Date("2026-05-12T05:00:00.000Z"),
    ),
  ).toBeNull();
});

it("buildPremiumPaddockSignature handles bulletins with all-null optional fields", async () => {
  const { buildPremiumPaddockSignature } = await import("./worker");
  const sig = await buildPremiumPaddockSignature([
    {
      commentText: null,
      evaluationText: null,
      frameNumber: null,
      groupKey: "favorite",
      horseName: null,
      horseNumber: "1",
    },
  ]);
  expect(typeof sig).toBe("string");
  expect(sig.length).toBe(64);
});

it("buildFallbackRaceRow returns null when babaCode is not in BABA_CODE_TO_LOCAL_KEIBAJO", () => {
  const result = buildFallbackRaceRow(
    "20260512",
    { babaCode: "ZZ", raceNumber: "01", url: "https://x.test" },
    "<html></html>",
  );
  expect(result).toBeNull();
});

it("buildFallbackRaceRow returns null when metadata.startTime is missing", () => {
  const result = buildFallbackRaceRow(
    "20260512",
    { babaCode: "36", raceNumber: "01", url: "https://x.test" },
    "<html><body>no time</body></html>",
  );
  expect(result).toBeNull();
});

it("buildFallbackRaceRow returns a LocalRaceRow when parsing succeeds", () => {
  const html = `<html><body>
    <h4>15:30発走</h4>
    <section class="raceTitle"><h3>テストレース</h3></section>
  </body></html>`;
  const result = buildFallbackRaceRow(
    "20260512",
    { babaCode: "36", raceNumber: "5", url: "https://x.test" },
    html,
  );
  if (result === null) {
    throw new Error("buildFallbackRaceRow returned null unexpectedly");
  }
  expect(result.kaisai_nen).toBe("2026");
  expect(result.kaisai_tsukihi).toBe("0512");
  expect(result.keibajo_code).toBe("30");
  expect(result.race_bango).toBe("5");
  expect(result.hasso_jikoku).toBe("1530");
  expect(result.kyosomei_hondai).toBe("テストレース");
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
  expect(buildDetailUrl(RACE)).toBe("https://pc-keiba-viewer.kkk4oru.com/races/2026/05/12/55/01");
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
    commentText: null,
    evaluationText: "◯",
    frameNumber: "2",
    groupKey: "favorite",
    horseName: "サンプル",
    horseNumber: "2",
  });
  expect(line.includes("人気馬")).toBe(true);
});

it("formatPremiumPaddockBulletinLine substitutes '-' when horseName and evaluationText are null", () => {
  const line = formatPremiumPaddockBulletinLine({
    commentText: "短コメント",
    evaluationText: null,
    frameNumber: "1",
    groupKey: "favorite",
    horseName: null,
    horseNumber: "3",
  });
  expect(line).toBe("**3 番 -**　人気馬 / -\n> 短コメント");
});

it("formatPremiumPaddockBulletinLine renders コメントなし when commentText is empty", () => {
  const line = formatPremiumPaddockBulletinLine({
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
      commentText: "A",
      evaluationText: "◎",
      frameNumber: "1",
      groupKey: "value",
      horseName: "Aホース",
      horseNumber: "1",
    },
    {
      commentText: "B",
      evaluationText: "◯",
      frameNumber: "2",
      groupKey: "favorite",
      horseName: "Bホース",
      horseNumber: "2",
    },
  ]);
  const right = await buildPremiumPaddockSignature([
    {
      commentText: "B",
      evaluationText: "◯",
      frameNumber: "2",
      groupKey: "favorite",
      horseName: "Bホース",
      horseNumber: "2",
    },
    {
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

it("getPremiumPaddockRetryDelaySeconds returns 15 seconds in the hot zone at 5 minutes before race", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T03:55:00Z"));
  expect(delay).toBe(15);
});

it("getPremiumPaddockRetryDelaySeconds returns the default delay far outside the window", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-11T00:00:00Z"));
  expect(delay).toBe(120);
});

it("getPremiumPaddockRetryDelaySeconds returns 15 seconds at exactly 20 minutes before race (hot-zone upper bound)", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T03:40:00Z"));
  expect(delay).toBe(15);
});

it("getPremiumPaddockRetryDelaySeconds returns 15 seconds at 10 minutes before race (hot zone)", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T03:50:00Z"));
  expect(delay).toBe(15);
});

it("getPremiumPaddockRetryDelaySeconds returns 30 seconds at 30 minutes before race (warm zone)", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T03:30:00Z"));
  expect(delay).toBe(30);
});

it("getPremiumPaddockRetryDelaySeconds returns 30 seconds at exactly 40 minutes before race (warm-zone upper bound)", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T03:20:00Z"));
  expect(delay).toBe(30);
});

it("getPremiumPaddockRetryDelaySeconds returns 120 seconds at 50 minutes before race (cold zone)", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T03:10:00Z"));
  expect(delay).toBe(120);
});

it("getPremiumPaddockRetryDelaySeconds returns 120 seconds at 5 minutes after race (past grace window)", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T04:05:00Z"));
  expect(delay).toBe(120);
});

it("getPremiumPaddockRetryDelaySeconds returns 15 seconds at 1 minute after race (still within grace window)", () => {
  const delay = getPremiumPaddockRetryDelaySeconds(RACE, new Date("2026-05-12T04:01:00Z"));
  expect(delay).toBe(15);
});

it("getPremiumPaddockRetryDelaySeconds returns 120 seconds when race start cannot be parsed", () => {
  const raceWithBadStart: NarRaceSource = { ...RACE, raceStartAtJst: "2026-05-12TXX:XX:00+09:00" };
  const delay = getPremiumPaddockRetryDelaySeconds(
    raceWithBadStart,
    new Date("2026-05-12T03:40:00Z"),
  );
  expect(delay).toBe(120);
});

it("raceKeyFromRequest delegates to raceKeyFromRealtimePath", () => {
  expect(
    raceKeyFromRequest(new URL("https://x.test/api/jra/races/2026/05/12/08/01/realtime")),
  ).toBe("jra:2026:0512:08:01");
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

it("raceTrendDailyTrackQueryFromRequest parses well-formed query parameters", () => {
  expect(
    raceTrendDailyTrackQueryFromRequest(
      new URL(
        "https://x.test/internal/race-trend-daily-track?source=jra&ymd=20260531&keibajo=06&beforeRaceBango=05",
      ),
    ),
  ).toStrictEqual({
    beforeRaceBango: "05",
    keibajoCode: "06",
    source: "jra",
    targetYmd: "20260531",
  });
});

it("raceTrendDailyTrackQueryFromRequest returns null when the pathname does not match", () => {
  expect(
    raceTrendDailyTrackQueryFromRequest(
      new URL("https://x.test/api/other?source=jra&ymd=20260531&keibajo=06&beforeRaceBango=05"),
    ),
  ).toBeNull();
});

it("raceTrendDailyTrackQueryFromRequest returns null when source is missing or invalid", () => {
  expect(
    raceTrendDailyTrackQueryFromRequest(
      new URL(
        "https://x.test/internal/race-trend-daily-track?ymd=20260531&keibajo=06&beforeRaceBango=05",
      ),
    ),
  ).toBeNull();
});

it("raceTrendDailyTrackQueryFromRequest returns null when ymd is not 8 digits", () => {
  expect(
    raceTrendDailyTrackQueryFromRequest(
      new URL(
        "https://x.test/internal/race-trend-daily-track?source=nar&ymd=2026531&keibajo=06&beforeRaceBango=05",
      ),
    ),
  ).toBeNull();
});

it("raceTrendDailyTrackQueryFromRequest returns null when keibajo is malformed", () => {
  expect(
    raceTrendDailyTrackQueryFromRequest(
      new URL(
        "https://x.test/internal/race-trend-daily-track?source=jra&ymd=20260531&keibajo=ABC&beforeRaceBango=05",
      ),
    ),
  ).toBeNull();
});

it("raceTrendDailyTrackQueryFromRequest returns null when beforeRaceBango is non-numeric", () => {
  expect(
    raceTrendDailyTrackQueryFromRequest(
      new URL(
        "https://x.test/internal/race-trend-daily-track?source=jra&ymd=20260531&keibajo=06&beforeRaceBango=xx",
      ),
    ),
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
  const job: Job = { raceKey: "k", type: "fetch-weights" };
  await enqueueJobs(env, [job]);
  expect(send).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledWith(job);
});

it("enqueueJobs routes materialize-running-style-features to REALTIME_JOBS.send like plan-running-style-predictions", async () => {
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const premiumSend = vi.fn(async () => {});
  const env = {
    PREMIUM_RACE_JOBS: { send: premiumSend, sendBatch: vi.fn() },
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  const job: Job = { date: "20260602", type: "materialize-running-style-features" };
  await enqueueJobs(env, [job]);
  expect(send).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledWith(job);
  expect(premiumSend).not.toHaveBeenCalled();
});

it("enqueueJobs batches multiple non-premium jobs via sendBatch", async () => {
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  const jobs: Job[] = [
    { raceKey: "k1", type: "fetch-weights" },
    { raceKey: "k2", type: "fetch-weights" },
  ];
  await enqueueJobs(env, jobs);
  expect(sendBatch).toHaveBeenCalledTimes(1);
});

it("enqueueJobs routes non-premium jobs via REALTIME_JOBS.send within a mixed chunk", async () => {
  const realtimeSend = vi.fn(async () => {});
  const premiumSend = vi.fn(async () => {});
  const env = {
    PREMIUM_RACE_JOBS: { send: premiumSend, sendBatch: vi.fn() },
    REALTIME_JOBS: { send: realtimeSend, sendBatch: vi.fn() },
  } as unknown as Env;
  const jobs: Job[] = [
    { date: "20260512", type: "discover-premium-races" },
    { raceKey: "k", type: "fetch-weights" },
  ];
  await enqueueJobs(env, jobs);
  expect(premiumSend).toHaveBeenCalledTimes(1);
  expect(realtimeSend).toHaveBeenCalledTimes(1);
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
  assertJraHorseWeightsComplete(
    "k",
    [{ horseName: "h", horseNumber: "1", jockeyName: "j", status: null }],
    [],
  );
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

it("assertNarHorseWeightsComplete returns silently when weights array is empty", () => {
  assertNarHorseWeightsComplete(
    "k",
    [{ horseName: "h", horseNumber: "1", jockeyName: "j", status: null }],
    [],
  );
});

it("assertNarHorseWeightsComplete returns silently when all active entries have weights", () => {
  assertNarHorseWeightsComplete(
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

it("assertNarHorseWeightsComplete skips scratched entries when checking completeness", () => {
  assertNarHorseWeightsComplete(
    "k",
    [
      { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
      { horseName: "h2", horseNumber: "2", jockeyName: "j", status: "出走取消" },
    ],
    [{ changeAmount: 0, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 }],
  );
});

it("assertNarHorseWeightsComplete throws when an active entry has no weight row", () => {
  expect(() =>
    assertNarHorseWeightsComplete(
      "nar:2026:0528:30:10",
      [
        { horseName: "h6", horseNumber: "6", jockeyName: "j", status: null },
        { horseName: "h7", horseNumber: "7", jockeyName: "j", status: null },
      ],
      [{ changeAmount: 0, changeSign: null, horseName: "h6", horseNumber: "6", weight: 500 }],
    ),
  ).toThrow("NAR horse weight rows are sparse: nar:2026:0528:30:10 missing=7");
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

// resolveResultFetchOutcome truth table — pins the new 2026-06-05 routing
// (complete / retry-short / retry-medium / retry-long / give-up) so a
// regression to the old NAR_RESULT_COMPLETION_BACKSTOP_MINUTES force-complete
// path is caught immediately. Each case mirrors a row in the spec's fixed
// truth table.

it("resolveResultFetchOutcome returns complete when saved equals expected (NAR full publish)", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 11,
    minutesAfterRaceStart: 5,
    source: "nar",
  });
  expect(outcome).toBe("complete");
});

it("resolveResultFetchOutcome returns retry-short for NAR partial within first 10 minutes", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 5,
    source: "nar",
  });
  expect(outcome).toBe("retry-short");
});

it("resolveResultFetchOutcome returns retry-medium for NAR partial between 10 and 60 minutes", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 30,
    source: "nar",
  });
  expect(outcome).toBe("retry-medium");
});

it("resolveResultFetchOutcome returns retry-long for NAR partial between 60 minutes and 24 hours", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 120,
    source: "nar",
  });
  expect(outcome).toBe("retry-long");
});

it("resolveResultFetchOutcome returns give-up for NAR partial past 24 hours after race start", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 1450,
    source: "nar",
  });
  expect(outcome).toBe("give-up");
});

it("resolveResultFetchOutcome returns complete when expectedHorseCount is zero (no entries case)", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 0,
    inserted: 0,
    minutesAfterRaceStart: 10,
    source: "nar",
  });
  expect(outcome).toBe("complete");
});

it("resolveResultFetchOutcome returns complete for JRA partial (no progressive-publish retry on JRA)", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 5,
    source: "jra",
  });
  expect(outcome).toBe("complete");
});

it("resolveResultFetchOutcome returns complete for JRA full publish past the legacy backstop window", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 18,
    inserted: 18,
    minutesAfterRaceStart: 120,
    source: "jra",
  });
  expect(outcome).toBe("complete");
});

it("resolveResultFetchOutcome returns complete when minutesAfterRaceStart is null", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: null,
    source: "nar",
  });
  expect(outcome).toBe("complete");
});

// Regression guard: the legacy NAR_RESULT_COMPLETION_BACKSTOP_MINUTES (60)
// force-complete path is gone — at exactly 60 minutes after race start with
// inserted < expected the resolver must NOT return "complete" or "give-up".
// 2026-06-05 routes this to retry-long instead so the upstream still gets
// the rest of the 24h give-up window to publish the missing finishers.
it("resolveResultFetchOutcome no longer force-completes NAR partial at the legacy 60min backstop boundary", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 60,
    source: "nar",
  });
  expect(outcome).toBe("retry-long");
});

// Boundary cases: just-under and just-on each phase threshold so a future
// off-by-one regression in resolveResultFetchOutcome surfaces immediately.

it("resolveResultFetchOutcome routes NAR partial at the 10min boundary to retry-medium", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 10,
    source: "nar",
  });
  expect(outcome).toBe("retry-medium");
});

it("resolveResultFetchOutcome routes NAR partial just under 24h to retry-long", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 1439,
    source: "nar",
  });
  expect(outcome).toBe("retry-long");
});

it("resolveResultFetchOutcome routes NAR partial at exactly 24h to give-up", () => {
  const outcome = resolveResultFetchOutcome({
    expectedHorseCount: 11,
    inserted: 3,
    minutesAfterRaceStart: 1440,
    source: "nar",
  });
  expect(outcome).toBe("give-up");
});

// resolveRetryLockMinutes truth table — pins the per-phase lock duration
// so a future tweak that swaps the constants without updating the helper
// (or vice versa) is caught immediately.

it("resolveRetryLockMinutes returns 2 minutes for retry-short", () => {
  expect(resolveRetryLockMinutes("retry-short")).toBe(2);
});

it("resolveRetryLockMinutes returns 5 minutes for retry-medium", () => {
  expect(resolveRetryLockMinutes("retry-medium")).toBe(5);
});

it("resolveRetryLockMinutes returns 15 minutes for retry-long", () => {
  expect(resolveRetryLockMinutes("retry-long")).toBe(15);
});

it("resolveRetryLockMinutes throws when called with the non-retry outcome complete", () => {
  expect(() => resolveRetryLockMinutes("complete")).toThrowError(
    "resolveRetryLockMinutes called with non-retry outcome: complete",
  );
});

it("resolveRetryLockMinutes throws when called with the non-retry outcome give-up", () => {
  expect(() => resolveRetryLockMinutes("give-up")).toThrowError(
    "resolveRetryLockMinutes called with non-retry outcome: give-up",
  );
});

it("horseWeightRaceKeyFromRequest parses a percent-encoded nar race key", () => {
  expect(
    horseWeightRaceKeyFromRequest(
      new URL("https://x.test/api/horse-weight/nar%3A2026%3A0610%3A44%3A01"),
    ),
  ).toBe("nar:2026:0610:44:01");
});

it("horseWeightRaceKeyFromRequest parses a percent-encoded jra race key", () => {
  expect(
    horseWeightRaceKeyFromRequest(
      new URL("https://x.test/api/horse-weight/jra%3A2026%3A0607%3A05%3A11"),
    ),
  ).toBe("jra:2026:0607:05:11");
});

it("horseWeightRaceKeyFromRequest returns null for a non-matching path", () => {
  expect(horseWeightRaceKeyFromRequest(new URL("https://x.test/api/other"))).toBeNull();
});

it("horseWeightRaceKeyFromRequest returns null when race key has wrong source", () => {
  expect(
    horseWeightRaceKeyFromRequest(
      new URL("https://x.test/api/horse-weight/bad%3A2026%3A0610%3A44%3A01"),
    ),
  ).toBeNull();
});

it("horseWeightRaceKeyFromRequest returns null when race key has wrong format", () => {
  expect(
    horseWeightRaceKeyFromRequest(new URL("https://x.test/api/horse-weight/not-a-race-key")),
  ).toBeNull();
});
