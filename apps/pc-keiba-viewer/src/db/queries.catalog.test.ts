// Run with bun. Catalog routing must not invoke PostgreSQL on absence or failure.
import { beforeEach, expect, it, vi } from "vitest";

import type { readCatalogRaceCalendar } from "../lib/race-calendar-catalog";
import type { readCatalogRaceDayList } from "../lib/race-day-list-catalog";
import type { readCatalogRaceDetail } from "../lib/race-detail-catalog";
import type { RaceListItem, RaceDetail } from "../lib/race-types";
import type { readCatalogRaceYears } from "../lib/race-years-catalog";

const mocks = vi.hoisted(() => ({
  read: vi.fn<typeof readCatalogRaceDetail>(),
  calendar: vi.fn<typeof readCatalogRaceCalendar>(),
  years: vi.fn<typeof readCatalogRaceYears>(),
  dayList: vi.fn<typeof readCatalogRaceDayList>(),
  dayListWithJockeys:
    vi.fn<(binding: unknown, kv: unknown, date: string) => Promise<RaceListItem[]>>(),
  target: vi.fn<() => "cloudflare" | "local" | "neon">(),
  db: vi.fn<() => unknown>(() => {
    throw new Error("PostgreSQL must not be used");
  }),
  fetch: vi.fn<typeof fetch>(),
  env: vi.fn<() => Promise<CloudflareEnv | undefined>>(),
  keys: vi.fn<(key: readonly unknown[]) => void>(),
}));
vi.mock("./client", () => ({ getDatabaseTarget: mocks.target, getDb: mocks.db }));
vi.mock("../lib/cloudflare-context.server", () => ({ safeGetCloudflareEnv: mocks.env }));
vi.mock("../lib/race-detail-catalog", () => ({ readCatalogRaceDetail: mocks.read }));
vi.mock("../lib/race-calendar-catalog", () => ({ readCatalogRaceCalendar: mocks.calendar }));
vi.mock("../lib/race-years-catalog", () => ({ readCatalogRaceYears: mocks.years }));
vi.mock("../lib/race-day-list-catalog", () => ({
  readCatalogRaceDayList: mocks.dayList,
  readCatalogRaceDayListWithJockeysOrStale: mocks.dayListWithJockeys,
}));
vi.mock("./query-cache", () => ({
  withDbQueryCache: async <T>(key: readonly unknown[], load: () => Promise<T>): Promise<T> => {
    mocks.keys(key);
    return await load();
  },
}));
import {
  getRaceDetail,
  getRaceSourceByRoute,
  getRaceDaySummaries,
  getRaceYears,
  getRacesByDate,
  getRacesByDateWithoutJockeyNames,
  getSameVenueRacesByDate,
} from "./queries";

const row: RaceDetail = {
  source: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0816",
  keibajoCode: "A8",
  raceBango: "04",
  kyosomeiHondai: "競走　 ",
  kyosomeiFukudai: null,
  gradeCode: null,
  kyosoShubetsuCode: null,
  kyosoKigoCode: null,
  juryoShubetsuCode: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyori: null,
  trackCode: null,
  hassoJikoku: null,
  shussoTosu: null,
  kaisaiKai: null,
  kaisaiNichime: null,
  kyosomeiKakkonai: null,
  torokuTosu: null,
  tenkoCode: null,
  babajotaiCodeShiba: null,
  babajotaiCodeDirt: null,
};
beforeEach(() => {
  vi.clearAllMocks();
  mocks.read.mockReset();
  mocks.calendar.mockReset();
  mocks.years.mockReset();
  mocks.dayList.mockReset();
  mocks.dayListWithJockeys.mockReset();
  mocks.target.mockReturnValue("cloudflare");
  mocks.db.mockImplementation(() => {
    throw new Error("PostgreSQL must not be used");
  });
  mocks.env.mockResolvedValue({ R2_RACE_DETAIL: { fetch: mocks.fetch } });
});
it.each(["local", "neon"] satisfies ("local" | "neon")[])(
  "preserves explicit %s database selection",
  async (target) => {
    mocks.target.mockReturnValue(target);
    const limit = vi.fn<() => Promise<RaceDetail[]>>().mockResolvedValue([row]);
    mocks.db.mockReturnValue({ select: () => ({ from: () => ({ where: () => ({ limit }) }) }) });
    expect((await getRaceDetail("jra", "2026", "08", "16", "A8", "04"))?.kyosomeiHondai).toBe(
      "競走　 ",
    );
    expect(limit).toHaveBeenCalledWith(1);
    expect(mocks.read).not.toHaveBeenCalled();
    expect(mocks.env).not.toHaveBeenCalled();
  },
);
it.each(["local", "neon"] satisfies ("local" | "neon")[])(
  "preserves %s calendar reads",
  async (target) => {
    mocks.target.mockReturnValue(target);
    const execute = vi.fn<() => Promise<{ rows: Record<string, string>[] }>>().mockResolvedValue({
      rows: [{ year: "2026", month: "09", day: "17", jra_count: "0", nar_count: "24" }],
    });
    mocks.db.mockReturnValue({ execute });
    await expect(getRaceDaySummaries("2026")).resolves.toStrictEqual([
      { year: "2026", month: "09", day: "17", jraCount: 0, narCount: 24 },
    ]);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(mocks.calendar).not.toHaveBeenCalled();
    expect(mocks.env).not.toHaveBeenCalled();
  },
);
it.each(["local", "neon"] satisfies ("local" | "neon")[])(
  "preserves %s year-summary reads",
  async (target) => {
    mocks.target.mockReturnValue(target);
    const execute = vi
      .fn<() => Promise<{ rows: Record<string, string>[] }>>()
      .mockResolvedValue({ rows: [{ year: "2026", race_count: "500", day_count: "260" }] });
    mocks.db.mockReturnValue({ execute });
    await expect(getRaceYears()).resolves.toStrictEqual([
      { year: "2026", raceCount: 500, dayCount: 260 },
    ]);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(mocks.keys).toHaveBeenCalledWith(["getRaceYears", "postgres-v1"]);
    expect(mocks.years).not.toHaveBeenCalled();
    expect(mocks.env).not.toHaveBeenCalled();
  },
);
it.each(["local", "neon"] satisfies ("local" | "neon")[])(
  "preserves %s day-list and venue reads",
  async (target) => {
    mocks.target.mockReturnValue(target);
    const execute = vi
      .fn<() => Promise<{ rows: RaceDetail[] }>>()
      .mockResolvedValue({ rows: [row] });
    mocks.db.mockReturnValue({ execute });
    expect(
      (await getRacesByDateWithoutJockeyNames("2026", "08", "16")).map((race) => race.raceBango),
    ).toStrictEqual(["04"]);
    expect((await getRacesByDate("2026", "08", "16")).map((race) => race.raceBango)).toStrictEqual([
      "04",
    ]);
    expect(
      (await getSameVenueRacesByDate("jra", "2026", "08", "16", "A8")).map(
        (race) => race.raceBango,
      ),
    ).toStrictEqual(["04"]);
    expect(
      (await getSameVenueRacesByDate("nar", "2026", "08", "16", "36")).map(
        (race) => race.raceBango,
      ),
    ).toStrictEqual(["04"]);
    expect(execute).toHaveBeenCalledTimes(4);
    expect(mocks.dayList).not.toHaveBeenCalled();
    expect(mocks.env).not.toHaveBeenCalled();
    expect(mocks.keys).toHaveBeenCalledWith([
      "getRacesByDateWithoutJockeyNames",
      "postgres-v1",
      "2026",
      "08",
      "16",
    ]);
    expect(mocks.keys).toHaveBeenCalledWith(["getRacesByDate", "postgres-v1", "2026", "08", "16"]);
    expect(mocks.keys).toHaveBeenCalledWith([
      "getSameVenueRacesByDate",
      "postgres-v1",
      "jra",
      "2026",
      "08",
      "16",
      "A8",
    ]);
  },
);
it("routes complete jockey day lists through Catalog with separate cache keys", async () => {
  mocks.dayListWithJockeys.mockResolvedValue([{ ...row, jockeyNames: ["池添謙一"] }]);
  expect((await getRacesByDate("2026", "08", "16")).map((race) => race.raceBango)).toStrictEqual([
    "04",
  ]);
  expect(mocks.dayListWithJockeys).toHaveBeenCalledWith(
    { fetch: mocks.fetch },
    undefined,
    "20260816",
  );
  expect(mocks.keys).toHaveBeenCalledWith(["getRacesByDate", "catalog-v1", "2026", "08", "16"]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("propagates provider failures for jockey day lists without fallback", async () => {
  mocks.dayListWithJockeys.mockRejectedValue(new Error("Catalog race day list unavailable"));
  await expect(getRacesByDate("2026", "08", "16")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(mocks.db).not.toHaveBeenCalled();
});
it("routes complete no-jockey day lists through Catalog with separate cache keys", async () => {
  mocks.dayList.mockResolvedValue([{ ...row, jockeyNames: [] }]);
  expect(
    (await getRacesByDateWithoutJockeyNames("2026", "08", "16")).map((race) => race.raceBango),
  ).toStrictEqual(["04"]);
  expect(mocks.dayList).toHaveBeenCalledWith({ fetch: mocks.fetch }, "20260816");
  expect(mocks.keys).toHaveBeenCalledWith([
    "getRacesByDateWithoutJockeyNames",
    "catalog-v1",
    "2026",
    "08",
    "16",
  ]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("filters source and venue and restores race-number navigation order", async () => {
  mocks.dayList.mockResolvedValue([
    { ...row, raceBango: "04", hassoJikoku: "0900" },
    { ...row, source: "nar", raceBango: "03", hassoJikoku: "1000" },
    { ...row, keibajoCode: "05", raceBango: "02", hassoJikoku: "1100" },
    { ...row, raceBango: "01", hassoJikoku: "1200" },
  ]);
  expect(
    (await getSameVenueRacesByDate("jra", "2026", "08", "16", "A8")).map((race) => race.raceBango),
  ).toStrictEqual(["01", "04"]);
  expect(mocks.dayList).toHaveBeenCalledWith({ fetch: mocks.fetch }, "20260816");
  expect(mocks.keys).toHaveBeenCalledWith([
    "getSameVenueRacesByDate",
    "catalog-v1",
    "jra",
    "2026",
    "08",
    "16",
    "A8",
  ]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("preserves empty day and venue results without database fallback", async () => {
  mocks.dayList.mockResolvedValue([]);
  expect(await getRacesByDateWithoutJockeyNames("2026", "08", "16")).toStrictEqual([]);
  expect(await getSameVenueRacesByDate("jra", "2026", "08", "16", "A8")).toStrictEqual([]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("propagates provider failures for both callers without fallback", async () => {
  mocks.dayList.mockRejectedValue(new Error("Catalog race day list unavailable"));
  await expect(getRacesByDateWithoutJockeyNames("2026", "08", "16")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  await expect(getSameVenueRacesByDate("jra", "2026", "08", "16", "A8")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(mocks.dayList).toHaveBeenCalledTimes(2);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("propagates missing binding failures for both callers", async () => {
  mocks.env.mockResolvedValue(undefined);
  mocks.dayList.mockRejectedValue(new Error("Catalog race day list unavailable"));
  await expect(getRacesByDateWithoutJockeyNames("2026", "08", "16")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  await expect(getSameVenueRacesByDate("nar", "2026", "08", "16", "36")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(mocks.dayList).toHaveBeenCalledWith(undefined, "20260816");
  expect(mocks.db).not.toHaveBeenCalled();
});
it("routes Cloudflare year summaries through Catalog and isolates the legacy cache", async () => {
  mocks.years.mockResolvedValue([{ year: "2026", raceCount: 500, dayCount: 260 }]);
  await expect(getRaceYears()).resolves.toStrictEqual([
    { year: "2026", raceCount: 500, dayCount: 260 },
  ]);
  expect(mocks.years).toHaveBeenCalledWith({ fetch: mocks.fetch });
  expect(mocks.keys).toHaveBeenCalledWith(["getRaceYears", "catalog-v1"]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("does not fall back on an empty Catalog year list", async () => {
  mocks.years.mockResolvedValue([]);
  await expect(getRaceYears()).resolves.toStrictEqual([]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("does not fall back when Catalog years fail", async () => {
  mocks.years.mockRejectedValue(new Error("Catalog race years unavailable"));
  await expect(getRaceYears()).rejects.toThrow("Catalog race years unavailable");
  expect(mocks.years).toHaveBeenCalledTimes(1);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("propagates missing year binding failure without a PostgreSQL fallback", async () => {
  mocks.env.mockResolvedValue(undefined);
  mocks.years.mockRejectedValue(new Error("Catalog race years unavailable"));
  await expect(getRaceYears()).rejects.toThrow("Catalog race years unavailable");
  expect(mocks.years).toHaveBeenCalledWith(undefined);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("uses Catalog for the Cloudflare calendar with an isolated cache namespace", async () => {
  mocks.calendar.mockResolvedValue([
    { year: "2026", month: "09", day: "17", jraCount: 0, narCount: 24 },
  ]);
  await expect(getRaceDaySummaries("2026")).resolves.toStrictEqual([
    { year: "2026", month: "09", day: "17", jraCount: 0, narCount: 24 },
  ]);
  expect(mocks.calendar).toHaveBeenCalledWith({ fetch: mocks.fetch }, "2026");
  expect(mocks.keys).toHaveBeenCalledWith(["getRaceDaySummaries", "catalog-v1", "2026"]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("does not fall back when the Catalog calendar is empty", async () => {
  mocks.calendar.mockResolvedValue([]);
  await expect(getRaceDaySummaries("2026")).resolves.toStrictEqual([]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("propagates a calendar failure without PostgreSQL fallback", async () => {
  mocks.calendar.mockRejectedValue(new Error("Catalog race calendar unavailable"));
  await expect(getRaceDaySummaries("2026")).rejects.toThrow("Catalog race calendar unavailable");
  expect(mocks.calendar).toHaveBeenCalledTimes(1);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("propagates absent calendar binding failure without PostgreSQL fallback", async () => {
  mocks.env.mockResolvedValue(undefined);
  mocks.calendar.mockRejectedValue(new Error("Catalog race calendar unavailable"));
  await expect(getRaceDaySummaries("2026")).rejects.toThrow("Catalog race calendar unavailable");
  expect(mocks.calendar).toHaveBeenCalledWith(undefined, "2026");
  expect(mocks.db).not.toHaveBeenCalled();
});
it("preserves local missing-race behavior", async () => {
  mocks.target.mockReturnValue("local");
  const limit = vi.fn<() => Promise<RaceDetail[]>>().mockResolvedValue([]);
  mocks.db.mockReturnValue({ select: () => ({ from: () => ({ where: () => ({ limit }) }) }) });
  await expect(getRaceDetail("jra", "2026", "08", "16", "A8", "04")).resolves.toBe(null);
  expect(mocks.read).not.toHaveBeenCalled();
});
it("routes race detail through the separate binding and a new cache namespace", async () => {
  mocks.read.mockResolvedValue(row);
  expect((await getRaceDetail("jra", "2026", "08", "16", "A8", "04"))?.kyosomeiHondai).toBe(
    "競走　 ",
  );
  expect(mocks.read).toHaveBeenCalledWith(
    { fetch: mocks.fetch },
    { source: "jra", date: "20260816", keibajoCode: "A8", raceBango: "04" },
  );
  expect(mocks.keys).toHaveBeenCalledWith([
    "getRaceDetail",
    "catalog-v1",
    "jra",
    "2026",
    "08",
    "16",
    "A8",
    "04",
  ]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("does not fall back to PostgreSQL for authoritative absence", async () => {
  mocks.read.mockResolvedValue(null);
  await expect(getRaceDetail("jra", "2026", "08", "16", "A8", "04")).resolves.toBe(null);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("propagates unavailable binding failure without PostgreSQL fallback", async () => {
  mocks.env.mockResolvedValue(undefined);
  mocks.read.mockRejectedValue(new Error("Catalog race detail unavailable"));
  await expect(getRaceDetail("jra", "2026", "08", "16", "A8", "04")).rejects.toThrow(
    "Catalog race detail unavailable",
  );
  expect(mocks.read).toHaveBeenCalledWith(undefined, {
    source: "jra",
    date: "20260816",
    keibajoCode: "A8",
    raceBango: "04",
  });
  expect(mocks.db).not.toHaveBeenCalled();
});
it("resolves JRA source from Catalog without the old PostgreSQL source cache", async () => {
  mocks.read.mockResolvedValue(row);
  await expect(getRaceSourceByRoute("2026", "08", "16", "A8", "04")).resolves.toBe("jra");
  expect(mocks.read).toHaveBeenCalledTimes(1);
  expect(mocks.keys).toHaveBeenCalledWith([
    "getRaceSourceByRoute",
    "catalog-v1",
    "2026",
    "08",
    "16",
    "A8",
    "04",
  ]);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("checks the secondary Catalog source only after authoritative absence", async () => {
  mocks.read.mockResolvedValueOnce(null).mockResolvedValueOnce({ ...row, source: "nar" });
  await expect(getRaceSourceByRoute("2026", "08", "16", "A8", "04")).resolves.toBe("nar");
  expect(mocks.read).toHaveBeenCalledTimes(2);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("returns no source only when both Catalog sources are absent", async () => {
  mocks.read.mockResolvedValue(null);
  await expect(getRaceSourceByRoute("2026", "08", "16", "A8", "04")).resolves.toBe(null);
  expect(mocks.read).toHaveBeenCalledTimes(2);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("does not try another source after a Catalog failure", async () => {
  mocks.read.mockRejectedValue(new Error("Catalog race detail unavailable"));
  await expect(getRaceSourceByRoute("2026", "08", "16", "A8", "04")).rejects.toThrow(
    "Catalog race detail unavailable",
  );
  expect(mocks.read).toHaveBeenCalledTimes(1);
  expect(mocks.db).not.toHaveBeenCalled();
});
it("retains NAR-first lookup for NAR venues", async () => {
  mocks.read.mockResolvedValue({ ...row, source: "nar", keibajoCode: "30" });
  await expect(getRaceSourceByRoute("2026", "08", "16", "30", "04")).resolves.toBe("nar");
  expect(mocks.read).toHaveBeenCalledWith(
    { fetch: mocks.fetch },
    { source: "nar", date: "20260816", keibajoCode: "30", raceBango: "04" },
  );
  expect(mocks.db).not.toHaveBeenCalled();
});
