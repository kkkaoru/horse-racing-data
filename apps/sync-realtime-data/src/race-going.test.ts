// run with: bun run test
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("./keiba-go", () => ({ fetchRacePage: vi.fn() }));
vi.mock("./storage", () => ({ getRaceSource: vi.fn(), logFetch: vi.fn(async () => {}) }));

const { fetchRacePage } = await import("./keiba-go");
const { getRaceSource, logFetch } = await import("./storage");
const {
  listRaceGoings,
  parseJraRaceGoing,
  parseNarRaceGoing,
  raceGoingRaceKeyFromRequest,
  readRaceGoing,
  upsertRaceGoings,
} = await import("./race-going");

// Header excerpts saved verbatim from the live pages on 2026-10-25 / 2026-10-24.
const JRA_KYOTO_11R_TURF =
  '<div class="cell baba"><ul><li class="weather"><span class="inner"><span class="cap">天候</span><span class="txt">曇</span></span></li><li class="turf"><span class="inner"><span class="cap">芝</span><span class="txt">稍重</span></span></li></ul></div>';
const JRA_KYOTO_1R_DIRT =
  '<div class="cell baba"><ul><li class="weather"><span class="inner"><span class="cap">天候</span><span class="txt">曇</span></span></li><li class="durt"><span class="inner"><span class="cap">ダート</span><span class="txt">重</span></span></li></ul></div>';
const JRA_TOKYO_4R_TURF_TO_DIRT =
  '<div class="cell baba"><ul><li class="weather"><span class="inner"><span class="cap">天候</span><span class="txt">曇</span></span></li><li class="turf"><span class="inner"><span class="cap">芝</span><span class="txt">稍重</span></span></li><li class="durt"><span class="inner"><span class="cap">ダート</span><span class="txt">稍重</span></span></li></ul></div>';
const NAR_KOCHI_1R_RESULTED =
  '<ul class="dataArea">\n            <li>ダート　1400ｍ（右）　　天候：曇　　馬場：不良　　サラブレッド系　一般 定量　　＊電話投票コード：55#            </li>';
const NAR_KANAZAWA_1R_PRE_ANNOUNCE =
  '<ul class="dataArea">\n            <li>ダート　1400ｍ（右）　　サラブレッド系　一般 定量　　＊電話投票コード：41#            </li>';
const NAR_OBIHIRO_1R_MOISTURE =
  '<ul class="dataArea">\n            <li>ダート　200ｍ（直）　　天候：曇　　馬場：2.2　　混合　3歳以上 規定　　＊電話投票コード：03#            </li>';

const buildDb = (rows: unknown[]) => {
  const bind = vi.fn(() => ({ all: vi.fn(async () => ({ results: rows })) }));
  const prepare = vi.fn(() => ({ bind }));
  const batch = vi.fn(async () => []);
  return { batch, bind, db: { batch, prepare } as unknown as D1Database, prepare };
};

const RACE_SOURCE = {
  debaUrl: "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0108202604091120261025/39",
  raceStartAtJst: "2026-10-25T15:40:00+09:00",
  source: "jra",
};

beforeEach(() => {
  vi.mocked(fetchRacePage).mockReset();
  vi.mocked(getRaceSource).mockReset();
  vi.mocked(logFetch).mockClear();
});

it("parseJraRaceGoing reads the turf going from a turf race header", () => {
  expect(parseJraRaceGoing(JRA_KYOTO_11R_TURF)).toStrictEqual([{ going: "稍重", surface: "turf" }]);
});

it("parseJraRaceGoing reads the dirt going from a dirt race header", () => {
  expect(parseJraRaceGoing(JRA_KYOTO_1R_DIRT)).toStrictEqual([{ going: "重", surface: "dirt" }]);
});

it("parseJraRaceGoing keeps both surfaces for a turf-start dirt race", () => {
  expect(parseJraRaceGoing(JRA_TOKYO_4R_TURF_TO_DIRT)).toStrictEqual([
    { going: "稍重", surface: "turf" },
    { going: "稍重", surface: "dirt" },
  ]);
});

it("parseJraRaceGoing returns nothing before the going is announced", () => {
  expect(
    parseJraRaceGoing(
      '<div class="cell baba"><ul><li class="weather"><span class="inner"><span class="cap">天候</span><span class="txt">曇</span></span></li><li class="turf"><span class="inner"><span class="cap">芝</span><span class="txt"></span></span></li></ul></div>',
    ),
  ).toStrictEqual([]);
});

it("parseNarRaceGoing reads the going from a DebaTable header", () => {
  expect(parseNarRaceGoing(NAR_KOCHI_1R_RESULTED)).toStrictEqual([
    { going: "不良", surface: "dirt" },
  ]);
});

it("parseNarRaceGoing reads a turf race going", () => {
  expect(
    parseNarRaceGoing(
      '<ul class="dataArea">\n<li>芝　1600ｍ（左）　　天候：晴　　馬場：良　　</li>',
    ),
  ).toStrictEqual([{ going: "良", surface: "turf" }]);
});

it("parseNarRaceGoing returns nothing before the going is announced", () => {
  expect(parseNarRaceGoing(NAR_KANAZAWA_1R_PRE_ANNOUNCE)).toStrictEqual([]);
});

it("parseNarRaceGoing ignores ban-ei moisture readings", () => {
  expect(parseNarRaceGoing(NAR_OBIHIRO_1R_MOISTURE)).toStrictEqual([]);
});

it("raceGoingRaceKeyFromRequest decodes a percent-encoded race key", () => {
  expect(
    raceGoingRaceKeyFromRequest(
      new URL("https://x.test/api/race-going/jra%3A2026%3A1025%3A08%3A11"),
    ),
  ).toBe("jra:2026:1025:08:11");
});

it("raceGoingRaceKeyFromRequest rejects malformed keys and other paths", () => {
  expect(raceGoingRaceKeyFromRequest(new URL("https://x.test/api/race-going/jra%3A2026"))).toBe(
    null,
  );
  expect(raceGoingRaceKeyFromRequest(new URL("https://x.test/api/horse-weight/x"))).toBe(null);
});

it("upsertRaceGoings writes one row per surface in a single batch", async () => {
  const { batch, bind, db } = buildDb([]);
  await upsertRaceGoings({
    db,
    goings: [
      { going: "稍重", surface: "turf" },
      { going: "重", surface: "dirt" },
    ],
    observedAt: "2026-10-25T07:20:00+09:00",
    raceKey: "jra:2026:1025:05:04",
  });
  expect(batch).toHaveBeenCalledTimes(1);
  expect(bind.mock.calls).toStrictEqual([
    ["jra:2026:1025:05:04", "turf", "稍重", "2026-10-25T07:20:00+09:00"],
    ["jra:2026:1025:05:04", "dirt", "重", "2026-10-25T07:20:00+09:00"],
  ]);
});

it("upsertRaceGoings skips the write when nothing was observed", async () => {
  const { batch, db } = buildDb([]);
  await upsertRaceGoings({
    db,
    goings: [],
    observedAt: "2026-10-25T07:20:00+09:00",
    raceKey: "nar:2026:1025:46:01",
  });
  expect(batch).not.toHaveBeenCalled();
});

it("listRaceGoings maps stored rows", async () => {
  const { db } = buildDb([
    { going: "稍重", observed_at: "2026-10-25T07:20:00+09:00", surface: "turf" },
  ]);
  expect(await listRaceGoings(db, "jra:2026:1025:08:11")).toStrictEqual([
    { going: "稍重", observedAt: "2026-10-25T07:20:00+09:00", surface: "turf" },
  ]);
});

it("readRaceGoing returns null for an unknown race", async () => {
  const { db } = buildDb([]);
  vi.mocked(getRaceSource).mockResolvedValueOnce(null);
  expect(
    await readRaceGoing({
      db,
      now: new Date("2026-10-25T00:00:00Z"),
      raceKey: "jra:2026:1025:08:11",
    }),
  ).toBe(null);
  expect(fetchRacePage).not.toHaveBeenCalled();
});

it("readRaceGoing refreshes from the JRA race page before post time", async () => {
  const { batch, bind, db } = buildDb([
    { going: "稍重", observed_at: "2026-10-25T09:00:00+09:00", surface: "turf" },
  ]);
  vi.mocked(getRaceSource).mockResolvedValueOnce(RACE_SOURCE as never);
  vi.mocked(fetchRacePage).mockResolvedValueOnce(JRA_KYOTO_11R_TURF);
  expect(
    await readRaceGoing({
      db,
      now: new Date("2026-10-25T00:00:00Z"),
      raceKey: "jra:2026:1025:08:11",
    }),
  ).toStrictEqual({
    goings: [{ going: "稍重", observedAt: "2026-10-25T09:00:00+09:00", surface: "turf" }],
    raceKey: "jra:2026:1025:08:11",
  });
  expect(fetchRacePage).toHaveBeenCalledWith(
    "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0108202604091120261025/39",
  );
  expect(batch).toHaveBeenCalledTimes(1);
  expect(bind.mock.calls[0]).toStrictEqual([
    "jra:2026:1025:08:11",
    "turf",
    "稍重",
    "2026-10-25T09:00:00+09:00",
  ]);
});

it("readRaceGoing parses NAR pages with the NAR parser", async () => {
  const { bind, db } = buildDb([]);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    debaUrl: "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceNo=1",
    raceStartAtJst: "2026-10-24T15:05:00+09:00",
    source: "nar",
  } as never);
  vi.mocked(fetchRacePage).mockResolvedValueOnce(NAR_KOCHI_1R_RESULTED);
  await readRaceGoing({
    db,
    now: new Date("2026-10-24T05:00:00Z"),
    raceKey: "nar:2026:1024:54:01",
  });
  expect(bind.mock.calls[0]).toStrictEqual([
    "nar:2026:1024:54:01",
    "dirt",
    "不良",
    "2026-10-24T14:00:00+09:00",
  ]);
});

it("readRaceGoing serves only the stored observation after post time", async () => {
  const { batch, db } = buildDb([]);
  vi.mocked(getRaceSource).mockResolvedValueOnce(RACE_SOURCE as never);
  expect(
    await readRaceGoing({
      db,
      now: new Date("2026-10-25T07:00:00Z"),
      raceKey: "jra:2026:1025:08:11",
    }),
  ).toStrictEqual({ goings: [], raceKey: "jra:2026:1025:08:11" });
  expect(fetchRacePage).not.toHaveBeenCalled();
  expect(batch).not.toHaveBeenCalled();
});

it("readRaceGoing logs a failed page fetch and keeps the stored rows", async () => {
  const { batch, db } = buildDb([]);
  vi.mocked(getRaceSource).mockResolvedValueOnce(RACE_SOURCE as never);
  vi.mocked(fetchRacePage).mockRejectedValueOnce(new Error("upstream 503"));
  expect(
    await readRaceGoing({
      db,
      now: new Date("2026-10-25T00:00:00Z"),
      raceKey: "jra:2026:1025:08:11",
    }),
  ).toStrictEqual({ goings: [], raceKey: "jra:2026:1025:08:11" });
  expect(batch).not.toHaveBeenCalled();
  expect(vi.mocked(logFetch).mock.calls[0]?.slice(1, 4)).toStrictEqual([
    "race-going",
    "error",
    "jra:2026:1025:08:11",
  ]);
});
