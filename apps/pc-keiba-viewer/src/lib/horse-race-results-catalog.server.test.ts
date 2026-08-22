// Run with bun (bunx vitest).
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { safeGetCloudflareEnvMock } = vi.hoisted(() => ({
  safeGetCloudflareEnvMock: vi.fn<() => Promise<CloudflareEnv | null>>(),
}));

vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareEnv: safeGetCloudflareEnvMock,
}));

import {
  buildHorseRaceResultsCatalogUrl,
  fetchHorseRaceResultsFromCatalog,
} from "./horse-race-results-catalog.server";

const query = {
  day: "8",
  keibajoCode: "5",
  month: "8",
  raceBango: "11",
  source: "jra" as const,
  sourceScope: "all" as const,
  year: "2026",
};

const catalogResult = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: "1",
  bamei: "イクイノックス",
  banushimei: "社台レースホース",
  barei: "4",
  bataiju: "472",
  chokyoshimeiRyakusho: "木村哲也",
  corner1: "1",
  corner2: "1",
  corner3: "1",
  corner4: "1",
  currentBarei: "5",
  currentJockey: "ルメール",
  currentSeibetsuCode: "1",
  currentUmaban: "07",
  futanJuryo: "570",
  gradeCode: "A",
  hassoJikoku: "1530",
  juryoShubetsuCode: "1",
  kaisaiNen: "2025",
  kaisaiTsukihi: "1228",
  kakuteiChakujun: "01",
  keibajoCode: "06",
  kettoTorokuBango: "2019100001",
  kishumeiRyakusho: "ルメール",
  kohan3f: "339",
  kyori: "2500",
  kyosoJokenCode: "999",
  kyosoJokenMeisho: "オープン",
  kyosoKigoCode: null,
  kyosoShubetsuCode: "11",
  kyosomeiFukudai: null,
  kyosomeiHondai: "有馬記念",
  kyosomeiKakkonai: null,
  raceBango: "11",
  seibetsuCode: "1",
  sohaTime: "2315",
  tanshoNinkijun: "01",
  tanshoOdds: "150",
  tenkoCode: "1",
  timeSa: "000",
  trackCode: "10",
  umaban: "07",
  wakuban: "7",
  zogenFugo: "+",
  zogenSa: "4",
};

beforeEach(() => {
  safeGetCloudflareEnvMock.mockReset();
});

it("builds the horse-race-results Catalog URL with padded race identity", () => {
  expect(buildHorseRaceResultsCatalogUrl(query).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/horse-race-results?date=20260808&keibajoCode=05&raceBango=11&source=jra&sourceScope=all",
  );
});

it("returns Catalog horse race result rows from the service binding", async () => {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValue(Response.json({ rows: [catalogResult] }));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchHorseRaceResultsFromCatalog(query)).resolves.toStrictEqual([
    {
      babajotaiCodeDirt: null,
      babajotaiCodeShiba: "1",
      bamei: "イクイノックス",
      banushimei: "社台レースホース",
      barei: "4",
      bataiju: "472",
      chokyoshimeiRyakusho: "木村哲也",
      corner1: "1",
      corner2: "1",
      corner3: "1",
      corner4: "1",
      currentBarei: "5",
      currentJockey: "ルメール",
      currentSeibetsuCode: "1",
      currentUmaban: "07",
      futanJuryo: "570",
      gradeCode: "A",
      hassoJikoku: "1530",
      juryoShubetsuCode: "1",
      kaisaiNen: "2025",
      kaisaiTsukihi: "1228",
      kakuteiChakujun: "01",
      keibajoCode: "06",
      kettoTorokuBango: "2019100001",
      kishumeiRyakusho: "ルメール",
      kohan3f: "339",
      kyori: "2500",
      kyosoJokenCode: "999",
      kyosoJokenMeisho: "オープン",
      kyosoKigoCode: null,
      kyosoShubetsuCode: "11",
      kyosomeiFukudai: null,
      kyosomeiHondai: "有馬記念",
      kyosomeiKakkonai: null,
      raceBango: "11",
      seibetsuCode: "1",
      sohaTime: "2315",
      tanshoNinkijun: "01",
      tanshoOdds: "150",
      tenkoCode: "1",
      timeSa: "000",
      trackCode: "10",
      umaban: "07",
      wakuban: "7",
      zogenFugo: "+",
      zogenSa: "4",
    },
  ]);
  const request = fetchMock.mock.calls[0]?.[0];
  expect(request).toBeInstanceOf(Request);
  if (!(request instanceof Request)) throw new Error("Catalog Request expected");
  expect(request.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/horse-race-results?date=20260808&keibajoCode=05&raceBango=11&source=jra&sourceScope=all",
  );
});

it("coerces numeric Catalog fields and keeps optional result columns", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
    Response.json({
      rows: [
        {
          ...catalogResult,
          blinkerShiyoKubun: "1",
          kaisaiNen: 2025,
          raceBango: 11,
          shussoTosu: 16,
          umaban: 7,
        },
      ],
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchHorseRaceResultsFromCatalog(query)).resolves.toStrictEqual([
    {
      babajotaiCodeDirt: null,
      babajotaiCodeShiba: "1",
      bamei: "イクイノックス",
      banushimei: "社台レースホース",
      barei: "4",
      bataiju: "472",
      blinkerShiyoKubun: "1",
      chokyoshimeiRyakusho: "木村哲也",
      corner1: "1",
      corner2: "1",
      corner3: "1",
      corner4: "1",
      currentBarei: "5",
      currentJockey: "ルメール",
      currentSeibetsuCode: "1",
      currentUmaban: "07",
      futanJuryo: "570",
      gradeCode: "A",
      hassoJikoku: "1530",
      juryoShubetsuCode: "1",
      kaisaiNen: "2025",
      kaisaiTsukihi: "1228",
      kakuteiChakujun: "01",
      keibajoCode: "06",
      kettoTorokuBango: "2019100001",
      kishumeiRyakusho: "ルメール",
      kohan3f: "339",
      kyori: "2500",
      kyosoJokenCode: "999",
      kyosoJokenMeisho: "オープン",
      kyosoKigoCode: null,
      kyosoShubetsuCode: "11",
      kyosomeiFukudai: null,
      kyosomeiHondai: "有馬記念",
      kyosomeiKakkonai: null,
      raceBango: "11",
      seibetsuCode: "1",
      shussoTosu: "16",
      sohaTime: "2315",
      tanshoNinkijun: "01",
      tanshoOdds: "150",
      tenkoCode: "1",
      timeSa: "000",
      trackCode: "10",
      umaban: "7",
      wakuban: "7",
      zogenFugo: "+",
      zogenSa: "4",
    },
  ]);
});

it("returns null when the Catalog binding is unavailable", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  await expect(fetchHorseRaceResultsFromCatalog(query)).resolves.toBeNull();
});

it("throws when Catalog HTTP, payload, or rows are invalid", async () => {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("unavailable", { status: 502 }))
    .mockResolvedValueOnce(Response.json({ rows: "invalid" }))
    .mockResolvedValueOnce(Response.json({ rows: [{ ...catalogResult, kaisaiNen: "" }] }))
    .mockResolvedValueOnce(Response.json({ rows: [{ ...catalogResult, bamei: true }] }))
    .mockResolvedValueOnce(Response.json({ rows: [{ ...catalogResult, shussoTosu: true }] }))
    .mockResolvedValueOnce(Response.json({ rows: [{ ...catalogResult, blinkerShiyoKubun: true }] }))
    .mockResolvedValueOnce(Response.json({ rows: [null] }));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog horse race results failed: 502",
  );
  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog horse race results payload is malformed",
  );
  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog horse race results rows are malformed",
  );
  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog horse race results rows are malformed",
  );
  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog horse race results rows are malformed",
  );
  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog horse race results rows are malformed",
  );
  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog horse race results rows are malformed",
  );
});

it("propagates Catalog fetch failures", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error("catalog timeout"));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });
  await expect(fetchHorseRaceResultsFromCatalog(query)).rejects.toThrow("catalog timeout");
});
