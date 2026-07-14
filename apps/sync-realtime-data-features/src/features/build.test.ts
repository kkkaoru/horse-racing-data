// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { buildRaceFeatures } from "./build";
import type { RaceJobKey } from "../types";

const JRA_JOB = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0529",
  keibajoCode: "5",
  raceBango: "1",
  raceKey: "jra:2026:0529:05:01",
  source: "jra",
} satisfies RaceJobKey;

it("buildRaceFeatures requests only the specified race from the catalog", async () => {
  const requests: Request[] = [];
  const fetch = vi.fn(async (request: Request): Promise<Response> => {
    requests.push(request);
    return Response.json([
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "05",
        ketto_toroku_bango: "2023100001",
        kyori: "1600",
        race_bango: "01",
        race_date: "20260529",
        source: "jra",
        umaban: "3",
      },
    ]);
  });
  const rows = await buildRaceFeatures(JRA_JOB, { PC_KEIBA_R2_CATALOG: { fetch } });
  expect(requests[0]?.url).toBe(
    "https://pc-keiba-r2-catalog/v1/race-features?date=20260529&source=jra&keibajoCode=05&raceBango=01",
  );
  expect(requests[0]?.method).toBe("GET");
  expect(rows).toStrictEqual([
    {
      babajotai_code_dirt: null,
      babajotai_code_shiba: null,
      bamei: null,
      banushimei: null,
      barei: null,
      bataiju: null,
      chokyoshimei_ryakusho: null,
      corner1_norm: null,
      corner2_norm: null,
      corner3_norm: null,
      corner4_norm: null,
      corner_1: null,
      corner_2: null,
      corner_3: null,
      corner_4: null,
      finish_norm: null,
      finish_position: null,
      futan_juryo: null,
      grade_code: null,
      hasso_jikoku: null,
      juryo_shubetsu_code: null,
      kaisai_nen: "2026",
      kaisai_tsukihi: "0529",
      keibajo_code: "05",
      ketto_toroku_bango: "2023100001",
      kishumei_ryakusho: null,
      kohan_3f: null,
      kyori: 1600,
      kyoso_joken_code: null,
      kyoso_shubetsu_code: null,
      race_bango: "01",
      race_date: "20260529",
      race_name: null,
      seibetsu_code: null,
      shusso_tosu: null,
      soha_time: null,
      source: "jra",
      tansho_ninkijun: null,
      tansho_odds: null,
      time_sa: null,
      track_code: null,
      umaban: 3,
      wakuban: null,
      zogen_fugo: null,
      zogen_sa: null,
    },
  ]);
});

it("buildRaceFeatures sends Ban-ei jobs as nar catalog requests", async () => {
  const requests: Request[] = [];
  const fetch = vi.fn(async (request: Request): Promise<Response> => {
    requests.push(request);
    return Response.json([]);
  });
  await buildRaceFeatures(
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "83",
      raceBango: "11",
      raceKey: "nar:2026:0529:83:11",
      source: "nar",
    },
    { PC_KEIBA_R2_CATALOG: { fetch } },
  );
  expect(requests[0]?.url).toBe(
    "https://pc-keiba-r2-catalog/v1/race-features?date=20260529&source=nar&keibajoCode=83&raceBango=11",
  );
});

it("buildRaceFeatures rejects non-object catalog rows", async () => {
  const fetch = vi.fn(async (): Promise<Response> => Response.json([null]));
  await expect(buildRaceFeatures(JRA_JOB, { PC_KEIBA_R2_CATALOG: { fetch } })).rejects.toThrowError(
    "PC_KEIBA_R2_CATALOG /v1/race-features returned an invalid row",
  );
});

it("buildRaceFeatures fails without archive or PostgreSQL fallback when catalog is unavailable", async () => {
  const fetch = vi.fn(async (): Promise<Response> => new Response(null, { status: 503 }));
  await expect(buildRaceFeatures(JRA_JOB, { PC_KEIBA_R2_CATALOG: { fetch } })).rejects.toThrowError(
    "PC_KEIBA_R2_CATALOG /v1/race-features failed with HTTP 503",
  );
});
