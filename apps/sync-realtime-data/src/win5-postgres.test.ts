// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import type { Pool } from "pg";
import {
  buildWin5LegInputsFromPostgres,
  buildWin5ScheduleFromJvdWfRow,
  enrichWin5ScheduleLegs,
  getAverageWin5PayoutYen,
  resolveWin5LegFromPostgres,
} from "./win5-postgres";
import type { Win5Schedule } from "../../pc-keiba-viewer/src/lib/win5/types";

vi.mock("../../pc-keiba-viewer/src/lib/win5/leg-inputs", () => ({
  buildWin5LegInputsWithPool: vi.fn(),
}));

afterEach(() => {
  vi.restoreAllMocks();
});

it("resolveWin5LegFromPostgres returns null when no row is found", async () => {
  const query = vi.fn(async () => ({ rows: [] }));
  const pool = { query } as unknown as Pool;
  const result = await resolveWin5LegFromPostgres(pool, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    keibajoCode: "05",
    raceBango: "09",
  });
  expect(result).toBeNull();
});

it("resolveWin5LegFromPostgres builds a leg from the row, stripping leading zeros", async () => {
  const query = vi.fn(async () => ({
    rows: [
      {
        kaisai_kai: "02",
        kaisai_nichime: "06",
        keibajo_code: "05",
        kyosomei_hondai: "  名前  ",
        race_bango: "09",
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const result = await resolveWin5LegFromPostgres(pool, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    keibajoCode: "05",
    raceBango: "09",
  });
  expect(result).toStrictEqual({
    kaisaiKai: "02",
    kaisaiNichime: "06",
    keibajoCode: "05",
    legIndex: 0,
    raceBango: "9",
    raceLabel: "名前",
  });
});

it("resolveWin5LegFromPostgres preserves all-zero raceBango", async () => {
  const query = vi.fn(async () => ({
    rows: [
      {
        kaisai_kai: "01",
        kaisai_nichime: "01",
        keibajo_code: "05",
        kyosomei_hondai: null,
        race_bango: "00",
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const result = await resolveWin5LegFromPostgres(pool, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    keibajoCode: "05",
    raceBango: "0",
  });
  expect(result!.raceBango).toBe("00");
  expect(result!.raceLabel).toBeUndefined();
});

it("enrichWin5ScheduleLegs replaces matching legs and renumbers legIndex when no row found", async () => {
  const query = vi.fn(async () => ({ rows: [] }));
  const pool = { query } as unknown as Pool;
  const schedule: Win5Schedule = {
    fetchedAt: "2026-05-10T09:00:00+09:00",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: [
      {
        kaisaiKai: "01",
        kaisaiNichime: "01",
        keibajoCode: "05",
        legIndex: 0,
        raceBango: "9",
      },
      {
        kaisaiKai: "01",
        kaisaiNichime: "01",
        keibajoCode: "06",
        legIndex: 0,
        raceBango: "11",
      },
    ],
    saleDeadline: null,
    source: "jra_web",
  };
  const enriched = await enrichWin5ScheduleLegs(pool, schedule);
  expect(enriched.legs.map((leg) => leg.legIndex)).toStrictEqual([1, 2]);
});

it("enrichWin5ScheduleLegs merges resolved row, preserving caller-provided keibajoName and startTime", async () => {
  const query = vi.fn(async () => ({
    rows: [
      {
        kaisai_kai: "02",
        kaisai_nichime: "06",
        keibajo_code: "05",
        kyosomei_hondai: "サンプル",
        race_bango: "09",
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const schedule: Win5Schedule = {
    fetchedAt: "2026-05-10T09:00:00+09:00",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: [
      {
        kaisaiKai: "",
        kaisaiNichime: "",
        keibajoCode: "05",
        keibajoName: "東京",
        legIndex: 0,
        raceBango: "9",
        startTime: "15:30",
      },
    ],
    saleDeadline: null,
    source: "jra_web",
  };
  const enriched = await enrichWin5ScheduleLegs(pool, schedule);
  expect(enriched.legs[0]).toStrictEqual({
    kaisaiKai: "02",
    kaisaiNichime: "06",
    keibajoCode: "05",
    keibajoName: "東京",
    legIndex: 1,
    raceBango: "9",
    raceLabel: "サンプル",
    startTime: "15:30",
  });
});

it("buildWin5LegInputsFromPostgres enriches the schedule then calls leg-inputs builder", async () => {
  const { buildWin5LegInputsWithPool } =
    await import("../../pc-keiba-viewer/src/lib/win5/leg-inputs");
  vi.mocked(buildWin5LegInputsWithPool).mockResolvedValue([]);

  const query = vi.fn(async () => ({ rows: [] }));
  const pool = { query } as unknown as Pool;
  const schedule: Win5Schedule = {
    fetchedAt: "2026-05-10T09:00:00+09:00",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: [
      {
        kaisaiKai: "01",
        kaisaiNichime: "01",
        keibajoCode: "05",
        legIndex: 0,
        raceBango: "9",
      },
    ],
    saleDeadline: null,
    source: "jra_web",
  };
  await buildWin5LegInputsFromPostgres(pool, schedule);
  expect(buildWin5LegInputsWithPool).toHaveBeenCalledTimes(1);
});

it("buildWin5ScheduleFromJvdWfRow returns null when fewer than 5 legs parse cleanly", () => {
  const result = buildWin5ScheduleFromJvdWfRow({
    kaisai_nen: "2026",
    kaisai_tsukihi: "0511",
    race_joho_1: "05020609",
    race_joho_2: "INVALID",
    race_joho_3: "05020610",
    race_joho_4: "05020611",
    race_joho_5: "05020612",
  });
  expect(result).toBeNull();
});

it("buildWin5ScheduleFromJvdWfRow returns schedule with 5 legs when all race_joho_* parse", () => {
  vi.spyOn(Date.prototype, "toISOString").mockReturnValue("2026-05-11T00:00:00.000Z");
  const result = buildWin5ScheduleFromJvdWfRow({
    kaisai_nen: "2026",
    kaisai_tsukihi: "0511",
    race_joho_1: "05020609",
    race_joho_2: "05020610",
    race_joho_3: "05020611",
    race_joho_4: "05020612",
    race_joho_5: "05020613",
  });
  expect(result!.kaisaiNen).toBe("2026");
  expect(result!.kaisaiTsukihi).toBe("0511");
  expect(result!.source).toBe("jvd_wf");
  expect(result!.saleDeadline).toBeNull();
  expect(result!.legs.length).toBe(5);
});

it("buildWin5ScheduleFromJvdWfRow defaults kaisaiNen / kaisaiTsukihi to empty string when missing", () => {
  const row: Record<string, string> = {
    race_joho_1: "05020609",
    race_joho_2: "05020610",
    race_joho_3: "05020611",
    race_joho_4: "05020612",
    race_joho_5: "05020613",
  };
  const result = buildWin5ScheduleFromJvdWfRow(row);
  expect(result!.kaisaiNen).toBe("");
  expect(result!.kaisaiTsukihi).toBe("");
});

it("getAverageWin5PayoutYen returns the average when payout is finite and positive", async () => {
  const query = vi.fn(async () => ({ rows: [{ average_payout: "300000" }] }));
  const pool = { query } as unknown as Pool;
  expect(await getAverageWin5PayoutYen(pool)).toBe(300000);
});

it("getAverageWin5PayoutYen falls back to 250000 when no row returned", async () => {
  const query = vi.fn(async () => ({ rows: [] }));
  const pool = { query } as unknown as Pool;
  expect(await getAverageWin5PayoutYen(pool)).toBe(250000);
});

it("getAverageWin5PayoutYen falls back to 250000 when row payout is null", async () => {
  const query = vi.fn(async () => ({ rows: [{ average_payout: null }] }));
  const pool = { query } as unknown as Pool;
  expect(await getAverageWin5PayoutYen(pool)).toBe(250000);
});
