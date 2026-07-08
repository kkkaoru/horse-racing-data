// Run with bun.
import { expect, it, vi } from "vitest";

import {
  buildFinalBackupR2Key,
  putFinalBackupRowToR2,
  type FinalBackupGroupRow,
} from "./r2-archive";
import type { Env } from "../types";

const buildEnv = (overrides: Partial<Env> = {}): Env => {
  const r2 = {
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    head: vi.fn(async () => null),
    list: vi.fn(async () => ({ objects: [] })),
    put: vi.fn(async () => ({})),
  } as unknown as R2Bucket;
  return { ODDS_ARCHIVE: r2, ...overrides } as Env;
};

const sampleFinalBackupRow = (): FinalBackupGroupRow => ({
  fetchedAt: "2026-05-20T10:00:00+09:00",
  oddsType: "tansho",
  payloadJson: '[{"combination":"01","odds":2.5}]',
  raceKey: "nar:20260520:42:01",
});

it("buildFinalBackupR2Key produces odds-final-backup-old-d1/YYYY/MM/DD path", () => {
  expect(buildFinalBackupR2Key(sampleFinalBackupRow())).toBe(
    "odds-final-backup-old-d1/2026/05/20/nar:20260520:42:01/tansho.json",
  );
});

it("buildFinalBackupR2Key sanitizes unsafe characters", () => {
  expect(
    buildFinalBackupR2Key({
      fetchedAt: "2026-05-20T10:00:00+09:00",
      oddsType: "tan/sho",
      payloadJson: "[]",
      raceKey: "nar/20260520/42/01",
    }),
  ).toBe("odds-final-backup-old-d1/2026/05/20/nar_20260520_42_01/tan_sho.json");
});

it("putFinalBackupRowToR2 calls R2.put with sanitized key", async () => {
  const env = buildEnv();
  await putFinalBackupRowToR2(env, sampleFinalBackupRow());
  expect(env.ODDS_ARCHIVE.put).toHaveBeenCalledWith(
    "odds-final-backup-old-d1/2026/05/20/nar:20260520:42:01/tansho.json",
    '[{"combination":"01","odds":2.5}]',
    { httpMetadata: { contentType: "application/json" } },
  );
});
