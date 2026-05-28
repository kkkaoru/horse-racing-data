// Run with bun.
import { expect, it, vi } from "vitest";

import {
  buildArchiveR2Key,
  buildFinalBackupR2Key,
  computeArchiveCutoffIso,
  putArchiveRowToR2,
  putFinalBackupRowToR2,
  type ArchiveCandidateRow,
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

const sampleRow = (): ArchiveCandidateRow => ({
  fetchedAt: "2026-05-20T10:00:00+09:00",
  oddsType: "tansho",
  raceKey: "nar:20260520:42:01",
  snapshotJson: '{"combination":"01","odds":2.5}',
});

it("buildArchiveR2Key produces YYYY/MM/DD/race_key/odds_type.json path", () => {
  expect(buildArchiveR2Key(sampleRow())).toBe(
    "odds-archive/2026/05/20/nar:20260520:42:01/tansho.json",
  );
});

it("buildArchiveR2Key sanitizes unsafe characters in race_key", () => {
  expect(
    buildArchiveR2Key({
      fetchedAt: "2026-05-20T10:00:00+09:00",
      oddsType: "tan/sho",
      raceKey: "nar/20260520/42/01",
      snapshotJson: "{}",
    }),
  ).toBe("odds-archive/2026/05/20/nar_20260520_42_01/tan_sho.json");
});

it("computeArchiveCutoffIso defaults to 7 days ago", () => {
  expect(computeArchiveCutoffIso(buildEnv(), new Date("2026-05-28T00:00:00Z"))).toBe(
    "2026-05-21T00:00:00.000Z",
  );
});

it("computeArchiveCutoffIso honors env override days", () => {
  expect(
    computeArchiveCutoffIso(
      buildEnv({ ODDS_R2_ARCHIVE_RETENTION_DAYS: "14" }),
      new Date("2026-05-28T00:00:00Z"),
    ),
  ).toBe("2026-05-14T00:00:00.000Z");
});

it("computeArchiveCutoffIso falls back to default on invalid env", () => {
  expect(
    computeArchiveCutoffIso(
      buildEnv({ ODDS_R2_ARCHIVE_RETENTION_DAYS: "bad" }),
      new Date("2026-05-28T00:00:00Z"),
    ),
  ).toBe("2026-05-21T00:00:00.000Z");
});

it("computeArchiveCutoffIso falls back to default when env value is zero", () => {
  expect(
    computeArchiveCutoffIso(
      buildEnv({ ODDS_R2_ARCHIVE_RETENTION_DAYS: "0" }),
      new Date("2026-05-28T00:00:00Z"),
    ),
  ).toBe("2026-05-21T00:00:00.000Z");
});

it("putArchiveRowToR2 calls R2.put with sanitized key and JSON content-type", async () => {
  const env = buildEnv();
  await putArchiveRowToR2(env, sampleRow());
  expect(env.ODDS_ARCHIVE.put).toHaveBeenCalledWith(
    "odds-archive/2026/05/20/nar:20260520:42:01/tansho.json",
    '{"combination":"01","odds":2.5}',
    { httpMetadata: { contentType: "application/json" } },
  );
});

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
