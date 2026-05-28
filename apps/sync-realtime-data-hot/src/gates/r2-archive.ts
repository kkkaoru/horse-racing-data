import type { Env } from "../types";

const R2_ARCHIVE_KEY_PREFIX = "odds-archive";
const DEFAULT_ARCHIVE_RETENTION_DAYS = 7;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const JSON_CONTENT_TYPE = "application/json";

export interface ArchiveCandidateRow {
  raceKey: string;
  oddsType: string;
  fetchedAt: string;
  snapshotJson: string;
}

const resolveRetentionDays = (env: Env): number => {
  const raw = env.ODDS_R2_ARCHIVE_RETENTION_DAYS;
  if (!raw) {
    return DEFAULT_ARCHIVE_RETENTION_DAYS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_ARCHIVE_RETENTION_DAYS;
};

const sanitizePathSegment = (value: string): string => value.replace(/[^A-Za-z0-9_:-]/g, "_");

export const buildArchiveR2Key = (row: ArchiveCandidateRow): string => {
  const isoDate = row.fetchedAt.slice(0, 10);
  const [yyyy, mm, dd] = isoDate.split("-");
  return `${R2_ARCHIVE_KEY_PREFIX}/${yyyy}/${mm}/${dd}/${sanitizePathSegment(row.raceKey)}/${sanitizePathSegment(row.oddsType)}.json`;
};

export const computeArchiveCutoffIso = (env: Env, now: Date): string => {
  const days = resolveRetentionDays(env);
  return new Date(now.getTime() - days * MS_PER_DAY).toISOString();
};

export const putArchiveRowToR2 = async (env: Env, row: ArchiveCandidateRow): Promise<void> => {
  await env.ODDS_ARCHIVE.put(buildArchiveR2Key(row), row.snapshotJson, {
    httpMetadata: { contentType: JSON_CONTENT_TYPE },
  });
};
