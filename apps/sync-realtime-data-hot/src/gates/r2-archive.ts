import type { Env } from "../types";

const R2_FINAL_BACKUP_KEY_PREFIX = "odds-final-backup-old-d1";
const JSON_CONTENT_TYPE = "application/json";

export interface FinalBackupGroupRow {
  raceKey: string;
  oddsType: string;
  fetchedAt: string;
  payloadJson: string;
}

const sanitizePathSegment = (value: string): string => value.replace(/[^A-Za-z0-9_:-]/g, "_");

export const buildFinalBackupR2Key = (row: FinalBackupGroupRow): string => {
  const isoDate = row.fetchedAt.slice(0, 10);
  const [yyyy, mm, dd] = isoDate.split("-");
  return `${R2_FINAL_BACKUP_KEY_PREFIX}/${yyyy}/${mm}/${dd}/${sanitizePathSegment(row.raceKey)}/${sanitizePathSegment(row.oddsType)}.json`;
};

export const putFinalBackupRowToR2 = async (env: Env, row: FinalBackupGroupRow): Promise<void> => {
  await env.ODDS_ARCHIVE.put(buildFinalBackupR2Key(row), row.payloadJson, {
    httpMetadata: { contentType: JSON_CONTENT_TYPE },
  });
};
