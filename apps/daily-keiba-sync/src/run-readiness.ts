// Runs with bun; deployed as a read-only Cloudflare D1 status projection.
import type { RunRow } from "./types";

export interface RunReadiness extends RunRow {
  catalog_ready: boolean;
  neon_backup_complete: boolean;
}

interface ReadinessRow extends RunRow {
  catalog_ready_flag: number;
  neon_backup_complete_flag: number;
}

// One statement keeps the run plan and table receipts in the same SQLite snapshot.
// Legacy aggregate status remains unchanged so backup recovery keeps running.
const RUN_READINESS_SQL: string = `
  with readiness as (
    select r.*,
      case when r.catalog_tables > 0
        and r.catalog_tables = (
          select count(*) from sync_run_tables t
          where t.run_id = r.run_id and t.catalog_status = 'succeeded'
        )
        and not exists (
          select 1 from sync_run_tables t where t.run_id = r.run_id
          and (t.catalog_status is null or t.catalog_status not in ('succeeded', 'not_configured'))
        )
      then 1 else 0 end as catalog_ready_flag
    from sync_runs r where r.run_id = ?
  )
  select readiness.*,
    case when catalog_ready_flag = 1 and not exists (
      select 1 from sync_run_tables t where t.run_id = readiness.run_id
      and t.catalog_status = 'succeeded'
      and (t.neon_status is null or t.neon_status != 'succeeded')
    ) then 1 else 0 end as neon_backup_complete_flag
  from readiness`;

export const getRunReadiness = async (
  db: D1Database,
  runId: string,
): Promise<RunReadiness | null> => {
  const row: ReadinessRow | null = await db
    .prepare(RUN_READINESS_SQL)
    .bind(runId)
    .first<ReadinessRow>();
  if (row === null) return null;
  const { catalog_ready_flag, neon_backup_complete_flag, ...run } = row;
  return {
    ...run,
    catalog_ready: catalog_ready_flag === 1,
    neon_backup_complete: neon_backup_complete_flag === 1,
  };
};
