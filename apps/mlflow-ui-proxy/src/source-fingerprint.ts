// Run with bun. Read source generations without booting the MLflow Container.
import { neon } from "@neondatabase/serverless";
import type { SyncDay } from "./sync-checkpoint";

export interface SourceFingerprintInput {
  connectionString: string;
  dateFrom: string;
  dateTo: string;
}

const DATE_PATTERN: RegExp = /^\d{8}$/;
const PROBE_TIMEOUT_MS: number = 5000;

export const readSourceFingerprints = async (input: SourceFingerprintInput): Promise<SyncDay[]> => {
  if (!DATE_PATTERN.test(input.dateFrom) || !DATE_PATTERN.test(input.dateTo)) {
    throw new Error("Invalid MLflow sync date window");
  }
  const sql = neon(input.connectionString, {
    fetchOptions: { signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) },
  });
  // Include complete source rows: changes/deletions with unchanged counts or
  // timestamps must invalidate the checkpoint too. Dates with no rows are kept.
  const rows = await sql`
    WITH source_rows AS (
      SELECT kaisai_nen || kaisai_tsukihi AS day, 'fp' AS task,
             md5(row_to_json(p)::text) AS fingerprint
      FROM race_finish_position_model_predictions p
      WHERE source IN ('jra', 'nar')
        AND (kaisai_nen, kaisai_tsukihi) >= (${input.dateFrom.slice(0, 4)}, ${input.dateFrom.slice(4)})
        AND (kaisai_nen, kaisai_tsukihi) <= (${input.dateTo.slice(0, 4)}, ${input.dateTo.slice(4)})
      UNION ALL
      SELECT kaisai_nen || kaisai_tsukihi AS day, 'rs' AS task,
             md5(row_to_json(p)::text) AS fingerprint
      FROM race_running_style_model_predictions p
      WHERE source IN ('jra', 'nar')
        AND (kaisai_nen, kaisai_tsukihi) >= (${input.dateFrom.slice(0, 4)}, ${input.dateFrom.slice(4)})
        AND (kaisai_nen, kaisai_tsukihi) <= (${input.dateTo.slice(0, 4)}, ${input.dateTo.slice(4)})
    ), fingerprints AS (
      SELECT day, md5(string_agg(task || fingerprint, ',' ORDER BY task, fingerprint)) AS fingerprint
      FROM source_rows GROUP BY day
    )
    SELECT to_char(days.day, 'YYYYMMDD') AS date,
           coalesce(f.fingerprint, 'empty') AS fingerprint
    FROM generate_series(to_date(${input.dateFrom}, 'YYYYMMDD'),
                         to_date(${input.dateTo}, 'YYYYMMDD'), interval '1 day') AS days(day)
    LEFT JOIN fingerprints f ON f.day = to_char(days.day, 'YYYYMMDD')
    ORDER BY days.day
  `;
  if (rows.length === 0) throw new Error("Empty MLflow source fingerprint window");
  return rows.map((row): SyncDay => {
    if (typeof row.date !== "string" || typeof row.fingerprint !== "string") {
      throw new Error("Invalid MLflow source fingerprint response");
    }
    return { date: row.date, fingerprint: row.fingerprint };
  });
};
