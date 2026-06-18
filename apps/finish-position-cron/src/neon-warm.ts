// Run with bun. Best-effort Neon compute warm-up via a SELECT 1 HTTP query.

import { neon } from "@neondatabase/serverless";

const WARM_QUERY = "SELECT 1";

// Issue a lightweight SELECT 1 against the Neon database to wake the compute
// endpoint before the prediction window. Best-effort: errors are logged as
// warnings and never propagate to the caller. The URL value is never logged.
export const warmNeon = async (neonDatabaseUrl: string): Promise<void> => {
  try {
    const sql = neon(neonDatabaseUrl);
    await sql.query(WARM_QUERY);
    console.log("neon warm ok");
  } catch (error) {
    console.warn("neon warm failed", String(error));
  }
};
