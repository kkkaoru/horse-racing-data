// Run with bun through Vitest.
import { expect, test } from "vitest";

import { buildSql } from "./build-corner-feature-table";

test("buildSql parses signed time differences for local and Neon feature rows", () => {
  const sql = buildSql({
    buildVectorIndex: false,
    fromDate: "20250101",
    sourceScope: "nar",
    target: "local",
    toDate: "20250131",
  });
  expect(sql).toMatch(
    "case when trim(time_sa) ~ '^[+-]?[0-9]+$' then nullif(trim(time_sa), '0000')::numeric / 10 else null end time_sa",
  );
});
