import { expect, it, vi } from "vitest";

import {
  buildBulkFreshRaceEntriesQuery,
  buildFreshRaceEntriesQuery,
  buildRaceFeaturesQuery,
  buildRaceKeysQuery,
  executeR2Sql,
  normaliseBulkFreshRaceEntries,
  normaliseFreshRaceEntries,
  R2SqlQueryError,
} from "./r2-sql";
import type { Env, Fetcher } from "./types";

const kv = {
  delete: vi.fn(async () => undefined),
  get: vi.fn(async () => null),
  put: vi.fn(async () => undefined),
};

const env = (): Env => ({
  CATALOG_KV: kv,
  R2_SQL_ACCOUNT_ID: "account-id",
  R2_SQL_BUCKET_NAME: "bucket-name",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "secret-token",
});

it("builds race-key SQL from only the partition-pruned raw race tables", () => {
  expect(buildRaceKeysQuery(env(), "20260715")).toBe(`
SELECT
  'jra' AS source,
  concat(kaisai_nen, kaisai_tsukihi) AS race_date,
  kaisai_nen,
  kaisai_tsukihi,
  lpad(keibajo_code, 2, '0') AS keibajo_code,
    lpad(race_bango, 2, '0') AS race_bango,
    nullif(btrim(grade_code), '') AS grade_code
FROM pc_keiba.jvd_ra
WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0715'
UNION ALL
SELECT
  'nar' AS source,
  concat(kaisai_nen, kaisai_tsukihi) AS race_date,
  kaisai_nen,
  kaisai_tsukihi,
  lpad(keibajo_code, 2, '0') AS keibajo_code,
    lpad(race_bango, 2, '0') AS race_bango,
    nullif(btrim(grade_code), '') AS grade_code
FROM pc_keiba.nvd_ra
WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0715'
ORDER BY source, keibajo_code, race_bango`);
});

it("builds JRA features by joining partition-pruned jvd_se and jvd_ra", () => {
  const sql = buildRaceFeaturesQuery(env(), {
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
  });
  expect(sql).toMatch("FROM pc_keiba.jvd_se");
  expect(sql).toMatch("FROM pc_keiba.jvd_ra");
  expect(sql).not.toMatch("nvd_se");
  expect(sql).not.toMatch("nvd_ra");
  expect(sql.match(/kaisai_nen = '2026'/gu)).toHaveLength(2);
  expect(sql.match(/kaisai_tsukihi = '0715'/gu)).toHaveLength(2);
  expect(sql.match(/keibajo_code = '05'/gu)).toHaveLength(2);
  expect(sql.match(/race_bango = '01'/gu)).toHaveLength(2);
  expect(sql).toMatch("INNER JOIN jra_ra ra");
  expect(sql).toMatch("INNER JOIN jra_counts counts");
  expect(sql).toMatch("counts.entry_count");
  expect(sql).toMatch("nullif(ra.shusso_tosu, '00')");
  expect(sql).toMatch("try_cast(nullif(se.umaban, '') AS INT) AS umaban");
  expect(sql).toMatch("END AS finish_norm");
  expect(sql).toMatch("END AS corner4_norm");
});

it("builds NAR and ban-ei scopes from only partition-pruned NAR raw tables", () => {
  const narSql = buildRaceFeaturesQuery(env(), { date: "20260715", source: "nar" });
  const banEiSql = buildRaceFeaturesQuery(env(), {
    date: "20260715",
    keibajoCode: "83",
    raceBango: "09",
    source: "ban-ei",
  });
  expect(narSql).toMatch("FROM pc_keiba.nvd_se");
  expect(narSql).toMatch("FROM pc_keiba.nvd_ra");
  expect(narSql.match(/keibajo_code <> '83'/gu)).toHaveLength(2);
  expect(banEiSql.match(/keibajo_code = '83'/gu)).toHaveLength(4);
  expect(banEiSql.match(/race_bango = '09'/gu)).toHaveLength(2);
  expect(banEiSql).not.toMatch("jvd_se");
});

it("builds all-source features from all four raw tables with pruning on every scan", () => {
  const sql = buildRaceFeaturesQuery(env(), { date: "20260715", source: "all" });
  expect(sql).toMatch("FROM pc_keiba.jvd_se");
  expect(sql).toMatch("FROM pc_keiba.jvd_ra");
  expect(sql).toMatch("FROM pc_keiba.nvd_se");
  expect(sql).toMatch("FROM pc_keiba.nvd_ra");
  expect(sql.match(/kaisai_nen = '2026'/gu)).toHaveLength(4);
  expect(sql.match(/kaisai_tsukihi = '0715'/gu)).toHaveLength(4);
  expect(sql.match(/UNION ALL/gu)).toHaveLength(1);
  expect(sql).not.toMatch("daily_race_entries");
  expect(sql).not.toMatch("race_entry_corner_features");
  expect(sql).not.toMatch("features_archive");
});

it("builds a partition-pruned exact JRA fresh entrant query with active horses only", () => {
  const sql = buildFreshRaceEntriesQuery(env(), {
    date: "20260823",
    keibajoCode: "07",
    raceBango: "09",
    source: "jra",
  });
  expect(sql).toMatch("FROM pc_keiba.jvd_se");
  expect(sql).not.toMatch("nvd_se");
  expect(sql).toMatch("kaisai_nen = '2026'");
  expect(sql).toMatch("kaisai_tsukihi = '0823'");
  expect(sql).toMatch("keibajo_code = '07'");
  expect(sql).toMatch("race_bango = '09'");
  expect(sql).toMatch("coalesce(btrim(ijo_kubun_code), '0') NOT IN ('1', '2')");
  expect(sql).toMatch("ORDER BY umaban, ketto_toroku_bango");
  expect(sql).not.toMatch("DISTINCT");
});

it("keeps NAR and ban-ei fresh entrant source scopes disjoint", () => {
  const narSql = buildFreshRaceEntriesQuery(env(), {
    date: "20260823",
    keibajoCode: "30",
    raceBango: "11",
    source: "nar",
  });
  const banEiSql = buildFreshRaceEntriesQuery(env(), {
    date: "20260823",
    keibajoCode: "83",
    raceBango: "01",
    source: "ban-ei",
  });
  expect(narSql).toMatch("FROM pc_keiba.nvd_se");
  expect(narSql).toMatch("keibajo_code <> '83'");
  expect(banEiSql).toMatch("FROM pc_keiba.nvd_se");
  expect(banEiSql).toMatch("keibajo_code = '83'");
});

it("builds one partition-pruned bulk fresh entrant query for each exact source", () => {
  const jraSql = buildBulkFreshRaceEntriesQuery(env(), { date: "20260824", source: "jra" });
  const narSql = buildBulkFreshRaceEntriesQuery(env(), { date: "20260824", source: "nar" });
  const banEiSql = buildBulkFreshRaceEntriesQuery(env(), {
    date: "20260824",
    source: "ban-ei",
  });
  expect(jraSql).toMatch("'jra' AS source");
  expect(jraSql).toMatch("FROM pc_keiba.jvd_se");
  expect(jraSql).not.toMatch("nvd_se");
  expect(jraSql).toMatch("kaisai_nen = '2026'");
  expect(jraSql).toMatch("kaisai_tsukihi = '0824'");
  expect(jraSql).not.toMatch("race_bango =");
  expect(narSql).toMatch("'nar' AS source");
  expect(narSql).toMatch("FROM pc_keiba.nvd_se");
  expect(narSql).toMatch("keibajo_code <> '83'");
  expect(banEiSql).toMatch("'ban-ei' AS source");
  expect(banEiSql).toMatch("FROM pc_keiba.nvd_se");
  expect(banEiSql).toMatch("keibajo_code = '83'");
  expect(banEiSql).toMatch("coalesce(btrim(ijo_kubun_code), '0') NOT IN ('1', '2')");
  expect(banEiSql).toMatch("ORDER BY keibajo_code, race_bango, umaban, ketto_toroku_bango");
});

it("normalises and sorts an exact fresh entrant set", () => {
  expect(
    normaliseFreshRaceEntries([
      { ketto_toroku_bango: " 2023100002 ", umaban: "2" },
      { ketto_toroku_bango: "2023100001", umaban: 1 },
    ]),
  ).toStrictEqual([
    { kettoTorokuBango: "2023100001", umaban: 1 },
    { kettoTorokuBango: "2023100002", umaban: 2 },
  ]);
});

it("fails closed for empty, invalid, or duplicate fresh entrants", () => {
  expect(() => normaliseFreshRaceEntries([])).toThrow("fresh race entries are empty");
  expect(() => normaliseFreshRaceEntries([{ ketto_toroku_bango: "horse", umaban: 1 }])).toThrow(
    "fresh race entries contain an invalid ketto_toroku_bango",
  );
  expect(() =>
    normaliseFreshRaceEntries([{ ketto_toroku_bango: "2023100001", umaban: 0 }]),
  ).toThrow("fresh race entries contain an invalid umaban");
  expect(() =>
    normaliseFreshRaceEntries([{ ketto_toroku_bango: "2023100001", umaban: null }]),
  ).toThrow("fresh race entries contain an invalid umaban");
  expect(() => normaliseFreshRaceEntries([{ ketto_toroku_bango: null, umaban: 1 }])).toThrow(
    "fresh race entries contain an invalid ketto_toroku_bango",
  );
  expect(() =>
    normaliseFreshRaceEntries([
      { ketto_toroku_bango: "2023100001", umaban: 1 },
      { ketto_toroku_bango: "2023100001", umaban: 2 },
    ]),
  ).toThrow("fresh race entries contain duplicates");
  expect(() =>
    normaliseFreshRaceEntries([
      { ketto_toroku_bango: "2023100001", umaban: 1 },
      { ketto_toroku_bango: "2023100002", umaban: 1 },
    ]),
  ).toThrow("fresh race entries contain duplicates");
});

it("normalises, scopes, sorts, and permits the same horse number in different bulk races", () => {
  expect(
    normaliseBulkFreshRaceEntries(
      [
        {
          keibajo_code: "35",
          ketto_toroku_bango: " 2023100002 ",
          race_bango: "02",
          source: "nar",
          umaban: "1",
        },
        {
          keibajo_code: "35",
          ketto_toroku_bango: "2023100001",
          race_bango: "01",
          source: "nar",
          umaban: 1,
        },
      ],
      "nar",
    ),
  ).toStrictEqual([
    {
      keibajoCode: "35",
      kettoTorokuBango: "2023100001",
      raceBango: "01",
      source: "nar",
      umaban: 1,
    },
    {
      keibajoCode: "35",
      kettoTorokuBango: "2023100002",
      raceBango: "02",
      source: "nar",
      umaban: 1,
    },
  ]);
});

it("fails closed for empty, malformed, mismatched-source, or duplicate bulk entrants", () => {
  expect(() => normaliseBulkFreshRaceEntries([], "jra")).toThrow("fresh race entries are empty");
  expect(() =>
    normaliseBulkFreshRaceEntries(
      [
        {
          keibajo_code: "7",
          ketto_toroku_bango: "2023100001",
          race_bango: "01",
          source: "jra",
          umaban: 1,
        },
      ],
      "jra",
    ),
  ).toThrow("fresh race entries contain an invalid keibajo_code");
  expect(() =>
    normaliseBulkFreshRaceEntries(
      [
        {
          keibajo_code: "07",
          ketto_toroku_bango: "2023100001",
          race_bango: "1",
          source: "jra",
          umaban: 1,
        },
      ],
      "jra",
    ),
  ).toThrow("fresh race entries contain an invalid race_bango");
  expect(() =>
    normaliseBulkFreshRaceEntries(
      [
        {
          keibajo_code: "07",
          ketto_toroku_bango: "2023100001",
          race_bango: "01",
          source: "nar",
          umaban: 1,
        },
      ],
      "jra",
    ),
  ).toThrow("fresh race entries contain an invalid source");
  expect(() =>
    normaliseBulkFreshRaceEntries(
      [
        {
          keibajo_code: "07",
          ketto_toroku_bango: "2023100001",
          race_bango: "01",
          source: "jra",
          umaban: 1,
        },
        {
          keibajo_code: "07",
          ketto_toroku_bango: "2023100002",
          race_bango: "01",
          source: "jra",
          umaban: 1,
        },
      ],
      "jra",
    ),
  ).toThrow("fresh race entries contain duplicates");
  expect(() =>
    normaliseBulkFreshRaceEntries(
      [
        {
          keibajo_code: "07",
          ketto_toroku_bango: "horse",
          race_bango: "01",
          source: "jra",
          umaban: 1,
        },
      ],
      "jra",
    ),
  ).toThrow("fresh race entries contain an invalid ketto_toroku_bango");
});

it("rejects unsafe namespace, date, and race filters", () => {
  expect(() =>
    buildRaceKeysQuery({ ...env(), R2_SQL_NAMESPACE: "pc_keiba;drop" }, "20260715"),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  expect(() => buildRaceKeysQuery(env(), "2026-07-15")).toThrow("date must match YYYYMMDD");
  expect(() =>
    buildRaceFeaturesQuery(env(), {
      date: "20260715",
      keibajoCode: "5",
      source: "jra",
    }),
  ).toThrow("keibajoCode must contain two digits");
  expect(() =>
    buildRaceFeaturesQuery(env(), {
      date: "20260715",
      raceBango: "1;",
      source: "jra",
    }),
  ).toThrow("raceBango must contain two digits");
  expect(() =>
    buildFreshRaceEntriesQuery(env(), {
      date: "20260715",
      keibajoCode: "5",
      raceBango: "01",
      source: "jra",
    }),
  ).toThrow("keibajoCode must contain two digits");
});

it("keeps DailyRaceEntryRow normalization in fixed raw-table SQL", () => {
  const sql = buildRaceFeaturesQuery(env(), { date: "20260715", source: "jra" });
  expect(sql).toMatch("coalesce(\n    nullif(btrim(coalesce(ra.kyosomei_hondai, '')), ''),");
  expect(sql).toMatch("try_cast(nullif(se.futan_juryo, '000') AS FLOAT) / 10.0");
  expect(sql).toMatch("try_cast(nullif(se.tansho_odds, '0000') AS FLOAT) / 10.0");
  expect(sql).toMatch("try_cast(nullif(se.kohan_3f, '000') AS FLOAT) / 10.0");
  expect(sql).toMatch("nullif(btrim(coalesce(se.zogen_fugo, '')), '') AS zogen_fugo");
});

it("posts a query to the R2 SQL REST API and returns object rows", async () => {
  const fetchMock = vi.fn<Fetcher>(async () =>
    Response.json({ result: { rows: [{ source: "jra" }] }, success: true }),
  );
  await expect(
    executeR2Sql(env(), "SELECT source FROM pc_keiba.jvd_ra", fetchMock),
  ).resolves.toStrictEqual([{ source: "jra" }]);
  expect(fetchMock).toHaveBeenCalledOnce();
  expect(fetchMock.mock.calls[0]?.[0]).toBe(
    "https://api.sql.cloudflarestorage.com/api/v1/accounts/account-id/r2-sql/query/bucket-name",
  );
  expect(fetchMock.mock.calls[0]?.[1]).toStrictEqual({
    body: '{"query":"SELECT source FROM pc_keiba.jvd_ra","warehouse":"account-id_bucket-name"}',
    headers: {
      Authorization: "Bearer secret-token",
      "Content-Type": "application/json",
    },
    method: "POST",
  });
});

it("rejects R2 SQL HTTP and payload failures", async () => {
  const httpFailure = vi.fn<Fetcher>(async () =>
    Response.json(
      {
        errors: [{ code: 40004, message: "Expected: ), found: when" }],
        query: "SELECT secret_query",
        token: "secret-token",
      },
      { status: 400 },
    ),
  );
  const plainHttpFailure = vi.fn<Fetcher>(async () => new Response("denied", { status: 403 }));
  const malformed = vi.fn<Fetcher>(async () => new Response("not-json"));
  const queryFailure = vi.fn<Fetcher>(async () =>
    Response.json({ errors: [{ code: "80001", message: "edge unavailable" }], success: false }),
  );
  const missingRows = vi.fn<Fetcher>(async () => Response.json({ result: {}, success: true }));
  const invalidRow = vi.fn<Fetcher>(async () =>
    Response.json({ result: { rows: ["bad"] }, success: true }),
  );

  const httpError = await executeR2Sql(env(), "SELECT secret_query", httpFailure).catch(
    (error: unknown) => error,
  );
  expect(httpError).toBeInstanceOf(Error);
  expect(httpError).toBeInstanceOf(R2SqlQueryError);
  expect((httpError as R2SqlQueryError).code).toBe(40004);
  expect(String(httpError)).toContain("R2 SQL HTTP 400: 40004 Expected: ), found: when");
  expect(String(httpError)).not.toContain("secret_query");
  expect(String(httpError)).not.toContain("secret-token");
  await expect(executeR2Sql(env(), "SELECT 1", plainHttpFailure)).rejects.toThrow(
    "R2 SQL HTTP 403",
  );
  await expect(executeR2Sql(env(), "SELECT 1", malformed)).rejects.toThrow(
    "R2 SQL returned malformed JSON",
  );
  await expect(executeR2Sql(env(), "SELECT 1", queryFailure)).rejects.toThrow(
    "R2 SQL query failed: 80001 edge unavailable",
  );
  const queryError = await executeR2Sql(env(), "SELECT 1", queryFailure).catch(
    (error: unknown) => error,
  );
  expect((queryError as R2SqlQueryError).code).toBe("80001");
  await expect(executeR2Sql(env(), "SELECT 1", missingRows)).rejects.toThrow(
    "R2 SQL response has invalid rows",
  );
  await expect(executeR2Sql(env(), "SELECT 1", invalidRow)).rejects.toThrow(
    "R2 SQL response has invalid rows",
  );
});
