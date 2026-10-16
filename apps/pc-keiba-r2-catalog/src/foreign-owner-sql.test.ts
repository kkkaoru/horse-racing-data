// Run with bun. Local SQL fixtures; no network or persisted data.
import { DuckDBInstance } from "@duckdb/node-api";
import { expect, test } from "vitest";
import { buildForeignOwnerQuery, normaliseForeignOwnerRows } from "./foreign-owner-reader";

test("executes cohort edges, cancellations, duplicates, missing sources and conflicts", async () => {
  const db = await DuckDBInstance.create(":memory:");
  const connection = await db.connect();
  try {
    await connection.run("CREATE SCHEMA pc_keiba");
    await connection.run(
      "CREATE MACRO regexp_match(value, pattern) AS CASE WHEN regexp_matches(value, pattern) THEN [value] ELSE NULL END",
    );
    await connection.run(`CREATE TABLE pc_keiba.oversea_runner_source_id AS
      SELECT 'netkeiba' AS source, 'jra' AS race_source, '2026' AS kaisai_nen, '1004' AS kaisai_tsukihi, 'A8' AS keibajo_code, '05' AS race_bango, umaban, source_owner_id
      FROM (VALUES ('01','a002e7'),('02','000000'),('03','bbbbbb'),('04','cccccc'),('05','dddddd'),('06','eeeeee'),('13',NULL),('1','a002e7'),('17','a002e7')) t(umaban, source_owner_id)`);
    await connection.run(`CREATE TABLE pc_keiba.oversea_person_race_history AS
      SELECT 'netkeiba' AS source, 'owner' AS person_kind, 'a002e7' AS source_person_id,
      id AS source_race_id, 'horse' AS source_horse_id, 'Horse' AS horse_name,
      CAST(d AS DATE) AS race_date, venue, '芝' AS surface, title AS race_name, finish AS finish_position
      FROM (VALUES
        ('old','1996-10-03','パリロンシャン','凱旋門賞(GI)',1),
        ('edge30','1996-10-04','パリロンシャン','凱旋門賞(GI)',2),
        ('before10','2016-10-03','パリロンシャン','凱旋門賞(GI)',3),
        ('edge10','2016-10-04','パリロンシャン','凱旋門賞(GI)',1),
        ('edge10','2016-10-04','パリロンシャン','凱旋門賞(GI)',1),
        ('edge10','2016-10-04',NULL,'Cancelled',0),
        ('other','2025-10-01','パリロンシャン','オペラ賞(GI)',2),
        ('nullvenue','2025-10-02',NULL,'凱旋門賞(GI)',3),
        ('alias','2025-10-03','Longchamp','凱旋門賞(GI)',4),
        ('english','2025-10-04','パリロンシャン','Prix de l Arc',4),
        ('today','2026-10-04','パリロンシャン','凱旋門賞(GI)',1),
        ('future','2026-10-05','パリロンシャン','凱旋門賞(GI)',1),
        ('cancel','2025-10-05','パリロンシャン','凱旋門賞(GI)',0)
      ) t(id,d,venue,title,finish)`);
    await connection.run(
      "INSERT INTO pc_keiba.oversea_person_race_history VALUES ('netkeiba','owner','bbbbbb','zero-wins','verifiedhorse','Horse',DATE '2025-10-03','パリロンシャン','芝','凱旋門賞',4)",
    );
    const sql: string = buildForeignOwnerQuery({
      R2_SQL_NAMESPACE: "pc_keiba",
      R2_SQL_ACCOUNT_ID: "a",
      R2_SQL_BUCKET_NAME: "b",
      R2_SQL_TOKEN: "t",
    });
    const result = normaliseForeignOwnerRows(
      (await connection.runAndReadAll(sql)).getRowObjectsJson(),
    );
    expect(result.similarRows.map((r) => [r.umaban, r.statsScope, r.starts, r.wins])).toStrictEqual(
      [
        [1, "stored-results-10y", 3, 1],
        [1, "stored-results-race-10y", 1, 1],
        [3, "stored-results-10y", 1, 0],
        [3, "stored-results-race-10y", 1, 0],
      ],
    );
    expect(result.foreignOwnerVenueRows.map((r) => [r.umaban, r.starts])).toStrictEqual([
      [1, 3],
      [3, 1],
    ]);
    expect(result.foreignOwnerVenueRows.map((r) => [r.umaban, r.wins])).toStrictEqual([
      [1, 1],
      [3, 0],
    ]);
    await connection.run(
      "INSERT INTO pc_keiba.oversea_person_race_history SELECT source,person_kind,source_person_id,source_race_id,source_horse_id,horse_name,race_date,venue,surface,'オペラ賞(GI)',finish_position FROM pc_keiba.oversea_person_race_history WHERE source_race_id='edge10' AND finish_position=1 LIMIT 1",
    );
    const conflict = (await connection.runAndReadAll(sql)).getRowObjectsJson();
    expect(() => normaliseForeignOwnerRows(conflict)).toThrow("Invalid foreign owner aggregate");
  } finally {
    connection.closeSync();
    db.closeSync();
  }
});
