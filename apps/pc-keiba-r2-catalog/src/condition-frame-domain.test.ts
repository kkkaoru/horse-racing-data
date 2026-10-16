// Run with bun. Execute the shared gate join against adversarial race/provider keys.
import { expect, it } from "vitest";
import { DuckDBInstance } from "@duckdb/node-api";
import { overseasGateJoinSql } from "./win-rate-heatmap-stats";
it("maps raw 1 to gate 16, excludes wrong provider/venue/race and keeps absent history unknown", async () => {
  const db = await DuckDBInstance.create(":memory:");
  const connection = await db.connect();
  try {
    await connection.run(`CREATE SCHEMA pc_keiba;
CREATE TABLE pc_keiba.oversea_runner_source_id (race_source VARCHAR, source VARCHAR, kaisai_nen VARCHAR, kaisai_tsukihi VARCHAR, keibajo_code VARCHAR, race_bango VARCHAR, umaban VARCHAR, gate_number INTEGER);
INSERT INTO pc_keiba.oversea_runner_source_id VALUES
('jra','netkeiba','2026','1004','A8','05','01',16),
('jra','netkeiba','2025','1005','A8','05','01',16),
('jra','jra-van','2025','1005','A8','05','01',1),
('nar','netkeiba','2025','1005','A8','05','01',1),
('jra','netkeiba','2025','1005','A9','05','01',1),
('jra','netkeiba','2025','1005','A8','04','01',1);
CREATE TABLE entries (kaisai_nen VARCHAR, kaisai_tsukihi VARCHAR, keibajo_code VARCHAR, race_bango VARCHAR, umaban VARCHAR, wakuban VARCHAR);
INSERT INTO entries VALUES ('2026','1004','A8','05','01','1'), ('2025','1005','A8','05','01','1'), ('2024','1006','A8','05','01','1'), ('2025','1005','77','05','01','1');`);
    const reader = await connection.runAndReadAll(
      `SELECT se.kaisai_nen, se.keibajo_code, gate_mapping.gate_number FROM entries se ${overseasGateJoinSql({ R2_SQL_NAMESPACE: "pc_keiba", R2_SQL_ACCOUNT_ID: "account", R2_SQL_BUCKET_NAME: "bucket", R2_SQL_TOKEN: "token" }, "jra")} ORDER BY se.kaisai_nen DESC, se.keibajo_code`,
    );
    expect(reader.getRowObjects()).toStrictEqual([
      { kaisai_nen: "2026", keibajo_code: "A8", gate_number: 16 },
      { kaisai_nen: "2025", keibajo_code: "77", gate_number: null },
      { kaisai_nen: "2025", keibajo_code: "A8", gate_number: 16 },
      { kaisai_nen: "2024", keibajo_code: "A8", gate_number: null },
    ]);
  } finally {
    connection.closeSync();
    db.closeSync();
  }
});
