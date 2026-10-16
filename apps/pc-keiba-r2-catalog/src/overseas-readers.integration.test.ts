// Runs with bun. Executes the reader SQL against saved publication data in memory.
import { DuckDBInstance } from "@duckdb/node-api";
import { expect, it } from "vitest";
import fixture from "./fixtures/overseas-published.json";
import { readRaceRunners } from "./race-runners-read";
import { readOverseasRaceHistory } from "./race-history-read";
import { buildWinRateHeatmapSimilarQuery } from "./win-rate-heatmap-stats";

const jsonSql = (value: unknown): string => `'${JSON.stringify(value).replaceAll("'", "''")}'`;
const projection = (columns: string[]): string =>
  columns.map((column) => `json_extract_string(value, '$.${column}') AS ${column}`).join(", ");

it("round-trips every mapped ID/gate and real history, and selects actual gate targets for stats", async () => {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    await connection.run("CREATE SCHEMA pc_keiba");
    await connection.run("CREATE MACRO btrim(value) AS trim(value)");
    // R2 SQL returns a nullable match; DuckDB exposes a boolean matcher.
    await connection.run(
      "CREATE MACRO regexp_match(value, pattern) AS CASE WHEN regexp_matches(value, pattern) THEN [value] ELSE NULL END",
    );
    const current = fixture.runners;
    const history = current
      .filter((row) => row.umaban === "04" || row.umaban === "05")
      .map((row) => ({
        ...row,
        kaisai_tsukihi: "0901",
        kakutei_chakujun: row.umaban === "04" ? "01" : "02",
      }));
    await connection.run(
      `CREATE TABLE pc_keiba.jvd_se AS SELECT ${projection(Object.keys(current[0] ?? {}))} FROM json_each(${jsonSql([...current, ...history])})`,
    );
    await connection.run(
      `CREATE TABLE pc_keiba.jvd_ra AS SELECT ${projection(Object.keys(fixture.races[0] ?? {}))} FROM json_each(${jsonSql([...fixture.races, ...fixture.races.map((row) => ({ ...row, kaisai_tsukihi: "0901" }))])})`,
    );
    await connection.run(
      "CREATE TABLE pc_keiba.jvd_um (ketto_toroku_bango VARCHAR, ketto_joho_01b VARCHAR, ketto_joho_03b VARCHAR, ketto_joho_05b VARCHAR)",
    );
    await connection.run(
      "CREATE TABLE pc_keiba.oversea_runner_identity (umaban VARCHAR, source VARCHAR, source_horse_id VARCHAR, source_url VARCHAR, horse_name_full VARCHAR, jockey_name_full VARCHAR, trainer_name_full VARCHAR, owner_name_full VARCHAR, race_source VARCHAR, kaisai_nen VARCHAR, kaisai_tsukihi VARCHAR, keibajo_code VARCHAR, race_bango VARCHAR)",
    );
    const historicalMappings = fixture.mappings
      .filter((row) => row.umaban === "04" || row.umaban === "05")
      .map((row) => ({ ...row, kaisai_tsukihi: "0901" }));
    const wrongProvider = fixture.mappings.map((row) => ({
      ...row,
      source: "other",
      gate_number: 99,
      source_horse_id: "wrong00000",
    }));
    const mappingColumns = Object.keys(fixture.mappings[0] ?? {}).filter(
      (column) => column !== "gate_number",
    );
    await connection.run(
      `CREATE TABLE pc_keiba.oversea_runner_source_id AS SELECT ${projection(mappingColumns)}, cast(json_extract_string(value, '$.gate_number') AS SMALLINT) AS gate_number FROM json_each(${jsonSql([...fixture.mappings, ...historicalMappings, ...wrongProvider])})`,
    );
    await connection.run(
      `CREATE TABLE pc_keiba.oversea_horse_race_history AS SELECT 'netkeiba' AS source, json_extract_string(value,'$.sourceHorseId') AS source_horse_id, json_extract_string(value,'$.raceDate') AS race_date, json_extract_string(value,'$.distanceMetres') AS distance_metres FROM json_each(${jsonSql(fixture.histories)})`,
    );
    const query = async (sql: string): Promise<unknown[]> =>
      (await connection.runAndReadAll(sql)).getRowObjects();
    const runners = await readRaceRunners({
      input: {
        namespace: "pc_keiba",
        source: "jra",
        date: "20261004",
        keibajoCode: "A8",
        raceBango: "05",
      },
      query,
    });
    expect(runners.runners).toHaveLength(16);
    expect(runners.sourceMappings?.map((row) => row.gateNumber)).toStrictEqual([
      5, 9, 2, 12, 13, 14, 15, 10, 6, 7, 4, 1, 8, 11, 3, 16,
    ]);
    const histories = await readOverseasRaceHistory({
      input: {
        namespace: "pc_keiba",
        source: "overseas",
        horseIds: fixture.mappings.map((row) => row.source_horse_id),
        beforeDate: "20261004",
        minDate: null,
        limit: 4000,
      },
      query,
    });
    expect(
      fixture.mappings.map(
        (mapping) =>
          histories.filter((row) => row.sourceHorseId === mapping.source_horse_id).length,
      ),
    ).toStrictEqual([11, 10, 10, 15, 17, 16, 14, 13, 18, 9, 5, 8, 6, 7, 7, 7]);
    const statsSql = buildWinRateHeatmapSimilarQuery(
      {
        R2_SQL_ACCOUNT_ID: "offline",
        R2_SQL_BUCKET_NAME: "offline",
        R2_SQL_NAMESPACE: "pc_keiba",
        R2_SQL_TOKEN: "offline",
      },
      {
        source: "jra",
        date: "20261004",
        keibajoCode: "A8",
        raceBango: "05",
        years: 3,
        includeVenue: true,
        includeDistance: false,
        includeSurface: false,
        includeTurn: false,
        includeJockeyFrame: true,
      },
    );
    const stats = (await connection.runAndReadAll(statsSql)).getRowObjects();
    expect(stats.filter((row) => row.kind === "jockeyFrame")).toHaveLength(16);
    expect(stats.find((row) => row.kind === "jockeyFrame" && row.umaban === 4)).toMatchObject({
      starts: 1n,
      wins: 1n,
      places: 1n,
      shows: 1n,
    });
    expect(stats.find((row) => row.kind === "jockeyFrame" && row.umaban === 5)).toMatchObject({
      starts: 1n,
      wins: 0n,
      places: 1n,
      shows: 1n,
    });
    expect(stats.find((row) => row.kind === "jockeyFrame" && row.umaban === 1)).toMatchObject({
      starts: 0n,
      wins: 0n,
      places: 0n,
      shows: 0n,
    });
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
});
