import { expect, it } from "vitest";
import {
  DEFAULT_INDEX_HEALTH_CONFIG,
  buildAmcheckCallSql,
  buildDedupeUniqueHeapSql,
  buildDuplicateCountSql,
  buildEnsureAmcheckExtensionSql,
  buildDropOrphanTempRelationSql,
  buildHealthReport,
  buildListBtreeIndexesSql,
  buildListOrphanTempRelationsSql,
  buildReindexIndexSql,
  buildReindexSystemCatalogsSql,
  buildSystemCatalogAmcheckSql,
  buildTableColumnsSql,
  isIndexCorruptionMessage,
  isOrphanTempCorrupt,
  isPriorityTable,
  orderTargetsForCheck,
  parseAmcheckErrorMessage,
  parseIndexTargetRow,
  parsePgTextArray,
  planRepairActions,
  quoteIdentifier,
  quoteLiteral,
  resolveDedupePreferColumn,
  summarizeHealthReport,
  type IndexTarget,
} from "./core";

it("quotes identifiers and literals safely", () => {
  expect(quoteIdentifier("nvd_bn_pk")).toStrictEqual('"nvd_bn_pk"');
  expect(quoteIdentifier('weird"name')).toStrictEqual('"weird""name"');
  expect(quoteLiteral("O'Brien")).toStrictEqual("'O''Brien'");
});

it("detects priority PC-KEIBA ingest tables", () => {
  expect(isPriorityTable("nvd_bn")).toStrictEqual(true);
  expect(isPriorityTable("jvd_ra")).toStrictEqual(true);
  expect(isPriorityTable("race_finish_position_features")).toStrictEqual(false);
});

it("builds amcheck extension and index list SQL", () => {
  expect(buildEnsureAmcheckExtensionSql()).toStrictEqual("CREATE EXTENSION IF NOT EXISTS amcheck;");
  const listSql = buildListBtreeIndexesSql();
  expect(listSql.includes("'public'")).toStrictEqual(true);
  expect(listSql.includes("am.amname = 'btree'")).toStrictEqual(true);
});

it("builds amcheck and reindex SQL without DROP INDEX", () => {
  expect(buildAmcheckCallSql("nvd_bn_pk")).toStrictEqual(
    "SELECT bt_index_check('nvd_bn_pk'::regclass, true);",
  );
  expect(buildAmcheckCallSql("nvd_bn_pk", false)).toStrictEqual(
    "SELECT bt_index_check('nvd_bn_pk'::regclass, false);",
  );
  const reindex = buildReindexIndexSql("nvd_bn_pk");
  expect(reindex).toStrictEqual('REINDEX INDEX "nvd_bn_pk";');
  expect(reindex.includes("DROP")).toStrictEqual(false);
});

it("builds system catalog amcheck only for known catalog indexes", () => {
  expect(buildSystemCatalogAmcheckSql("pg_class_oid_index")).toStrictEqual(
    "SELECT bt_index_check('pg_class_oid_index'::regclass, true);",
  );
  expect(buildSystemCatalogAmcheckSql("nvd_bn_pk")).toStrictEqual(null);
  expect(buildReindexSystemCatalogsSql()).toStrictEqual([
    'REINDEX INDEX "pg_class_oid_index";',
    'REINDEX INDEX "pg_class_relname_nsp_index";',
    'REINDEX INDEX "pg_class_tblspc_relfilenode_index";',
  ]);
});

it("builds duplicate and dedupe SQL for unique indexes only", () => {
  const uniqueTarget: IndexTarget = {
    schemaName: "public",
    tableName: "nvd_bn",
    indexName: "nvd_bn_pk",
    kind: "unique",
    keyColumns: ["banushi_code", "banushimei"],
  };
  const nonUnique: IndexTarget = {
    ...uniqueTarget,
    kind: "nonunique",
    indexName: "nvd_bn_lookup",
  };
  expect(buildDuplicateCountSql(nonUnique)).toStrictEqual(null);
  expect(buildDedupeUniqueHeapSql(nonUnique, null)).toStrictEqual(null);

  const countSql = buildDuplicateCountSql(uniqueTarget);
  expect(countSql?.includes('GROUP BY "banushi_code", "banushimei"')).toStrictEqual(true);

  const dedupeWithPrefer = buildDedupeUniqueHeapSql(uniqueTarget, "data_sakusei_nengappi");
  expect(dedupeWithPrefer?.includes('"data_sakusei_nengappi" DESC NULLS LAST')).toStrictEqual(true);
  expect(dedupeWithPrefer?.includes("rn > 1")).toStrictEqual(true);
  expect(dedupeWithPrefer?.includes("DROP INDEX")).toStrictEqual(false);

  const dedupeCtidOnly = buildDedupeUniqueHeapSql(uniqueTarget, null);
  expect(dedupeCtidOnly?.includes("ORDER BY ctid DESC")).toStrictEqual(true);
});

it("resolves preferred newest column from available columns", () => {
  expect(resolveDedupePreferColumn(["banushi_code", "data_sakusei_nengappi"])).toStrictEqual(
    "data_sakusei_nengappi",
  );
  expect(resolveDedupePreferColumn(["banushi_code"])).toStrictEqual(null);
  expect(
    resolveDedupePreferColumn(
      ["updated_at"],
      DEFAULT_INDEX_HEALTH_CONFIG.dedupePreferNewestColumns,
    ),
  ).toStrictEqual("updated_at");
});

it("parses amcheck errors and corruption messages", () => {
  expect(
    parseAmcheckErrorMessage(
      'ERROR:  high key invariant violated for index "nvd_bn_pk"\nDETAIL:  Index tid=(94,20) points to index tid=(34,2)',
    ),
  ).toStrictEqual("Index tid=(94,20) points to index tid=(34,2)");
  expect(parseAmcheckErrorMessage("ERROR:  boom")).toStrictEqual("boom");
  expect(parseAmcheckErrorMessage("DETAIL:")).toStrictEqual("DETAIL:");
  expect(parseAmcheckErrorMessage("ERROR:")).toStrictEqual("ERROR:");
  expect(parseAmcheckErrorMessage("plain")).toStrictEqual("plain");

  expect(isIndexCorruptionMessage("high key invariant violated for index nvd_bn_pk")).toStrictEqual(
    true,
  );
  expect(
    isIndexCorruptionMessage(
      "table tid from new index tuple overlaps with invalid duplicate tuple",
    ),
  ).toStrictEqual(true);
  expect(isIndexCorruptionMessage("relation does not exist")).toStrictEqual(false);
});

it("detects corrupt orphan temp relations", () => {
  expect(isOrphanTempCorrupt(2, 0)).toStrictEqual(true);
  expect(isOrphanTempCorrupt(2, 2)).toStrictEqual(false);
  expect(isOrphanTempCorrupt(0, 0)).toStrictEqual(false);
  expect(buildListOrphanTempRelationsSql().includes("pg_temp_%")).toStrictEqual(true);
  expect(buildDropOrphanTempRelationSql(1874457).includes("DELETE FROM pg_class")).toStrictEqual(
    true,
  );
});

it("plans repair order: orphans, catalogs, dedupe, then REINDEX", () => {
  const actions = planRepairActions({
    orphanTempRelations: [
      { oid: 1, relname: "hand_ingest_insert_counts", relnatts: 2, attrCount: 0 },
    ],
    systemCatalogFailures: [
      {
        indexName: "pg_class_oid_index",
        tableName: "pg_class",
        message: "lacks matching index tuple",
      },
    ],
    duplicateGroups: [
      {
        tableName: "nvd_bn",
        indexName: "nvd_bn_pk",
        keyColumns: ["banushi_code", "banushimei"],
        duplicateRowCount: 13,
      },
    ],
    amcheckFailures: [
      { indexName: "nvd_bn_pk", tableName: "nvd_bn", message: "high key invariant violated" },
      { indexName: "nvd_bn_pk", tableName: "nvd_bn", message: "duplicate report" },
      { indexName: "nvd_um_pk", tableName: "nvd_um", message: "item order invariant violated" },
    ],
    preferNewestByColumnByTable: { nvd_bn: "data_sakusei_nengappi" },
  });

  expect(actions.map((a) => a.type)).toStrictEqual([
    "drop_orphan_temp_relation",
    "reindex_system_catalogs",
    "dedupe_unique_heap",
    "reindex_index",
    "reindex_index",
  ]);
  expect(actions[2]).toStrictEqual({
    type: "dedupe_unique_heap",
    tableName: "nvd_bn",
    indexName: "nvd_bn_pk",
    keyColumns: ["banushi_code", "banushimei"],
    preferNewestByColumn: "data_sakusei_nengappi",
  });
  expect(actions[3]).toStrictEqual({
    type: "reindex_index",
    indexName: "nvd_bn_pk",
    tableName: "nvd_bn",
  });
});

it("skips zero-count duplicates and healthy reports", () => {
  const healthy = buildHealthReport({
    checkedIndexCount: 10,
    amcheckFailures: [],
    duplicateGroups: [
      {
        tableName: "nvd_bn",
        indexName: "nvd_bn_pk",
        keyColumns: ["banushi_code"],
        duplicateRowCount: 0,
      },
    ],
    orphanTempRelations: [{ oid: 9, relname: "tmp", relnatts: 1, attrCount: 1 }],
    systemCatalogFailures: [],
    preferNewestByColumnByTable: {},
  });
  expect(healthy.healthy).toStrictEqual(true);
  expect(healthy.repairActions).toStrictEqual([]);
  expect(summarizeHealthReport(healthy)).toStrictEqual(
    "index-health: OK (checked 10 btree indexes)",
  );

  const unhealthy = buildHealthReport({
    checkedIndexCount: 3,
    amcheckFailures: [{ indexName: "nvd_bn_pk", tableName: "nvd_bn", message: "bad" }],
    duplicateGroups: [],
    orphanTempRelations: [],
    systemCatalogFailures: [],
    preferNewestByColumnByTable: {},
  });
  expect(unhealthy.healthy).toStrictEqual(false);
  expect(summarizeHealthReport(unhealthy).includes("UNHEALTHY")).toStrictEqual(true);
  expect(summarizeHealthReport(unhealthy).includes("repair_actions=1")).toStrictEqual(true);
});

it("orders priority tables ahead of others for checking", () => {
  const targets: IndexTarget[] = [
    {
      schemaName: "public",
      tableName: "zzz_other",
      indexName: "zzz_pk",
      kind: "unique",
      keyColumns: ["id"],
    },
    {
      schemaName: "public",
      tableName: "nvd_bn",
      indexName: "nvd_bn_pk",
      kind: "unique",
      keyColumns: ["banushi_code"],
    },
  ];
  expect(orderTargetsForCheck(targets).map((t) => t.tableName)).toStrictEqual([
    "nvd_bn",
    "zzz_other",
  ]);
});

it("parses index rows and postgres text arrays", () => {
  expect(parsePgTextArray("")).toStrictEqual([]);
  expect(parsePgTextArray("   ")).toStrictEqual([]);
  expect(parsePgTextArray("{}")).toStrictEqual([]);
  expect(parsePgTextArray("{banushi_code,banushimei}")).toStrictEqual([
    "banushi_code",
    "banushimei",
  ]);
  expect(parsePgTextArray('{"a b",c}')).toStrictEqual(["a b", "c"]);
  expect(parsePgTextArray("plain")).toStrictEqual(["plain"]);

  expect(
    parseIndexTargetRow({
      schema_name: "public",
      table_name: "nvd_bn",
      index_name: "nvd_bn_pk",
      kind: "unique",
      key_columns: "{banushi_code,banushimei}",
    }),
  ).toStrictEqual({
    schemaName: "public",
    tableName: "nvd_bn",
    indexName: "nvd_bn_pk",
    kind: "unique",
    keyColumns: ["banushi_code", "banushimei"],
  });

  expect(
    parseIndexTargetRow({
      schema_name: "public",
      table_name: "nvd_se",
      index_name: "nvd_se_idx1",
      kind: "nonunique",
      key_columns: ["kaisai_nen"],
    }).kind,
  ).toStrictEqual("nonunique");
});

it("builds table column listing SQL", () => {
  expect(buildTableColumnsSql("public", "nvd_bn").includes("'nvd_bn'")).toStrictEqual(true);
});
