#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5,<1.6",
#   "pyarrow>=19",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///

from __future__ import annotations

import argparse
import contextlib
import io
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pyarrow as pa
from pyiceberg.types import TimestamptzType

import sync_r2_catalog as subject


def sample_data(*, value: tuple[str, ...] = ("a", "b")) -> pa.Table:
    return pa.table(
        {
            "kaisai_nen": pa.array(["2026", "2026"]),
            "kaisai_tsukihi": pa.array(["0715", "0715"]),
            "keibajo_code": pa.array(["42", "43"]),
            "race_bango": pa.array(["01", "02"]),
            "value": pa.array(value),
        }
    )


TEST_SPEC = subject.TableSpec(
    "test_table",
    ("kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango"),
    ("kaisai_nen", "kaisai_tsukihi"),
)
FIVE_YEAR_SCOPE = subject.YearScope("2025", "2030")


class FakeScan:
    def __init__(self, data: pa.Table) -> None:
        self.data = data

    def to_arrow(self) -> pa.Table:
        return self.data

    def to_arrow_batch_reader(self) -> pa.RecordBatchReader:
        return pa.RecordBatchReader.from_batches(
            self.data.schema, self.data.to_batches()
        )


class FakeSnapshot:
    snapshot_id = 1234


class FakeTable:
    def __init__(self, data: pa.Table, spec: subject.TableSpec = TEST_SPEC) -> None:
        normalized = subject.require_primary_key_fields(data, spec)
        self._schema = subject.build_iceberg_schema(normalized, spec)
        self._spec = subject.build_partition_spec(self._schema, spec)
        self.data = subject.schema_to_pyarrow(self._schema).empty_table()
        self.refresh_calls = 0
        self.dynamic_calls = 0
        self.overwrite_calls = 0
        self.delete_calls = 0
        self.append_calls = 0
        self.events: list[str] = []

    def schema(self):
        return self._schema

    def spec(self):
        return self._spec

    def refresh(self) -> None:
        self.refresh_calls += 1

    def scan(self, **_kwargs):
        self.events.append("scan")
        return FakeScan(self.data)

    def dynamic_partition_overwrite(self, data, **_kwargs) -> None:
        self.dynamic_calls += 1
        self.events.append("dynamic_partition_overwrite")
        self.data = data

    def overwrite(self, data, **_kwargs) -> None:
        self.overwrite_calls += 1
        self.events.append("overwrite")
        self.data = data

    def delete(self, _filter, **_kwargs) -> None:
        self.delete_calls += 1
        self.events.append("delete")
        self.data = self.data.slice(0, 0)

    def append(self, data, **_kwargs) -> None:
        self.append_calls += 1
        self.data = pa.concat_tables([self.data, data])

    def current_snapshot(self):
        return FakeSnapshot()


class FakeCatalog:
    def __init__(self, table: FakeTable | None = None) -> None:
        self.table = table
        self.created = False

    def table_exists(self, _identifier: str) -> bool:
        return self.table is not None

    def load_table(self, _identifier: str) -> FakeTable:
        assert self.table is not None
        return self.table

    def create_table(self, _identifier: str, *, schema, partition_spec, properties):
        self.created = True
        table = FakeTable.__new__(FakeTable)
        table._schema = schema
        table._spec = partition_spec
        table.data = subject.schema_to_pyarrow(schema).empty_table()
        table.refresh_calls = 0
        table.dynamic_calls = 0
        table.overwrite_calls = 0
        table.delete_calls = 0
        table.append_calls = 0
        table.events = []
        self.table = table
        return table


class ValidationTests(unittest.TestCase):
    def test_table_inventory_contains_only_raw_local_postgresql_tables(self) -> None:
        self.assertEqual(
            list(subject.TABLE_SPECS),
            [
                "jvd_se",
                "jvd_ra",
                "jvd_um",
                "jvd_hc",
                "nvd_se",
                "nvd_ra",
                "nvd_um",
                "nvd_nu",
                "jvd_hn",
                "jvd_bt",
            ],
        )
        masters = {spec.name for spec in subject.TABLE_SPECS.values() if spec.is_master}
        self.assertEqual(masters, {"jvd_um", "nvd_um", "nvd_nu", "jvd_hn", "jvd_bt"})

    def test_parse_date_rejects_format_and_calendar_errors(self) -> None:
        self.assertEqual(subject.parse_date("20260715"), "20260715")
        for value in ("2026-07-15", "20260230", "abc"):
            with (
                self.subTest(value=value),
                self.assertRaises(argparse.ArgumentTypeError),
            ):
                subject.parse_date(value)

    def test_parse_tables_allowlists_deduplicates_and_rejects_empty(self) -> None:
        selected = subject.parse_tables("nvd_se,jvd_ra,nvd_se")
        self.assertEqual([spec.name for spec in selected], ["nvd_se", "jvd_ra"])
        with self.assertRaisesRegex(ValueError, "unsupported table"):
            subject.parse_tables("nvd_se;drop table x")
        with self.assertRaisesRegex(ValueError, "non-empty CSV"):
            subject.parse_tables("nvd_se,")

    def test_parse_args_requires_exactly_one_mode(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                subject.parse_args([])
            with self.assertRaises(SystemExit):
                subject.parse_args(["--full", "--date", "20260715"])
        self.assertTrue(subject.parse_args(["--full"]).full)

    def test_settings_use_token_fallback_and_defaults(self) -> None:
        settings = subject.load_settings(
            {
                "CLOUDFLARE_DEBUG_TOKEN": "debug",
                "SOURCE_DATABASE_URL": "postgresql://neon.example/production",
                "NEON_DATABASE_URL": "postgresql://neon.example/production",
            }
        )
        self.assertEqual(settings.token, "debug")
        self.assertEqual(settings.pg_url, subject.DEFAULT_PG_URL)
        self.assertEqual(settings.namespace, "pc_keiba")
        preferred = subject.load_settings(
            {
                "R2_CATALOG_TOKEN": "catalog",
                "CLOUDFLARE_DEBUG_TOKEN": "debug",
                "R2_CATALOG_URI": "https://example.test/catalog",
                "R2_CATALOG_WAREHOUSE": "warehouse",
            }
        )
        self.assertEqual(preferred.token, "catalog")
        self.assertEqual(preferred.catalog_uri, "https://example.test/catalog")

    def test_source_url_must_be_loopback_postgresql(self) -> None:
        for url in (
            "postgresql://user:pass@127.0.0.1:15432/db",
            "postgres://user:pass@localhost:5432/db",
            "postgresql://user:pass@[::1]:5432/db",
        ):
            with self.subTest(url=url):
                subject.validate_local_pg_url(url)
        for url in (
            "postgresql://neon.example/db",
            "postgresql://10.0.0.2/db",
            "https://127.0.0.1/db",
        ):
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, "loopback"):
                subject.validate_local_pg_url(url)

    def test_sync_implementation_has_no_alternate_data_source(self) -> None:
        source = Path(subject.__file__).read_text(encoding="utf-8").lower()
        for forbidden in (
            "neon_database_url",
            "read_parquet",
            "daily_race_entries",
            "features_archive",
            "feature archive",
            "race_entry_corner_features",
            "race_running_style_model_predictions",
            "20260715",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)
        self.assertNotIn('env.get("source_database_url")', source)
        self.assertNotIn(".read_all(", source)
        self.assertNotIn("merge_date_into_scope", source)

    def test_source_query_uses_bound_date_and_allowlisted_identifier(self) -> None:
        sql, params = subject.source_query(subject.TABLE_SPECS["nvd_se"], "20260715")
        self.assertIn('FROM source_pg.public."nvd_se"', sql)
        self.assertIn('"kaisai_nen" = ?', sql)
        self.assertEqual(params, ["2026", "0715"])
        hc_sql, hc_params = subject.source_query(
            subject.TABLE_SPECS["jvd_hc"], "20260715"
        )
        self.assertIn('"chokyo_nengappi" = ?', hc_sql)
        self.assertEqual(hc_params, ["20260715"])

    def test_source_query_uses_year_equality_or_hc_range(self) -> None:
        sql, params = subject.source_query(
            subject.TABLE_SPECS["nvd_se"], None, target_scope=FIVE_YEAR_SCOPE
        )
        self.assertIn('"kaisai_nen" >= ?', sql)
        self.assertIn('"kaisai_nen" < ?', sql)
        self.assertEqual(params, ["2025", "2030"])
        hc_sql, hc_params = subject.source_query(
            subject.TABLE_SPECS["jvd_hc"], None, target_scope=FIVE_YEAR_SCOPE
        )
        self.assertIn('"chokyo_nengappi" >= ?', hc_sql)
        self.assertIn('"chokyo_nengappi" < ?', hc_sql)
        self.assertEqual(hc_params, ["20250101", "20300101"])
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            subject.source_query(
                subject.TABLE_SPECS["nvd_se"],
                "20260715",
                target_scope=FIVE_YEAR_SCOPE,
            )

    def test_source_years_enumerates_only_distinct_year_chunks(self) -> None:
        executed: list[str] = []

        class Result:
            @staticmethod
            def fetchall() -> list[tuple[str]]:
                return [("2025",), ("2026",)]

        class Connection:
            @staticmethod
            def execute(sql: str) -> Result:
                executed.append(sql)
                return Result()

        self.assertEqual(
            subject.source_years(Connection(), subject.TABLE_SPECS["jvd_hc"]),
            ["2025", "2026"],
        )
        self.assertIn('substring("chokyo_nengappi", 1, 4)', executed[0])

    def test_year_sets_map_to_stable_five_year_scopes(self) -> None:
        scopes = subject.year_scopes(["2004", "2005", "2009", "2010"])
        self.assertEqual(
            [scope.label for scope in scopes],
            ["2000-2004", "2005-2009", "2010-2014"],
        )

    def test_every_transfer_query_reads_only_the_local_pg_allowlist(self) -> None:
        for spec in subject.TABLE_SPECS.values():
            date = None if spec.is_master else "20260715"
            sql, _ = subject.source_query(spec, date)
            with self.subTest(table=spec.name):
                self.assertIn(f'FROM source_pg.public."{spec.name}"', sql)
                self.assertNotIn("read_parquet", sql.lower())
                self.assertNotIn("daily_race_entries", sql.lower())

    def test_record_batch_collection_enforces_memory_safety_limits(self) -> None:
        table = pa.table({"id": [1, 2], "value": ["a", "b"]})
        reader = pa.RecordBatchReader.from_batches(
            table.schema, table.to_batches(max_chunksize=1)
        )
        with self.assertRaisesRegex(RuntimeError, "Arrow safety limit"):
            subject.collect_arrow_batches(
                reader, "master", max_rows=1, max_bytes=10_000
            )
        reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        collected = subject.collect_arrow_batches(
            reader,
            "master",
            max_rows=2,
            max_bytes=10_000,
        )
        self.assertEqual(collected.to_pylist(), table.to_pylist())

    def test_timestamptz_is_normalized_to_utc_without_changing_instants(self) -> None:
        tokyo_type = pa.timestamp("us", tz="Asia/Tokyo")
        source = pa.table(
            {
                "id": pa.array([1, 2]),
                "updated_at": pa.array(
                    [
                        datetime(2026, 7, 15, 12, 34, tzinfo=ZoneInfo("Asia/Tokyo")),
                        None,
                    ],
                    type=tokyo_type,
                ),
                "naive_at": pa.array(
                    [datetime(2026, 7, 15, 12, 34), None],
                    type=pa.timestamp("us"),
                ),
            }
        )
        source_epoch = source["updated_at"].cast(pa.int64()).to_pylist()
        normalized = subject.normalize_timestamptz_to_utc(source)
        self.assertEqual(normalized.schema.field("updated_at").type.tz, "UTC")
        self.assertIsNone(normalized.schema.field("naive_at").type.tz)
        self.assertEqual(
            normalized["updated_at"].cast(pa.int64()).to_pylist(), source_epoch
        )
        self.assertEqual(
            normalized["updated_at"].to_pylist()[0],
            datetime(2026, 7, 15, 3, 34, tzinfo=ZoneInfo("UTC")),
        )
        iceberg_schema = subject.build_iceberg_schema(
            subject.require_primary_key_fields(
                normalized, subject.TableSpec("timestamped", ("id",))
            ),
            subject.TableSpec("timestamped", ("id",)),
        )
        self.assertIsInstance(
            iceberg_schema.find_field("updated_at").field_type, TimestamptzType
        )

    def test_record_batch_collection_normalizes_all_aware_timestamp_columns(
        self,
    ) -> None:
        source = pa.table(
            {
                "tokyo": pa.array([0], type=pa.timestamp("ms", tz="Asia/Tokyo")),
                "etc_utc": pa.array([0], type=pa.timestamp("ns", tz="Etc/UTC")),
            }
        )
        reader = pa.RecordBatchReader.from_batches(source.schema, source.to_batches())
        collected = subject.collect_arrow_batches(
            reader,
            "timestamps",
            max_rows=1,
            max_bytes=10_000,
        )
        self.assertEqual(
            collected.schema.field("tokyo").type, pa.timestamp("ms", tz="UTC")
        )
        self.assertEqual(
            collected.schema.field("etc_utc").type, pa.timestamp("ns", tz="UTC")
        )

    def test_primary_key_validation_detects_null_and_duplicate(self) -> None:
        duplicate = pa.table({"id": ["a", "a"], "value": [1, 2]})
        spec = subject.TableSpec("duplicates", ("id",))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            subject.validate_primary_key(duplicate, spec)
        null = pa.table({"id": pa.array(["a", None]), "value": [1, 2]})
        with self.assertRaisesRegex(ValueError, "NULL"):
            subject.validate_primary_key(null, spec)

    def test_fingerprint_is_order_independent_and_data_sensitive(self) -> None:
        original = pa.table({"id": [2, 1], "value": ["b", "a"]})
        reordered = pa.table({"id": [1, 2], "value": ["a", "b"]})
        changed = pa.table({"id": [1, 2], "value": ["a", "c"]})
        self.assertEqual(
            subject.arrow_fingerprint(original, ("id",)),
            subject.arrow_fingerprint(reordered, ("id",)),
        )
        self.assertNotEqual(
            subject.arrow_fingerprint(original, ("id",)),
            subject.arrow_fingerprint(changed, ("id",)),
        )

    def test_schema_has_identifiers_and_identity_partitions(self) -> None:
        normalized = subject.require_primary_key_fields(sample_data(), TEST_SPEC)
        schema = subject.build_iceberg_schema(normalized, TEST_SPEC)
        identifiers = {
            schema.find_field(field_id).name for field_id in schema.identifier_field_ids
        }
        self.assertEqual(identifiers, set(TEST_SPEC.primary_key))
        partition_spec = subject.build_partition_spec(schema, TEST_SPEC)
        self.assertEqual(
            tuple(field.name for field in partition_spec.fields), ("kaisai_nen",)
        )
        self.assertIsInstance(
            partition_spec.fields[0].transform, subject.IdentityTransform
        )

    def test_jvd_hc_uses_stable_truncate_four_partition(self) -> None:
        spec = subject.TABLE_SPECS["jvd_hc"]
        data = pa.table(
            {
                "tracen_kubun": ["1"],
                "chokyo_nengappi": ["20260715"],
                "chokyo_jikoku": ["0630"],
                "ketto_toroku_bango": ["2020100001"],
            }
        )
        schema = subject.build_iceberg_schema(
            subject.require_primary_key_fields(data, spec), spec
        )
        partition_spec = subject.build_partition_spec(schema, spec)
        self.assertEqual(partition_spec.fields[0].name, "chokyo_nengappi_year")
        self.assertIsInstance(
            partition_spec.fields[0].transform, subject.TruncateTransform
        )
        self.assertEqual(partition_spec.fields[0].transform.width, 4)

    def test_redaction_hides_tokens_and_postgres_urls(self) -> None:
        settings = subject.Settings(
            "postgresql://u:p@host/db", "uri", "wh", "ns", "secret"
        )
        redacted = subject.redact_error(
            RuntimeError("secret postgresql://u:p@host/db connection failed"), settings
        )
        self.assertNotIn("secret", redacted)
        self.assertNotIn("u:p", redacted)
        self.assertIn("[redacted]", redacted)


class SyncTests(unittest.TestCase):
    def test_first_date_sync_creates_table_and_dynamic_overwrites(self) -> None:
        catalog = FakeCatalog()
        result = subject.sync_table(
            catalog,
            "pc_keiba",
            TEST_SPEC,
            sample_data(),
            full=False,
            target_date="20260715",
            target_scope=subject.single_year_scope("2026"),
            run_id="run",
        )
        self.assertTrue(catalog.created)
        self.assertEqual(catalog.table.dynamic_calls, 1)
        self.assertEqual(result.status, "replaced")
        self.assertEqual(result.target_rows, 2)
        self.assertEqual(result.snapshot_id, 1234)

    def test_matching_year_is_still_replaced_from_local_postgres(self) -> None:
        table = FakeTable(sample_data())
        table.data = subject.conform_to_table(sample_data(), table, TEST_SPEC)
        catalog = FakeCatalog(table)
        result = subject.sync_table(
            catalog,
            "pc_keiba",
            TEST_SPEC,
            sample_data(),
            full=False,
            target_date="20260715",
            target_scope=subject.single_year_scope("2026"),
            run_id="run",
        )
        self.assertEqual(result.status, "replaced")
        self.assertEqual(table.dynamic_calls, 1)
        self.assertGreaterEqual(table.refresh_calls, 1)

    def test_changed_partition_uses_dynamic_overwrite(self) -> None:
        table = FakeTable(sample_data())
        table.data = subject.conform_to_table(
            sample_data(value=("old", "b")), table, TEST_SPEC
        )
        result = subject.sync_table(
            FakeCatalog(table),
            "pc_keiba",
            TEST_SPEC,
            sample_data(),
            full=False,
            target_date="20260715",
            target_scope=subject.single_year_scope("2026"),
            run_id="run",
        )
        self.assertEqual(table.dynamic_calls, 1)
        self.assertEqual(table.overwrite_calls, 0)
        self.assertEqual(result.source_fingerprint, result.target_fingerprint)

    def test_five_year_scope_replaces_all_year_partitions_in_one_predicate_overwrite(
        self,
    ) -> None:
        annual = sample_data().set_column(0, "kaisai_nen", pa.array(["2026", "2027"]))
        annual = annual.set_column(1, "kaisai_tsukihi", pa.array(["0101", "0715"]))
        stale = sample_data().set_column(0, "kaisai_nen", pa.array(["2028", "2028"]))
        table = FakeTable(stale)
        table.data = subject.conform_to_table(stale, table, TEST_SPEC)
        catalog = FakeCatalog(table)
        result = subject.sync_table(
            catalog,
            "pc_keiba",
            TEST_SPEC,
            annual,
            full=False,
            target_date=None,
            target_scope=FIVE_YEAR_SCOPE,
            run_id="run",
            max_rows=subject.FIVE_YEAR_MAX_ROWS,
            max_bytes=subject.FIVE_YEAR_MAX_BYTES,
        )
        self.assertEqual(catalog.table.overwrite_calls, 1)
        self.assertEqual(catalog.table.dynamic_calls, 0)
        self.assertEqual(result.target_rows, 2)
        self.assertEqual(result.source_fingerprint, result.target_fingerprint)
        self.assertEqual(catalog.table.data.to_pylist(), annual.to_pylist())

    def test_empty_five_year_scope_deletes_target_only_scope_once(self) -> None:
        annual = sample_data().set_column(
            1,
            "kaisai_tsukihi",
            pa.array(["0101", "0715"]),
        )
        table = FakeTable(annual)
        table.data = subject.conform_to_table(annual, table, TEST_SPEC)
        result = subject.sync_table(
            FakeCatalog(table),
            "pc_keiba",
            TEST_SPEC,
            annual.slice(0, 0),
            full=False,
            target_date=None,
            target_scope=FIVE_YEAR_SCOPE,
            run_id="run",
            max_rows=subject.FIVE_YEAR_MAX_ROWS,
            max_bytes=subject.FIVE_YEAR_MAX_BYTES,
        )
        self.assertEqual(table.delete_calls, 1)
        self.assertEqual(result.target_rows, 0)

    def test_date_sync_replaces_year_from_pg_before_reading_catalog(self) -> None:
        target = sample_data().set_column(
            1, "kaisai_tsukihi", pa.array(["0713", "0713"])
        )
        table = FakeTable(target)
        table.data = subject.conform_to_table(target, table, TEST_SPEC)
        local_year = sample_data().set_column(
            1, "kaisai_tsukihi", pa.array(["0714", "0715"])
        )
        result = subject.sync_table(
            FakeCatalog(table),
            "pc_keiba",
            TEST_SPEC,
            local_year,
            full=False,
            target_date="20260715",
            target_scope=subject.single_year_scope("2026"),
            run_id="run",
        )
        self.assertEqual(table.dynamic_calls, 1)
        self.assertEqual(result.target_rows, 2)
        self.assertEqual(table.data.to_pylist(), local_year.to_pylist())
        self.assertEqual(table.events, ["dynamic_partition_overwrite", "scan"])

    def test_date_sync_rejects_source_rows_outside_containing_year(self) -> None:
        escaped = sample_data().set_column(0, "kaisai_nen", pa.array(["2026", "2027"]))
        catalog = FakeCatalog()
        with self.assertRaisesRegex(ValueError, "escaped year scope 2026-2026"):
            subject.sync_table(
                catalog,
                "pc_keiba",
                TEST_SPEC,
                escaped,
                full=False,
                target_date="20260715",
                target_scope=subject.single_year_scope("2026"),
                run_id="run",
            )
        self.assertEqual(catalog.table.dynamic_calls, 0)

    def test_jvd_hc_scope_uses_predicate_overwrite_not_dynamic(self) -> None:
        spec = subject.TABLE_SPECS["jvd_hc"]
        data = pa.table(
            {
                "tracen_kubun": ["1"],
                "chokyo_nengappi": ["20260715"],
                "chokyo_jikoku": ["0630"],
                "ketto_toroku_bango": ["2020100001"],
            }
        )
        catalog = FakeCatalog()
        subject.sync_table(
            catalog,
            "pc_keiba",
            spec,
            data,
            full=False,
            target_date=None,
            target_scope=FIVE_YEAR_SCOPE,
            run_id="run",
        )
        self.assertEqual(catalog.table.overwrite_calls, 1)
        self.assertEqual(catalog.table.dynamic_calls, 0)

    def test_target_years_collapses_partition_metadata(self) -> None:
        table = FakeTable(sample_data())
        partitions = pa.Table.from_pylist(
            [
                {
                    "partition": {
                        "kaisai_nen": "2025",
                    }
                },
                {
                    "partition": {
                        "kaisai_nen": "2026",
                    }
                },
                {
                    "partition": {
                        "kaisai_nen": "2026",
                    }
                },
            ]
        )
        table.inspect = SimpleNamespace(partitions=lambda: partitions)
        self.assertEqual(
            subject.target_years(FakeCatalog(table), "pc_keiba", TEST_SPEC),
            {"2025", "2026"},
        )

    def test_full_sync_uses_full_overwrite(self) -> None:
        table = FakeTable(sample_data())
        table.data = subject.conform_to_table(
            sample_data(value=("old", "b")), table, TEST_SPEC
        )
        subject.sync_table(
            FakeCatalog(table),
            "pc_keiba",
            TEST_SPEC,
            sample_data(),
            full=True,
            target_date=None,
            run_id="run",
        )
        self.assertEqual(table.overwrite_calls, 1)
        self.assertEqual(table.dynamic_calls, 0)

    def test_empty_date_partition_deletes_stale_target(self) -> None:
        table = FakeTable(sample_data())
        table.data = subject.conform_to_table(sample_data(), table, TEST_SPEC)
        empty = sample_data().slice(0, 0)
        result = subject.sync_table(
            FakeCatalog(table),
            "pc_keiba",
            TEST_SPEC,
            empty,
            full=False,
            target_date="20260715",
            target_scope=subject.single_year_scope("2026"),
            run_id="run",
        )
        self.assertEqual(table.delete_calls, 1)
        self.assertEqual(result.target_rows, 0)

    def test_existing_identifier_mismatch_stops_sync(self) -> None:
        table = FakeTable(sample_data())
        table._schema = subject.Schema(*table._schema.fields, identifier_field_ids=[])
        with self.assertRaisesRegex(ValueError, "identifier fields differ"):
            subject.sync_table(
                FakeCatalog(table),
                "pc_keiba",
                TEST_SPEC,
                sample_data(),
                full=False,
                target_date="20260715",
                target_scope=subject.single_year_scope("2026"),
                run_id="run",
            )

    def test_manifest_contains_snapshot_fingerprints_and_pk(self) -> None:
        result = subject.SyncResult("nvd_se", "replaced", 502, 502, "abc", "abc", 99)
        data = subject.manifest_arrow(
            result,
            subject.TABLE_SPECS["nvd_se"],
            run_id="run",
            mode="date",
            target_date="20260715",
        )
        row = data.to_pylist()[0]
        self.assertEqual(row["snapshot_id"], 99)
        self.assertEqual(row["source_fingerprint"], "abc")
        self.assertIn("ketto_toroku_bango", row["primary_key"])

    def test_five_year_scope_is_recorded_in_snapshot_and_manifest(self) -> None:
        properties = subject.snapshot_properties(
            "run",
            subject.TABLE_SPECS["nvd_se"],
            "year",
            None,
            "abc",
            100,
            target_scope=FIVE_YEAR_SCOPE,
        )
        self.assertEqual(properties["sync.partition-year-scope"], "2025-2029")
        result = subject.SyncResult("nvd_se", "replaced", 100, 100, "abc", "abc", 99)
        manifest = subject.manifest_arrow(
            result,
            subject.TABLE_SPECS["nvd_se"],
            run_id="run",
            mode="full-5year",
            target_date="2025-2029",
        ).to_pylist()[0]
        self.assertEqual(manifest["mode"], "full-5year")
        self.assertEqual(manifest["partition_date"], "2025-2029")

    def test_first_manifest_creation_marks_identifier_required(self) -> None:
        catalog = FakeCatalog()
        result = subject.SyncResult("nvd_se", "replaced", 502, 502, "abc", "abc", 99)
        subject.append_manifest(
            catalog,
            "pc_keiba",
            result,
            subject.TABLE_SPECS["nvd_se"],
            run_id="run",
            mode="date",
            target_date="20260715",
        )
        self.assertIsNotNone(catalog.table)
        schema = catalog.table.schema()
        identifier = schema.find_field("manifest_id")
        self.assertTrue(identifier.required)

    def test_create_catalog_requires_token(self) -> None:
        settings = subject.Settings("pg", "uri", "warehouse", "pc_keiba", None)
        with self.assertRaisesRegex(ValueError, "R2_CATALOG_TOKEN"):
            subject.create_catalog(settings)

    def test_create_catalog_uses_bounded_r2_transport_timeouts(self) -> None:
        settings = subject.Settings("pg", "uri", "warehouse", "pc_keiba", "token")
        with patch.object(subject, "load_catalog", return_value="catalog") as loader:
            self.assertEqual(subject.create_catalog(settings), "catalog")
        loader.assert_called_once_with(
            "r2_pc_keiba",
            type="rest",
            uri="uri",
            warehouse="warehouse",
            token="token",
            **{
                "s3.connect-timeout": "30",
                "s3.request-timeout": "120",
            },
        )

    def test_main_returns_nonzero_and_redacts_failure(self) -> None:
        settings = subject.Settings(
            "postgresql://u:p@host/db", "uri", "wh", "ns", "token"
        )
        output = io.StringIO()
        with (
            patch.object(subject, "load_settings", return_value=settings),
            patch.object(subject, "run", side_effect=RuntimeError("token failed")),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(subject.main(["--full"]), 1)
        self.assertNotIn("token failed", output.getvalue())
        self.assertIn("[redacted] failed", output.getvalue())

    def test_date_mode_extracts_whole_local_pg_year(self) -> None:
        class Connection:
            closed = False

            def close(self) -> None:
                self.closed = True

        connection = Connection()
        calls: list[tuple[str | None, subject.YearScope | None]] = []

        def extract_source(
            _connection,
            _spec,
            target_date,
            *,
            target_scope,
            max_rows,
            max_bytes,
        ):
            self.assertEqual(max_rows, subject.PARTITION_MAX_ROWS)
            self.assertEqual(max_bytes, subject.PARTITION_MAX_BYTES)
            calls.append((target_date, target_scope))
            return sample_data()

        args = SimpleNamespace(
            full=False, date="20260715", tables="nvd_ra", dry_run=True
        )
        settings = subject.Settings("pg", "uri", "wh", "ns", None)
        with (
            patch.object(subject, "connect_source", return_value=connection),
            patch.object(subject, "extract_source", side_effect=extract_source),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(subject.run(args, settings), 0)
        self.assertEqual(calls, [(None, subject.single_year_scope("2026"))])
        self.assertTrue(connection.closed)

    def test_full_mode_extracts_stable_five_year_scopes(self) -> None:
        class Connection:
            def close(self) -> None:
                pass

        calls: list[subject.YearScope | None] = []

        def extract_source(
            _connection,
            _spec,
            target_date,
            *,
            target_scope,
            max_rows,
            max_bytes,
        ):
            self.assertIsNone(target_date)
            self.assertEqual(max_rows, subject.FIVE_YEAR_MAX_ROWS)
            self.assertEqual(max_bytes, subject.FIVE_YEAR_MAX_BYTES)
            calls.append(target_scope)
            return sample_data().slice(0, 0)

        args = SimpleNamespace(full=True, date=None, tables="nvd_ra", dry_run=True)
        settings = subject.Settings("pg", "uri", "wh", "ns", None)
        output = io.StringIO()
        with (
            patch.object(subject, "connect_source", return_value=Connection()),
            patch.object(
                subject,
                "source_years",
                return_value=["2024", "2025", "2029", "2030"],
            ),
            patch.object(subject, "extract_source", side_effect=extract_source),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(subject.run(args, settings), 0)
        self.assertEqual(
            calls,
            [
                subject.YearScope("2020", "2025"),
                subject.YearScope("2025", "2030"),
                subject.YearScope("2030", "2035"),
            ],
        )
        self.assertEqual(output.getvalue().count('"mode": "full-5year"'), 3)

    def test_date_mode_skips_masters_and_rejects_master_only(self) -> None:
        args = SimpleNamespace(
            full=False, date="20260715", tables="jvd_um", dry_run=True
        )
        settings = subject.Settings("pg", "uri", "wh", "ns", None)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaisesRegex(ValueError, "masters require --full"):
                subject.run(args, settings)
        self.assertIn("skip_master_requires_full", output.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
