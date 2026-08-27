#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "aiohttp>=3.13,<4",
# ]
# ///
"""Tests for the entity-history object uploader."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from upload_entity_history_objects import (
    object_url,
    parse_args,
    required_env,
    select_generations,
)


class EntityHistoryObjectUploaderTest(unittest.TestCase):
    def test_object_url_quotes_bucket_and_key_components(self) -> None:
        self.assertEqual(
            object_url("account", "bucket name", "prefix/a b.json"),
            "https://api.cloudflare.com/client/v4/accounts/account/r2/buckets/"
            "bucket%20name/objects/prefix/a%20b.json",
        )

    def test_parse_args_validates_concurrency(self) -> None:
        args = parse_args(["/tmp/objects", "--concurrency", "25", "--year", "2026"])
        self.assertEqual(args.concurrency, 25)
        self.assertEqual(args.year, [2026])
        with self.assertRaises(SystemExit):
            parse_args(["--concurrency", "0"])
        with self.assertRaises(SystemExit):
            parse_args(["--concurrency", "101"])

    def test_select_generations_bounds_incremental_uploads(self) -> None:
        manifest = {"years": {"2025": "old", "2026": "new"}}
        self.assertEqual(select_generations(manifest, {"2026"}), {"2026": "new"})
        self.assertEqual(select_generations(manifest, None), manifest["years"])
        with self.assertRaisesRegex(ValueError, "2024"):
            select_generations(manifest, {"2024"})
        with self.assertRaisesRegex(TypeError, "malformed"):
            select_generations({"years": {"2026": 1}}, None)

    def test_required_env_uses_first_available_name(self) -> None:
        with patch.dict(os.environ, {"SECOND": "value"}, clear=True):
            self.assertEqual(required_env("FIRST", "SECOND"), "value")
        with (
            patch.dict(os.environ, {}, clear=True),
            self.assertRaisesRegex(RuntimeError, "FIRST, SECOND"),
        ):
            required_env("FIRST", "SECOND")


if __name__ == "__main__":
    unittest.main()
