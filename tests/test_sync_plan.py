from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from flylight_cli import cache
from flylight_cli import cli
from flylight_cli import core


FIXTURES = Path(__file__).parent / "fixtures"


def load_json_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class SyncPlanTests(unittest.TestCase):
    def test_cache_info_reports_suffix_counts_and_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "http-cache"
            cache.write_cached_bytes(
                "https://example.org/manifest.json",
                b'{"ok":true}',
                cache_dir=cache_dir,
            )
            cache.write_cached_bytes(
                "https://example.org/listing.xml",
                b"<xml />",
                cache_dir=cache_dir,
            )

            args = argparse.Namespace(cache_dir=cache_dir, json=True)
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_cache_info(args)
            payload = json.loads(stdout.getvalue())

            self.assertEqual(payload["entries"], 2)
            self.assertEqual(payload["suffix_counts"][".json"], 1)
            self.assertEqual(payload["suffix_counts"][".xml"], 1)
            self.assertIsNotNone(payload["oldest_cached_at"])
            self.assertIsNotNone(payload["newest_cached_at"])

    def test_sync_plan_reports_cache_and_incremental_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "plan.sqlite"
            cache_dir = Path(tmpdir) / "http-cache"
            conn = core.connect_db(db_path)
            plan = core.ReleasePlan(
                release="MB Paper 2014",
                source_kind="manifest",
                source_locator="MB Paper 2014/MB_Paper_2014.metadata.json",
                source_token="manifest-token",
                manifest_object={
                    "key": "MB Paper 2014/MB_Paper_2014.metadata.json",
                    "last_modified": "2022-01-18T15:57:49.000Z",
                },
            )

            with mock.patch.object(core, "fetch_json", return_value=load_json_fixture("release_manifest.json")):
                core.sync_release_from_plan(conn, plan, raw_dir=None)

            cache.write_cached_bytes(
                core.s3_url_for_key(plan.manifest_object["key"]),
                json.dumps(load_json_fixture("release_manifest.json")).encode("utf-8"),
                cache_dir=cache_dir,
            )

            args = argparse.Namespace(
                cache_dir=cache_dir,
                offline=False,
                refresh_cache=False,
                db=db_path,
                release=["MB Paper 2014"],
                all=False,
                incremental=True,
                force=False,
                workers=1,
                json=True,
            )
            with mock.patch.object(cli, "plan_release", return_value=plan):
                with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                    cli.cmd_sync_plan(args)
            payload = json.loads(stdout.getvalue())

            self.assertEqual(payload["incremental"], True)
            row = payload["releases"][0]
            self.assertEqual(row["release"], "MB Paper 2014")
            self.assertEqual(row["action"], "skip")
            self.assertEqual(row["reason"], "up_to_date")
            self.assertEqual(row["cache"]["cached_inputs"], 1)
            self.assertEqual(row["cache"]["total_inputs"], 1)
            self.assertEqual(row["source_cache_ready"], True)
            self.assertEqual(row["db"]["source_token_match"], True)
            conn.close()


if __name__ == "__main__":
    unittest.main()
