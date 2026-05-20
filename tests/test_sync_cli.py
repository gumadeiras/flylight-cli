from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from flylight_cli import cli
from flylight_cli import core


FIXTURES = Path(__file__).parent / "fixtures"


def load_json_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class SyncCliTests(unittest.TestCase):
    def test_cmd_sync_refresh_cache_does_not_skip_matching_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "refresh.sqlite"
            raw_dir = Path(tmpdir) / "raw"
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
                core.sync_release_from_plan(conn, plan, raw_dir=raw_dir)
            conn.close()

            args = argparse.Namespace(
                cache_dir=Path(tmpdir) / "cache",
                offline=False,
                refresh_cache=True,
                db=db_path,
                raw_dir=raw_dir,
                no_raw=False,
                release=["MB Paper 2014"],
                all=False,
                incremental=True,
                force=False,
                json=True,
                verbose=False,
            )
            with mock.patch.object(cli, "plan_release", return_value=plan):
                result = {"release": "MB Paper 2014", "source_kind": "manifest", "lines": 1, "images": 1}
                with mock.patch.object(cli, "sync_release_from_plan", return_value=result) as sync:
                    with mock.patch("sys.stdout", new_callable=io.StringIO):
                        with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
                            cli.cmd_sync(args)
            sync.assert_called_once()
            progress = stderr.getvalue()
            self.assertIn("selecting releases", progress)
            self.assertIn("planning MB Paper 2014", progress)
            self.assertIn("syncing MB Paper 2014", progress)
            self.assertIn("synced MB Paper 2014", progress)


if __name__ == "__main__":
    unittest.main()
