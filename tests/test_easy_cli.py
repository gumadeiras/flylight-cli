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


class EasyCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "easy.sqlite"
        self.conn = core.connect_db(self.db_path)
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
            core.sync_release_from_plan(self.conn, plan, raw_dir=None)

    def tearDown(self) -> None:
        self.conn.close()
        self.tmpdir.cleanup()

    def test_find_images_line_image_release_shortcuts(self) -> None:
        parser = cli.build_parser()

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            args = parser.parse_args(["find", "MB005B", "--db", str(self.db_path), "--json"])
            args.func(args)
        self.assertEqual(json.loads(stdout.getvalue())[0]["line"], "MB005B")

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            args = parser.parse_args(["images", "MB005B", "--db", str(self.db_path), "--json"])
            args.func(args)
        self.assertEqual(json.loads(stdout.getvalue())[0]["image_id"], 6878306)

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            args = parser.parse_args(["line", "MB005B", "--db", str(self.db_path)])
            args.func(args)
        self.assertEqual(json.loads(stdout.getvalue())["line"], "MB005B")

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            args = parser.parse_args(["image", "6878306", "--db", str(self.db_path)])
            args.func(args)
        self.assertEqual(json.loads(stdout.getvalue())["line"], "MB005B")

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            args = parser.parse_args(["release", "MB Paper 2014", "--db", str(self.db_path)])
            args.func(args)
        self.assertEqual(json.loads(stdout.getvalue())["release"], "MB Paper 2014")

    def test_update_defaults_to_all_incremental_sync(self) -> None:
        args = argparse.Namespace(db=self.db_path, raw_dir=Path(self.tmpdir.name), no_raw=False, workers=1, json=True)
        with mock.patch.object(cli, "cmd_sync", return_value=0) as sync:
            self.assertEqual(cli.cmd_update(args), 0)
        self.assertEqual(args.release, None)
        self.assertEqual(args.all, True)
        self.assertEqual(args.incremental, True)
        self.assertEqual(args.force, False)
        sync.assert_called_once_with(args)


if __name__ == "__main__":
    unittest.main()
