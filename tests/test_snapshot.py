from __future__ import annotations

import argparse
import io
import json
import tarfile
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


class SnapshotTests(unittest.TestCase):
    def test_snapshot_export_import_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_root = Path(tmpdir) / "source"
            import_root = Path(tmpdir) / "imported"
            db_path = source_root / "data.sqlite"
            raw_dir = source_root / "raw"
            cache_dir = source_root / "cache"
            archive_path = Path(tmpdir) / "snapshot.tar.gz"

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
            cache.write_cached_bytes(
                core.s3_url_for_key("MB Paper 2014/MB_Paper_2014.metadata.json"),
                json.dumps(load_json_fixture("release_manifest.json")).encode("utf-8"),
                cache_dir=cache_dir,
            )

            export_args = argparse.Namespace(db=db_path, raw_dir=raw_dir, cache_dir=cache_dir, out=archive_path, json=True)
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_snapshot_export(export_args)
            export_payload = json.loads(stdout.getvalue())
            self.assertEqual(export_payload["db_present"], True)
            self.assertEqual(export_payload["raw_file_count"], 1)
            self.assertEqual(export_payload["cache_entries"], 1)

            import_args = argparse.Namespace(
                archive=archive_path,
                db=import_root / "restored.sqlite",
                raw_dir=import_root / "raw",
                cache_dir=import_root / "cache",
                force=False,
                json=True,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_snapshot_import(import_args)
            import_payload = json.loads(stdout.getvalue())
            self.assertEqual(import_payload["imported"]["db"], True)
            self.assertEqual(import_payload["imported"]["raw_files"], 1)
            self.assertEqual(import_payload["imported"]["cache_files"], 2)

            restored_conn = core.connect_db(import_args.db)
            restored_record = core.get_line_record(restored_conn, "MB Paper 2014", "MB005B")
            self.assertEqual(restored_record["line"], "MB005B")
            self.assertTrue((import_args.raw_dir / "mb_paper_2014.json").exists())
            imported_cache_stats = cache.cache_stats(import_args.cache_dir)
            self.assertEqual(imported_cache_stats["entries"], 1)
            restored_conn.close()
            conn.close()

    def test_snapshot_import_rejects_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / "bad-snapshot.tar.gz"
            payload = b"bad"
            with tarfile.open(archive_path, "w:gz") as tar:
                info = tarfile.TarInfo("raw_manifests/../../escape.txt")
                info.size = len(payload)
                tar.addfile(info, io.BytesIO(payload))

            args = argparse.Namespace(
                archive=archive_path,
                db=Path(tmpdir) / "data.sqlite",
                raw_dir=Path(tmpdir) / "raw",
                cache_dir=Path(tmpdir) / "cache",
                force=False,
                json=True,
            )
            with self.assertRaises(SystemExit) as raised:
                cli.cmd_snapshot_import(args)

            self.assertIn("unsafe snapshot path", str(raised.exception))
            self.assertFalse((Path(tmpdir) / "escape.txt").exists())

    def test_snapshot_import_rejects_empty_member_paths(self) -> None:
        for member_name in ["raw_manifests/", "http_cache/"]:
            with self.subTest(member_name=member_name):
                with tempfile.TemporaryDirectory() as tmpdir:
                    archive_path = Path(tmpdir) / "bad-snapshot.tar.gz"
                    payload = b"bad"
                    with tarfile.open(archive_path, "w:gz") as tar:
                        info = tarfile.TarInfo(member_name)
                        info.size = len(payload)
                        tar.addfile(info, io.BytesIO(payload))

                    args = argparse.Namespace(
                        archive=archive_path,
                        db=Path(tmpdir) / "data.sqlite",
                        raw_dir=Path(tmpdir) / "raw",
                        cache_dir=Path(tmpdir) / "cache",
                        force=False,
                        json=True,
                    )
                    with self.assertRaises(SystemExit) as raised:
                        cli.cmd_snapshot_import(args)

                    self.assertIn("unsafe snapshot path", str(raised.exception))
                    self.assertFalse(args.raw_dir.exists())
                    self.assertFalse(args.cache_dir.exists())


if __name__ == "__main__":
    unittest.main()
