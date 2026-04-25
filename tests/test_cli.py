from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import janelia_splitgal4 as cli


FIXTURES = Path(__file__).parent / "fixtures"


def load_json_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def load_text_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class FlylightCliTests(unittest.TestCase):
    def test_parse_release_summary_html(self) -> None:
        rows = cli.parse_release_summary_html(load_text_fixture("release_summary.html"))
        self.assertEqual(rows["SS00724"]["robot_id"], "3007645")
        self.assertEqual(rows["SS00724"]["expressed_in_text"], "DNp04")
        self.assertIn("31B08-p65ADZp", rows["SS00724"]["ad_text"])
        self.assertIn("24A03-ZpGdbd", rows["SS00724"]["dbd_text"])

    def test_sync_manifest_release_and_export_image_ndjson(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "manifest.sqlite"
            conn = cli.connect_db(db_path)
            plan = cli.ReleasePlan(
                release="MB Paper 2014",
                source_kind="manifest",
                source_locator="MB Paper 2014/MB_Paper_2014.metadata.json",
                source_token="manifest-token",
                manifest_object={
                    "key": "MB Paper 2014/MB_Paper_2014.metadata.json",
                    "last_modified": "2022-01-18T15:57:49.000Z",
                },
            )

            with mock.patch.object(cli, "fetch_json", return_value=load_json_fixture("release_manifest.json")):
                result = cli.sync_release_from_plan(conn, plan, raw_dir=None)

            self.assertEqual(result["source_kind"], "manifest")
            self.assertEqual(result["lines"], 1)
            self.assertEqual(result["images"], 1)

            args = argparse.Namespace(
                db=db_path,
                entity="image",
                release="MB Paper 2014",
                line="MB005B",
                annotation=None,
                roi=None,
                term=None,
                limit=10,
                raw=False,
                out=None,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_export_ndjson(args)
            row = json.loads(stdout.getvalue().strip())
            self.assertEqual(row["line"], "MB005B")
            self.assertEqual(row["image_id"], 6878306)
            self.assertTrue(any(url.endswith("unaligned_stack.h5j") for url in row["asset_urls"]))

    def test_sync_line_metadata_release_with_html_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "fallback.sqlite"
            conn = cli.connect_db(db_path)
            html_summary = cli.parse_release_summary_html(load_text_fixture("release_summary.html"))
            plan = cli.ReleasePlan(
                release="Descending Neurons 2018",
                source_kind="line-metadata",
                source_locator="Descending Neurons 2018/",
                source_token="fallback-token",
                metadata_objects=[
                    {
                        "key": "Descending Neurons 2018/SS00724/SS00724-20140623_34_F1-f-20x-brain-Split_GAL4-6930340-metadata.json",
                        "last_modified": "2021-12-14T20:58:04.000Z",
                    },
                    {
                        "key": "Descending Neurons 2018/SS00724/SS00724-20140623_34_F1-f-20x-ventral_nerve_cord-Split_GAL4-6930343-metadata.json",
                        "last_modified": "2021-12-14T20:57:12.000Z",
                    },
                ],
                line_prefixes=["Descending Neurons 2018/SS00724/"],
                html_summary=html_summary,
            )

            fixture_map = {
                cli.s3_url_for_key(plan.metadata_objects[0]["key"]): load_json_fixture("line_metadata_brain.json"),
                cli.s3_url_for_key(plan.metadata_objects[1]["key"]): load_json_fixture("line_metadata_vnc.json"),
            }

            def fake_fetch_json(url: str):
                return fixture_map[url]

            with mock.patch.object(cli, "fetch_json", side_effect=fake_fetch_json):
                result = cli.sync_release_from_plan(conn, plan, raw_dir=None)

            self.assertEqual(result["source_kind"], "line-metadata")
            self.assertEqual(result["lines"], 1)
            self.assertEqual(result["images"], 2)

            record = cli.get_line_record(conn, "Descending Neurons 2018", "SS00724")
            self.assertEqual(record["robot_ids_text"], "3007645")
            self.assertEqual(record["expressed_in_text"], "DNp04")
            self.assertIn("31B08-p65ADZp", record["ad_text"])
            self.assertIn("24A03-ZpGdbd", record["dbd_text"])

    def test_cmd_sync_incremental_skips_up_to_date_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "incremental.sqlite"
            raw_dir = Path(tmpdir) / "raw"
            conn = cli.connect_db(db_path)
            plan = cli.ReleasePlan(
                release="MB Paper 2014",
                source_kind="manifest",
                source_locator="MB Paper 2014/MB_Paper_2014.metadata.json",
                source_token="manifest-token",
                manifest_object={
                    "key": "MB Paper 2014/MB_Paper_2014.metadata.json",
                    "last_modified": "2022-01-18T15:57:49.000Z",
                },
            )
            with mock.patch.object(cli, "fetch_json", return_value=load_json_fixture("release_manifest.json")):
                cli.sync_release_from_plan(conn, plan, raw_dir=raw_dir)

            args = argparse.Namespace(
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
                with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                    cli.cmd_sync(args)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["synced"], [])
            self.assertEqual(payload["skipped"][0]["reason"], "up_to_date")


if __name__ == "__main__":
    unittest.main()
