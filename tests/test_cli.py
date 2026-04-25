from __future__ import annotations

import argparse
import copy
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


def load_text_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class FlylightCliTests(unittest.TestCase):
    def test_normalize_helpers_expand_agent_friendly_fields(self) -> None:
        normalized = core.normalize_line_record(
            {
                "annotations_text": "A | B",
                "rois_text": "alpha | beta",
                "robot_ids_text": "1 | 2",
                "expressed_in_text": "DNp04,DNp05",
                "genotype_text": "w; a; b",
                "ad_text": "a; b",
                "dbd_text": "c; d",
            }
        )
        self.assertEqual(normalized["annotations"], ["A", "B"])
        self.assertEqual(normalized["rois"], ["alpha", "beta"])
        self.assertEqual(normalized["robot_ids"], ["1", "2"])
        self.assertEqual(normalized["expressed_in"], ["DNp04", "DNp05"])
        self.assertEqual(normalized["genotype_parts"], ["w", "a", "b"])

    def test_parse_release_summary_html(self) -> None:
        rows = core.parse_release_summary_html(load_text_fixture("release_summary.html"))
        self.assertEqual(rows["SS00724"]["robot_id"], "3007645")
        self.assertEqual(rows["SS00724"]["expressed_in_text"], "DNp04")
        self.assertIn("31B08-p65ADZp", rows["SS00724"]["ad_text"])
        self.assertIn("24A03-ZpGdbd", rows["SS00724"]["dbd_text"])

    def test_sync_manifest_release_and_export_image_ndjson(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "manifest.sqlite"
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
                result = core.sync_release_from_plan(conn, plan, raw_dir=None)

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
            self.assertEqual(row["roi_terms"], ["alpha'/beta'ap", "alpha'/beta'm"])
            self.assertTrue(any(url.endswith("unaligned_stack.h5j") for url in row["asset_urls"]))

            show_image_args = argparse.Namespace(db=db_path, image_id=6878306, raw=False)
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_show_image(show_image_args)
            image_row = json.loads(stdout.getvalue())
            self.assertEqual(image_row["image_id"], 6878306)
            self.assertEqual(image_row["source_kind"], "manifest")
            self.assertEqual(image_row["line"], "MB005B")

    def test_sync_line_metadata_release_with_html_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "fallback.sqlite"
            conn = core.connect_db(db_path)
            html_summary = core.parse_release_summary_html(load_text_fixture("release_summary.html"))
            plan = core.ReleasePlan(
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
                core.s3_url_for_key(plan.metadata_objects[0]["key"]): load_json_fixture("line_metadata_brain.json"),
                core.s3_url_for_key(plan.metadata_objects[1]["key"]): load_json_fixture("line_metadata_vnc.json"),
            }

            def fake_fetch_json(url: str):
                return fixture_map[url]

            with mock.patch.object(core, "fetch_json", side_effect=fake_fetch_json):
                result = core.sync_release_from_plan(conn, plan, raw_dir=None)

            self.assertEqual(result["source_kind"], "line-metadata")
            self.assertEqual(result["lines"], 1)
            self.assertEqual(result["images"], 2)

            record = core.get_line_record(conn, "Descending Neurons 2018", "SS00724")
            self.assertEqual(record["robot_ids_text"], "3007645")
            self.assertEqual(record["robot_ids"], ["3007645"])
            self.assertEqual(record["expressed_in"], ["DNp04"])
            self.assertEqual(record["expressed_in_text"], "DNp04")
            self.assertIn("31B08-p65ADZp", record["ad_text"])
            self.assertIn("24A03-ZpGdbd", record["dbd_text"])

            search_args = argparse.Namespace(
                db=db_path,
                release="Descending Neurons 2018",
                line=None,
                annotation=None,
                roi=None,
                robot_id="3007645",
                expressed_in="DNp04",
                genotype=None,
                ad="31B08",
                dbd=None,
                source_kind="line-metadata",
                min_images=2,
                min_samples=1,
                term=None,
                limit=10,
                json=True,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_search(search_args)
            search_rows = json.loads(stdout.getvalue())
            self.assertEqual(len(search_rows), 1)
            self.assertEqual(search_rows[0]["line"], "SS00724")
            self.assertEqual(search_rows[0]["robot_ids"], ["3007645"])

            image_args = argparse.Namespace(
                db=db_path,
                release="Descending Neurons 2018",
                line="SS00724",
                annotation=None,
                roi=None,
                robot_id="3007645",
                area="Brain",
                objective="20x",
                gender="f",
                source_kind="line-metadata",
                term=None,
                limit=10,
                raw=False,
                json=True,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_search_images(image_args)
            image_rows = json.loads(stdout.getvalue())
            self.assertEqual(len(image_rows), 1)
            self.assertEqual(image_rows[0]["line"], "SS00724")
            self.assertEqual(image_rows[0]["area"], "Brain")
            self.assertEqual(image_rows[0]["source_kind"], "line-metadata")

    def test_cmd_sync_incremental_skips_up_to_date_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "incremental.sqlite"
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

    def test_release_export_and_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "release.sqlite"
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

            export_args = argparse.Namespace(
                db=db_path,
                entity="release",
                release="MB Paper 2014",
                line=None,
                annotation=None,
                roi=None,
                term=None,
                limit=10,
                raw=False,
                out=None,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_export_ndjson(export_args)
            row = json.loads(stdout.getvalue().strip())
            self.assertEqual(row["release"], "MB Paper 2014")
            self.assertEqual(row["publication"]["doi"], "10.7554/eLife.04577")

            stats_args = argparse.Namespace(db=db_path, release=None, json=True)
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_stats(stats_args)
            stats = json.loads(stdout.getvalue())
            self.assertEqual(stats["release_count"], 1)
            self.assertEqual(stats["line_count"], 1)
            self.assertEqual(stats["image_count"], 1)

            release_args = argparse.Namespace(
                db=db_path,
                release="MB Paper 2014",
                include_lines=True,
                line="MB005B",
                annotation=None,
                roi=None,
                robot_id=None,
                expressed_in=None,
                genotype="34A03",
                ad=None,
                dbd=None,
                source_kind="manifest",
                min_images=1,
                min_samples=1,
                term=None,
                limit=10,
                raw=False,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_show_release(release_args)
            release_payload = json.loads(stdout.getvalue())
            self.assertEqual(release_payload["release"], "MB Paper 2014")
            self.assertEqual(len(release_payload["lines"]), 1)
            self.assertEqual(release_payload["lines"][0]["line"], "MB005B")

    def test_search_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "compare.sqlite"
            conn = core.connect_db(db_path)
            plans = [
                core.ReleasePlan(
                    release="MB Paper 2014",
                    source_kind="manifest",
                    source_locator="MB Paper 2014/MB_Paper_2014.metadata.json",
                    source_token="manifest-token-1",
                    manifest_object={
                        "key": "MB Paper 2014/MB_Paper_2014.metadata.json",
                        "last_modified": "2022-01-18T15:57:49.000Z",
                    },
                ),
                core.ReleasePlan(
                    release="MB Paper 2015",
                    source_kind="manifest",
                    source_locator="MB Paper 2015/MB_Paper_2015.metadata.json",
                    source_token="manifest-token-2",
                    manifest_object={
                        "key": "MB Paper 2015/MB_Paper_2015.metadata.json",
                        "last_modified": "2022-01-19T15:57:49.000Z",
                    },
                ),
            ]

            manifests = []
            for offset in [0, 1]:
                manifest = copy.deepcopy(load_json_fixture("release_manifest.json"))
                manifest["images"][0]["id"] += offset
                manifests.append(manifest)

            with mock.patch.object(core, "fetch_json", side_effect=manifests):
                for plan in plans:
                    core.sync_release_from_plan(conn, plan, raw_dir=None)

            text_args = argparse.Namespace(
                db=db_path,
                query="MB005B",
                release=None,
                source_kind="manifest",
                limit=10,
                json=True,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_search_text(text_args)
            rows = json.loads(stdout.getvalue())
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["release"] for row in rows}, {"MB Paper 2014", "MB Paper 2015"})


if __name__ == "__main__":
    unittest.main()
