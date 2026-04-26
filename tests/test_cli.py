from __future__ import annotations

import argparse
import copy
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


def load_text_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def mock_http_response(payload: bytes) -> mock.MagicMock:
    response = mock.MagicMock()
    response.__enter__.return_value.read.return_value = payload
    return response


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

    def test_parse_release_catalog_html(self) -> None:
        html = """
        <a href="https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi?_gsearch=Search&alps_release=MB+Paper+2014">View lines</a>
        <a href='https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi?_gsearch=Search&alps_release=Rubin+%26+Aso+2023'>View lines</a>
        <a href='https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi?_gsearch=Search&amp;alps_release=MB+Paper+2014'>View lines</a>
        """
        self.assertEqual(
            core.parse_release_catalog_html(html),
            ["MB Paper 2014", "Rubin & Aso 2023"],
        )

    def test_schema_command(self) -> None:
        all_args = argparse.Namespace(entity=None, json=True)
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            cli.cmd_schema(all_args)
        payload = json.loads(stdout.getvalue())
        self.assertIn("line", payload)
        self.assertIn("compare-release", payload)

        single_args = argparse.Namespace(entity="compare-line", json=True)
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            cli.cmd_schema(single_args)
        entity_payload = json.loads(stdout.getvalue())
        self.assertEqual(list(entity_payload.keys()), ["compare-line"])
        self.assertIn("shared", entity_payload["compare-line"]["fields"])

    def test_examples_command(self) -> None:
        all_args = argparse.Namespace(topic=None, json=True)
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            cli.cmd_examples(all_args)
        payload = json.loads(stdout.getvalue())
        self.assertIn("warm-offline", payload)
        self.assertIn("release-diff", payload)

        single_args = argparse.Namespace(topic="schema-introspection", json=True)
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            cli.cmd_examples(single_args)
        topic_payload = json.loads(stdout.getvalue())
        self.assertEqual(list(topic_payload.keys()), ["schema-introspection"])
        self.assertIn("flylight schema --entity line", topic_payload["schema-introspection"]["commands"])

    def test_list_releases_uses_cache_offline(self) -> None:
        html = b"""
<a href="https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi?_gsearch=Search&alps_release=MB+Paper+2014">View lines</a>
<a href='https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi?_gsearch=Search&alps_release=Descending+Neurons+2018'>View lines</a>
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "http-cache"
            cache.set_cache_options(cache_dir=cache_dir, offline=False, refresh=False)
            try:
                with mock.patch("flylight_cli.cache.urlopen", return_value=mock_http_response(html)):
                    releases = core.list_releases()
                self.assertEqual(releases, ["MB Paper 2014", "Descending Neurons 2018"])

                cache.set_cache_options(cache_dir=cache_dir, offline=True, refresh=False)
                with mock.patch("flylight_cli.cache.urlopen", side_effect=AssertionError("network should not be used")):
                    offline_releases = core.list_releases()
                self.assertEqual(offline_releases, releases)
            finally:
                cache.set_cache_options(cache_dir=cache.DEFAULT_CACHE_DIR, offline=False, refresh=False)

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
            conn.close()

    def test_sync_manifest_release_works_offline_from_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "http-cache"
            db_path = Path(tmpdir) / "offline.sqlite"
            manifest_url = core.s3_url_for_key("MB Paper 2014/MB_Paper_2014.metadata.json")
            manifest_bytes = json.dumps(load_json_fixture("release_manifest.json")).encode("utf-8")
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
            try:
                cache.set_cache_options(cache_dir=cache_dir, offline=False, refresh=False)
                with mock.patch("flylight_cli.cache.urlopen", return_value=mock_http_response(manifest_bytes)):
                    core.fetch_json(manifest_url)

                cache.set_cache_options(cache_dir=cache_dir, offline=True, refresh=False)
                conn = core.connect_db(db_path)
                with mock.patch("flylight_cli.cache.urlopen", side_effect=AssertionError("network should not be used")):
                    result = core.sync_release_from_plan(conn, plan, raw_dir=None)
                self.assertEqual(result["lines"], 1)
                self.assertEqual(result["images"], 1)
                conn.close()
            finally:
                cache.set_cache_options(cache_dir=cache.DEFAULT_CACHE_DIR, offline=False, refresh=False)

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
            conn.close()

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
            conn.close()

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
            conn.close()

    def test_search_text_and_compare_line(self) -> None:
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

            compare_args = argparse.Namespace(db=db_path, line="MB005B", release=None, json=True)
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_compare_line(compare_args)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["release_count"], 2)
            self.assertEqual(
                set(payload["shared"]["genotype_parts"]),
                {"13F02-p65ADZp in attP40/CyO", "34A03-ZpGdbd in attP2", "w"},
            )
            self.assertEqual(len(payload["releases"]), 2)
            conn.close()

    def test_compare_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "release-diff.sqlite"
            conn = core.connect_db(db_path)

            left_manifest = copy.deepcopy(load_json_fixture("release_manifest.json"))
            right_manifest = copy.deepcopy(load_json_fixture("release_manifest.json"))
            right_manifest["images"][0]["id"] = 6878307
            right_manifest["images"][0]["genotype"] = "w; changed-ad; changed-dbd"
            right_manifest["lines"].append("MB999Z")
            right_manifest["images"].append(
                {
                    "id": 9990001,
                    "line": "MB999Z",
                    "robot_id": "9999999",
                    "slide_code": "20140410_01_A1",
                    "objective": "40x",
                    "area": "Brain",
                    "tile": "brain",
                    "gender": "m",
                    "roi": "gamma1",
                    "genotype": "w; added-line",
                    "multichannel_mip": "MB999Z-20140410_01_A1-m-40x-brain-Split_GAL4-multichannel_mip.png",
                    "unaligned_stack": "MB999Z-20140410_01_A1-m-40x-brain-Split_GAL4-unaligned_stack.h5j",
                }
            )

            plans = [
                core.ReleasePlan(
                    release="MB Paper 2014",
                    source_kind="manifest",
                    source_locator="MB Paper 2014/MB_Paper_2014.metadata.json",
                    source_token="manifest-left",
                    manifest_object={
                        "key": "MB Paper 2014/MB_Paper_2014.metadata.json",
                        "last_modified": "2022-01-18T15:57:49.000Z",
                    },
                ),
                core.ReleasePlan(
                    release="MB Paper 2015",
                    source_kind="manifest",
                    source_locator="MB Paper 2015/MB_Paper_2015.metadata.json",
                    source_token="manifest-right",
                    manifest_object={
                        "key": "MB Paper 2015/MB_Paper_2015.metadata.json",
                        "last_modified": "2022-01-19T15:57:49.000Z",
                    },
                ),
            ]

            with mock.patch.object(core, "fetch_json", side_effect=[left_manifest, right_manifest]):
                for plan in plans:
                    core.sync_release_from_plan(conn, plan, raw_dir=None)

            args = argparse.Namespace(
                db=db_path,
                left_release="MB Paper 2014",
                right_release="MB Paper 2015",
                include_lines=True,
                json=True,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_compare_release(args)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["summary"]["added_count"], 1)
            self.assertEqual(payload["summary"]["removed_count"], 0)
            self.assertEqual(payload["summary"]["changed_count"], 1)
            self.assertEqual(payload["added_lines"], ["MB999Z"])
            self.assertEqual(payload["changed_lines"], ["MB005B"])
            self.assertEqual(payload["added_records"][0]["line"], "MB999Z")
            self.assertEqual(payload["changed_records"][0]["left"]["line"], "MB005B")
            self.assertEqual(payload["changed_records"][0]["right"]["genotype_text"], "w; changed-ad; changed-dbd")

            export_args = argparse.Namespace(
                db=db_path,
                entity="compare-release",
                release=None,
                line=None,
                left_release="MB Paper 2014",
                right_release="MB Paper 2015",
                annotation=None,
                roi=None,
                robot_id=None,
                expressed_in=None,
                genotype=None,
                ad=None,
                dbd=None,
                area=None,
                objective=None,
                gender=None,
                source_kind=None,
                min_images=None,
                min_samples=None,
                term=None,
                limit=100,
                raw=True,
                out=None,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_export_ndjson(export_args)
            rows = [json.loads(line) for line in stdout.getvalue().splitlines()]
            self.assertEqual(rows[0]["entity"], "compare-release-summary")
            self.assertEqual(rows[0]["changed_count"], 1)
            added = next(row for row in rows if row.get("status") == "added")
            changed = next(row for row in rows if row.get("status") == "changed")
            self.assertEqual(added["line"], "MB999Z")
            self.assertEqual(changed["left"]["line"], "MB005B")
            conn.close()

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

            export_args = argparse.Namespace(
                db=db_path,
                raw_dir=raw_dir,
                cache_dir=cache_dir,
                out=archive_path,
                json=True,
            )
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

    def test_compare_line_export_requires_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "empty.sqlite"
            core.connect_db(db_path).close()
            args = argparse.Namespace(
                db=db_path,
                entity="compare-line",
                release=None,
                line=None,
                left_release=None,
                right_release=None,
                annotation=None,
                roi=None,
                robot_id=None,
                expressed_in=None,
                genotype=None,
                ad=None,
                dbd=None,
                area=None,
                objective=None,
                gender=None,
                source_kind=None,
                min_images=None,
                min_samples=None,
                term=None,
                limit=100,
                raw=False,
                out=None,
            )
            with self.assertRaises(SystemExit) as exc:
                cli.cmd_export_ndjson(args)
            self.assertEqual(str(exc.exception), "choose --line")


if __name__ == "__main__":
    unittest.main()
