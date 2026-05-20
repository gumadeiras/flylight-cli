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


def load_text_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class FlewSourceTests(unittest.TestCase):
    def test_parse_flew_catalog_html_reads_only_line_select(self) -> None:
        lines = core.parse_flew_catalog_html(load_text_fixture("flew_catalog.html"))
        self.assertEqual(lines, ["R10A01", "R10A02", "VT000001"])

    def test_parse_flew_line_html_extracts_image_metadata(self) -> None:
        payload = core.parse_flew_line_html("R10A01", load_text_fixture("flew_line.html"))
        self.assertEqual(payload["line_summary"]["Robot ID"], "1120385")
        self.assertEqual(len(payload["images"]), 1)
        image = payload["images"][0]
        self.assertEqual(image["id"], 4106609)
        self.assertEqual(image["area"], "Brain")
        self.assertEqual(image["driver"], "GAL4")
        self.assertEqual(image["gender"], "Female")
        self.assertEqual(image["robot_id"], "1120385")
        self.assertIn("Gene: CG11641, pdm3", image["annotations"])
        self.assertNotIn("toggle: Brain (GAL4)", image["annotations"])
        self.assertTrue(any(url.endswith("R10A01_total.jpg") for url in image["urls"]))
        self.assertTrue(any(url.endswith("R10A01.t.mp4") for url in image["urls"]))

    def test_list_releases_includes_flew_catalog_source(self) -> None:
        html = """
        <a href="https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi?_gsearch=Search&alps_release=MB+Paper+2014">View lines</a>
        """
        with mock.patch.object(core, "fetch_text", return_value=html):
            self.assertEqual(core.list_releases(), ["MB Paper 2014", core.FLEW_RELEASE])

    def test_plan_flew_catalog_uses_cached_line_pages_in_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            old_options = cache.get_cache_options()
            cache_dir = Path(tmpdir) / "cache"
            try:
                cache.set_cache_options(cache_dir=cache_dir, offline=False, refresh=False)
                catalog = load_text_fixture("flew_catalog.html")
                line_html = load_text_fixture("flew_line.html")
                for line in ["R10A01", "R10A02", "VT000001"]:
                    cache.write_cached_bytes(
                        core.flew_imagery_url(line),
                        line_html.replace("R10A01", line).encode("utf-8"),
                        cache_dir=cache_dir,
                    )
                with mock.patch.object(core, "fetch_text", return_value=catalog):
                    plan = core.plan_flew_catalog()
            finally:
                cache.set_cache_options(
                    cache_dir=old_options.cache_dir,
                    offline=old_options.offline,
                    refresh=old_options.refresh,
                )

        self.assertIn("pages=", plan.source_token)
        self.assertNotIn("missing=", plan.source_token)

    def test_sync_flew_catalog_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "flew.sqlite"
            raw_dir = Path(tmpdir) / "raw"
            conn = core.connect_db(db_path)
            plan = core.ReleasePlan(
                release=core.FLEW_RELEASE,
                source_kind="flew-html",
                source_locator=core.FLEW_INDEX_URL,
                source_token="flew-token",
                flew_lines=["R10A01", "R10A02", "VT000001"],
            )

            def fake_fetch_text(url: str) -> str:
                html = load_text_fixture("flew_line.html")
                if "R10A02" in url:
                    return html.replace("R10A01", "R10A02").replace("4106609", "4106610")
                if "VT000001" in url:
                    return html.split('<div class="boxed"')[0] + '<div id="footer"></div>'
                return html

            with mock.patch.object(core, "fetch_text", side_effect=fake_fetch_text):
                result = core.sync_release_from_plan(conn, plan, raw_dir=raw_dir, workers=1)

            self.assertEqual(result["source_kind"], "flew-html")
            self.assertEqual(result["lines"], 3)
            self.assertEqual(result["images"], 2)
            self.assertEqual(result["failed_lines"], 0)
            record = core.get_line_record(conn, core.FLEW_RELEASE, "R10A01")
            self.assertEqual(record["line"], "R10A01")
            self.assertEqual(record["source_kind"], "flew-html")
            self.assertEqual(record["robot_ids"], ["1120385"])
            self.assertEqual(record["images"][0]["area"], "Brain")
            self.assertTrue(any(url.endswith("R10A01_total.jpg") for url in record["images"][0]["asset_urls"]))
            no_image_record = core.get_line_record(conn, core.FLEW_RELEASE, "VT000001")
            self.assertEqual(no_image_record["image_count"], 0)
            self.assertEqual(no_image_record["robot_ids"], ["1120385"])
            self.assertIn("Gene: CG11641, pdm3", no_image_record["annotations"])

            search_args = argparse.Namespace(
                db=db_path,
                release=core.FLEW_RELEASE,
                line="R10A",
                annotation=None,
                roi=None,
                robot_id=None,
                expressed_in=None,
                genotype=None,
                ad=None,
                dbd=None,
                em_cell_type=None,
                source_kind="flew-html",
                min_images=None,
                min_samples=None,
                term=None,
                limit=10,
                json=True,
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                cli.cmd_search(search_args)
            rows = json.loads(stdout.getvalue())
            self.assertEqual([row["line"] for row in rows], ["R10A01", "R10A02"])
            self.assertTrue((raw_dir / "flylight_gal4_lexa_collection.json").exists())
            conn.close()

    def test_sync_flew_catalog_keeps_good_lines_when_one_page_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = core.connect_db(Path(tmpdir) / "flew.sqlite")
            plan = core.ReleasePlan(
                release=core.FLEW_RELEASE,
                source_kind="flew-html",
                source_locator=core.FLEW_INDEX_URL,
                source_token="catalog-token",
                flew_lines=["R10A01", "R10A02", "VT000001"],
            )

            def fake_fetch_text(url: str) -> str:
                if "R10A02" in url:
                    raise RuntimeError("temporary 503")
                html = load_text_fixture("flew_line.html")
                if "VT000001" in url:
                    return html.replace("R10A01", "VT000001").replace("4106609", "4106611")
                return html

            with mock.patch.object(core, "fetch_text", side_effect=fake_fetch_text):
                progress: list[str] = []
                result = core.sync_release_from_plan(conn, plan, raw_dir=Path(tmpdir), workers=2, progress=progress.append)

            self.assertEqual(result["lines"], 3)
            self.assertEqual(result["images"], 2)
            self.assertEqual(result["failed_lines"], 1)
            self.assertTrue(any("fetching 3 FLEW line pages" in item for item in progress))
            self.assertTrue(any("fetched 3/3 FLEW pages, errors=1" in item for item in progress))
            release = core.get_release_record(conn, core.FLEW_RELEASE)
            self.assertIn("errors=", release["source_token"])
            conn.close()

    def test_sync_flew_catalog_fails_loud_when_all_pages_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = core.connect_db(Path(tmpdir) / "flew.sqlite")
            plan = core.ReleasePlan(
                release=core.FLEW_RELEASE,
                source_kind="flew-html",
                source_locator=core.FLEW_INDEX_URL,
                source_token="catalog-token",
                flew_lines=["R10A01"],
            )
            with mock.patch.object(core, "fetch_text", side_effect=RuntimeError("down")):
                with self.assertRaises(SystemExit):
                    core.sync_release_from_plan(conn, plan, raw_dir=None, workers=1)
            conn.close()


if __name__ == "__main__":
    unittest.main()
