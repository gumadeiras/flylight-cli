from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

from flylight_cli import cli
from flylight_cli import core


class ExportTests(unittest.TestCase):
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
