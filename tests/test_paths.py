from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from flylight_cli import cache
from flylight_cli import cli
from flylight_cli import core
from flylight_cli import paths


class PathDefaultsTests(unittest.TestCase):
    def test_default_cache_dir_uses_os_cache_location(self) -> None:
        self.assertEqual(
            paths.default_cache_dir(environ={}, platform="darwin", home=Path("/Users/gustavo")),
            Path("/Users/gustavo/Library/Caches/flylight"),
        )
        self.assertEqual(
            paths.default_cache_dir(
                environ={"XDG_CACHE_HOME": "/tmp/xdg-cache"},
                platform="linux",
                home=Path("/home/gustavo"),
            ),
            Path("/tmp/xdg-cache/flylight"),
        )
        self.assertEqual(
            paths.default_cache_dir(
                environ={"XDG_CACHE_HOME": "relative-cache"},
                platform="linux",
                home=Path("/home/gustavo"),
            ),
            Path("/home/gustavo/.cache/flylight"),
        )
        self.assertEqual(
            paths.default_cache_dir(
                environ={"LOCALAPPDATA": r"C:\Users\gustavo\AppData\Local"},
                platform="win32",
                home=Path("/unused"),
            ),
            Path(r"C:\Users\gustavo\AppData\Local") / "flylight" / "Cache",
        )

    def test_default_data_dir_uses_os_data_location(self) -> None:
        self.assertEqual(
            paths.default_data_dir(environ={}, platform="darwin", home=Path("/Users/gustavo")),
            Path("/Users/gustavo/Library/Application Support/flylight"),
        )
        self.assertEqual(
            paths.default_data_dir(
                environ={"XDG_DATA_HOME": "/tmp/xdg-data"},
                platform="linux",
                home=Path("/home/gustavo"),
            ),
            Path("/tmp/xdg-data/flylight"),
        )
        self.assertEqual(
            paths.default_data_dir(
                environ={"XDG_DATA_HOME": "relative-data"},
                platform="linux",
                home=Path("/home/gustavo"),
            ),
            Path("/home/gustavo/.local/share/flylight"),
        )
        self.assertEqual(
            paths.default_data_dir(
                environ={"LOCALAPPDATA": r"C:\Users\gustavo\AppData\Local"},
                platform="win32",
                home=Path("/unused"),
            ),
            Path(r"C:\Users\gustavo\AppData\Local") / "flylight",
        )

    def test_default_paths_do_not_depend_on_cwd(self) -> None:
        cwd = Path.cwd()
        first = (
            cache.cache_path_for_url("https://example.org/manifest.json", cache_dir=cache.DEFAULT_CACHE_DIR),
            core.DEFAULT_DB,
            core.DEFAULT_RAW_DIR,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                os.chdir(tmpdir)
                second = (
                    cache.cache_path_for_url("https://example.org/manifest.json", cache_dir=cache.DEFAULT_CACHE_DIR),
                    core.DEFAULT_DB,
                    core.DEFAULT_RAW_DIR,
                )
            finally:
                os.chdir(cwd)

        self.assertEqual(first, second)
        self.assertTrue(all(path.is_absolute() for path in second))

    def test_parser_defaults_are_absolute(self) -> None:
        parser = cli.build_parser()
        sync_args = parser.parse_args(["sync", "--release", "MB Paper 2014"])
        cache_args = parser.parse_args(["cache-info"])

        self.assertEqual(sync_args.cache_dir, cache.DEFAULT_CACHE_DIR)
        self.assertEqual(sync_args.db, core.DEFAULT_DB)
        self.assertEqual(sync_args.raw_dir, core.DEFAULT_RAW_DIR)
        self.assertEqual(cache_args.cache_dir, cache.DEFAULT_CACHE_DIR)
        self.assertTrue(sync_args.cache_dir.is_absolute())
        self.assertTrue(sync_args.db.is_absolute())
        self.assertTrue(sync_args.raw_dir.is_absolute())


if __name__ == "__main__":
    unittest.main()
