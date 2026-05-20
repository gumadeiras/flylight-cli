from __future__ import annotations

import io
import unittest
from unittest import mock

from flylight_cli import cli


class ParserTests(unittest.TestCase):
    def test_all_subcommands_have_handlers(self) -> None:
        parser = cli.build_parser()
        commands = {
            "update": ["update", "--all"],
            "sources": ["sources"],
            "find": ["find", "MB005B"],
            "images": ["images", "MB005B"],
            "line": ["line", "MB005B"],
            "image": ["image", "6878306"],
            "release": ["release", "MB Paper 2014"],
            "releases": ["releases"],
            "sync": ["sync", "--release", "MB Paper 2014"],
            "sync-plan": ["sync-plan", "--release", "MB Paper 2014"],
            "cache-info": ["cache-info"],
            "schema": ["schema"],
            "examples": ["examples"],
            "snapshot-export": ["snapshot-export", "--out", "data/snapshot.tar.gz"],
            "snapshot-import": ["snapshot-import", "data/snapshot.tar.gz"],
            "reindex": ["reindex"],
            "search": ["search", "--line", "MB005B"],
            "search-text": ["search-text", "MB005B"],
            "search-images": ["search-images", "--line", "MB005B"],
            "show-line": ["show-line", "MB005B"],
            "show-image": ["show-image", "6878306"],
            "show-release": ["show-release", "MB Paper 2014"],
            "compare-line": ["compare-line", "MB005B"],
            "compare-release": ["compare-release", "MB Paper 2014", "MB Paper 2015"],
            "stats": ["stats"],
            "export-ndjson": ["export-ndjson", "--entity", "line"],
        }
        for command, argv in commands.items():
            with self.subTest(command=command):
                args = parser.parse_args(argv)
                self.assertEqual(args.command, command)
                self.assertTrue(callable(args.func))

    def test_main_without_args_prints_main_help(self) -> None:
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            self.assertEqual(cli.main([]), 0)
        output = stdout.getvalue()
        self.assertIn("usage:", output)
        self.assertIn("update", output)
        self.assertIn("find", output)

    def test_naked_subcommands_print_command_help(self) -> None:
        parser = cli.build_parser()
        for command in sorted(parser.get_default("subcommands")):
            with self.subTest(command=command):
                with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                    self.assertEqual(cli.main([command]), 0)
                output = stdout.getvalue()
                self.assertIn("usage:", output)
                self.assertIn(command, output)


if __name__ == "__main__":
    unittest.main()
