from __future__ import annotations

import shlex
import unittest

from flylight_cli import cli
from flylight_cli.examples import EXAMPLES


class ExampleCoverageTests(unittest.TestCase):
    def test_every_documented_example_command_parses(self) -> None:
        parser = cli.build_parser()
        for topic, example in EXAMPLES.items():
            for command in example["commands"]:
                with self.subTest(topic=topic, command=command):
                    parts = shlex.split(command)
                    self.assertEqual(parts[0], "flylight")
                    args = parser.parse_args(parts[1:])
                    self.assertTrue(callable(args.func))

    def test_examples_are_not_sparse(self) -> None:
        self.assertGreaterEqual(len(EXAMPLES), 8)
        for topic, example in EXAMPLES.items():
            with self.subTest(topic=topic):
                self.assertGreaterEqual(len(example["commands"]), 3)


if __name__ == "__main__":
    unittest.main()
