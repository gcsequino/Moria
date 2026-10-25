"""Regression checks for benchmark grading boundaries and repair measurements."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("repair_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class GradeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.task = runner.CATALOG["tasks"][0]
        source = self.root / self.task["file"]
        source.parent.mkdir(parents=True)
        source.write_text(self.task["before"] + "\n")
        self.metadata = self.root / "task.json"
        self.metadata.write_text(
            json.dumps(
                {
                    "task": self.task["id"],
                    "baseline": runner.CATALOG["baseline"],
                    "initialImplementation": self.task["after"] + "\n",
                }
            )
        )
        self.changed = ""
        self.untracked = ""

    def git(self, root, *args):
        if args == ("rev-parse", "--show-toplevel"):
            return str(self.root)
        if args[:2] == ("diff", "--name-status"):
            return self.changed
        if args[0] == "ls-files":
            return self.untracked
        return ""

    def grade(self):
        output = io.StringIO()
        with (
            patch.object(runner, "metadata_path", return_value=self.metadata),
            patch.object(runner, "git", side_effect=self.git),
            patch.object(runner.subprocess, "run") as process,
            contextlib.redirect_stdout(output),
        ):
            process.return_value.returncode = 0
            code = runner.grade(self.task, self.root)
            return code, json.loads(output.getvalue()), process.call_args_list

    def test_exact_inverse_repair_has_nonzero_size_and_runs_both_gates(self):
        code, report, calls = self.grade()
        self.assertEqual(code, 0)
        self.assertEqual(report["repairLinesAdded"], 1)
        self.assertEqual(report["repairLinesRemoved"], 1)
        self.assertEqual(calls[-2].args[0], ["pnpm", "exec", "tsx", "--test", self.task["test"]])
        self.assertEqual(calls[-1].args[0], ["pnpm", "check"])

    def test_nested_tests_are_rejected_for_tracked_and_untracked_files(self):
        for tracked in (True, False):
            with self.subTest(tracked=tracked):
                self.changed = "A\ttests/nested/failing.test.ts" if tracked else ""
                self.untracked = "" if tracked else "tests/nested/failing.test.ts"
                with self.assertRaises(ValueError):
                    self.grade()

    def test_baseline_test_edits_are_rejected(self):
        self.changed = "M\t" + self.task["test"]
        with self.assertRaises(ValueError):
            self.grade()


if __name__ == "__main__":
    unittest.main()
