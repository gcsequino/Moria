"""Prepare isolated regression tasks and grade repairs using unchanged baseline tests."""

import argparse
import difflib
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
CATALOG = json.loads(Path(__file__).with_name("tasks.json").read_text())


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def metadata_path(destination):
    return Path(git(destination, "rev-parse", "--absolute-git-dir")) / "moria-task.json"


def new_test(name):
    path = Path(name)
    return path.parent == Path("tests") and path.name.endswith(".test.ts")


def prepare(task, destination):
    if destination.exists() or destination == ROOT or ROOT in destination.parents:
        raise ValueError("Destination must be a new directory outside this checkout")
    baseline = CATALOG["baseline"]
    original = git(ROOT, "show", f"{baseline}:{task['file']}") + "\n"
    if original.count(task["before"]) != 1:
        raise ValueError("Mutation does not match exactly once; revalidate the catalog")
    subprocess.run(
        ["git", "-C", str(ROOT), "worktree", "add", "--detach", str(destination), baseline],
        check=True,
    )
    mutated = original.replace(task["before"], task["after"])
    (destination / task["file"]).write_text(mutated, encoding="utf-8")
    metadata_path(destination).write_text(
        json.dumps(
            {
                "task": task["id"],
                "baseline": baseline,
                "initialImplementation": mutated,
            }
        ),
        encoding="utf-8",
    )
    print(task["prompt"])
    print(f"\nWorkspace: {destination}\nBaseline: {baseline}")
    print("Provision using bash .openhands/setup.sh in non-root Linux before starting the timer.")


def grade(task, destination):
    baseline = CATALOG["baseline"]
    if Path(git(destination, "rev-parse", "--show-toplevel")).resolve() != destination:
        raise ValueError("Destination must be the task checkout root")
    metadata = json.loads(metadata_path(destination).read_text())
    if metadata["task"] != task["id"] or metadata["baseline"] != baseline:
        raise ValueError("Task does not match the prepared workspace")
    subprocess.run(
        ["git", "-C", str(destination), "merge-base", "--is-ancestor", baseline, "HEAD"],
        check=True,
    )
    # Only the scoped implementation and new test files may differ from the baseline.
    # Existing tests and the gate cannot be edited to award a passing score.
    changes = git(destination, "diff", "--name-status", "--no-renames", baseline)
    added_tests = set()
    for line in changes.splitlines():
        status, name = line.split("\t", 1)
        if status == "A" and new_test(name):
            added_tests.add(name)
            continue
        if name == task["file"] and status == "M":
            continue
        raise ValueError(f"Out-of-scope or altered baseline evidence: {name}")
    for name in git(destination, "ls-files", "--others", "--exclude-standard").splitlines():
        if not new_test(name):
            raise ValueError(f"Unexpected untracked file: {name}")
        added_tests.add(name)
    delta = list(
        difflib.ndiff(
            metadata["initialImplementation"].splitlines(),
            (destination / task["file"]).read_text().splitlines(),
        )
    )
    start = time.monotonic()
    result = subprocess.run(["pnpm", "exec", "tsx", "--test", task["test"]], cwd=destination)
    if result.returncode == 0:
        result = subprocess.run(["pnpm", "check"], cwd=destination)
    print(
        json.dumps(
            {
                "task": task["id"],
                "baseline": baseline,
                "head": git(destination, "rev-parse", "HEAD"),
                "workingTree": git(destination, "status", "--porcelain"),
                "validationSeconds": round(time.monotonic() - start, 3),
                "exitCode": result.returncode,
                "repairLinesAdded": sum(line.startswith("+ ") for line in delta),
                "repairLinesRemoved": sum(line.startswith("- ") for line in delta),
                "newTestLines": sum(
                    len((destination / name).read_text().splitlines()) for name in added_tests
                ),
                "baselineDiffStat": git(destination, "diff", "--stat", baseline),
                "note": "Automated gate only; independent scope review and run metrics are still required.",
            },
            indent=2,
        )
    )
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "grade"])
    parser.add_argument("task", choices=[task["id"] for task in CATALOG["tasks"]])
    parser.add_argument("destination", type=lambda value: Path(value).resolve())
    args = parser.parse_args()
    task = next(task for task in CATALOG["tasks"] if task["id"] == args.task)
    if args.action == "prepare":
        prepare(task, args.destination)
        return 0
    if sys.platform != "linux":
        raise ValueError("Grade in the supported non-root Linux development environment")
    return grade(task, args.destination)


if __name__ == "__main__":
    sys.exit(main())
