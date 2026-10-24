"""Validate M0 calibration structure and cross-field invariants; not capacity evidence."""

import argparse
import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks.workloads import (
    CalibrationError,
    check_capabilities,
    load_calibration,
    validate_calibration,
)

PROFILE = ROOT / "docs/production/workloads.v0.2.json"


def validate(data):
    """Compatibility wrapper for the document validator's list-of-errors API."""
    try:
        validate_calibration(data)
    except CalibrationError as error:
        return error.errors
    return []


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("profile", nargs="?", type=Path, default=PROFILE)
    parser.add_argument("--scope", choices=["full-profile", "capability-scoped"])
    parser.add_argument("--operations", nargs="+")
    args = parser.parse_args()
    if (args.scope is None) != (args.operations is None):
        parser.error("--scope and --operations must be supplied together")
    try:
        data = load_calibration(args.profile)
        if args.scope:
            result = check_capabilities(data, args.operations, scope=args.scope)
            print(json.dumps(result, indent=2, allow_nan=False))
    except CalibrationError as error:
        raise SystemExit(str(error)) from error
    if args.self_test:
        mutations = [
            lambda d: d["mixPercent"].update(detail=34),
            lambda d: d["approval"].update(productionClaimsAllowed=True),
            lambda d: d["profiles"][0].update(backgroundSteadyPerSecond=50),
            lambda d: d["profiles"][0].update(peakRps=1),
            lambda d: d["sharedFixture"].update(appliesTo=["T-E"]),
            lambda d: d["loadModel"].update(generatorLimitedRunCanCertify=True),
        ]
        for index, mutate in enumerate(mutations):
            invalid = copy.deepcopy(data)
            mutate(invalid)
            if not validate(invalid):
                raise SystemExit(f"Invalid mutation {index} was accepted")
        print(f"Rejected {len(mutations)} invalid calibration mutations.")
    print("M0 v0.2 calibration invariants passed; no runtime or capacity claim.")


if __name__ == "__main__":
    main()
