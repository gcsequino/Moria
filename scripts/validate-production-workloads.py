"""Validate M0 calibration structure and cross-field invariants; not capacity evidence."""

import argparse
import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "docs/production/workloads.v0.2.json"


def validate(data):
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    require(data["contractVersion"] == "0.2", "unexpected contract version")
    require(
        data["approval"] == {"numericTargetsApproved": False, "productionClaimsAllowed": False},
        "provisional contract must not claim approval or certification",
    )
    mix = data["mixPercent"]
    require(
        all(
            isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0 for v in mix.values()
        ),
        "mix values must be nonnegative numbers",
    )
    require(sum(mix.values()) == 100, "operation mix must total 100")
    require(
        set(mix)
        == {
            "detail",
            "page",
            "filteredSorted",
            "singleObjectAction",
            "multiObjectAction",
            "aggregateOrRelation",
        },
        "operation coverage differs from contract",
    )
    profiles = data["profiles"]
    require(
        [p["id"] for p in profiles] == ["T-S", "T-B", "T-E"],
        "transaction card IDs must be unique and complete",
    )
    require(
        data["sharedFixture"]["appliesTo"] == [p["id"] for p in profiles],
        "fixture coverage differs from cards",
    )
    for p in profiles:
        require(
            0 < p["steadyRps"] <= p["busyHourRps"] <= p["peakRps"],
            f'{p["id"]}: rates must be positive and ordered',
        )
        require(
            p["backgroundSteadyPerSecond"] == p["steadyRps"] / 40,
            f'{p["id"]}: background rate must match declared scaling',
        )
        require(
            p["hourlyBulkUpdateObjects"] == p["steadyRps"] * 50,
            f'{p["id"]}: bulk load must match declared scaling',
        )
        require(
            p["objects"] > 0 and all(n > p["objects"] for n in p["repeatObjects"]),
            f'{p["id"]}: invalid populations',
        )
    targets = data["proposedTargets"]
    for prefix, unit in [("query", "Ms"), ("action", "Ms"), ("continuousFreshness", "Seconds")]:
        require(
            0 < targets[f"{prefix}P95{unit}"] <= targets[f"{prefix}P99{unit}"],
            f"{prefix}: percentile targets must be ordered",
        )
    for key in ["unsuccessfulValidArrivalFraction", "unexpectedErrorAttemptFraction"]:
        require(0 <= targets[key] <= 1, f"{key}: fraction outside [0,1]")
    require(data["repetitions"] >= 3, "contract requires at least three repetitions")
    require(
        all(v > 0 for v in data["windowsSeconds"].values()), "measurement windows must be positive"
    )
    require(
        data["background"]["bulkFirstOffsetSeconds"] < data["windowsSeconds"]["steady"],
        "steady window must include bulk update",
    )
    require(
        [s["id"] for s in data["ingestionScenarios"]] == ["D-S", "D-M", "D-L"],
        "data cards must be unique and complete",
    )
    for scenario in data["ingestionScenarios"]:
        require(
            all(scenario[k] > 0 for k in ["inputBytes", "intervalSeconds", "retentionSeconds"]),
            "data volumes and durations must be positive",
        )
    require(
        len(data["requiredRunInputs"]) == len(set(data["requiredRunInputs"])),
        "run input names must be unique",
    )
    require(
        all(v is None for v in data["unresolvedDecisions"].values()),
        "unresolved decisions must remain explicitly null in this revision",
    )
    require(
        data["acceptance"]["safetyViolationsAllowed"] == 0, "safety violations cannot be accepted"
    )
    require(
        data["loadModel"]["generatorLimitedRunCanCertify"] is False,
        "generator-limited runs cannot certify capacity",
    )
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    data = json.loads(PROFILE.read_text(encoding="utf-8-sig"))
    errors = validate(data)
    if errors:
        raise SystemExit("\n".join(errors))
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
