"""Small, deterministic calibration tests; no runtime, load, or model calls."""

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from benchmarks.workloads import (
    CalibrationError,
    OPERATIONS,
    check_capabilities,
    load_calibration,
    parse_calibration,
    validate_calibration,
)

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "docs/production/workloads.v0.2.json"


class CalibrationTests(unittest.TestCase):
    def setUp(self):
        self.source = PROFILE.read_text()
        self.data = json.loads(self.source)

    def reject(self, path, value, expected):
        data = copy.deepcopy(self.data)
        parent = data
        for key in path[:-1]:
            parent = parent[key]
        parent[path[-1]] = value
        with self.assertRaises(CalibrationError) as caught:
            validate_calibration(data)
        self.assertIn(expected, str(caught.exception))

    def test_valid_deterministic_detached_and_read_only(self):
        before = PROFILE.read_bytes()
        first = load_calibration(PROFILE)
        self.assertEqual(first, parse_calibration("\ufeff" + self.source))
        self.assertEqual(first, validate_calibration(self.data))
        first["profiles"][0]["steadyRps"] = 0
        self.assertEqual(self.data["profiles"][0]["steadyRps"], 20)
        self.assertEqual(PROFILE.read_bytes(), before)

    def test_malformed_json_and_non_object_roots(self):
        for source in ('{"profiles":', "", "null", "[]", "true", "3"):
            with self.subTest(source=source), self.assertRaises(CalibrationError) as caught:
                parse_calibration(source)
            self.assertIn("$", str(caught.exception))

    def test_duplicate_fields_and_overflow_report_errors(self):
        with self.assertRaises(CalibrationError) as caught:
            parse_calibration('{"contractVersion":"0.2","contractVersion":"0.1"}')
        self.assertIn("contractVersion", str(caught.exception))
        self.reject(["profiles", 0, "steadyRps"], 10**400, "steadyRps")

    def test_alternate_calibration_values_are_not_frozen(self):
        self.data["profiles"][0].update(
            steadyRps=40,
            busyHourRps=80,
            peakRps=160,
            backgroundSteadyPerSecond=1,
            hourlyBulkUpdateObjects=2000,
        )
        self.data["mixPercent"].update(detail=30, page=35)
        self.assertEqual(validate_calibration(self.data), self.data)

    def test_version_units_and_certification_flags(self):
        self.reject(["contractVersion"], "0.1", "contractVersion")
        for name in self.data["units"]:
            self.reject(["units", name], "requests/minute", name)
        for value in (True, 0):
            self.reject(["approval", "productionClaimsAllowed"], value, "productionClaimsAllowed")
        for value in (0, 1, "true"):
            self.reject(["approval", "numericTargetsApproved"], value, "numericTargetsApproved")
        for value in ("production-certified", "", 0):
            self.reject(["approval", "approvalScope"], value, "approvalScope")
        for value in ("2026-02-30", "2026-11-04T00:00:00Z", "20261104", "", 0):
            self.reject(["approval", "approvedOn"], value, "approvedOn")
        self.reject(["status"], "production-certified", "status")
        for name in self.data["acceptance"]:
            if isinstance(self.data["acceptance"][name], bool):
                self.reject(["acceptance", name], not self.data["acceptance"][name], name)
        self.reject(
            ["loadModel", "generatorLimitedRunCanCertify"], True, "generatorLimitedRunCanCertify"
        )

    def test_every_required_field_and_wrong_type(self):
        # Walk every checked-in field, including array members, to exercise the
        # shape barrier before any cross-field indexing or arithmetic occurs.
        def walk(value, path=()):
            if isinstance(value, dict):
                for key, child in value.items():
                    data = copy.deepcopy(self.data)
                    parent = data
                    for part in path:
                        parent = parent[part]
                    del parent[key]
                    with (
                        self.subTest(missing=path + (key,)),
                        self.assertRaises(CalibrationError) as caught,
                    ):
                        validate_calibration(data)
                    self.assertIn(key, str(caught.exception))
                    self.reject(list(path + (key,)), [] if isinstance(child, dict) else {}, key)
                    walk(child, path + (key,))
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    walk(child, path + (index,))

        walk(self.data)

    def test_rates_and_nonfinite_numbers(self):
        for field in ("steadyRps", "busyHourRps", "peakRps", "backgroundSteadyPerSecond"):
            for value in (True, False, -1, "20", None, float("inf"), float("nan")):
                self.reject(["profiles", 0, field], value, field)
        for literal in ("NaN", "Infinity", "-Infinity", "1e999"):
            source = self.source.replace('"steadyRps": 20', f'"steadyRps": {literal}', 1)
            with self.assertRaises(CalibrationError) as caught:
                parse_calibration(source)
            self.assertIn("steadyRps", str(caught.exception))

    def test_mix(self):
        for value in (True, -1, "35", float("nan"), 34):
            self.reject(["mixPercent", "detail"], value, "mixPercent")
        self.reject(["mixPercent"], {"unknown": 100}, "mixPercent")
        self.reject(["mixPercent"], {}, "mixPercent")

    def test_cross_field_invariants(self):
        cases = [
            (["profiles", 0, "id"], "T-B", "profiles"),
            (["sharedFixture", "appliesTo"], ["T-E"], "appliesTo"),
            (["profiles", 0, "busyHourRps"], 10, "rates"),
            (["profiles", 0, "peakRps"], 1, "rates"),
            (["profiles", 0, "backgroundSteadyPerSecond"], 50, "backgroundSteadyPerSecond"),
            (["profiles", 0, "hourlyBulkUpdateObjects"], 1, "hourlyBulkUpdateObjects"),
            (["profiles", 2, "repeatObjects"], [1], "repeatObjects"),
            (["repetitions"], 2, "repetitions"),
            (["windowsSeconds", "steady"], 0, "steady"),
            (["background", "bulkFirstOffsetSeconds"], 1800, "bulkFirstOffsetSeconds"),
            (["ingestionScenarios", 0, "id"], "D-M", "ingestionScenarios"),
            (["ingestionScenarios", 0, "inputBytes"], 0, "inputBytes"),
            (["ingestionScenarios", 0, "intervalSeconds"], -1, "intervalSeconds"),
            (["ingestionScenarios", 0, "retentionSeconds"], 0, "retentionSeconds"),
            (["requiredRunInputs"], ["seed", "seed"], "requiredRunInputs"),
            (["unresolvedDecisions", "costCeilings"], 0, "costCeilings"),
            (["acceptance", "safetyViolationsAllowed"], 1, "safetyViolationsAllowed"),
        ]
        for path, value, message in cases:
            with self.subTest(path=path):
                self.reject(path, value, message)
        for prefix, unit in (("query", "Ms"), ("action", "Ms"), ("continuousFreshness", "Seconds")):
            self.reject(["proposedTargets", f"{prefix}P95{unit}"], 10000, prefix)
        for name in ("unsuccessfulValidArrivalFraction", "unexpectedErrorAttemptFraction"):
            self.reject(["proposedTargets", name], 1.1, name)

    def test_full_coverage_is_not_certification(self):
        result = check_capabilities(self.data, list(reversed(OPERATIONS)), scope="full-profile")
        self.assertEqual(result["selectedOperations"], list(OPERATIONS))
        self.assertEqual(result["missingOperations"], [])
        self.assertEqual(result["effectiveMixPercent"], self.data["mixPercent"])
        self.assertTrue(result["fullProfileOperationCoverage"])
        self.assertTrue(result["numericTargetsApproved"])
        for flag in ("fullProfileCertified", "productionClaimsAllowed"):
            self.assertFalse(result[flag])

    def test_proposed_and_approved_status_cannot_be_mixed(self):
        self.reject(["status"], "proposed-calibration-not-certified", "numericTargetsApproved")
        proposed = copy.deepcopy(self.data)
        proposed["status"] = "proposed-calibration-not-certified"
        proposed["approval"] = {
            "numericTargetsApproved": False,
            "productionClaimsAllowed": False,
        }
        self.assertEqual(validate_calibration(proposed), proposed)
        result = check_capabilities(proposed, list(OPERATIONS), scope="full-profile")
        for flag in ("numericTargetsApproved", "fullProfileCertified", "productionClaimsAllowed"):
            self.assertFalse(result[flag])
        proposed["approval"]["approvedOn"] = "2026-11-04"
        with self.assertRaises(CalibrationError):
            validate_calibration(proposed)
        del proposed["approval"]["approvedOn"]
        proposed["status"] = "implementation-targets-not-certified"
        with self.assertRaisesRegex(CalibrationError, "numericTargetsApproved"):
            validate_calibration(proposed)

    def test_full_profile_rejects_each_missing_class_even_zero_weight(self):
        for operation in OPERATIONS:
            selected = [name for name in OPERATIONS if name != operation]
            with self.subTest(operation=operation), self.assertRaises(CalibrationError) as caught:
                check_capabilities(self.data, selected, scope="full-profile")
            self.assertIn(operation, str(caught.exception))
        with self.assertRaises(CalibrationError) as caught:
            check_capabilities(self.data, ["detail"], scope="full-profile")
        self.assertEqual(len(caught.exception.errors), len(OPERATIONS) - 1)
        self.data["mixPercent"]["detail"] += 5
        self.data["mixPercent"]["multiObjectAction"] = 0
        with self.assertRaises(CalibrationError):
            check_capabilities(
                self.data,
                [name for name in OPERATIONS if name != "multiObjectAction"],
                scope="full-profile",
            )

    def test_partial_mix_explicit_and_input_unchanged(self):
        before = copy.deepcopy(self.data)
        result = check_capabilities(self.data, ["page", "detail"], scope="capability-scoped")
        self.assertEqual(result["missingOperations"], list(OPERATIONS[2:]))
        self.assertEqual(result["originalMixPercent"], before["mixPercent"])
        self.assertEqual(result["selectedOriginalPercent"], 65)
        self.assertEqual(result["effectiveMixPercent"], {"detail": 3500 / 65, "page": 3000 / 65})
        self.assertFalse(result["fullProfileCertified"])
        self.assertEqual(self.data, before)
        result = check_capabilities(self.data, list(OPERATIONS), scope="capability-scoped")
        self.assertEqual(result["scope"], "capability-scoped")
        self.assertFalse(result["fullProfileCertified"])

    def test_invalid_selection_and_scope(self):
        for selection in ([], None, "detail", {}, [True], [{}], ["unknown"], ["detail", "detail"]):
            with self.subTest(selection=selection), self.assertRaises(CalibrationError):
                check_capabilities(self.data, selection, scope="capability-scoped")
        with self.assertRaises(CalibrationError):
            check_capabilities(self.data, ["detail"], scope="implicit")
        self.data["mixPercent"]["detail"] = 0
        self.data["mixPercent"]["page"] += 35
        with self.assertRaises(CalibrationError):
            check_capabilities(self.data, ["detail"], scope="capability-scoped")

    def test_cli_errors_and_read_only_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.json"
            path.write_text('{"profiles":')
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/validate-production-workloads.py"), str(path)],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("invalid JSON", result.stderr)
            self.assertNotIn("Traceback", result.stderr)
            self.assertEqual(path.read_text(), '{"profiles":')
            with self.assertRaises(CalibrationError):
                load_calibration(Path(directory) / "missing.json")


if __name__ == "__main__":
    unittest.main()
