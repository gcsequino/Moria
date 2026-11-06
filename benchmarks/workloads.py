"""Read-only v0.2 calibration validation; not a run manifest or capacity proof."""

import copy
import json
import math
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

OPERATIONS = (
    "detail",
    "page",
    "filteredSorted",
    "singleObjectAction",
    "multiObjectAction",
    "aggregateOrRelation",
)


def _object(properties):
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def _array(items, **limits):
    return {"type": "array", "items": items, **limits}


def _constant(value):
    # A boolean type prevents Python's False == 0 equality from accepting numbers.
    result = {"const": value}
    if isinstance(value, bool):
        result["type"] = "boolean"
    return result


NUMBER = {"type": "number", "minimum": 0}
POSITIVE = {"type": "number", "exclusiveMinimum": 0}
COUNT = {"type": "integer", "minimum": 1}
FRACTION = {"type": "number", "minimum": 0, "maximum": 1}
TEXT = {"type": "string", "minLength": 1}

SCHEMA = _object(
    {
        "contractVersion": _constant("0.2"),
        "status": {
            "enum": [
                "proposed-calibration-not-certified",
                "implementation-targets-not-certified",
            ]
        },
        "referenceWorkload": TEXT,
        "approval": {
            **_object(
                {
                    "numericTargetsApproved": {"type": "boolean"},
                    "approvalScope": _constant("implementation-targets"),
                    "approvedOn": {"type": "string", "format": "date"},
                    "productionClaimsAllowed": _constant(False),
                }
            ),
            "required": ["numericTargetsApproved", "productionClaimsAllowed"],
            "if": {"properties": {"numericTargetsApproved": _constant(True)}},
            "then": {"required": ["approvalScope", "approvedOn"]},
            "else": {"properties": {"approvalScope": False, "approvedOn": False}},
        },
        "units": _object(
            {
                "externalRates": _constant("scheduled logical API arrivals per second"),
                "inputVolume": _constant("decimal bytes"),
                "payloadAndMemory": _constant("binary bytes"),
            }
        ),
        "profiles": _array(
            _object(
                {
                    "id": TEXT,
                    "objects": COUNT,
                    "repeatObjects": _array(COUNT, uniqueItems=True),
                    "steadyRps": POSITIVE,
                    "busyHourRps": POSITIVE,
                    "peakRps": POSITIVE,
                    "backgroundSteadyPerSecond": NUMBER,
                    "hourlyBulkUpdateObjects": COUNT,
                }
            )
        ),
        "sharedFixture": _object(
            {
                "appliesTo": _array(TEXT),
                "meanObjectBytes": COUNT,
                "sensitivityMeanObjectBytes": _array(COUNT, minItems=1),
                "overlayFraction": FRACTION,
                "retainedGenerations": COUNT,
                "editsPerEditedObject": COUNT,
                "relationFanoutTrials": _array({"type": "integer", "minimum": 0}, minItems=1),
                "pageRows": COUNT,
                "projectedPageResponseCapBytes": COUNT,
                "multiActionMeanObjects": COUNT,
                "multiActionMaximumObjects": COUNT,
                "humanPrincipalFractionTrials": _array(FRACTION, minItems=1),
                "regionAttributeCount": COUNT,
                "minimumMarkings": COUNT,
                "visibleFractionTrials": _array(FRACTION, minItems=1),
                "normalTrafficHotFraction": FRACTION,
                "hotPopulationFraction": FRACTION,
                "contentionTrialActionFraction": FRACTION,
                "contentionTrialObjects": COUNT,
            }
        ),
        "mixPercent": _object({name: NUMBER for name in OPERATIONS}),
        "background": _object(
            {
                "updatesPerExecution": COUNT,
                "simulatedDeliveriesPerExecution": COUNT,
                "busyHourMultiplier": POSITIVE,
                "peakMultiplier": _constant("profile.peakRps / profile.steadyRps"),
                "warmupRecoverySoakRate": _constant("profile.backgroundSteadyPerSecond"),
                "bulkFirstOffsetSeconds": NUMBER,
                "bulkIntervalSeconds": POSITIVE,
                "destinations": _constant("deterministic local fixtures only"),
            }
        ),
        "loadModel": _object(
            {
                "mode": _constant("open-loop"),
                "humanIdentityAddsThinkTime": _constant(False),
                "negativeProbes": _constant(
                    "additional separately scheduled arrivals outside primary mix"
                ),
                "retryAccounting": _constant(
                    "same logical arrival; separate attempt counter; no replacement arrivals"
                ),
                "latencyOrigin": _constant("scheduled arrival"),
                "incompleteAtDrainDeadline": _constant("unsuccessful logical outcome"),
                "generatorLimitedRunCanCertify": _constant(False),
            }
        ),
        "windowsSeconds": _object(
            {
                name: POSITIVE
                for name in ("warmup", "steady", "peak", "recoveryMinimum", "busyHour", "soak")
            }
        ),
        "repetitions": {"type": "integer", "minimum": 3},
        "proposedTargets": _object(
            {
                "queryOperations": _constant(["detail", "page", "filteredSorted"]),
                **{
                    name: POSITIVE
                    for name in (
                        "queryP95Ms",
                        "queryP99Ms",
                        "actionP95Ms",
                        "actionP99Ms",
                        "haFailoverRtoSeconds",
                        "drRpoSeconds",
                        "drRtoSeconds",
                        "burstRecoveryMaximumSeconds",
                        "continuousFreshnessP95Seconds",
                        "continuousFreshnessP99Seconds",
                    )
                },
                "unsuccessfulValidArrivalFraction": FRACTION,
                "unexpectedErrorAttemptFraction": FRACTION,
                "haAvailabilityPercent": {"type": "number", "minimum": 0, "maximum": 100},
                "haFailureDomainRpoSeconds": NUMBER,
                "drScope": TEXT,
            }
        ),
        "ingestionScenarios": _array(
            _object(
                {
                    "id": TEXT,
                    "inputBytes": COUNT,
                    "intervalSeconds": POSITIVE,
                    "retentionSeconds": POSITIVE,
                }
            )
        ),
        "dataExperiments": _object(
            {
                "burstMultiplier": POSITIVE,
                "burstSeconds": POSITIVE,
                "fileSizeBytesTrials": _array(COUNT, minItems=1),
                "analyticalConcurrencyTrials": _array(COUNT, minItems=1),
                "operations": _constant(
                    [
                        "append",
                        "keyedMerge",
                        "schemaEvolution",
                        "fullRefresh",
                        "incrementalIndexing",
                        "wideJoin",
                        "skewedJoin",
                        "groupAggregation",
                        "compaction",
                        "snapshotPlanning",
                        "expiration",
                    ]
                ),
            }
        ),
        "requiredRunInputs": _array(TEXT, minItems=1, uniqueItems=True),
        "unresolvedDecisions": _object(
            {
                name: {"type": "null"}
                for name in (
                    "selectedTransactionAndIngestionCards",
                    "highFanoutStressCap",
                    "baselineBacklogAgeSeconds",
                    "aggregateAndRelationshipDeadlinesMs",
                    "scheduledPipelineDeadlinesSeconds",
                    "availabilityDenominatorAndExclusions",
                    "costCeilings",
                )
            }
        ),
        "acceptance": _object(
            {
                "safetyViolationsAllowed": {"type": "integer", "const": 0},
                "evidenceKinds": _constant(["exploratory", "capability-scoped", "full-profile"]),
                "unsupportedOperationsAreSuccesses": _constant(False),
                "allRepetitionsMustPass": _constant(True),
                "fullProfileRequiresAllOperationClasses": _constant(True),
                "fullProfileRequiresApprovedTargetsAndResolvedInputs": _constant(True),
            }
        ),
    }
)


class CalibrationError(ValueError):
    """Validation failed; errors is a deterministic list of field-specific messages."""

    def __init__(self, errors):
        self.errors = errors
        super().__init__("\n".join(errors))


def _finite_errors(value, path="$"):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            return [f"{path}: expected a finite number representable for rate arithmetic"]
    if isinstance(value, dict):
        return [
            error for key, item in value.items() for error in _finite_errors(item, f"{path}.{key}")
        ]
    if isinstance(value, list):
        return [
            error
            for index, item in enumerate(value)
            for error in _finite_errors(item, f"{path}[{index}]")
        ]
    return []


def validate_calibration(data):
    """Return a detached validated JSON object, or raise CalibrationError. Never mutate input."""
    errors = _finite_errors(data)
    for error in Draft202012Validator(SCHEMA, format_checker=FormatChecker()).iter_errors(data):
        path = "$" + "".join(
            f"[{part}]" if isinstance(part, int) else f".{part}" for part in error.absolute_path
        )
        errors.append(f"{path}: {error.message}")
    if errors:
        raise CalibrationError(sorted(set(errors)))
    errors = _cross_field_errors(data)
    if errors:
        raise CalibrationError(errors)
    return copy.deepcopy(data)


def _unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CalibrationError([f"$.{key}: duplicate JSON field"])
        result[key] = value
    return result


def parse_calibration(source):
    """Parse JSON text (including a UTF-8 BOM), validating before returning any profile."""
    if not isinstance(source, str):
        raise CalibrationError(["$: expected JSON text"])
    try:
        data = json.loads(source.lstrip("\ufeff"), object_pairs_hook=_unique_fields)
    except CalibrationError:
        raise
    except (ValueError, RecursionError) as error:
        raise CalibrationError([f"$: invalid JSON: {error}"]) from error
    return validate_calibration(data)


def load_calibration(path):
    """Read a calibration file without rewriting it."""
    try:
        source = Path(path).read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError) as error:
        raise CalibrationError([f"{path}: cannot read UTF-8 calibration: {error}"]) from error
    return parse_calibration(source)


def check_capabilities(data, operations, *, scope):
    """Check caller-declared external API classes; this does not test runtime support.

    Scope must be explicit. Data/background experiments and run inputs are not a
    runnable plan here and remain independent obligations for future harnesses.
    """
    profile = validate_calibration(data)
    if scope not in ("full-profile", "capability-scoped"):
        raise CalibrationError(["scope: expected full-profile or capability-scoped"])
    if not isinstance(operations, (list, tuple)) or not operations:
        raise CalibrationError(["operations: select a nonempty list of operation classes"])
    errors = []
    selected = set()
    for index, operation in enumerate(operations):
        if not isinstance(operation, str) or operation not in OPERATIONS:
            errors.append(f"operations[{index}]: expected one of {', '.join(OPERATIONS)}")
        elif operation in selected:
            errors.append(f"operations[{index}]: duplicate class {operation}")
        else:
            selected.add(operation)
    if errors:
        raise CalibrationError(errors)
    missing = [name for name in OPERATIONS if name not in selected]
    if scope == "full-profile" and missing:
        raise CalibrationError(
            [f"operations.{name}: required for full-profile coverage" for name in missing]
        )
    mix = profile["mixPercent"]
    weight = sum(mix[name] for name in OPERATIONS if name in selected)
    if weight == 0:
        raise CalibrationError(["operations: selected classes have zero total mix weight"])
    return {
        "scope": scope,
        "selectedOperations": [name for name in OPERATIONS if name in selected],
        "missingOperations": missing,
        "originalMixPercent": mix,
        "selectedOriginalPercent": weight,
        "effectiveMixPercent": {
            name: mix[name] * 100 / weight for name in OPERATIONS if name in selected
        },
        "fullProfileOperationCoverage": not missing,
        "fullProfileCertified": False,
        "numericTargetsApproved": profile["approval"]["numericTargetsApproved"],
        "productionClaimsAllowed": False,
    }


def _cross_field_errors(data):
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    require(
        data["approval"]["numericTargetsApproved"]
        == (data["status"] == "implementation-targets-not-certified"),
        "$.approval.numericTargetsApproved: approval must match $.status",
    )
    require(sum(data["mixPercent"].values()) == 100, "$.mixPercent: operation mix must total 100")
    profiles = data["profiles"]
    require(
        [p["id"] for p in profiles] == ["T-S", "T-B", "T-E"],
        "$.profiles: IDs must be T-S, T-B, T-E in that order",
    )
    require(
        data["sharedFixture"]["appliesTo"] == [p["id"] for p in profiles],
        "$.sharedFixture.appliesTo: coverage differs from cards",
    )
    for index, p in enumerate(profiles):
        require(
            0 < p["steadyRps"] <= p["busyHourRps"] <= p["peakRps"],
            f"$.profiles[{index}].steadyRps/busyHourRps/peakRps: rates must be positive and ordered",
        )
        require(
            p["backgroundSteadyPerSecond"] == p["steadyRps"] / 40,
            f"$.profiles[{index}].backgroundSteadyPerSecond: must equal steadyRps / 40",
        )
        require(
            p["hourlyBulkUpdateObjects"] == p["steadyRps"] * 50,
            f"$.profiles[{index}].hourlyBulkUpdateObjects: must equal steadyRps * 50",
        )
        require(
            p["objects"] > 0 and all(n > p["objects"] for n in p["repeatObjects"]),
            f"$.profiles[{index}].repeatObjects: populations must exceed objects",
        )
    targets = data["proposedTargets"]
    for prefix, unit in [("query", "Ms"), ("action", "Ms"), ("continuousFreshness", "Seconds")]:
        require(
            0 < targets[f"{prefix}P95{unit}"] <= targets[f"{prefix}P99{unit}"],
            f"$.proposedTargets.{prefix}: percentile targets must be positive and ordered",
        )
    require(
        data["background"]["bulkFirstOffsetSeconds"] < data["windowsSeconds"]["steady"],
        "$.background.bulkFirstOffsetSeconds: must be less than windowsSeconds.steady",
    )
    require(
        [s["id"] for s in data["ingestionScenarios"]] == ["D-S", "D-M", "D-L"],
        "$.ingestionScenarios: IDs must be D-S, D-M, D-L in that order",
    )
    return errors
