import json
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from workers.value_codec import (
    CodecError,
    VALUE_VERSION,
    canonical_value,
    decode_native,
    encode_native,
    normalize_type,
    normalize_wire,
    normalize_row,
    OntologyObject,
)

CASES = json.loads(
    (Path(__file__).resolve().parents[1] / "packages/contracts/value-conformance.json").read_text(
        encoding="utf-8"
    )
)


class ValueContractTests(unittest.TestCase):
    def test_shared_wire_cases(self):
        self.assertEqual(CASES["version"], VALUE_VERSION)
        for case in CASES["valid"]:
            with self.subTest(case["name"]):
                args = (case["schema"], case.get("definitions"))
                expected = normalize_wire(case["normalized"], *args)
                self.assertEqual(normalize_wire(case["value"], *args), expected)
                self.assertEqual(
                    encode_native(decode_native(case["value"], *args), *args), expected
                )
                canonical_value(case["value"], *args)
        for case in CASES["invalid"]:
            with self.subTest(case["name"]), self.assertRaises(CodecError):
                normalize_wire(case["value"], case["schema"])
        for case in CASES["invalidTypes"]:
            with self.subTest(case["name"]), self.assertRaises(CodecError):
                normalize_type(case["schema"], case.get("definitions"))

    def test_python_native_exactness(self):
        object_type = UUID("00000000-0000-4000-8000-000000000004")
        object_uid = UUID("00000000-0000-4000-8000-000000000006")
        descriptor = {"type": "ontologyObject", "objectType": str(object_type), "nullable": False}
        customer = OntologyObject(object_type, object_uid)
        self.assertEqual(decode_native(encode_native(customer, descriptor), descriptor), customer)
        with self.assertRaises(CodecError):
            encode_native(OntologyObject(object_uid, object_uid), descriptor)
        self.assertEqual(
            normalize_row(
                {"name": "a"},
                {
                    "name": {"type": "string", "nullable": False},
                    "note": {"type": "string", "nullable": True},
                },
            ),
            {"name": "a", "note": None},
        )
        self.assertEqual(
            normalize_type({"type": "decimal", "nullable": False, "precision": 4.0, "scale": 2.0}),
            {"type": "decimal", "nullable": False, "precision": 4, "scale": 2},
        )
        decimal = {"type": "decimal", "nullable": False, "precision": 38, "scale": 6}
        self.assertEqual(
            encode_native(Decimal("123456789012345678901234567890.123456"), decimal),
            "123456789012345678901234567890.123456",
        )
        self.assertEqual(encode_native(Decimal("0"), {**decimal, "precision": 6}), "0.000000")
        integer = {"type": "int64", "nullable": False}
        self.assertEqual(encode_native(2**63 - 1, integer), "9223372036854775807")
        self.assertIs(type(decode_native("9223372036854775807", integer)), int)
        stamp = {"type": "timestamp", "nullable": False}
        value = datetime(2026, 10, 29, 12, 30, 40, 123456, tzinfo=timezone(timedelta(hours=2)))
        self.assertEqual(encode_native(value, stamp), "2026-10-29T10:30:40.123456Z")
        self.assertEqual(
            encode_native(date(1, 1, 1), {"type": "date", "nullable": False}), "0001-01-01"
        )
        uid = UUID("00000000-0000-0000-0000-000000000000")
        self.assertEqual(encode_native(uid, {"type": "uid", "nullable": False}), str(uid))
        for value in (float("nan"), float("inf"), -float("inf")):
            with self.assertRaises(CodecError):
                normalize_wire(value, {"type": "float64", "nullable": False})
        with self.assertRaises(CodecError):
            encode_native(datetime(2026, 1, 1), stamp)
        with self.assertRaises(CodecError):
            encode_native(datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1))), stamp)


if __name__ == "__main__":
    unittest.main()
