"""Moria value model v1: exact JSON wire values and Python native bindings."""

import json
import hashlib
import math
import re
import struct
from datetime import date, datetime, timezone
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

VALUE_VERSION = 1


@dataclass(frozen=True)
class OntologyObject:
    object_type: UUID
    uid: UUID


class CodecError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def normalize_type(schema, definitions=None, profile="value"):
    definitions = {} if definitions is None else definitions
    nodes = 0

    def invalid():
        raise CodecError("TYPE_INVALID", "Invalid or unsupported value type")

    if type(definitions) is not dict:
        invalid()

    def visit(item, stack=(), depth=0):
        nonlocal nodes
        nodes += 1
        if nodes > 10000 or depth > 20 or type(item) is not dict:
            invalid()

        def keys(allowed):
            if set(item) - set(allowed):
                invalid()

        if "ref" in item:
            keys(["ref"])
            name = item["ref"]
            if type(name) is not str or name not in definitions or name in stack:
                invalid()
            return visit(definitions[name], (*stack, name), depth + 1)
        kind = item.get("type")
        if kind not in ("object", "list") and type(item.get("nullable")) is not bool:
            invalid()
        if "nullable" in item and type(item["nullable"]) is not bool:
            invalid()
        nullable = item.get("nullable", False)
        result = {"type": kind, "nullable": nullable}
        if kind == "ontologyObject":
            keys(["type", "nullable", "objectType"])
            identity = item.get("objectType")
            if type(identity) is not str or not re.fullmatch(
                r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", identity
            ):
                invalid()
            result["objectType"] = identity
        elif kind == "object":
            keys(["type", "nullable", "properties", "optional"])
            properties = item.get("properties")
            optional = item.get("optional", [])
            if type(properties) is not dict or type(optional) is not list:
                invalid()
            if any(type(key) is not str or key not in properties for key in optional):
                invalid()
            if len(set(optional)) != len(optional):
                invalid()
            fields = {}
            if any(type(key) is not str for key in properties):
                invalid()
            for key in sorted(properties):
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,99}", key) or key in (
                    "__proto__",
                    "prototype",
                    "constructor",
                ):
                    invalid()
                fields[key] = visit(properties[key], stack, depth + 1)
            result["properties"] = fields
            if optional:
                result["optional"] = sorted(optional)
        elif kind in ("list", "array"):
            keys(["type", "nullable", "items"])
            result["type"] = "array"
            result["items"] = visit(item.get("items"), stack, depth + 1)
        elif kind == "decimal":
            keys(["type", "nullable", "precision", "scale"])
            precision, scale = item.get("precision"), item.get("scale")
            if (
                not (
                    type(precision) is int
                    or (
                        type(precision) is float
                        and math.isfinite(precision)
                        and precision.is_integer()
                    )
                )
                or not (
                    type(scale) is int
                    or (type(scale) is float and math.isfinite(scale) and scale.is_integer())
                )
                or not 1 <= precision <= 38
                or not 0 <= scale <= precision
            ):
                invalid()
            result.update(precision=int(precision), scale=int(scale))
        else:
            keys(["type", "nullable"])
            if kind not in ("string", "boolean", "int64", "float64", "date", "timestamp", "uid"):
                invalid()
        return result

    result = visit(schema)
    if profile not in ("value", "column"):
        invalid()
    if profile == "column" and (
        type(schema) is not dict
        or schema.get("type")
        not in (
            "string",
            "boolean",
            "int64",
            "float64",
            "decimal",
            "date",
            "timestamp",
            "uid",
            "array",
        )
    ):
        invalid()
    if profile == "column" and (
        result["type"] == "object"
        or (
            result["type"] == "array"
            and (
                result["items"]["nullable"]
                or result["items"]["type"] in ("array", "object", "ontologyObject")
            )
        )
    ):
        invalid()
    return result


def _convert(value, schema, mode, budget):
    budget[0] += 1
    kind = schema["type"]

    def invalid():
        raise CodecError("VALUE_INVALID", f"Invalid {kind} value")

    if budget[0] > 100000:
        invalid()
    if value is None:
        if schema["nullable"]:
            return None
        invalid()
    if kind == "ontologyObject":
        if mode == "encode" and isinstance(value, OntologyObject):
            value = {"objectType": str(value.object_type), "uid": str(value.uid)}
        if (
            type(value) is not dict
            or set(value) != {"objectType", "uid"}
            or value["objectType"] != schema["objectType"]
        ):
            invalid()
        uid = _convert(value["uid"], {"type": "uid", "nullable": False}, "wire", budget)
        return (
            OntologyObject(UUID(schema["objectType"]), UUID(uid))
            if mode == "decode"
            else {"objectType": schema["objectType"], "uid": uid}
        )
    if kind == "object":
        fields = schema["properties"]
        if type(value) is not dict or set(value) - set(fields):
            invalid()
        result = {}
        for key, field in fields.items():
            if key not in value:
                if key in schema.get("optional", []):
                    continue
                invalid()
            result[key] = _convert(value[key], field, mode, budget)
        return result
    if kind == "array":
        if type(value) is not list or len(value) > 100000:
            invalid()
        return [_convert(item, schema["items"], mode, budget) for item in value]
    if mode == "encode":
        if kind == "int64" and type(value) is int:
            if not -(2**63) <= value < 2**63:
                invalid()
            value = str(value)
        elif kind == "decimal" and isinstance(value, Decimal):
            if (
                not value.is_finite()
                or value.as_tuple().exponent < -schema["scale"]
                or (value != 0 and value.adjusted() > schema["precision"] - schema["scale"] - 1)
            ):
                invalid()
            value = format(value, "f")
        elif kind == "uid" and isinstance(value, UUID):
            value = str(value)
        elif kind == "timestamp" and isinstance(value, datetime):
            if value.utcoffset() is None:
                invalid()
            try:
                value = (
                    value.astimezone(timezone.utc)
                    .isoformat(timespec="microseconds")
                    .replace("+00:00", "Z")
                )
            except (ValueError, OverflowError):
                invalid()
        elif kind == "date" and type(value) is date:
            value = value.isoformat()
    if kind == "boolean":
        if type(value) is not bool:
            invalid()
        return value
    if kind == "float64":
        if type(value) not in (int, float):
            invalid()
        try:
            value = float(value)
        except OverflowError:
            invalid()
        if not math.isfinite(value):
            invalid()
        return 0.0 if value == 0 else value
    if type(value) is not str:
        invalid()
    if kind == "string":
        try:
            value.encode("utf-8", "strict")
        except UnicodeEncodeError:
            invalid()
        if "\0" in value:
            invalid()
        return value
    if kind == "uid":
        if not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value):
            invalid()
        return UUID(value) if mode == "decode" else value
    if kind == "int64":
        if (
            len(value) > 20
            or not re.fullmatch(r"-?(0|[1-9][0-9]*)", value)
            or not -(2**63) <= int(value) < 2**63
        ):
            invalid()
        return int(value) if mode == "decode" else str(int(value))
    if kind == "decimal":
        if len(value) > 80 or not re.fullmatch(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?", value):
            invalid()
        whole, _, fraction = value.lstrip("-").partition(".")
        scale = schema["scale"]
        if len(fraction) > scale or len(whole.lstrip("0")) > schema["precision"] - scale:
            invalid()
        normalized = whole + ("." + fraction.ljust(scale, "0") if scale else "")
        if value.startswith("-") and any(char in "123456789" for char in normalized):
            normalized = "-" + normalized
        return Decimal(normalized) if mode == "decode" else normalized
    if kind == "date":
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            invalid()
        try:
            result = date.fromisoformat(value)
        except ValueError:
            invalid()
        return result if mode == "decode" else result.isoformat()
    if kind == "timestamp":
        if not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z", value
        ):
            invalid()
        try:
            result = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError:
            invalid()
        return (
            result
            if mode == "decode"
            else result.isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
    invalid()


def normalize_wire(value, schema, definitions=None):
    return _convert(value, normalize_type(schema, definitions), "wire", [0])


def normalize_row(value, columns, profile="column"):
    schema = normalize_type({"type": "object", "properties": columns})
    if profile not in ("column", "value"):
        raise CodecError("TYPE_INVALID", "Invalid row schema")
    if profile == "column":
        for field in columns.values():
            normalize_type(field, profile="column")
    if type(value) is not dict or set(value) - set(columns):
        raise CodecError("ROW_INVALID", "Expected an object row with declared fields")
    value = dict(value)
    for key, field in schema["properties"].items():
        if key not in value and field["nullable"]:
            value[key] = None
    return _convert(value, schema, "wire", [0])


def decode_native(value, schema, definitions=None):
    return _convert(value, normalize_type(schema, definitions), "decode", [0])


def encode_native(value, schema, definitions=None):
    return _convert(value, normalize_type(schema, definitions), "encode", [0])


def canonical_value(value, schema, definitions=None):
    schema = normalize_type(schema, definitions)
    normalized = _convert(value, schema, "wire", [0])

    def typed(value, schema):
        if value is None:
            return None
        if schema["type"] == "float64":
            return {"$float64": struct.pack(">d", value).hex()}
        if schema["type"] == "array":
            return [typed(item, schema["items"]) for item in value]
        if schema["type"] == "object":
            return {key: typed(item, schema["properties"][key]) for key, item in value.items()}
        return value

    return json.dumps(
        {"version": VALUE_VERSION, "schema": schema, "value": typed(normalized, schema)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def value_fingerprint(value, schema, definitions=None):
    return (
        "sha256:"
        + hashlib.sha256(canonical_value(value, schema, definitions).encode("utf-8")).hexdigest()
    )
