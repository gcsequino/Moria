"""Trusted local Iceberg staging worker. JSON stdin/stdout; no public API.

The ephemeral SQL catalog is a private library workspace, never the reader
authority. Only Moria's atomic PostgreSQL publication exposes its final pointer.
Failed attempts leave unreachable files for future retention-aware GC.
"""

import json
import sys
import re
import struct
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from workers.value_codec import VALUE_VERSION, decode_native, encode_native


def value_schema(physical, nullable=False):
    if isinstance(physical, dict):
        if physical["type"] == "struct":
            return {
                "type": "object",
                "nullable": nullable,
                "properties": {
                    field["name"]: value_schema(field["type"], not field["required"])
                    for field in physical["fields"]
                },
            }
        if physical["type"] == "list":
            return {
                "type": "array",
                "nullable": nullable,
                "items": value_schema(physical["element"], not physical["element-required"]),
            }
        raise ValueError("Unsupported physical value type")
    decimal = re.fullmatch(r"decimal\(([0-9]+),\s*([0-9]+)\)", physical)
    if decimal:
        return {
            "type": "decimal",
            "nullable": nullable,
            "precision": int(decimal[1]),
            "scale": int(decimal[2]),
        }
    kind = {
        "string": "string",
        "boolean": "boolean",
        "int": "int64",
        "long": "int64",
        "float": "float64",
        "double": "float64",
        "date": "date",
        "timestamptz": "timestamp",
        "uuid": "uid",
    }.get(physical)
    if not kind:
        raise ValueError("Unsupported physical value type")
    return {"type": kind, "nullable": nullable}


def incoming(value, schema):
    if value is None:
        return None
    if schema["type"] == "int64" and type(value) in (int, float):
        if not -(2**53 - 1) <= value <= 2**53 - 1 or int(value) != value:
            raise ValueError(
                "Engine integers outside the safe JSON range require exact wire strings"
            )
        return str(int(value))
    if schema["type"] == "object":
        if type(value) is not dict or set(value) - set(schema["properties"]):
            raise ValueError("Invalid engine row")
        return {key: incoming(value.get(key), field) for key, field in schema["properties"].items()}
    if schema["type"] == "array" and type(value) is list:
        return [incoming(item, schema["items"]) for item in value]
    return value


def arrow_value(value, physical):
    if value is None:
        return None
    if isinstance(value, UUID):
        return value.bytes
    if isinstance(physical, dict):
        if physical["type"] == "struct":
            return {
                field["name"]: arrow_value(value[field["name"]], field["type"])
                for field in physical["fields"]
            }
        if physical["type"] == "list":
            return [arrow_value(item, physical["element"]) for item in value]
    if physical == "float" and struct.unpack(">f", struct.pack(">f", value))[0] != value:
        raise ValueError("Float32 narrowing would lose precision")
    return value


def check_version(request):
    if (
        type(request.get("valueVersion")) not in (int, float)
        or request["valueVersion"] != VALUE_VERSION
    ):
        raise ValueError("Unsupported worker value version")


def arrow_array(values, kind):
    if isinstance(kind, pa.BaseExtensionType):
        return pa.ExtensionArray.from_storage(kind, arrow_array(values, kind.storage_type))
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        offsets, elements = [0], []
        for value in values:
            elements.extend(value or [])
            offsets.append(len(elements))
        factory = pa.LargeListArray if pa.types.is_large_list(kind) else pa.ListArray
        return factory.from_arrays(
            offsets,
            arrow_array(elements, kind.value_type),
            type=kind,
            mask=pa.array([value is None for value in values], type=pa.bool_()),
        )
    if pa.types.is_struct(kind):
        return pa.StructArray.from_arrays(
            [
                arrow_array(
                    [None if value is None else value[field.name] for value in values], field.type
                )
                for field in kind
            ],
            fields=list(kind),
            mask=pa.array([value is None for value in values], type=pa.bool_()),
        )
    return pa.array(values, type=kind)


def stream(request):
    check_version(request)
    size = request.get("batchRows", 1000)
    if type(size) is not int or not 1 <= size <= 1000:
        raise ValueError("Invalid batch size")
    table = StaticTable.from_metadata(request["metadataLocation"])
    snapshot = table.current_snapshot()
    if snapshot is None or str(snapshot.snapshot_id) != request["snapshotId"]:
        raise ValueError("Snapshot does not match pinned metadata")
    logical = value_schema(table.schema().model_dump(by_alias=True))

    def emit(message):
        message["valueVersion"] = VALUE_VERSION
        line = json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(line) > 4 * 1024 * 1024:
            raise ValueError("Stream frame exceeds limit")
        sys.stdout.buffer.write(line + b"\n")
        sys.stdout.buffer.flush()

    emit(
        {
            "type": "start",
            "snapshotId": str(snapshot.snapshot_id),
            "tableUuid": str(table.metadata.table_uuid),
        }
    )
    count = 0
    # Arrow owns scan-batch memory; Python dictionaries are bounded to batchRows.
    # Manifest/file planning can still scale with file count.
    with table.scan(snapshot_id=snapshot.snapshot_id).to_arrow_batch_reader() as reader:
        for batch in reader:
            for offset in range(0, batch.num_rows, size):
                rows = [
                    encode_native(row, logical) for row in batch.slice(offset, size).to_pylist()
                ]
                emit({"type": "batch", "rows": rows})
                count += len(rows)
    emit({"type": "end", "rows": str(count)})


def run(request):
    check_version(request)
    if request["operation"] == "read":
        table = StaticTable.from_metadata(request["metadataLocation"])
        logical = value_schema(table.schema().model_dump(by_alias=True))
        with table.scan().to_arrow_batch_reader() as reader:
            rows = [encode_native(row, logical) for batch in reader for row in batch.to_pylist()]
        return {
            "valueVersion": VALUE_VERSION,
            "rows": rows,
            "snapshotId": str(table.current_snapshot().snapshot_id),
        }
    if request["operation"] != "stage":
        raise ValueError("Unknown operation")
    warehouse = Path(request["warehouse"]).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    # PyArrowFileIO recognizes drive-qualified paths; file:///C:/ is parsed as
    # /C:/ by PyIceberg 0.12 on Windows, which is not a valid Windows path.
    catalog = SqlCatalog("staging", uri="sqlite:///:memory:", warehouse=warehouse.as_posix())
    catalog.create_namespace("staged")
    identifier = ("staged", "attempt_" + uuid4().hex)
    base = request.get("baseMetadataLocation")
    if base:
        table = catalog.register_table(identifier, base)
        if request.get("schema") and table.schema() != Schema.model_validate(request["schema"]):
            raise ValueError("Schema evolution requires an explicit migration")
    else:
        table = catalog.create_table(
            identifier, Schema.model_validate(request["schema"]), properties={"format-version": "2"}
        )
    arrow_schema = table.schema().as_arrow()
    physical = table.schema().model_dump(by_alias=True)
    logical = value_schema(physical)
    rows = [
        arrow_value(decode_native(incoming(row, logical), logical), physical)
        for row in request["rows"]
    ]
    data = pa.Table.from_arrays(
        [arrow_array([row[field.name] for row in rows], field.type) for field in arrow_schema],
        schema=arrow_schema,
    )
    if request.get("mode", "replace") == "replace":
        table.overwrite(data)
    elif request["mode"] == "append":
        table.append(data)
    else:
        raise ValueError("Unsupported write mode")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise ValueError("Write did not produce a snapshot")
    return {
        "valueVersion": VALUE_VERSION,
        "metadataLocation": table.metadata_location,
        "snapshotId": str(snapshot.snapshot_id),
        "tableUuid": str(table.metadata.table_uuid),
    }


if __name__ == "__main__":
    request = json.load(sys.stdin)
    if request["operation"] == "stream":
        stream(request)
    else:
        json.dump(run(request), sys.stdout, allow_nan=False)
        sys.stdout.write("\n")
