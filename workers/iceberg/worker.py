"""Trusted local Iceberg staging worker. JSON stdin/stdout; no public API.

The ephemeral SQL catalog is a private library workspace, never the reader
authority. Only Moria's atomic PostgreSQL publication exposes its final pointer.
Failed attempts leave unreachable files for future retention-aware GC.
"""

import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable


def wire(value):
    """Moria wire scalars; never round int64/decimal through floating point."""
    if value is None or isinstance(value, (str, bool, float)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (int, UUID)):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Timestamp requires an explicit timezone")
        return (
            value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [wire(item) for item in value]
    raise ValueError("Unsupported wire value")


def stream(request):
    size = request.get("batchRows", 1000)
    if type(size) is not int or not 1 <= size <= 1000:
        raise ValueError("Invalid batch size")
    table = StaticTable.from_metadata(request["metadataLocation"])
    snapshot = table.current_snapshot()
    if snapshot is None or str(snapshot.snapshot_id) != request["snapshotId"]:
        raise ValueError("Snapshot does not match pinned metadata")

    def emit(message):
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
                    {key: wire(value) for key, value in row.items()}
                    for row in batch.slice(offset, size).to_pylist()
                ]
                emit({"type": "batch", "rows": rows})
                count += len(rows)
    emit({"type": "end", "rows": str(count)})


def run(request):
    if request["operation"] == "read":
        table = StaticTable.from_metadata(request["metadataLocation"])
        return {
            "rows": table.scan().to_arrow().to_pylist(),
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
    data = pa.Table.from_pylist(request["rows"], schema=arrow_schema)
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
        "metadataLocation": table.metadata_location,
        "snapshotId": str(snapshot.snapshot_id),
        "tableUuid": str(table.metadata.table_uuid),
    }


if __name__ == "__main__":
    request = json.load(sys.stdin)
    if request["operation"] == "stream":
        stream(request)
    else:
        json.dump(run(request), sys.stdout)
        sys.stdout.write("\n")
