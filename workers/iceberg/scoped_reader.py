"""Fixed trusted engine entrypoint; no catalog discovery or provider IO."""

import base64
import resource
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Bound native allocations as well as Python planning. This is a fixture-scale
# read envelope, not an arbitrary-code isolation boundary.
resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
resource.setrlimit(resource.RLIMIT_CPU, (60, 60))
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

import pyarrow as pa
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.table import StaticTable
from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.manifest import DataFileContent, FileFormat
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimestamptzType,
    StringType,
    UUIDType,
    ListType,
)
from scoped_io import ScopedIO, exchange, receive
from worker import wire


def main():
    request = receive()
    metadata = TableMetadataUtil.parse_raw(base64.b64decode(request["metadata"]).decode())
    if metadata.format_version != 2 or str(metadata.table_uuid) != request["tableUuid"]:
        raise ValueError("Table mismatch")
    if str(metadata.current_snapshot_id) != request["snapshotId"]:
        raise ValueError("Snapshot mismatch")
    if any(spec.fields for spec in metadata.partition_specs) or metadata.sort_orders[-1].fields:
        raise ValueError("Partitioned/sorted tables are not supported")
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    scoped = ScopedIO(request["objects"])
    table = StaticTable(("scoped",), metadata, "pinned", scoped, NoopCatalog("scoped"))
    scalars = (
        BooleanType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType,
        DateType,
        TimestamptzType,
        StringType,
        UUIDType,
    )
    for field in table.schema().fields:
        field_type = field.field_type
        if isinstance(field_type, ListType):
            if not field_type.element_required:
                raise ValueError("Nullable array elements are not supported")
            field_type = field_type.element_type
        if not isinstance(field_type, scalars):
            raise ValueError("Unsupported scalar schema")
    scan = table.scan(snapshot_id=int(request["snapshotId"]))
    tasks = list(scan.plan_files())
    if len(tasks) > 128 or any(
        task.delete_files
        or task.file.content != DataFileContent.DATA
        or task.file.file_format != FileFormat.PARQUET
        for task in tasks
    ):
        raise ValueError("Unsupported scan")
    if exchange(
        {
            "type": "start",
            "snapshotId": request["snapshotId"],
            "tableUuid": request["tableUuid"],
            "schema": table.schema().model_dump(mode="json", by_alias=True),
        }
    ) != {"next": True}:
        raise ValueError("Invalid acknowledgment")
    count = 0
    with scan.to_arrow_batch_reader() as reader:
        for batch in reader:
            for offset in range(0, batch.num_rows, 100):
                rows = [
                    {key: wire(value) for key, value in row.items()}
                    for row in batch.slice(offset, 100).to_pylist()
                ]
                count += len(rows)
                if count > 100000:
                    raise ValueError("Row limit")
                if exchange({"type": "batch", "rows": rows}) != {"next": True}:
                    raise ValueError("Invalid acknowledgment")
    if exchange({"type": "end", "rows": str(count)}) != {"next": True}:
        raise ValueError("Invalid acknowledgment")


if __name__ == "__main__":
    main()
