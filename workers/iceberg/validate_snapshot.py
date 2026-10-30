"""Bounded, fixed complete-snapshot validator; all FileIO is in-memory and exact."""

import base64
import io
import json
import resource
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
resource.setrlimit(resource.RLIMIT_CPU, (60, 60))
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.io import FileIO, InputFile
from pyiceberg.manifest import DataFileContent, FileFormat, ManifestContent
from pyiceberg.table import StaticTable
from pyiceberg.exceptions import ValidationError
from pyiceberg.table.metadata import TableMetadataUtil
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
from worker import value_schema, encode_native
from pyiceberg.io.pyarrow import pyarrow_to_schema

FRAME = 2 * 1024**2


def receive():
    line = sys.stdin.buffer.readline(FRAME + 2)
    if not line.endswith(b"\n") or len(line) > FRAME + 1:
        raise ValueError("Frame limit")
    return json.loads(line)


class MemoryInput(InputFile):
    def __init__(self, name, data):
        super().__init__(name)
        self.data = data

    def __len__(self):
        return len(self.data)

    def exists(self):
        return True

    def open(self, seekable=True):
        return io.BytesIO(self.data)


class MemoryIO(FileIO):
    def __init__(self, objects):
        super().__init__({})
        self.objects = objects
        self.used = set()
        self.roles = {}

    def reference(self, name, role):
        if name not in self.objects or self.roles.get(name, role) != role:
            raise ValueError("Missing or conflicting reference")
        self.roles[name] = role
        self.used.add(name)
        return self.objects[name]

    def new_input(self, location):
        if location not in self.objects:
            raise ValueError("Unbound reference")
        self.used.add(location)
        return MemoryInput(location, self.objects[location])

    def new_output(self, location):
        raise PermissionError("Read only")

    def delete(self, location):
        raise PermissionError("Read only")


def main():
    request = receive()
    declarations = request["objects"]
    if len(declarations) > 128:
        raise ValueError("Object limit")
    objects = {}
    total = 0
    for declaration in declarations:
        name, size = declaration["name"], declaration["size"]
        if name in objects or not 1 <= size <= 16 * 1024**2:
            raise ValueError("Invalid object")
        total += size
        if total > 64 * 1024**2:
            raise ValueError("Byte limit")
        data = bytearray()
        while len(data) < size:
            frame = receive()
            if set(frame) != {"data"}:
                raise ValueError("Invalid frame")
            chunk = base64.b64decode(frame["data"], validate=True)
            if not 1 <= len(chunk) <= min(256 * 1024, size - len(data)):
                raise ValueError("Chunk limit")
            data.extend(chunk)
        objects[name] = bytes(data)
    if sys.stdin.buffer.read(1):
        raise ValueError("Extra input")
    scoped = MemoryIO(objects)
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
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
    visited_metadata = set()
    active_metadata = set()
    decoded_rows = 0
    decoded_bytes = 0
    manifests = set()
    data_files = {}
    scanned_snapshots = set()
    snapshot_definitions = {}
    schema_definitions = {}
    row_count = 0
    row_bytes = 0

    def validate(raw, root=False):
        nonlocal row_count, row_bytes, decoded_rows, decoded_bytes
        if len(raw) > 1024**2:
            raise ValueError("Metadata limit")
        metadata = TableMetadataUtil.parse_raw(raw)
        if (
            metadata.format_version != 2
            or str(metadata.table_uuid) != request["tableUuid"]
            or metadata.location != request["location"]
        ):
            raise ValueError("Table mismatch")
        if metadata.current_snapshot_id is not None and metadata.current_snapshot() is None:
            raise ValueError("Unresolved current snapshot")
        snapshot_ids = {snapshot.snapshot_id for snapshot in metadata.snapshots}
        if any(ref.snapshot_id not in snapshot_ids for ref in metadata.refs.values()):
            raise ValueError("Unresolved snapshot reference")
        if root and str(metadata.current_snapshot_id) != request["snapshotId"]:
            raise ValueError("Snapshot mismatch")
        if (
            any(spec.fields for spec in metadata.partition_specs)
            or not metadata.sort_orders
            or any(order.fields for order in metadata.sort_orders)
            or metadata.statistics
            or metadata.partition_statistics
        ):
            raise ValueError("Unsupported metadata feature")
        schemas = {schema.schema_id: schema for schema in metadata.schemas}
        if len(schemas) != len(metadata.schemas):
            raise ValueError("Duplicate schema ID")
        if metadata.current_schema_id not in schemas:
            raise ValueError("Unresolved current schema")
        for snapshot in metadata.snapshots:
            if snapshot.schema_id is not None and snapshot.schema_id not in schemas:
                raise ValueError("Unresolved snapshot schema")
        for schema in metadata.schemas:
            definition = schema.model_dump_json()
            if schema_definitions.get(schema.schema_id, definition) != definition:
                raise ValueError("Conflicting immutable schema")
            schema_definitions[schema.schema_id] = definition
            if len(schema.fields) > 256:
                raise ValueError("Column limit")
            for field in schema.fields:
                kind = field.field_type
                if isinstance(kind, ListType):
                    if not kind.element_required:
                        raise ValueError("Nullable array elements")
                    kind = kind.element_type
                if not isinstance(kind, scalars):
                    raise ValueError("Unsupported schema")
        if len({entry.metadata_file for entry in metadata.metadata_log}) != len(
            metadata.metadata_log
        ):
            raise ValueError("Duplicate metadata reference")
        for previous in metadata.metadata_log:
            name = previous.metadata_file
            previous_raw = scoped.reference(name, "metadata")
            if name in active_metadata:
                raise ValueError("Cyclic metadata history")
            if name not in visited_metadata:
                active_metadata.add(name)
                validate(previous_raw)
                active_metadata.remove(name)
                visited_metadata.add(name)
        table = StaticTable(("scoped",), metadata, "pinned", scoped, NoopCatalog("scoped"))
        if len({snapshot.snapshot_id for snapshot in metadata.snapshots}) != len(
            metadata.snapshots
        ):
            raise ValueError("Duplicate snapshot")
        for snapshot in metadata.snapshots:
            definition = snapshot.model_dump_json()
            if snapshot_definitions.get(snapshot.snapshot_id, definition) != definition:
                raise ValueError("Conflicting immutable snapshot")
            snapshot_definitions[snapshot.snapshot_id] = definition
            scoped.reference(snapshot.manifest_list, "manifest-list")
            snapshot_manifests = snapshot.manifests(scoped)
            if len({manifest.manifest_path for manifest in snapshot_manifests}) != len(
                snapshot_manifests
            ):
                raise ValueError("Duplicate manifest reference")
            for manifest in snapshot_manifests:
                raw_manifest = scoped.reference(manifest.manifest_path, "manifest")
                if (
                    manifest.content != ManifestContent.DATA
                    or len(raw_manifest) != manifest.manifest_length
                ):
                    raise ValueError("Unsupported manifest")
                if manifest.manifest_path in manifests:
                    continue
                manifests.add(manifest.manifest_path)
                for entry in manifest.fetch_manifest_entry(scoped, discard_deleted=False):
                    file = entry.data_file
                    data = scoped.reference(file.file_path, "data")
                    if (
                        file.content != DataFileContent.DATA
                        or file.file_format != FileFormat.PARQUET
                        or file.file_size_in_bytes != len(data)
                        or file.key_metadata
                        or file.sort_order_id not in (None, 0)
                    ):
                        raise ValueError("Unsupported data file")
                    signature = (file.file_size_in_bytes, file.record_count)
                    if file.file_path in data_files and data_files[file.file_path] != signature:
                        raise ValueError("Conflicting data declaration")
                    if file.file_path not in data_files:
                        parquet = pq.ParquetFile(io.BytesIO(data))
                        if (
                            parquet.metadata.num_rows != file.record_count
                            or file.record_count > 100000
                        ):
                            raise ValueError("Parquet count mismatch")
                        # Include deleted historical entries: every supplied data page must parse.
                        logical = value_schema(
                            pyarrow_to_schema(parquet.schema_arrow).model_dump(by_alias=True)
                        )
                        for batch in parquet.iter_batches(batch_size=100):
                            rows = [encode_native(row, logical) for row in batch.to_pylist()]
                            for row in rows:
                                for value in row.values():
                                    if isinstance(value, list) and any(
                                        item is None or isinstance(item, (list, dict))
                                        for item in value
                                    ):
                                        raise ValueError("Unsupported array element")
                            encoded = json.dumps(rows, allow_nan=False).encode()
                            decoded_rows += len(rows)
                            decoded_bytes += len(encoded)
                            if (
                                len(encoded) > 4 * 1024**2
                                or decoded_rows > 100000
                                or decoded_bytes > 32 * 1024**2
                            ):
                                raise ValueError("Decoded data limit")
                    data_files[file.file_path] = signature
            # Parse actual Parquet pages through the same scoped Iceberg scan as readRows.
            # Missing schema IDs use this metadata's current schema in the pinned reader.
            # A historical scan is reusable only for the same resolved projection.
            scan = table.scan(snapshot_id=snapshot.snapshot_id)
            projection = scan.projection()
            # readRows advertises table.schema() but emits this snapshot's projection.
            # Compare fields, not schema IDs, before accepting a readable current head.
            if (
                snapshot.snapshot_id == metadata.current_snapshot_id
                and projection.fields != table.schema().fields
            ):
                raise ValueError("Current schema and snapshot projection mismatch")
            key = (
                snapshot.snapshot_id,
                snapshot.manifest_list,
                projection.model_dump_json(),
            )
            if key in scanned_snapshots:
                continue
            scanned_snapshots.add(key)
            if len(scanned_snapshots) > 128:
                raise ValueError("Snapshot limit")
            tasks = list(scan.plan_files())
            if (
                len(tasks) > 128
                or len({task.file.file_path for task in tasks}) != len(tasks)
                or any(task.delete_files for task in tasks)
            ):
                raise ValueError("Unsupported scan")
            logical = value_schema(projection.model_dump(by_alias=True))
            with scan.to_arrow_batch_reader() as reader:
                for batch in reader:
                    for offset in range(0, batch.num_rows, 100):
                        rows = [
                            encode_native(row, logical)
                            for row in batch.slice(offset, 100).to_pylist()
                        ]
                        encoded = json.dumps(
                            {"type": "batch", "rows": rows}, allow_nan=False
                        ).encode()
                        row_count += len(rows)
                        row_bytes += len(encoded)
                        if (
                            len(encoded) > 4 * 1024**2
                            or row_count > 100000
                            or row_bytes > 32 * 1024**2
                        ):
                            raise ValueError("Row limit")

    raw = base64.b64decode(request["metadata"], validate=True)
    validate(raw, root=True)
    if scoped.used != set(objects):
        raise ValueError("Extra objects")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, ValidationError):
        sys.exit(65)
