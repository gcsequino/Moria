"""Write real Iceberg capacity input in bounded Arrow batches from native UID allocation."""

import base64
import hashlib
import json
import sys
import time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import FileIO, OutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO, parquet_file_to_data_file
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DecimalType, LongType, NestedField, StringType

wide = sys.argv[-1] == "--wide-reducer-regression"
root, uid_file, count, row_bytes, seed = sys.argv[1:-1] if wide else sys.argv[1:]
root, count, row_bytes = Path(root), int(count), int(row_bytes)
valid = (
    (1 <= count <= 128 and row_bytes == 1024 * 1024)
    if wide
    else (1 <= count <= 100_000_000 and 1024 <= row_bytes <= 16 * 1024)
)
if not valid:
    raise ValueError("Invalid named population/width profile")
logical_table = "moria://warehouse/table"
objects = {}
started = time.monotonic()


class FixtureOutput(OutputFile):
    def __init__(self, name, path):
        super().__init__(name)
        self.delegate = PyArrowFileIO().new_output(path)

    def __len__(self):
        return len(self.delegate)

    def exists(self):
        return self.delegate.exists()

    def to_input_file(self):
        return self.delegate.to_input_file()

    def create(self, overwrite=False):
        return self.delegate.create(overwrite)


class FixtureIO(FileIO):
    def physical(self, name):
        if not name.startswith(logical_table + "/"):
            raise ValueError("Foreign fixture object")
        path = root / hashlib.sha256(name.encode()).hexdigest()
        objects[name] = str(path)
        return str(path)

    def new_input(self, location):
        return PyArrowFileIO().new_input(self.physical(location))

    def new_output(self, location):
        return FixtureOutput(location, self.physical(location))

    def delete(self, location):
        raise ValueError("Immutable input")


catalog = SqlCatalog(
    "capacity",
    uri="sqlite:///:memory:",
    warehouse=logical_table,
    **{"py-io-impl": "__main__.FixtureIO"},
)
catalog.create_namespace("fixture")
table = catalog.create_table(
    "fixture.data",
    Schema(
        NestedField(1, "uid", StringType(), required=True),
        NestedField(2, "id", LongType(), required=True),
        NestedField(3, "name", StringType(), required=True),
        NestedField(4, "region", StringType(), required=True),
        NestedField(5, "enabled", BooleanType(), required=True),
        NestedField(6, "rank", LongType(), required=True),
        NestedField(7, "amount", DecimalType(20, 2), required=True),
        NestedField(8, "payload", StringType(), required=True),
    ),
    location=logical_table,
    properties={"format-version": "2", "write.metadata.metrics.column.payload": "none"},
)
schema = table.schema().as_arrow()
io = table.io
rows_per_file = min(4096, max(1, 16 * 1024 * 1024 // row_bytes))
row_count = 0
logical_bytes = 0
physical_data_bytes = 0
source_digest = hashlib.sha256()
source_sum = 0
source_xor = 0
batch = []


def append_file(snapshot, rows):
    global physical_data_bytes
    name = f"{logical_table}/data/part-{rows[0]['id']:010d}.parquet"
    path = io.physical(name)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        path,
        row_group_size=1000,
        compression="NONE",
        use_dictionary=False,
        write_statistics=["uid", "id", "name", "region", "enabled", "rank", "amount"],
    )
    physical_data_bytes += Path(path).stat().st_size
    snapshot.append_data_file(parquet_file_to_data_file(io, table.metadata, name))


with table.transaction() as transaction:
    with transaction.update_snapshot().fast_append() as snapshot:
        with open(uid_file, encoding="utf-8") as identities:
            for line in identities:
                identity = json.loads(line)
                if identity["ordinal"] != row_count or row_count >= count:
                    raise ValueError("UID stream count/order mismatch")
                ordinal = row_count
                row = {
                    "uid": identity["uid"],
                    "id": ordinal,
                    "name": f"source-{ordinal % 1_000_000:06d}",
                    "region": ["a", "b", "c"][ordinal % 3],
                    "enabled": ordinal % 2 == 0,
                    "rank": 9007199254740993 + ordinal,
                    "amount": Decimal(ordinal % 1_000_000).scaleb(-2).quantize(Decimal("0.01")),
                    "payload": "",
                }
                encoded = {
                    **row,
                    "id": str(row["id"]),
                    "rank": str(row["rank"]),
                    "amount": str(row["amount"]),
                }
                size = row_bytes - len(json.dumps(encoded, separators=(",", ":")).encode())
                if size <= 0:
                    raise ValueError("Width profile cannot contain required fields")
                entropy = hashlib.shake_256(f"{seed}:{ordinal}".encode()).digest(
                    (size * 3 // 4) + 3
                )
                row["payload"] = base64.b64encode(entropy).decode()[:size]
                encoded["payload"] = row["payload"]
                raw = json.dumps(encoded, sort_keys=True, separators=(",", ":")).encode()
                if len(raw) != row_bytes:
                    raise ValueError("Logical row width mismatch")
                source_digest.update(len(raw).to_bytes(8, "big"))
                source_digest.update(raw)
                hashed = int.from_bytes(hashlib.sha256(raw).digest(), "big")
                source_sum = (source_sum + hashed) % (1 << 256)
                source_xor ^= hashed
                logical_bytes += len(raw)
                row_count += 1
                batch.append(row)
                if len(batch) == rows_per_file:
                    append_file(snapshot, batch)
                    batch = []
            if batch:
                append_file(snapshot, batch)
if row_count != count:
    raise ValueError("Incomplete native UID stream")

objects_file = root / "objects.jsonl"
with objects_file.open("x", encoding="utf-8") as inventory:
    for name, path in objects.items():
        inventory.write(json.dumps([name, path], separators=(",", ":")) + "\n")
result = {
    "profile": "wide-reducer-regression" if wide else "capacity",
    "metadata": table.metadata_location,
    "snapshotId": str(table.current_snapshot().snapshot_id),
    "tableUuid": str(table.metadata.table_uuid),
    "objectsFile": str(objects_file),
    "rows": str(row_count),
    "rowBytes": row_bytes,
    "logicalBytes": str(logical_bytes),
    "physicalDataBytes": str(physical_data_bytes),
    "objects": len(objects),
    "sourceDigest": source_digest.hexdigest(),
    "sourceSum": f"{source_sum:064x}",
    "sourceXor": f"{source_xor:064x}",
    "elapsedSeconds": time.monotonic() - started,
    "seed": seed,
    "rowsPerFile": rows_per_file,
}
(root / "source-manifest.json").write_text(json.dumps(result), encoding="utf-8")
print(json.dumps(result))
