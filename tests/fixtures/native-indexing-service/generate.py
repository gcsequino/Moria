"""Produce real UUID Parquet snapshots with an optional mapped maximum-width row."""

import hashlib
import json
import sys
import uuid
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import FileIO, OutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, UUIDType

root, uid_path = Path(sys.argv[1]), Path(sys.argv[2])
wide = len(sys.argv) > 3 and sys.argv[3] == "--wide"
logical = "moria://service/data"
objects = {}
property_name = "v" * 100 if wide else "value"
uids = uid_path.read_text().splitlines()


class Output(OutputFile):
    def __init__(self, name, physical):
        super().__init__(name)
        self.physical = PyArrowFileIO().new_output(physical)

    def __len__(self):
        return len(self.physical)

    def exists(self):
        return self.physical.exists()

    def to_input_file(self):
        return self.physical.to_input_file()

    def create(self, overwrite=False):
        return self.physical.create(overwrite)


class IO(FileIO):
    def physical(self, name):
        if not name.startswith(logical + "/"):
            raise ValueError("Unexpected reference")
        path = root / hashlib.sha256(name.encode()).hexdigest()
        objects[name] = str(path)
        return str(path)

    def new_input(self, name):
        return PyArrowFileIO().new_input(self.physical(name))

    def new_output(self, name):
        return Output(name, self.physical(name))

    def delete(self, name):
        raise ValueError("Immutable fixture")


catalog = SqlCatalog(
    "fixture", uri="sqlite:///:memory:", warehouse=logical, **{"py-io-impl": "__main__.IO"}
)
catalog.create_namespace("fixture")
table = catalog.create_table(
    "fixture.data",
    Schema(
        NestedField(1, "uid", UUIDType(), required=True),
        NestedField(2, property_name, StringType(), required=True),
        NestedField(3, "tenant", StringType(), required=True),
    ),
    location=logical,
    properties={"format-version": "2"},
)
for start in range(0, len(uids), 8 if wide else 100):
    rows = [
        {
            "uid": uuid.UUID(uid).bytes,
            property_name: ("x" * (4 * 1024 * 1024 - 80) if wide else f"row-{i}"),
            "tenant": "a" if i % 2 == 0 else "b",
        }
        for i, uid in enumerate(uids[start : start + (8 if wide else 100)], start)
    ]
    table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))
metadata_path = objects[table.metadata_location]
print(
    json.dumps(
        {
            "metadata": metadata_path,
            "metadataName": table.metadata_location,
            "snapshotId": str(table.current_snapshot().snapshot_id),
            "tableUuid": str(table.metadata.table_uuid),
            "icebergSchema": table.schema().model_dump(by_alias=True),
            "property": property_name,
            "objects": {name: path for name, path in objects.items() if path != metadata_path},
        }
    )
)
