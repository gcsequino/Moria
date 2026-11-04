"""Create real Iceberg case fixtures using the explicit UTF8 JSON case profile."""

import hashlib
import json
import sys
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import FileIO, OutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

root = Path(sys.argv[1])
logical = sys.argv[2]
rows = json.loads(Path(sys.argv[3]).read_text())
objects = {}


class PhysicalOutput(OutputFile):
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


class LogicalIO(FileIO):
    def physical(self, name):
        if not name.startswith(logical + "/"):
            raise ValueError("Unexpected fixture reference")
        path = root / hashlib.sha256(name.encode()).hexdigest()
        objects[name] = str(path)
        return str(path)

    def new_input(self, location):
        return PyArrowFileIO().new_input(self.physical(location))

    def new_output(self, location):
        return PhysicalOutput(location, self.physical(location))

    def delete(self, location):
        raise ValueError("Immutable fixture")


catalog = SqlCatalog(
    "evaluation-fixture",
    uri="sqlite:///:memory:",
    warehouse=logical,
    **{"py-io-impl": "__main__.LogicalIO"},
)
catalog.create_namespace("fixture")
table = catalog.create_table(
    "fixture.cases",
    Schema(
        NestedField(1, "caseId", StringType(), required=True),
        NestedField(2, "inputJson", StringType(), required=True),
        NestedField(3, "expectedJson", StringType(), required=True),
    ),
    location=logical,
    properties={"format-version": "2"},
)
table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))
metadata = f"{logical}/metadata/root.metadata.json"
objects[metadata] = objects.pop(table.metadata_location)
print(json.dumps({"metadata": metadata, "snapshotId": str(table.current_snapshot().snapshot_id), "tableUuid": str(table.metadata.table_uuid), "objects": objects}))
