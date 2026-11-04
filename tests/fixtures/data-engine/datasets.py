"""Real three-source Iceberg input fixtures with exact int64s and wide transport rows."""

import hashlib
import json
import sys
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import FileIO, OutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType

root = Path(sys.argv[1])
logical = ""
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
    "engine-fixture",
    uri="sqlite:///:memory:",
    warehouse="moria://enginewarehouse",
    **{"py-io-impl": "__main__.LogicalIO"},
)
catalog.create_namespace("fixture")
results = []
names = ["inputA", "inputB", "inputC"]
if "--fan-in" in sys.argv[2:]:
    names += [f"Fanin{index:02}" for index in range(64)]
for ordinal, name in enumerate(names):
    logical = f"moria://enginewarehouse/{name}"
    objects = {}
    table = catalog.create_table(
        f"fixture.{name}",
        Schema(*([
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "group", StringType(), required=True),
            NestedField(3, "amount", LongType(), required=True),
            NestedField(4, "padding", StringType(), required=True),
        ] if ordinal < 3 else [
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "total", LongType(), required=True),
        ])),
        location=logical,
        properties={"format-version": "2"},
    )
    rows = [
        {
            "id": index,
            "group": f"g{index % 10}",
            "amount": 9007199254740993 if ordinal == 0 else ordinal,
            "padding": "x" * 2048,
        }
        for index in range(6000)
    ] if ordinal < 3 else [{"id": ordinal, "total": ordinal}]
    table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))
    metadata = f"{logical}/metadata/root.metadata.json"
    objects[metadata] = objects.pop(table.metadata_location)
    results.append(
        {
            "name": name,
            "warehouseId": "enginewarehouse",
            "tableId": name,
            "metadata": metadata,
            "snapshotId": str(table.current_snapshot().snapshot_id),
            "tableUuid": str(table.metadata.table_uuid),
            "objects": objects,
            "physicalRows": str(len(rows)),
        }
    )
print(json.dumps(results))
