"""Generate standard immutable Iceberg objects with logical engine names."""

import hashlib
import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import FileIO, OutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    BooleanType,
    DoubleType,
    DecimalType,
    DateType,
    TimestamptzType,
    ListType,
)

root = Path(sys.argv[1])
objects = {}


class FixtureOutput(OutputFile):
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


class FixtureIO(FileIO):
    def physical(self, name):
        if not name.startswith("moria://warehouse/table/"):
            raise ValueError("Unexpected fixture reference")
        path = root / hashlib.sha256(name.encode()).hexdigest()
        objects[name] = str(path)
        return str(path)

    def new_input(self, location):
        return PyArrowFileIO().new_input(self.physical(location))

    def new_output(self, location):
        return FixtureOutput(location, self.physical(location))

    def delete(self, location):
        raise ValueError("Immutable fixture")


# The fixture's writer supplies logical names while its FileIO creates real files.
# No reader uses this implementation.
catalog = SqlCatalog(
    "fixture",
    uri="sqlite:///:memory:",
    warehouse="moria://warehouse/table",
    **{"py-io-impl": "__main__.FixtureIO"},
)
catalog.create_namespace("fixture")
table = catalog.create_table(
    "fixture.data",
    Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "value", StringType()),
        NestedField(3, "enabled", BooleanType()),
        NestedField(4, "large", LongType()),
        NestedField(5, "amount", DecimalType(20, 4)),
        NestedField(6, "day", DateType()),
        NestedField(7, "at", TimestamptzType()),
        NestedField(8, "ratio", DoubleType()),
        NestedField(9, "optional", StringType()),
        NestedField(
            10, "tags", ListType(element_id=11, element_type=StringType(), element_required=True)
        ),
    ),
    location="moria://warehouse/table",
    properties={"format-version": "2"},
)
for start in (0, 150):
    table.append(
        pa.Table.from_pylist(
            [
                {
                    "id": i,
                    "value": f"synthetic-{i}",
                    "enabled": i % 2 == 0,
                    "large": 9007199254740993,
                    "amount": Decimal("1234567890123456.1234"),
                    "day": date(2026, 1, 2),
                    "at": datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc),
                    "ratio": 1.25,
                    "optional": None,
                    "tags": ["one", "two"],
                }
                for i in range(start, start + 150)
            ],
            schema=table.schema().as_arrow(),
        )
    )
original_metadata = objects[table.metadata_location]
original_snapshot = str(table.current_snapshot().snapshot_id)
table.overwrite(
    pa.Table.from_pylist(
        [{"id": 301, "value": "x" * (4 * 1024 * 1024)}], schema=table.schema().as_arrow()
    )
)
print(
    json.dumps(
        {
            "metadata": original_metadata,
            "snapshotId": original_snapshot,
            "tableUuid": str(table.metadata.table_uuid),
            "wideMetadata": objects[table.metadata_location],
            "wideSnapshotId": str(table.current_snapshot().snapshot_id),
            "objects": objects,
        }
    )
)
