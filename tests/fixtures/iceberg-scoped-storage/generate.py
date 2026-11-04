"""Generate standard immutable Iceberg objects with logical engine names."""

import hashlib
import json
import sys
from datetime import date, datetime, timezone, timedelta
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
logical_table = sys.argv[3] if len(sys.argv) > 3 else "moria://warehouse/table"
fixture_mode = sys.argv[2] if len(sys.argv) > 2 else ""
index_fixture = fixture_mode in ("--index-snapshot", "--projection-snapshot")
projection_fixture = fixture_mode == "--projection-snapshot"


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
        if not name.startswith(logical_table + "/"):
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
    warehouse=logical_table,
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
        *([NestedField(12, "uid", StringType(), required=True)] if index_fixture else []),
        *([NestedField(13, "tenant", StringType(), required=True)] if projection_fixture else []),
    ),
    location=logical_table,
    properties={"format-version": "2"},
)
for start in (0, 150):
    table.append(
        pa.Table.from_pylist(
            [
                {
                    "id": i,
                    **({"uid": f"{i:08x}-0000-4000-8000-000000000000"} if index_fixture else {}),
                    **({"tenant": "a" if i % 2 == 0 else "b"} if projection_fixture else {}),
                    "value": (
                        [None, "", "😀", "\ue000", "e\u0301", "é", "東京", "x" * 8192][i]
                        if projection_fixture and i < 8
                        else f"synthetic-{i}"
                    ),
                    "enabled": i % 2 == 0,
                    "large": (
                        [
                            9007199254740993,
                            -9223372036854775808,
                            9223372036854775807,
                            -9007199254740993,
                            0,
                            1,
                        ][i]
                        if projection_fixture and i < 6
                        else 9007199254740993 + (i if projection_fixture else 0)
                    ),
                    "amount": (
                        [
                            Decimal("-9999999999999999.9999"),
                            Decimal("0.0000"),
                            Decimal("0.0001"),
                            Decimal("9999999999999999.9999"),
                        ][i % 4]
                        if projection_fixture
                        else Decimal("1234567890123456.1234")
                    ),
                    "day": (
                        date(2026, 1, 2) + timedelta(days=i % 28)
                        if projection_fixture
                        else date(2026, 1, 2)
                    ),
                    "at": datetime(
                        2026,
                        1,
                        2,
                        3,
                        4,
                        5,
                        [0, 1, 100000, 123456, 999999][i % 5] if projection_fixture else 123456,
                        tzinfo=timezone.utc,
                    ),
                    "ratio": (
                        [-1.7976931348623157e308, 1.7976931348623157e308, 5e-324, -0.0, 1.0, -1.25][
                            i % 6
                        ]
                        if projection_fixture
                        else 1.25
                    ),
                    "optional": None,
                    "tags": (
                        ([] if i == 1 else ["", "😀", "\ue000"] if i == 2 else ["one", "two"])
                        if projection_fixture
                        else ["one", "two"]
                    ),
                }
                for i in range(start, start + 150)
            ],
            schema=table.schema().as_arrow(),
        )
    )
retired_files = [name for name in objects if name.endswith(".parquet")]
if len(sys.argv) > 2 and sys.argv[2] == "--deleted-snapshot":
    table.overwrite(
        pa.Table.from_pylist(
            [{"id": 301, "value": "replacement"}],
            schema=table.schema().as_arrow(),
        )
    )
original_metadata = objects[table.metadata_location]
original_snapshot = str(table.current_snapshot().snapshot_id)
if fixture_mode in (
    "--snapshot",
    "--deleted-snapshot",
    "--index-snapshot",
    "--projection-snapshot",
):
    print(
        json.dumps(
            {
                "metadata": original_metadata,
                "snapshotId": original_snapshot,
                "tableUuid": str(table.metadata.table_uuid),
                "retiredFiles": retired_files,
                "objects": {
                    name: path for name, path in objects.items() if path != original_metadata
                },
            }
        )
    )
    sys.exit(0)
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
