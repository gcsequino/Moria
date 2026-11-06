"""Read an actual qualified Iceberg export without translating its metadata or deletes."""
import hashlib
import io
import json
from pathlib import Path
import sys
import threading
import uuid

import pyarrow
import pyiceberg
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFileContent
from pyiceberg.table import StaticTable

FILE_PATH_ID = 2147483546


class QualifiedExportFileIO(PyArrowFileIO):
    def __init__(self, properties):
        super().__init__(properties)
        manifest = Path(properties["moria.oracle.export"]).resolve(strict=True)
        root = manifest.parent
        spec = json.loads(manifest.read_text(encoding="utf-8"))
        self.objects = {}
        self.read_paths = set()
        self.lock = threading.Lock()
        total_bytes = 0
        entries = spec["objects"]
        if not 1 <= len(entries) <= 1024:
            raise ValueError("invalid bounded export count")
        for entry in entries:
            logical = entry["name"]
            physical = (root / entry["file"]).resolve(strict=True)
            if not physical.is_relative_to(root) or not physical.is_file():
                raise ValueError("export escaped its owned scratch directory")
            size = entry["size"]
            total_bytes += size
            if (
                not logical.startswith("moria://")
                or logical in self.objects
                or not 1 <= size <= 16 * 1024 * 1024
                or total_bytes > 64 * 1024 * 1024
                or physical.stat().st_size != size
            ):
                raise ValueError("invalid qualified export member")
            with physical.open("rb") as stream:
                digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
            if digest != entry["sha256"]:
                raise ValueError("qualified export bytes differ")
            self.objects[logical] = physical
        metadata = self.objects[spec["metadata"]]
        if not str(metadata).endswith(".metadata.json"):
            raise ValueError("expected standard metadata file suffix")
        self.metadata_location = str(metadata)
        self.objects[self.metadata_location] = metadata

    def new_input(self, location):
        if location not in self.objects:
            raise ValueError("standard reader requested an unexported member")
        with self.lock:
            self.read_paths.add(location)
        return super().new_input(str(self.objects[location]))

    def new_output(self, location):
        raise io.UnsupportedOperation("qualified export is read-only")

    def delete(self, location):
        raise io.UnsupportedOperation("qualified export is read-only")


def json_value(value):
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes) and len(value) == 16:
        return str(uuid.UUID(bytes=value))
    raise TypeError(f"unsupported oracle value type: {type(value).__name__}")


def main():
    export = str(Path(sys.argv[1]).resolve(strict=True))
    properties = {
        "py-io-impl": "iceberg_oracle.QualifiedExportFileIO",
        "moria.oracle.export": export,
    }
    adapter = QualifiedExportFileIO(properties)
    table = StaticTable.from_metadata(adapter.metadata_location, properties)
    if not isinstance(table.io, QualifiedExportFileIO):
        raise ValueError("unexpected Iceberg reader IO")
    scan = table.scan()
    tasks = list(scan.plan_files())
    deletes = set()
    matched_pairs = 0
    for task in tasks:
        target = task.file.file_path.encode("utf-8")
        for delete in task.delete_files:
            if (
                delete.content != DataFileContent.POSITION_DELETES
                or delete.lower_bounds.get(FILE_PATH_ID) != target
                or delete.upper_bounds.get(FILE_PATH_ID) != target
            ):
                raise ValueError("position delete was not planned for its exact old file")
            deletes.add(delete.file_path)
            matched_pairs += 1
    rows = scan.to_arrow().to_pylist()
    if not deletes.issubset(table.io.read_paths):
        raise ValueError("standard reader did not open all planned delete files")
    snapshot = table.current_snapshot()
    print(json.dumps({
        "pyiceberg": pyiceberg.__version__,
        "pyarrow": pyarrow.__version__,
        "snapshot": str(snapshot.snapshot_id),
        "sequence": str(snapshot.sequence_number),
        "dataFiles": len(tasks),
        "positionDeleteFiles": len(deletes),
        "matchedDeletePairs": matched_pairs,
        "rows": rows,
    }, default=json_value, sort_keys=True))


if __name__ == "__main__":
    sys.modules["iceberg_oracle"] = sys.modules[__name__]
    main()
