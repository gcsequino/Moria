"""Validate metadata identity without opening referenced files."""

import base64
import json
import sys

from pyiceberg.exceptions import ValidationError
from pyiceberg.table.metadata import TableMetadataUtil


def main():
    request = json.load(sys.stdin)
    metadata = TableMetadataUtil.parse_raw(base64.b64decode(request["metadata"], validate=True))
    snapshot = metadata.current_snapshot()
    if metadata.format_version != 2 or str(metadata.table_uuid) != request["tableUuid"]:
        raise ValueError("Metadata identity")
    if snapshot is None or str(snapshot.snapshot_id) != request["snapshotId"]:
        raise ValueError("Snapshot identity")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, ValidationError):
        sys.exit(65)
