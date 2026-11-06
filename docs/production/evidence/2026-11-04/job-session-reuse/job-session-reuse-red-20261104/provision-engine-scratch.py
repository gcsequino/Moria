#!/usr/bin/env python3
"""Provision fresh disposable hard-bounded engine test filesystems on the Docker host."""

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

if os.geteuid() != 0:
    raise SystemExit("Run this test infrastructure setup as root on the Docker host")
if len(sys.argv) > 2:
    raise SystemExit("Optional argument: exact new /tmp/moria-engine-scratch-UUID root")
root = (
    Path(sys.argv[1])
    if len(sys.argv) == 2
    else Path("/tmp") / ("moria-engine-scratch-" + str(uuid.uuid4()))
)
assert root.parent == Path("/tmp") and root.name.startswith("moria-engine-scratch-")
uuid.UUID(root.name.removeprefix("moria-engine-scratch-"))
assert root.resolve() == root and not root.exists()
root.mkdir(mode=0o755)
host = str(uuid.uuid4())
metadata = {"root": str(root), "hostId": host, "slots": [], "complete": False}
manifest = root / "pool.json"
manifest.write_text(json.dumps(metadata, indent=2) + "\n")
for index in range(2):
    backing = root / f"slot-{index}.ext4"
    mount = root / f"slot-{index}"
    assert mount.resolve().is_relative_to(root) and backing.resolve().is_relative_to(root)
    mount.mkdir(mode=0o755)
    with backing.open("xb") as file:
        file.truncate(128 * 1024 * 1024)
    backing.chmod(0o600)
    filesystem = str(uuid.uuid4())
    subprocess.run(
        ["/usr/sbin/mkfs.ext4", "-q", "-F", "-m", "0", "-U", filesystem, str(backing)], check=True
    )
    device = subprocess.check_output(
        ["/usr/sbin/losetup", "--find", "--show", str(backing)], text=True
    ).strip()
    record = {
        "registration": {
            "id": str(uuid.uuid4()),
            "hostId": host,
            "mountRoot": str(mount),
            "filesystemUuid": filesystem,
            "capacityBytes": 128 * 1024 * 1024,
            "resourceClass": "engine-spill",
        },
        "device": device,
    }
    metadata["slots"].append(record)
    manifest.write_text(json.dumps(metadata, indent=2) + "\n")
    subprocess.run(
        ["/usr/bin/mount", "-t", "ext4", "-o", "rw,nodev,nosuid,noexec", device, str(mount)],
        check=True,
    )
    lost = mount / "lost+found"
    if lost.exists():
        lost.rmdir()
    mount.chmod(0o1777)
    stats = os.statvfs(mount)
    size = stats.f_frsize * stats.f_blocks
    assert 32 * 1024 * 1024 <= size <= 128 * 1024 * 1024
    record["actualFilesystemBytes"] = size
metadata["complete"] = True
manifest.write_text(json.dumps(metadata, indent=2) + "\n")
print(json.dumps(metadata))
