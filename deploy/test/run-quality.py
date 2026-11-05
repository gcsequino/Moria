#!/usr/bin/env python3
"""Run an offline test command with fresh, independently fenced engine scratch."""

import json
import os
from pathlib import Path
import shutil
import re
import subprocess
import sys
import tempfile
import signal
import uuid


def cleanup_pool(root):
    assert root.parent == Path("/tmp") and root.name.startswith("moria-engine-scratch-")
    uuid.UUID(root.name.removeprefix("moria-engine-scratch-"))
    assert root.resolve() == root
    if not root.exists():
        return
    records = []
    for index in range(2):
        mount = root / f"slot-{index}"
        backing = root / f"slot-{index}.ext4"
        assert mount.resolve() == mount and backing.resolve() == backing
        devices = (
            json.loads(
                subprocess.check_output(
                    [
                        "losetup",
                        "--json",
                        "--associated",
                        str(backing),
                        "--output",
                        "NAME,BACK-FILE",
                    ],
                    text=True,
                )
            ).get("loopdevices", [])
            if backing.exists()
            else []
        )
        assert len(devices) <= 1
        for device in devices:
            assert device["back-file"] == str(backing)
        if os.path.ismount(mount):
            assert len(devices) == 1
            assert mount.stat().st_dev == Path(devices[0]["name"]).stat().st_rdev
        records.append((mount, devices))
    for mount, devices in records:
        if os.path.ismount(mount):
            subprocess.run(["umount", str(mount)], check=True, timeout=15)
        for device in devices:
            subprocess.run(["losetup", "--detach", device["name"]], check=True, timeout=15)
    shutil.rmtree(root)


def cleanup_registries(proof):
    ids = subprocess.check_output(
        ["docker", "ps", "--all", "--quiet", "--filter", "label=moria.test.proof=" + proof],
        text=True,
        timeout=15,
    ).split()
    for container in ids:
        assert re.fullmatch(r"[a-f0-9]{12,64}", container)
        metadata = json.loads(
            subprocess.check_output(["docker", "inspect", container], text=True, timeout=15)
        )[0]
        assert metadata["Config"]["Labels"]["moria.test.proof"] == proof
        subprocess.run(["docker", "rm", "--force", container], check=True, timeout=30)


def main():
    if os.geteuid() != 0 or len(sys.argv) < 2:
        raise SystemExit(
            "Run as root on the disposable Docker host: run-quality.py IMAGE [COMMAND ...]"
        )
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./:@-]{0,1023}", sys.argv[1]):
        raise SystemExit("An exact Docker image reference is required")
    provisioner = Path(__file__).with_name("provision-engine-scratch.py")
    name = "moria-quality-" + str(uuid.uuid4())
    temporary = Path(tempfile.mkdtemp(prefix="moria-quality-", dir="/tmp"))
    temporary.chmod(0o1777)
    pools = []
    owned_roots = []

    def interrupted(number, _frame):
        raise SystemExit(128 + number)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    try:
        for _ in range(3):
            root = Path("/tmp") / ("moria-engine-scratch-" + str(uuid.uuid4()))
            owned_roots.append(root)
            pool = json.loads(
                subprocess.check_output([sys.executable, str(provisioner), str(root)], text=True)
            )
            assert pool["root"] == str(root) and pool["complete"] and len(pool["slots"]) == 2
            for index, slot in enumerate(pool["slots"]):
                assert slot["registration"]["mountRoot"] == str(root / f"slot-{index}")
                assert re.fullmatch(r"/dev/loop[0-9]+", slot["device"])
            pools.append(pool)
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            name,
            "--network",
            "none",
            "--label",
            "moria.test.proof=" + name,
            "--mount",
            "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock",
            "--group-add",
            str(Path("/var/run/docker.sock").stat().st_gid),
            "--group-add",
            "6",
            "--mount",
            f"type=bind,src={temporary},dst={temporary}",
            "--env",
            "TMPDIR=" + str(temporary),
            "--env",
            "MORIA_TEST_CONTROLLER=" + name,
            "--env",
            "MORIA_TEST_PROOF_ID=" + name,
        ]
        for variable, pool in zip(
            [
                "MORIA_ENGINE_SCRATCH_POOL_JSON",
                "MORIA_NATIVE_INSTANCE_SCRATCH_POOL_JSON",
                "MORIA_CONNECTOR_SCRATCH_POOL_JSON",
            ],
            pools,
        ):
            command += ["--env", variable + "=" + json.dumps(pool)]
            command += ["--mount", f"type=bind,src={pool['root']},dst={pool['root']}"]
            for slot in pool["slots"]:
                command += ["--device", slot["device"] + ":" + slot["device"] + ":r"]
        command += [sys.argv[1], *(sys.argv[2:] or ["bash", ".openhands/hooks/quality-gate.sh"])]
        print(
            json.dumps(
                {
                    "controller": name,
                    "image": sys.argv[1],
                    "scratchPools": [pool["root"] for pool in pools],
                    "temporary": str(temporary),
                }
            ),
            flush=True,
        )
        return subprocess.run(command).returncode
    finally:
        subprocess.run(["docker", "stop", "--time", "15", name], capture_output=True, timeout=30)
        controllers = subprocess.check_output(
            ["docker", "ps", "--all", "--quiet", "--filter", "name=^/" + name + "$"],
            timeout=10,
        )
        if controllers.strip():
            raise RuntimeError(
                "Quality controller absence is unconfirmed; preserve scratch for inspection"
            )
        cleanup_registries(name)
        complete_roots = {Path(pool["root"]) for pool in pools}
        for root in owned_roots:
            if root not in complete_roots:
                print(
                    "Partial scratch retained for inspection: " + str(root / "pool.json"),
                    file=sys.stderr,
                )
                continue
            try:
                cleanup_pool(root)
            except BaseException:
                print(
                    "Owned scratch retained for inspection: " + str(root / "pool.json"),
                    file=sys.stderr,
                )
                raise
        assert temporary.parent == Path("/tmp") and temporary.name.startswith("moria-quality-")
        assert temporary.resolve() == temporary
        shutil.rmtree(temporary)


if __name__ == "__main__":
    raise SystemExit(main())
