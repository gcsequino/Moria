"""Record container/process memory and I/O without reading arguments or credentials."""

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

name, output, evidence_root = sys.argv[1:]
evidence_root = Path(evidence_root)
probe = """
import json,pathlib
root=pathlib.Path('/sys/fs/cgroup')
def text(name):
 try:return (root/name).read_text().strip()
 except OSError:return None
stats={}
for line in (text('memory.stat') or '').splitlines():
 key,value=line.split();stats[key]=int(value)
print(json.dumps({'current':text('memory.current'),'peak':text('memory.peak'),'max':text('memory.max'),'memory':stats,'io':text('io.stat'),'cpu':text('cpu.stat')}))
"""

with open(output, "x", encoding="utf-8", buffering=1) as stream:
    while True:
        running = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Running}}", name],
            capture_output=True,
            text=True,
        )
        if running.returncode or running.stdout.strip() != "true":
            break
        observed = datetime.now(timezone.utc).isoformat()
        phase = "initialization"
        for filename, label in [
            ("identity.log", "identity"),
            ("producer.log", "source-production"),
            ("publication.log", "native-publication"),
            ("index-1.log", "native-indexing-projection"),
            ("oracle.log", "native-verification"),
            ("serving.log", "protected-http"),
        ]:
            if (evidence_root / filename).exists():
                phase = label
        group = subprocess.run(
            ["docker", "exec", name, "python3", "-c", probe], capture_output=True, text=True
        )
        processes = subprocess.run(
            ["docker", "top", name, "-eo", "pid,comm,rss"], capture_output=True, text=True
        )
        rows = []
        for line in processes.stdout.splitlines()[1:]:
            parts = line.split()
            if len(parts) == 3:
                rows.append({"pid": parts[0], "name": parts[1], "rssBytes": int(parts[2]) * 1024})
        value = {
            "observedAt": observed,
            "phaseByOpenedLog": phase,
            "cgroup": json.loads(group.stdout) if group.returncode == 0 else None,
            "processes": rows,
        }
        stream.write(json.dumps(value, separators=(",", ":")) + "\n")
        time.sleep(5)
