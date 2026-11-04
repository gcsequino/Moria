"""Isolated codec probe; transport doubles do not prove leases or publication."""

import base64
import json
import sys

sys.path.insert(0, "/runtime")
import pyarrow as pa
import protocol

context = protocol.Context.__new__(protocol.Context)
context._connection = None
context.metadata = {
    "limits": {"memoryBytes": str(512 * 1024 * 1024), "spillBytes": str(64 * 1024 * 1024)}
}
schema = pa.schema(
    [pa.field("id", pa.int64(), nullable=False), pa.field("text", pa.string(), nullable=False)]
)
buffer = pa.BufferOutputStream()
with pa.ipc.new_stream(buffer, schema):
    pass
schema_bytes = base64.b64encode(buffer.getvalue().to_pybytes()).decode()
chunks = []


def control(path, method="GET", body=None):
    if path.endswith("/open"):
        return {"schema": schema_bytes}
    assert path.endswith("/finish")
    return {}


def append(path, method, body, media_type):
    assert path.endswith("/append") and method == "POST"
    assert 0 < len(body) <= 1024 * 1024
    chunks.append(bytes(body))
    return 200, b"{}"


protocol.json_request = control
protocol.request = append
connection = context.connection()
connection.register(
    "normal",
    pa.table(
        {"id": pa.array([9007199254740993] * 1000, type=pa.int64()), "text": ["x" * 2000] * 1000}
    ),
)
assert context.write("out", connection.sql("SELECT * FROM normal")) == 1000
decoded = pa.ipc.open_stream(b"".join(chunks))
batches = list(decoded)
assert sum(batch.num_rows for batch in batches) == 1000
assert len(batches) < 20, "Normal rows must coalesce before native writer admission"
assert batches[0].column("id")[0].as_py() == 9007199254740993
normal_frames = len(chunks)
chunks.clear()
wide = "x" * (4 * 1024 * 1024 - 100)
connection.register(
    "wide", pa.table({"id": pa.array([9007199254740993], type=pa.int64()), "text": [wide]})
)
assert context.write("out", connection.sql("SELECT * FROM wide")) == 1
result = pa.ipc.open_stream(b"".join(chunks)).read_all()
assert result.column("text")[0].as_py() == wide
assert len(chunks) >= 4
print(
    json.dumps(
        {
            "normalRows": "1000",
            "normalRecordBatches": str(len(batches)),
            "normalFrames": str(normal_frames),
            "wideBytes": str(len(wide)),
            "wideFrames": str(len(chunks)),
            "frameBound": "1048576",
        }
    )
)
context.close()
