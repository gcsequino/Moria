"""Dependency and configuration probe; it does not stand in for an engine lease/IO proof."""

import json
import sys

sys.path.insert(0, "/runtime")
import duckdb
import pyarrow as pa
import protocol

context = protocol.Context.__new__(protocol.Context)
context._connection = None
context.metadata = {
    "limits": {"memoryBytes": str(512 * 1024 * 1024), "spillBytes": str(64 * 1024 * 1024)}
}
connection = context.connection()
connection.register("source", pa.table({"i": pa.array([9007199254740993, 1], type=pa.int64())}))
batches = list(
    connection.sql("SELECT CAST(sum(i) AS BIGINT) AS total FROM source").to_arrow_reader(1000)
)
assert batches[0].column(0)[0].as_py() == 9007199254740994
try:
    connection.execute("SET enable_external_access=true")
except duckdb.Error:
    pass
else:
    raise AssertionError("Configuration was not locked")
try:
    connection.sql("SELECT * FROM read_csv('/etc/passwd')").fetchall()
except duckdb.Error:
    pass
else:
    raise AssertionError("External file read was allowed")
print(
    json.dumps(
        {
            "duckdb": duckdb.__version__,
            "pyarrow": pa.__version__,
            "total": str(batches[0].column(0)[0].as_py()),
            "configurationLocked": True,
            "externalFilesDenied": True,
        }
    )
)
context.close()
