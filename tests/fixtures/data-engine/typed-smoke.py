"""Probe the immutable engine's exact UID and scalar-array Arrow physical types."""

import json
import sys
import uuid

sys.path.insert(0, "/runtime")
import pyarrow as pa
import protocol

context = protocol.Context.__new__(protocol.Context)
context._connection = None
context.metadata = {
    "limits": {"memoryBytes": str(512 * 1024 * 1024), "spillBytes": str(64 * 1024 * 1024)}
}
connection = context.connection()
identity = uuid.UUID("a1832030-de00-4ba3-9fc2-cc7676092bc2")
values = pa.table(
    {
        "id": pa.array([identity.bytes], type=pa.uuid()),
        "items": pa.array([[9007199254740993, -1]], type=pa.list_(pa.int64())),
    }
)
connection.register("typed", values)
result = connection.sql("SELECT * FROM typed").to_arrow_table()
assert result.column("items")[0].as_py() == [9007199254740993, -1]
restored = protocol.output_array(result.column("id").chunk(0), pa.uuid())
assert restored[0].as_py() == identity
array_type = pa.list_(pa.field("element", pa.int64(), nullable=False))
restored_items = protocol.output_array(result.column("items").chunk(0), array_type)
assert restored_items.type == array_type
assert restored_items[0].as_py() == [9007199254740993, -1]
logical_field = pa.field(
    "identity",
    pa.uuid(),
    nullable=False,
    metadata={b"PARQUET:field_id": b"1", b"opaque": b"accepted"},
)
wire_field = protocol.transport_field(logical_field)
assert wire_field.type == pa.binary(16)
assert wire_field.metadata[b"ARROW:extension:name"] == b"arrow.uuid"
assert b"ARROW:extension:metadata" not in wire_field.metadata
assert wire_field.metadata[b"opaque"] == b"accepted"
wire_array = protocol.transport_array(restored, wire_field.type)
assert wire_array[0].as_py() == identity.bytes
list_field = pa.field(
    "identities", pa.list_(pa.field("element", pa.uuid(), nullable=False)), nullable=False
)
logical_list = pa.ListArray.from_arrays(
    pa.array([0, 1], type=pa.int32()),
    pa.array([identity.bytes], type=pa.uuid()),
    type=list_field.type,
)
wire_list_field = protocol.transport_field(list_field)
wire_list = protocol.transport_array(logical_list, wire_list_field.type)
assert wire_list[0].as_py() == [identity.bytes]
assert wire_list_field.type.value_field.metadata[b"ARROW:extension:name"] == b"arrow.uuid"
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, pa.schema([wire_field, wire_list_field])) as writer:
    writer.write_batch(
        pa.RecordBatch.from_arrays(
            [wire_array, wire_list], schema=pa.schema([wire_field, wire_list_field])
        )
    )
roundtrip = pa.ipc.open_stream(sink.getvalue()).read_all()
assert roundtrip.column("identity")[0].as_py() == identity
assert roundtrip.column("identities")[0].as_py() == [identity]
try:
    protocol.output_array(pa.array(["invalid"]), pa.uuid())
except ValueError:
    pass
else:
    raise AssertionError("Invalid UID text was accepted")
print(
    json.dumps(
        {
            "uidType": str(result.schema.field("id").type),
            "arrayType": str(result.schema.field("items").type),
            "canonicalUidRestored": restored.type == pa.uuid(),
            "canonicalUidWireMetadata": True,
            "nestedUidArrayRoundtrip": True,
        }
    )
)
context.close()
