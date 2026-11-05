"""Bounded sequential engine transport. All authority remains in the native broker."""

import base64
import io
import json
import sys
import threading
import uuid
from urllib.parse import quote

import duckdb
import pyarrow as pa

FRAME_BYTES = 2 * 1024 * 1024
BATCH_BYTES = 1024 * 1024
MESSAGE_BYTES = 16 * 1024 * 1024
_input = sys.stdin.buffer
_output = sys.stdout
_lock = threading.Lock()
_sequence = 0


def output_array(array, expected):
    """Restore canonical UUID storage and list field metadata without numeric casts."""
    if array.type == expected:
        return array
    if expected == pa.uuid() and pa.types.is_string(array.type):
        values = []
        for value in array:
            text = value.as_py()
            if text is None:
                values.append(None)
            else:
                identity = uuid.UUID(text)
                if str(identity) != text:
                    raise ValueError("ENGINE_OUTPUT_UID")
                values.append(identity.bytes)
        return pa.array(values, type=expected)
    if pa.types.is_list(array.type) and pa.types.is_list(expected):
        offsets = array.offsets.to_pylist()
        start, end = offsets[0], offsets[-1]
        child = output_array(array.values.slice(start, end - start), expected.value_type)
        if not expected.value_field.nullable and child.null_count:
            raise ValueError("ENGINE_OUTPUT_NULL")
        return pa.ListArray.from_arrays(
            pa.array([offset - start for offset in offsets], type=pa.int32()),
            child,
            type=expected,
            mask=array.is_null() if array.null_count else None,
        )
    raise ValueError("ENGINE_OUTPUT_SCHEMA")


def transport_field(field):
    """Keep the accepted canonical UUID metadata when PyArrow re-encodes IPC."""
    metadata = dict(field.metadata or {})
    kind = field.type
    if kind == pa.uuid():
        kind = pa.binary(16)
        metadata[b"ARROW:extension:name"] = b"arrow.uuid"
        metadata.pop(b"ARROW:extension:metadata", None)
    elif pa.types.is_list(kind):
        kind = pa.list_(transport_field(kind.value_field))
    return pa.field(field.name, kind, nullable=field.nullable, metadata=metadata or None)


def transport_array(array, expected):
    if array.type == pa.uuid():
        return array.storage
    if pa.types.is_list(array.type):
        offsets = array.offsets.to_pylist()
        start, end = offsets[0], offsets[-1]
        child = transport_array(array.values.slice(start, end - start), expected.value_type)
        return pa.ListArray.from_arrays(
            pa.array([offset - start for offset in offsets], type=pa.int32()),
            child,
            type=expected,
            mask=array.is_null() if array.null_count else None,
        )
    return array


def send(message):
    text = json.dumps({"version": 1, **message}, allow_nan=False, separators=(",", ":"))
    if len(text.encode("utf8")) > FRAME_BYTES:
        raise ValueError("ENGINE_FRAME_LIMIT")
    _output.write(text + "\n")
    _output.flush()


def receive():
    line = _input.readline(FRAME_BYTES + 1)
    if not line or len(line) > FRAME_BYTES or not line.endswith(b"\n"):
        raise ValueError("ENGINE_PROTOCOL")
    value = json.loads(line)
    if type(value) is not dict or value.get("version") != 1:
        raise ValueError("ENGINE_PROTOCOL")
    return value


def request(path, method="GET", body=None, media_type="application/json"):
    global _sequence
    if not path.startswith("/api/v1/engine/"):
        raise ValueError("ENGINE_OPERATION_DENIED")
    if body is not None and len(body) > BATCH_BYTES:
        raise ValueError("ENGINE_FRAME_LIMIT")
    with _lock:
        _sequence += 1
        identifier = str(_sequence)
        send(
            {
                "type": "http",
                "id": identifier,
                "method": method,
                "path": path,
                "headers": {"content-type": media_type},
                "body": None if body is None else base64.b64encode(body).decode("ascii"),
            }
        )
        response = receive()
        if response.get("id") != identifier:
            raise ValueError("ENGINE_PROTOCOL")
        if response.get("type") == "httpError":
            raise ValueError("ENGINE_BROKER_REJECTED")
        if response.get("type") != "httpResult" or type(response.get("value")) is not dict:
            raise ValueError("ENGINE_PROTOCOL")
        value = response["value"]
        status = value.get("status")
        if type(status) is not int or not 100 <= status <= 599:
            raise ValueError("ENGINE_PROTOCOL")
        encoded = value.get("body")
        if type(encoded) is not str or len(encoded) > FRAME_BYTES:
            raise ValueError("ENGINE_PROTOCOL")
        decoded = base64.b64decode(encoded, validate=True)
        if len(decoded) > BATCH_BYTES:
            raise ValueError("ENGINE_FRAME_LIMIT")
        if status >= 400:
            raise ValueError("ENGINE_BROKER_REJECTED")
        return status, decoded


def json_request(path, method="GET", value=None):
    body = (
        None
        if value is None
        else json.dumps(value, allow_nan=False, separators=(",", ":")).encode()
    )
    _, response = request(path, method, body)
    return json.loads(response)


def log(message):
    text = str(message)
    if len(text.encode("utf8")) > 4096:
        raise ValueError("ENGINE_LOG_LIMIT")
    with _lock:
        send({"type": "log", "message": text})


class ProtectedOutput:
    def write(self, text):
        if text and text != "\n":
            log(text.rstrip("\n"))
        return len(text)

    def flush(self):
        pass


class ArrowInput(io.RawIOBase):
    def __init__(self, path):
        self.path = path
        self.pending = memoryview(b"")
        self.ended = False

    def readable(self):
        return True

    def readinto(self, target):
        written = 0
        while written < len(target):
            if not self.pending:
                if self.ended:
                    break
                status, data = request(self.path)
                if status == 204:
                    self.ended = True
                    break
                if not data:
                    raise ValueError("ENGINE_PROTOCOL")
                self.pending = memoryview(data)
            count = min(len(target) - written, len(self.pending))
            target[written : written + count] = self.pending[:count]
            self.pending = self.pending[count:]
            written += count
        return written


class ArrowOutput(io.RawIOBase):
    def __init__(self, path):
        self.path = path
        self.pending = bytearray()

    def writable(self):
        return True

    def write(self, data):
        original = len(data)
        data = memoryview(data)
        while data:
            count = min(BATCH_BYTES - len(self.pending), len(data))
            self.pending.extend(data[:count])
            data = data[count:]
            if len(self.pending) == BATCH_BYTES:
                self.flush()
        return original

    def flush(self):
        if self.pending:
            request(self.path, "POST", self.pending, "application/vnd.apache.arrow.stream")
            self.pending.clear()


class Context:
    def __init__(self):
        self.metadata = json_request("/api/v1/engine/metadata")
        if self.metadata.get("version") != 1 or self.metadata.get("domain") not in (
            "transform",
            "sync",
        ):
            raise ValueError("ENGINE_PROTOCOL")
        self.configuration = self.metadata.get("configuration", {})
        self._connection = None

    def connection(self):
        if self._connection is None:
            # OS isolation, the fixed /spill mount and its hard capacity are native-owned.
            memory = int(self.metadata["limits"]["memoryBytes"])
            spill = int(self.metadata["limits"]["spillBytes"])
            if not 256 * 1024 * 1024 <= memory <= 4096 * 1024 * 1024 or spill <= 0:
                raise ValueError("ENGINE_RESOURCE_LIMIT")
            self._connection = duckdb.connect(
                "/spill/engine.duckdb",
                config={
                    "threads": 1,
                    "memory_limit": str(memory // 2) + "B",
                    "temp_directory": "/spill/temp",
                    "max_temp_directory_size": str(spill) + "B",
                    "autoload_known_extensions": False,
                    "autoinstall_known_extensions": False,
                    "allow_unsigned_extensions": False,
                    "allow_community_extensions": False,
                    "allow_persistent_secrets": False,
                },
            )
            self._connection.execute("SET enable_external_access=false")
            self._connection.execute("SET lock_configuration=true")
        return self._connection

    def read(self, name):
        prefix = "/api/v1/engine/inputs/" + quote(name, safe="")
        opened = json_request(prefix + "/open", "POST", {})
        scan = opened["scan"]
        schema = pa.ipc.read_schema(
            pa.BufferReader(base64.b64decode(opened["schema"], validate=True))
        )

        reader = pa.ipc.open_stream(ArrowInput(prefix + "/batches/" + quote(scan, safe="")))
        if not reader.schema.equals(schema, check_metadata=True):
            raise ValueError("ENGINE_INPUT_SCHEMA")
        return reader

    def write(self, name, data):
        prefix = "/api/v1/engine/outputs/" + quote(name, safe="")
        opened = json_request(prefix + "/open", "POST", {})
        schema = pa.ipc.read_schema(
            pa.BufferReader(base64.b64decode(opened["schema"], validate=True))
        )
        if isinstance(data, duckdb.DuckDBPyRelation):
            data = data.to_arrow_reader(1)
        elif isinstance(data, pa.Table):
            data = data.to_batches(max_chunksize=1000)
        total = 0
        transport = ArrowOutput(prefix + "/append")
        wire_schema = pa.schema(
            [transport_field(field) for field in schema], metadata=schema.metadata
        )
        with pa.ipc.new_stream(transport, wire_schema) as writer:
            pending = []
            retained = 0
            pending_rows = 0

            def flush():
                nonlocal pending, retained, pending_rows, total
                if pending:
                    combined = pa.Table.from_batches(pending, schema=wire_schema).combine_chunks()
                    for batch in combined.to_batches(max_chunksize=1000):
                        writer.write_batch(batch)
                        total += batch.num_rows
                    pending = []
                    retained = 0
                    pending_rows = 0

            for batch in data:
                if not isinstance(batch, pa.RecordBatch) or batch.schema.names != schema.names:
                    raise ValueError("ENGINE_OUTPUT_SCHEMA")
                arrays = [
                    output_array(array, field.type) for array, field in zip(batch.columns, schema)
                ]
                if any(
                    not field.nullable and array.null_count for array, field in zip(arrays, schema)
                ):
                    raise ValueError("ENGINE_OUTPUT_NULL")
                arrays = [
                    transport_array(array, field.type) for array, field in zip(arrays, wire_schema)
                ]
                batch = pa.RecordBatch.from_arrays(arrays, schema=wire_schema)
                offset = 0
                while offset < batch.num_rows:
                    count = min(1000, batch.num_rows - offset)
                    while True:
                        part = batch.slice(offset, count)
                        if pa.ipc.get_record_batch_size(part) <= MESSAGE_BYTES:
                            break
                        if count == 1:
                            raise ValueError("ENGINE_ROW_LIMIT")
                        count = max(1, count // 2)
                    encoded = part.serialize()
                    part = pa.ipc.read_record_batch(encoded, wire_schema)
                    if pending and (
                        retained + encoded.size > 512 * 1024 or pending_rows + count > 1000
                    ):
                        flush()
                    pending.append(part)
                    retained += encoded.size
                    pending_rows += count
                    if retained >= 512 * 1024 or pending_rows == 1000:
                        flush()
                    offset += count
            flush()
        transport.flush()
        json_request(prefix + "/finish", "POST", {})
        return total

    def http(self, method, path, body=None):
        if self.metadata["domain"] != "sync":
            raise ValueError("ENGINE_OPERATION_DENIED")
        return json_request(
            "/api/v1/engine/connector/request",
            "POST",
            {"method": method, "path": path, "body": body},
        )

    def propose_schema(self, proposal):
        if (
            self.metadata.get("capability") != "schemaDiscovery"
            or type(proposal) is not dict
            or set(proposal) != {"columns", "primaryKey"}
        ):
            raise ValueError("ENGINE_DISCOVERY_INVALID")
        return json_request(
            "/api/v1/engine/connector/schema",
            "POST",
            {"version": 1, "stream": self.metadata["stream"], **proposal},
        )

    def checkpoint(self, value):
        return json_request("/api/v1/engine/checkpoint", "POST", {"value": value})

    def close(self):
        if self._connection is not None:
            self._connection.close()
