"""Read-only FileIO over a serialized, parent-owned range capability protocol."""

import base64
import io
import json
import sys
import threading

from pyiceberg.io import FileIO, InputFile

FRAME = 4 * 1024 * 1024
RANGE = 256 * 1024
_lock = threading.Lock()


def receive():
    line = sys.stdin.buffer.readline(FRAME + 2)
    if not line.endswith(b"\n") or len(line) > FRAME + 1:
        raise ValueError("Invalid protocol frame")
    return json.loads(line)


def exchange(message):
    # At most one outstanding request, including row acknowledgments.
    with _lock:
        line = json.dumps(message, allow_nan=False).encode()
        if len(line) > FRAME:
            raise ValueError("Frame limit")
        sys.stdout.buffer.write(line + b"\n")
        sys.stdout.buffer.flush()
        return receive()


class RangeStream(io.RawIOBase):
    def __init__(self, name, size):
        super().__init__()
        self.name = name
        self.size = size
        self.position = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        self._checkClosed()
        position = offset + (0 if whence == 0 else self.position if whence == 1 else self.size)
        if whence not in (0, 1, 2) or not 0 <= position <= self.size:
            raise ValueError("Invalid seek")
        self.position = position
        return position

    def read(self, size=-1):
        self._checkClosed()
        size = self.size - self.position if size < 0 else min(size, self.size - self.position)
        result = bytearray()
        while len(result) < size:
            length = min(RANGE, size - len(result))
            response = exchange(
                {"type": "read", "name": self.name, "offset": self.position, "length": length}
            )
            if set(response) != {"data"}:
                raise ValueError("Invalid range response")
            data = base64.b64decode(response["data"], validate=True)
            if len(data) != length:
                raise ValueError("Short range")
            result.extend(data)
            self.position += length
        return bytes(result)

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


class ScopedInput(InputFile):
    def __init__(self, name, size):
        super().__init__(name)
        self.size = size

    def __len__(self):
        return self.size

    def exists(self):
        return True

    def open(self, seekable=True):
        return RangeStream(self.location, self.size)


class ScopedIO(FileIO):
    def __init__(self, objects):
        super().__init__({})
        self.objects = objects

    def new_input(self, location):
        if location not in self.objects:
            raise PermissionError("Unbound reference")
        return ScopedInput(location, self.objects[location])

    def new_output(self, location):
        raise PermissionError("Read only")

    def delete(self, location):
        raise PermissionError("Read only")
