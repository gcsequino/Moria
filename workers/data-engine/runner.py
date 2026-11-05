"""Single-attempt guest adapter for the native engine protocol."""

import importlib.util
import json
import signal
import sys
import traceback
import uuid

sys.path.insert(0, "/runtime")
from protocol import Context, ProtectedOutput, receive, send


def cancelled(_signal, _frame):
    raise RuntimeError("ENGINE_CANCELLED")


signal.signal(signal.SIGTERM, cancelled)
context = None
try:
    start = receive()
    if start.get("type") != "start" or start.get("valueVersion") != 1:
        raise ValueError("ENGINE_PROTOCOL")
    sys.stdout = ProtectedOutput()
    sys.stderr = ProtectedOutput()
    context = Context()
    module_spec = importlib.util.spec_from_file_location("program", sys.argv[1])
    program = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(program)
    if context.metadata["domain"] == "transform":
        program.transform(context)
    else:
        program.sync(context)
    context.close()
    context = None
    send({"type": "result", "value": {"complete": True}})
    completion = receive()
    nonce = completion.get("nonce")
    if (
        set(completion) != {"version", "type", "nonce"}
        or type(completion["version"]) is not int
        or completion["type"] != "complete"
        or type(nonce) is not str
    ):
        raise ValueError("ENGINE_PROTOCOL")
    challenge = uuid.UUID(nonce)
    if str(challenge) != nonce or challenge.version != 4:
        raise ValueError("ENGINE_PROTOCOL")
    send({"type": "completed", "nonce": nonce})
except BaseException as error:
    if context is not None:
        print(
            json.dumps(
                {
                    "engineFailure": {
                        "type": type(error).__name__[:128],
                        "lines": [
                            frame.lineno
                            for frame in traceback.extract_tb(error.__traceback__, limit=8)
                        ],
                    }
                }
            )
        )
    send({"type": "error"})
    raise SystemExit(1)
finally:
    if context is not None:
        context.close()
