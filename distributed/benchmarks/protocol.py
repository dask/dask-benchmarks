# Make sure it is installed as compression performance depends on it
import lz4  # noqa: F401
import numpy as np

from distributed import protocol

from benchmarks.common import rnd

messages = {}

messages["small1"] = {
    "active": 0,
    "address": ["127.0.0.1", 38308],
    "host_info": {
        "cpu": 100.0,
        "disk-read": 0,
        "disk-write": 0,
        "memory": 8274681856,
        "memory_percent": 50.2,
        "network-recv": 0,
        "network-send": 0,
        "time": 1480544664.9654167,
    },
    "keys": [],
    "memory_limit": 2482404556.0,
    "name": 1,
    "nbytes": {},
    "ncores": 2,
    "now": 1480544664.9654162,
    "op": "register",
    "reply": True,
    "resources": {},
    "services": {},
    "stored": 0,
}

messages["small2"] = {
    "args": b"\x80\x04\x95\x02\x00\x00\x00\x00\x00\x00\x00).",
    "close": True,
    "function": b"\x80\x04\x95\xf7\x00\x00\x00\x00\x00\x00\x00\x8c\x17cloudpi"
    b"ckle.cloudpickle\x94\x8c\x0e_fill_function\x94\x93\x94"
    b"(h\x00\x8c\x0f_make_skel_func\x94\x93\x94h\x00\x8c\r_builtin_"
    b"type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K"
    b"\x01K\x01KSC\x07|\x00\x00j\x00\x00S\x94N\x85\x94\x8c\x02"
    b"id\x94\x85\x94\x8c\x0bdask_worker\x94\x85\x94\x8c:/home/ant"
    b"oine/distributed/distributed/tests/test_worker.py\x94\x8c\x01"
    b"f\x94Mh\x01C\x02\x00\x01\x94))t\x94R\x94]\x94}\x94"
    b"\x87\x94R\x94}\x94N\x85\x94}\x94tR.",
    "kwargs": b"\x80\x04\x95\x03\x00\x00\x00\x00\x00\x00\x00}\x94.",
    "op": "run",
    "reply": True,
}

messages["small3"] = {
    "keys": [
        "add-6bc144bc0ae4e5f1e17a1b8e5cafe9c2",
        "add-c3cae4a08c3bbbbd94645c255cd52915",
    ],
    "op": "gather",
    "reply": True,
}


_MEDIUM = 100 * 1024
_LARGE = 100 * 1024**2
_source = rnd().bytes(_LARGE // 100) * 101

# Compressible
_medium1 = np.frombuffer(_source, dtype="int8", count=_MEDIUM // 8).repeat(8)
_large1 = np.frombuffer(_source, dtype="int8", count=_LARGE // 8).repeat(8)
assert _medium1.nbytes == _MEDIUM
assert _large1.nbytes == _LARGE
# Uncompressible
_medium2 = np.frombuffer(_source, dtype="int8", count=_MEDIUM).view("float64")
_large2 = np.frombuffer(_source, dtype="int8", count=_LARGE).view("float64")
assert _medium2.nbytes == _MEDIUM
assert _large2.nbytes == _LARGE
# Compressible bytes
_medium3 = _medium1.tobytes()
assert len(_medium3) == _MEDIUM


messages["medium1"] = {
    "status": "OK",
    "result": protocol.to_serialize(_medium1),
}

messages["medium2"] = {
    "status": "OK",
    "result": protocol.to_serialize(_medium2),
}

messages["medium3"] = {
    "status": "OK",
    "result": protocol.to_serialize(_medium3),
}

messages["large1"] = {
    "status": "OK",
    "result": protocol.to_serialize(_large1),
}

messages["large2"] = {
    "status": "OK",
    "result": protocol.to_serialize(_large2),
}


def _compute_frames(messages):
    frames = {k: protocol.dumps(v) for k, v in messages.items()}
    return frames


frames = _compute_frames(messages)


def frames_len(frames):
    assert isinstance(frames, (list, tuple))
    assert all(isinstance(f, (bytes, memoryview)) for f in frames)
    return sum(map(len, frames))


class Protocol:

    params = sorted(messages)

    def time_dumps(self, n):
        protocol.dumps(messages[n])

    def time_loads(self, n):
        protocol.loads(frames[n], deserialize=True)

    def track_size(self, n):
        return frames_len(frames[n])

    track_size.unit = "bytes"
