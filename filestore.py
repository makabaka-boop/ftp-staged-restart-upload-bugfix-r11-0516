"""Shared FTP file buffer used by passive uploads."""

import re

LIMIT = 32768
CAPACITY = 8


class UploadRejected(ValueError):
    pass


class UploadPlan:
    def __init__(self, store, name, restart):
        self.store = store
        self.filename = name
        self.restart = restart

    @property
    def prefix(self):
        return self.store.read(self.filename) or b""

    @property
    def version(self):
        return len(self.store.read(self.filename) or b"")


class FileStore:
    def __init__(self, files):
        self.files = {name: bytearray(data) for name, data in files.items()}

    def read(self, name):
        data = self.files.get(name)
        return None if data is None else bytes(data)

    def prepare(self, name, restart):
        if not isinstance(name, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", name
        ):
            raise UploadRejected("invalid filename")
        current = self.files.get(name)
        if restart is not None and (
            current is None or restart < 0 or restart > len(current)
        ):
            raise UploadRejected("invalid restart position")
        if current is None and len(self.files) >= CAPACITY:
            raise UploadRejected("file capacity exceeded")
        return UploadPlan(self, name, restart)

    def expose(self, plan, chunk):
        current = self.files.setdefault(plan.filename, bytearray())
        current.extend(chunk)
        if len(current) > LIMIT:
            raise UploadRejected("file size exceeded")

    def commit(self, plan, payload):
        current = self.files.setdefault(plan.filename, bytearray())
        if plan.restart is not None:
            current[:] = current[: plan.restart] + bytes(payload)
        else:
            current.extend(payload)
        if len(current) > LIMIT:
            raise UploadRejected("file size exceeded")
