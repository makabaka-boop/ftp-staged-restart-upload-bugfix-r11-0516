"""Shared FTP file buffer used by passive uploads."""

import re

LIMIT = 32768
CAPACITY = 8


class UploadRejected(ValueError):
    pass


class UploadPlan:
    """Validation result for an accepted STOR.

    ``version`` is the store version observed at prepare time.  ``commit``
    publishes only when the file is still at exactly that version, so two
    uploads accepted against the same content cannot both succeed.
    """

    def __init__(self, store, name, restart, version):
        self.store = store
        self.filename = name
        self.restart = restart
        self.version = version


class FileStore:
    def __init__(self, files):
        self.files = {name: bytearray(data) for name, data in files.items()}
        self.versions = {name: 1 for name in files}

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
        return UploadPlan(self, name, restart, self.versions.get(name))

    def commit(self, plan, payload):
        """Publish a fully received upload atomically.

        Every rule is re-checked before anything mutates, so a rejected
        commit leaves the store exactly as it was.  A successful commit
        always advances the file's version, even when the bytes are
        unchanged.
        """
        current = self.files.get(plan.filename)
        if self.versions.get(plan.filename) != plan.version:
            raise UploadRejected("file changed during upload")
        if current is None and len(self.files) >= CAPACITY:
            raise UploadRejected("file capacity exceeded")
        if plan.restart is not None:
            if current is None or plan.restart > len(current):
                raise UploadRejected("invalid restart position")
            new_content = bytes(current[: plan.restart]) + bytes(payload)
        else:
            new_content = bytes(payload)
        if len(new_content) > LIMIT:
            raise UploadRejected("file size exceeded")
        if current is None:
            self.files[plan.filename] = bytearray(new_content)
        else:
            current[:] = new_content
        self.versions[plan.filename] = (plan.version or 0) + 1
