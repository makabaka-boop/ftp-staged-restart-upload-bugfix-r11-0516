"""Shared FTP file buffer used by passive uploads.

Uploads publish through an atomic, optimistic-concurrency commit:

* Bytes received during an in-flight STOR are held by the receiving task
  only.  Nothing is visible through :meth:`FileStore.read` until the upload
  reaches its successful terminal state, so concurrent downloaders keep
  seeing the last published version.
* Every successful publish installs a brand new ``bytes`` value and bumps
  the file's version -- even when the bytes are identical.
* A plan remembers the version it started from (and, for REST, a snapshot
  of the prefix at that version).  A commit whose base version is no longer
  current fails atomically and changes nothing: of two sessions that
  started from the same version, only one can publish.
"""

import re

LIMIT = 32768
CAPACITY = 8

_FILENAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}")


class UploadRejected(ValueError):
    """The upload must not (or can no longer) be published."""


class UploadPlan:
    """An accepted upload's immutable view on the file it will publish to."""

    __slots__ = ("store", "filename", "restart", "base_version", "prefix")

    def __init__(self, store, name, restart, current, base_version):
        self.store = store
        self.filename = name
        # None means a full replacement; otherwise keep [0, restart) of the
        # version captured below and discard the old suffix.
        self.restart = restart
        self.base_version = base_version
        self.prefix = bytes(current) if restart is not None else b""


class FileStore:
    def __init__(self, files):
        # Each entry is [bytes payload, int version].  Published payloads are
        # immutable bytes, so readers and upload plans can hold references
        # without copying defensively.
        self._files = {}
        for name, data in files.items():
            self._files[name] = [bytes(data), 0]

    def read(self, name):
        """Return the last published content, or None for an unknown file."""
        entry = self._files.get(name)
        return None if entry is None else entry[0]

    def prepare(self, name, restart):
        """Validate a STOR and snapshot the version it is based on.

        This performs no mutation.  It never consumes the restart marker --
        that is the control session's job, only once STOR is accepted.
        """
        if not isinstance(name, str) or not _FILENAME.fullmatch(name):
            raise UploadRejected("invalid filename")

        entry = self._files.get(name)
        if restart is not None:
            if entry is None or restart < 0 or restart > len(entry[0]):
                raise UploadRejected("invalid restart position")
        elif entry is None and len(self._files) >= CAPACITY:
            raise UploadRejected("file capacity exceeded")

        if entry is None:
            return UploadPlan(self, name, restart, b"", 0)
        return UploadPlan(self, name, restart, entry[0], entry[1])

    def fits(self, plan, received_length):
        """Check the running byte budget without touching the store."""
        kept = plan.restart if plan.restart is not None else 0
        return kept + received_length <= LIMIT

    def commit(self, plan, payload):
        """Atomically publish received bytes.

        No state changes on any rejection.  Returns the new version.
        """
        if plan.restart is not None:
            content = plan.prefix[: plan.restart] + bytes(payload)
        else:
            content = bytes(payload)
        if len(content) > LIMIT:
            raise UploadRejected("file size exceeded")

        entry = self._files.get(plan.filename)
        if entry is None:
            if len(self._files) >= CAPACITY:
                raise UploadRejected("file capacity exceeded")
            # A file appearing here means another session created it after
            # our prepare; only the plan based on its absence may create it.
            self._files[plan.filename] = [content, 1]
            return 1

        if entry[1] != plan.base_version:
            raise UploadRejected("file changed during transfer")
        entry[0] = content
        entry[1] += 1
        return entry[1]
