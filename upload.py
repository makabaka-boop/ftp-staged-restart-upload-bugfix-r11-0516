"""Receive a passive upload and publish it only at the successful terminal.

The receiver mirrors ``Transfer.run`` (the download path):

* it exclusively owns its listener and accepted data socket;
* state transitions (accepting -> active -> terminal) happen under the
  transfer's state lock, so ABOR and completion race for one conclusion;
* received bytes are buffered locally -- the shared store is touched only by
  a single atomic ``commit`` after EOF, so an aborted, failed, oversized, or
  superseded upload leaves no file change behind;
* a terminal reply is written only while the transfer is still the current
  one for its session, so a stale task can never answer a new operation.
"""

import asyncio
import contextlib
import logging

from filestore import UploadRejected
from server import (
    STATE_ACCEPTING,
    STATE_ACCEPT_FAILED,
    STATE_ACTIVE,
    STATE_CANCELED,
    STATE_FAILED,
    STATE_SUCCEEDED,
    STATE_TIMED_OUT,
)

logger = logging.getLogger("readonly-ftp")

_NON_TERMINAL = (STATE_ACCEPTING, STATE_ACTIVE)


async def run_upload(transfer):
    loop = asyncio.get_running_loop()
    body = bytearray()
    try:
        try:
            sock, _ = await asyncio.wait_for(
                loop.sock_accept(transfer.listener.sock), transfer.accept_timeout
            )
        except asyncio.TimeoutError:
            await transfer._conclude(
                STATE_TIMED_OUT, 425, "Can't open data connection."
            )
            return
        except OSError:
            await transfer._conclude(
                STATE_ACCEPT_FAILED, 425, "Can't open data connection."
            )
            return

        # This transfer owns this socket; nothing else may accept on it.
        transfer.data_sock = sock
        transfer.listener.close()

        try:
            reader, writer = await asyncio.open_connection(sock=sock)
        except OSError:
            await transfer._conclude(
                STATE_ACCEPT_FAILED, 425, "Can't open data connection."
            )
            return
        transfer.data_writer = writer

        async with transfer.state_lock:
            if transfer.state != STATE_ACCEPTING:
                return
            transfer.state = STATE_ACTIVE

        try:
            while True:
                block = await reader.read(4096)
                if not block:
                    break
                body.extend(block)
                if not transfer.session.store.fits(transfer.plan, len(body)):
                    raise UploadRejected("file size exceeded")
                await transfer.hooks.after_write(transfer, len(body), block)
        except (asyncio.CancelledError, UploadRejected, OSError, EOFError):
            raise
        except Exception:
            logger.exception("Unhandled upload read error")
            raise OSError("upload receive failed")

        await transfer.hooks.before_finish(transfer)

        # Atomic publish: the optimistic-commit and the terminal-state claim
        # happen in the same critical section, so ABOR can never win after a
        # publish, and a rejected commit can never publish anyway.  The
        # commit itself is synchronous and never awaits.
        async with transfer.state_lock:
            if transfer.state != STATE_ACTIVE:
                # ABOR or session teardown won the race; do not publish.
                return
            try:
                transfer.session.store.commit(transfer.plan, body)
            except UploadRejected:
                # Superseded by another session, oversized, or capacity lost:
                # claim the failed conclusion without changing any file.
                transfer.state = STATE_FAILED
                publish_failed = True
            else:
                transfer.state = STATE_SUCCEEDED
                publish_failed = False

        if publish_failed:
            # Abort the data side: the upload was rejected (superseded,
            # oversized, or capacity lost), so a clean EOF would be wrong.
            transfer.close_data_and_listener()
            await transfer.session.transfer_reply(
                transfer, 426, "Connection closed; transfer aborted."
            )
            transfer.terminal_reply_done.set()
            return

        # Data-connection EOF before the successful control reply.
        with contextlib.suppress(Exception):
            writer.close()
            await writer.wait_closed()

        await transfer.session.transfer_reply(
            transfer, 226, "Transfer complete."
        )
        transfer.terminal_reply_done.set()

    except asyncio.CancelledError:
        async with transfer.state_lock:
            if transfer.state in _NON_TERMINAL:
                transfer.state = STATE_CANCELED
        # ABOR has already emitted its ordered replies; QUIT/close emit none.
        return
    except (OSError, EOFError, RuntimeError, UploadRejected):
        await transfer._conclude_failure()
    except Exception:
        logger.exception("Unhandled upload error")
        await transfer._conclude_failure()
    finally:
        transfer.close_data_and_listener()
        async with transfer.session.state_lock:
            if transfer.session.active_transfer is transfer:
                transfer.session.active_transfer = None
