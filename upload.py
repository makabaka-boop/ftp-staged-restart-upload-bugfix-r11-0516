"""Receive passive upload bytes and publish them atomically on success."""

import asyncio
import contextlib
import logging

from filestore import LIMIT, UploadRejected
from server import (
    STATE_ACCEPT_FAILED,
    STATE_ACCEPTING,
    STATE_ACTIVE,
    STATE_CANCELED,
    STATE_FAILED,
    STATE_SUCCEEDED,
    STATE_TIMED_OUT,
)

logger = logging.getLogger("readonly-ftp")


async def run_upload(transfer):
    """Drive one STOR data connection.

    Received bytes stay private to this transfer until every byte arrived
    and the transfer claimed the terminal state under its lock; only then
    is the store committed.  A failed, aborted, or superseded upload never
    changes any file, and its late callbacks can neither publish content
    nor answer for a newer transfer.
    """
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

        # This transfer owns this socket and must not accept anything else.
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

        while True:
            block = await reader.read(4096)
            if not block:
                break
            offset = len(body)
            body.extend(block)
            if len(body) > LIMIT:
                raise UploadRejected("file size exceeded")
            await transfer.hooks.after_write(transfer, offset, block)

        await transfer.hooks.before_finish(transfer)

        failure = None
        async with transfer.state_lock:
            if transfer.state != STATE_ACTIVE:
                # Aborted or canceled while finishing: publish nothing.
                return
            try:
                transfer.session.store.commit(transfer.plan, body)
            except UploadRejected as error:
                transfer.state = STATE_FAILED
                failure = error
            else:
                transfer.state = STATE_SUCCEEDED

        if failure is not None:
            await transfer.session.transfer_reply(
                transfer, 426, f"Upload rejected: {failure}."
            )
            transfer.terminal_reply_done.set()
            return

        # Data-connection EOF reaches the client before the 226 does.
        with contextlib.suppress(Exception):
            writer.close()
            await writer.wait_closed()

        await transfer.session.transfer_reply(transfer, 226, "Upload complete.")
        transfer.terminal_reply_done.set()

    except asyncio.CancelledError:
        async with transfer.state_lock:
            if transfer.state in (STATE_ACCEPTING, STATE_ACTIVE):
                transfer.state = STATE_CANCELED
        return
    except UploadRejected as error:
        await transfer._conclude(STATE_FAILED, 426, f"Upload rejected: {error}.")
    except (OSError, EOFError, RuntimeError):
        await transfer._conclude(
            STATE_FAILED, 426, "Connection closed; upload aborted."
        )
    except Exception:
        logger.exception("Unhandled upload error")
        await transfer._conclude(
            STATE_FAILED, 426, "Connection closed; upload aborted."
        )
    finally:
        transfer.close_data_and_listener()
        async with transfer.session.state_lock:
            if transfer.session.active_transfer is transfer:
                transfer.session.active_transfer = None
