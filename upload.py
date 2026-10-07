"""Receive passive upload bytes and notify the owning control session."""

import asyncio, contextlib
from filestore import LIMIT, UploadRejected


async def run_upload(transfer):
    loop = asyncio.get_running_loop()
    body = bytearray()
    try:
        try:
            sock, _ = await asyncio.wait_for(
                loop.sock_accept(transfer.listener.sock), transfer.accept_timeout
            )
        except (asyncio.TimeoutError, OSError):
            await transfer._conclude("timed-out", 425, "Can't open data connection.")
            return
        transfer.data_sock = sock
        transfer.listener.close()
        reader, writer = await asyncio.open_connection(sock=sock)
        transfer.data_writer = writer
        transfer.state = "active"
        while True:
            block = await reader.read(4096)
            if not block:
                break
            body.extend(block)
            transfer.session.store.expose(transfer.plan, block)
            if len(body) > LIMIT:
                raise UploadRejected("file size exceeded")
            await transfer.hooks.after_write(transfer, len(body), block)
        await transfer.hooks.before_finish(transfer)
        if transfer.is_terminal():
            return
        transfer.session.store.commit(transfer.plan, body)
        transfer.state = "succeeded"
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await transfer.session.transfer_reply(transfer, 226, "Upload complete.")
        transfer.terminal_reply_done.set()
    except asyncio.CancelledError:
        if transfer.state in ("accepting", "active"):
            transfer.state = "canceled"
    except Exception:
        await transfer._conclude("failed", 426, "Connection closed; upload aborted.")
    finally:
        transfer.close_data_and_listener()
        async with transfer.session.state_lock:
            if transfer.session.active_transfer is transfer:
                transfer.session.active_transfer = None
