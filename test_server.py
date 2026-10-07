import asyncio
import contextlib
import re
import socket
import unittest

from filestore import CAPACITY, LIMIT
from server import FILES, SessionHooks, start_server


class Gate:
    def __init__(self):
        self.entered = asyncio.Event()
        self.released = asyncio.Event()

    async def wait(self):
        self.entered.set()
        await self.released.wait()

    def release(self):
        self.released.set()


class SlowWriteHooks(SessionHooks):
    def __init__(self):
        self.gate = Gate()

    async def after_write(self, transfer, cursor, chunk):
        if cursor == 0:
            await self.gate.wait()


class CompletionRaceHooks(SessionHooks):
    """Synchronize the two instructions that race for the terminal state."""

    def __init__(self):
        self.finish_reached = asyncio.Event()
        self.finish_allowed = asyncio.Event()
        self.abort_at_decision = asyncio.Event()
        self.abort_allowed = asyncio.Event()

    async def before_finish(self, transfer):
        self.finish_reached.set()
        await self.finish_allowed.wait()

    async def before_abort_state(self, transfer):
        self.abort_at_decision.set()
        await self.abort_allowed.wait()


class ForcedSendFailureHooks(SessionHooks):
    def __init__(self):
        self.fired = False

    async def after_write(self, transfer, cursor, chunk):
        if not self.fired and cursor == 0:
            self.fired = True
            raise ConnectionResetError("simulated data failure")


class SlowUploadHooks(SessionHooks):
    """Park an upload after its first received chunk."""

    def __init__(self):
        self.gate = Gate()

    async def after_write(self, transfer, cursor, chunk):
        if getattr(transfer, "plan", None) is not None and cursor == 0:
            await self.gate.wait()


class FtpControl:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.replies = asyncio.Queue()
        self.reader_task = asyncio.create_task(self._read_replies())

    async def _read_replies(self):
        while True:
            line = await self.reader.readline()
            if not line:
                await self.replies.put(("closed", b""))
                return
            match = re.fullmatch(rb"(\d{3}) (.*)\r\n", line)
            if not match:
                raise AssertionError(f"malformed reply: {line!r}")
            await self.replies.put((int(match.group(1)), line))

    async def send(self, text, delay=0):
        data = text.encode("ascii")
        if delay:
            mid = len(data) // 2
            self.writer.write(data[:mid])
            await self.writer.drain()
            await asyncio.sleep(delay)
            self.writer.write(data[mid:])
        else:
            self.writer.write(data)
        await self.writer.drain()

    async def code(self, expected=None, timeout=5):
        code, line = await asyncio.wait_for(self.replies.get(), timeout)
        if expected is not None:
            assert code == expected, f"expected {expected}, got {line!r}"
        return code

    async def close(self):
        self.writer.close()
        self.reader_task.cancel()
        with contextlib.suppress(Exception):
            await self.writer.wait_closed()
        with contextlib.suppress(asyncio.CancelledError):
            await self.reader_task


async def connect_control(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    control = FtpControl(reader, writer)
    await control.code(220)
    return control


async def login(control, split_commands=False):
    delay = 0.02 if split_commands else 0
    await control.send("USER demo\r\n", delay=delay)
    await control.code(331)
    await control.send("PASS demo-pass\r\n", delay=delay)
    await control.code(230)


async def epsv(control):
    await control.send("EPSV\r\n")
    code, line = await asyncio.wait_for(control.replies.get(), 5)
    assert code == 229, line
    match = re.search(rb"\(\|\|\|(\d+)\|\)", line)
    assert match, line
    return int(match.group(1))


def data_socket(receive_buffer=None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if receive_buffer is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
    sock.setblocking(False)
    return sock


async def connect_data(port, receive_buffer=None):
    sock = data_socket(receive_buffer)
    try:
        await asyncio.get_running_loop().sock_connect(sock, ("127.0.0.1", port))
    except Exception:
        sock.close()
        raise
    return sock


async def recv_all(sock):
    chunks = []
    while True:
        chunk = await asyncio.get_running_loop().sock_recv(sock, 65536)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


class FtpServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=0.25,
        )
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    async def start_session(self):
        control = await connect_control(self.port)
        await login(control, split_commands=True)
        await control.send("TYPE I\r\n")
        await control.code(200)
        return control

    async def download(self, name, restart=None):
        control = await self.start_session()
        port = await epsv(control)
        if restart is not None:
            await control.send(f"REST {restart}\r\n")
            await control.code(350)
        data_sock = await connect_data(port)
        await control.send(f"RETR {name}\r\n")
        await control.code(150)
        data = await recv_all(data_sock)
        await control.code(226)
        data_sock.close()
        await control.close()
        return data

    async def start_upload(self, control, name, restart=None):
        port = await epsv(control)
        if restart is not None:
            await control.send(f"REST {restart}\r\n")
            await control.code(350)
        data_sock = await connect_data(port)
        await control.send(f"STOR {name}\r\n")
        await control.code(150)
        return data_sock

    @staticmethod
    async def send_eof(data_sock, payload=b""):
        if payload:
            await asyncio.get_running_loop().sock_sendall(data_sock, payload)
        data_sock.close()

    async def test_login_incremental_crlf_and_full_downloads(self):
        control = await self.start_session()

        # CRLF and a command can be split across TCP segments.
        await control.send("NO", delay=0.01)
        await control.send("OP\r\n", delay=0.01)
        await control.code(200)

        for name, content in FILES.items():
            port = await epsv(control)
            data_sock = await connect_data(port)
            await control.send(f"RETR {name}\r\n")
            await control.code(150)
            assert await recv_all(data_sock) == content
            await control.code(226)
            data_sock.close()

        await control.close()

    async def test_rest_zero_and_offset_are_consumed_once(self):
        assert await self.download("colors.bin", restart=0) == FILES["colors.bin"]
        expected = FILES["counter.bin"][1000:]
        assert await self.download("counter.bin", restart=1000) == expected

        control = await self.start_session()
        await control.send("REST 10\r\n")
        await control.code(350)
        await control.send("RETR missing.bin\r\n")
        await control.code(550)
        # Rejected RETR did not consume REST; but there is no listener yet.
        await control.send("RETR colors.bin\r\n")
        await control.code(425)
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(data_sock) == FILES["colors.bin"][10:]
        await control.code(226)
        data_sock.close()
        await control.close()

    async def test_data_accept_timeout_is_425_and_later_abor_is_225(self):
        control = await self.start_session()
        port = await epsv(control)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        await control.code(425)
        await control.send("ABOR\r\n")
        await control.code(225)
        assert port > 0
        await control.close()

    async def test_transfer_failure_is_426_then_session_remains_usable(self):
        self.server.close()
        await self.server.wait_closed()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=0.25,
            hooks=ForcedSendFailureHooks(),
        )
        self.port = self.server.sockets[0].getsockname()[1]

        control = await self.start_session()
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR patterns.bin\r\n")
        await control.code(150)
        await control.code(426)
        with contextlib.suppress(OSError):
            await asyncio.get_running_loop().sock_recv(data_sock, 4096)
        data_sock.close()

        # A new transfer gets a new listener and a completely fresh terminal.
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(data_sock) == FILES["colors.bin"]
        await control.code(226)
        data_sock.close()
        await control.close()

    async def test_abor_during_slow_data_send_does_not_share_conclusion(self):
        self.server.close()
        await self.server.wait_closed()
        hooks = SlowWriteHooks()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=1,
            hooks=hooks,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        control = await self.start_session()

        port = await epsv(control)
        data_sock = await connect_data(port, receive_buffer=256)
        await control.send("RETR patterns.bin\r\n")
        await control.code(150)
        await hooks.gate.entered.wait()

        # The data sender is blocked; the independent control loop still
        # enforces gating and processes ABOR.
        await control.send("TYPE I\r\n")
        await control.code(503)
        await control.send("ABOR\r\n")
        await control.code(426)
        await control.code(226)
        hooks.gate.release()

        # Drain/reset of the aborted socket must not produce another reply.
        await asyncio.sleep(0.05)
        assert control.replies.empty()
        with contextlib.suppress(OSError):
            while await asyncio.get_running_loop().sock_recv(data_sock, 4096):
                pass
        data_sock.close()

        # The old transfer consumed its listener and restart marker.
        await control.send("RETR patterns.bin\r\n")
        await control.code(425)
        port = await epsv(control)
        new_data = await connect_data(port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(new_data) == FILES["colors.bin"]
        await control.code(226)
        new_data.close()
        await control.close()

    async def test_completion_and_abor_race_success_wins(self):
        await self.race_test(success_wins=True)

    async def test_completion_and_abor_race_abort_wins(self):
        await self.race_test(success_wins=False)

    async def race_test(self, success_wins: bool):
        self.server.close()
        await self.server.wait_closed()
        hooks = CompletionRaceHooks()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=1,
            hooks=hooks,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        control = await self.start_session()
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR patterns.bin\r\n")
        await control.code(150)

        async def read_until_close():
            with contextlib.suppress(OSError):
                while True:
                    chunk = await asyncio.get_running_loop().sock_recv(data_sock, 4096)
                    if not chunk:
                        return

        reader_task = asyncio.create_task(read_until_close())

        await hooks.finish_reached.wait()
        await control.send("ABOR\r\n")
        await hooks.abort_at_decision.wait()

        if success_wins:
            hooks.finish_allowed.set()
            await control.code(226)
            hooks.abort_allowed.set()
            await control.code(225)
        else:
            hooks.abort_allowed.set()
            # Give ABOR its state-claim opportunity while completion remains
            # parked immediately before its own claim.
            await asyncio.sleep(0.02)
            hooks.finish_allowed.set()
            await control.code(426)
            await control.code(226)

        await asyncio.wait_for(reader_task, timeout=2)
        data_sock.close()
        await asyncio.sleep(0.02)
        assert control.replies.empty()
        await control.close()

    async def test_new_epsv_replaces_old_listener_including_late_connection(self):
        control = await self.start_session()
        first_port = await epsv(control)
        stale_sock = await connect_data(first_port)

        second_port = await epsv(control)
        assert second_port != first_port

        # The stale, accepted connection is closed when EPSV replaces its
        # listener.  Give the server a moment to drain that accept queue.
        await asyncio.sleep(0.05)
        late_sock = data_socket()
        with self.assertRaises(ConnectionRefusedError):
            await asyncio.get_running_loop().sock_connect(
                late_sock, ("127.0.0.1", first_port)
            )
        late_sock.close()
        with contextlib.suppress(OSError):
            await asyncio.get_running_loop().sock_recv(stale_sock, 4096)
        stale_sock.close()

        data_sock = await connect_data(second_port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(data_sock) == FILES["colors.bin"]
        await control.code(226)
        data_sock.close()
        await control.close()

    async def test_unsupported_commands_and_auth_boundary(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        control = FtpControl(reader, writer)
        await control.code(220)

        for command in (
            "PWD\r\n",
            "LIST\r\n",
            "DELE x\r\n",
            "PASV\r\n",
            "PORT 1,2,3,4,0,1\r\n",
            "MKD x\r\n",
        ):
            await control.send(command)
            await control.code(530)

        await control.send("USER demo\r\n")
        await control.code(331)
        await control.send("PASS wrong\r\n")
        await control.code(530)

        await login(control)
        await control.send("TYPE A\r\n")
        await control.code(504)
        for command in (
            "PWD\r\n",
            "LIST\r\n",
            "DELE x\r\n",
            "PASV\r\n",
            "PORT 1,2,3,4,0,1\r\n",
            "MKD x\r\n",
        ):
            await control.send(command)
            await control.code(502)
        await control.close()

    async def test_abor_without_transfer_is_225(self):
        control = await self.start_session()
        await control.send("ABOR\r\n")
        await control.code(225)
        await control.close()

    async def test_stor_replace_and_resume_semantics(self):
        control = await self.start_session()

        # A new file is downloadable after a complete upload.
        data_sock = await self.start_upload(control, "notes.bin")
        await self.send_eof(data_sock, b"hello world")
        await control.code(226)
        assert await self.download("notes.bin") == b"hello world"

        # Without REST an upload replaces the whole file; no old suffix.
        data_sock = await self.start_upload(control, "colors.bin")
        await self.send_eof(data_sock, b"SHORT")
        await control.code(226)
        assert await self.download("colors.bin") == b"SHORT"

        # REST k keeps the original prefix [0, k) and drops the old suffix.
        data_sock = await self.start_upload(control, "counter.bin", restart=1000)
        await self.send_eof(data_sock, b"TAIL")
        await control.code(226)
        expected = FILES["counter.bin"][:1000] + b"TAIL"
        assert await self.download("counter.bin") == expected

        # REST 0 is a valid restart position: it replaces everything.
        data_sock = await self.start_upload(control, "notes.bin", restart=0)
        await self.send_eof(data_sock, b"zero")
        await control.code(226)
        assert await self.download("notes.bin") == b"zero"
        await control.close()

    async def test_rejected_stor_consumes_neither_rest_nor_listener(self):
        control = await self.start_session()

        await control.send("REST 5\r\n")
        await control.code(350)
        # Invalid file name: rejected before anything is consumed.
        await control.send("STOR bad%name\r\n")
        await control.code(550)
        # No listener yet: 425, and the restart marker still survives.
        await control.send("STOR colors.bin\r\n")
        await control.code(425)

        # REST 5 is still pending and applies to this accepted STOR.
        data_sock = await self.start_upload(control, "colors.bin")
        await self.send_eof(data_sock, b"NEW")
        await control.code(226)
        assert await self.download("colors.bin") == FILES["colors.bin"][:5] + b"NEW"

        # A restart beyond end of file rejects the STOR; the listener and
        # its queued data connection survive for the next accepted STOR.
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("REST 99999\r\n")
        await control.code(350)
        await control.send("STOR counter.bin\r\n")
        await control.code(550)
        await control.send("REST 10\r\n")
        await control.code(350)
        await control.send("STOR counter.bin\r\n")
        await control.code(150)
        await self.send_eof(data_sock, b"XY")
        await control.code(226)
        expected = FILES["counter.bin"][:10] + b"XY"
        assert await self.download("counter.bin") == expected
        await control.close()

    async def test_upload_invisible_until_success_and_abort_leaves_no_trace(self):
        self.server.close()
        await self.server.wait_closed()
        hooks = SlowUploadHooks()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=1,
            hooks=hooks,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        control = await self.start_session()

        # A new file is invisible to other sessions while it uploads...
        data_sock = await self.start_upload(control, "fresh.bin")
        await asyncio.get_running_loop().sock_sendall(data_sock, b"partial data")
        await hooks.gate.entered.wait()

        probe = await self.start_session()
        await probe.send("RETR fresh.bin\r\n")
        await probe.code(550)
        # ...and concurrent downloads still see the saved versions.
        assert await self.download("colors.bin") == FILES["colors.bin"]

        # Aborting leaves no trace of the partial upload.
        await control.send("ABOR\r\n")
        await control.code(426)
        await control.code(226)
        hooks.gate.release()
        data_sock.close()
        await probe.send("RETR fresh.bin\r\n")
        await probe.code(550)

        # Aborting an overwrite leaves the saved file untouched.
        data_sock = await self.start_upload(control, "colors.bin")
        await asyncio.get_running_loop().sock_sendall(data_sock, b"SHORT")
        await control.send("ABOR\r\n")
        await control.code(426)
        await control.code(226)
        data_sock.close()
        assert await self.download("colors.bin") == FILES["colors.bin"]

        # A complete upload publishes exactly the received bytes.
        data_sock = await self.start_upload(control, "fresh.bin")
        await self.send_eof(data_sock, b"final")
        await control.code(226)
        assert await self.download("fresh.bin") == b"final"

        await probe.close()
        await control.close()

    async def test_control_disconnect_mid_upload_leaves_no_change(self):
        control = await self.start_session()
        data_sock = await self.start_upload(control, "colors.bin")
        await asyncio.get_running_loop().sock_sendall(data_sock, b"partial")
        await control.close()
        data_sock.close()
        await asyncio.sleep(0.05)
        assert await self.download("colors.bin") == FILES["colors.bin"]

        control = await self.start_session()
        data_sock = await self.start_upload(control, "ghost.bin")
        await asyncio.get_running_loop().sock_sendall(data_sock, b"partial")
        await control.close()
        data_sock.close()
        await asyncio.sleep(0.05)
        probe = await self.start_session()
        await probe.send("RETR ghost.bin\r\n")
        await probe.code(550)
        await probe.close()

    async def test_size_limit_leaves_no_change(self):
        control = await self.start_session()

        # Exactly LIMIT bytes is the largest allowed upload.
        payload = bytes(range(256)) * (LIMIT // 256)
        data_sock = await self.start_upload(control, "max.bin")
        await self.send_eof(data_sock, payload)
        await control.code(226)
        assert await self.download("max.bin") == payload

        # One byte more fails and stores nothing.
        data_sock = await self.start_upload(control, "huge.bin")
        await self.send_eof(data_sock, b"x" * (LIMIT + 1))
        await control.code(426)
        probe = await self.start_session()
        await probe.send("RETR huge.bin\r\n")
        await probe.code(550)

        # A resume whose prefix + body exceeds the limit fails too, and the
        # original file is untouched.
        data_sock = await self.start_upload(control, "colors.bin", restart=1000)
        await self.send_eof(data_sock, b"y" * (LIMIT - 999))
        await control.code(426)
        assert await self.download("colors.bin") == FILES["colors.bin"]
        await probe.close()
        await control.close()

    async def test_capacity_limit_rejects_without_consuming(self):
        control = await self.start_session()
        for index in range(CAPACITY - len(FILES)):
            data_sock = await self.start_upload(control, f"extra{index}.bin")
            await self.send_eof(data_sock, bytes([index]))
            await control.code(226)

        # The store is full: a new name is rejected...
        port = await epsv(control)
        await control.send("STOR overflow.bin\r\n")
        await control.code(550)
        # ...and the listener survived: an overwrite still works on it.
        data_sock = await connect_data(port)
        await control.send("STOR extra0.bin\r\n")
        await control.code(150)
        await self.send_eof(data_sock, b"replacement")
        await control.code(226)
        assert await self.download("extra0.bin") == b"replacement"
        await control.close()

    async def test_concurrent_uploads_only_one_publishes(self):
        control_a = await self.start_session()
        control_b = await self.start_session()

        # Both sessions are accepted against the same stored version.
        sock_a = await self.start_upload(control_a, "colors.bin")
        sock_b = await self.start_upload(control_b, "colors.bin")
        await self.send_eof(sock_b, b"from-B")
        await control_b.code(226)
        # The later finisher must not overwrite the published result.
        await self.send_eof(sock_a, b"from-A")
        await control_a.code(426)
        assert await self.download("colors.bin") == b"from-B"

        # Versions advance even when the bytes do not: an identical publish
        # still invalidates every other plan taken against the old version.
        sock_a = await self.start_upload(control_a, "colors.bin")
        sock_b = await self.start_upload(control_b, "colors.bin")
        await self.send_eof(sock_b, b"same")
        await control_b.code(226)
        await self.send_eof(sock_a, b"same")
        await control_a.code(426)
        assert await self.download("colors.bin") == b"same"

        # The same rule applies to two uploads of a brand-new name.
        sock_a = await self.start_upload(control_a, "brand.bin")
        sock_b = await self.start_upload(control_b, "brand.bin")
        await self.send_eof(sock_b, b"F")
        await control_b.code(226)
        await self.send_eof(sock_a, b"E")
        await control_a.code(426)
        assert await self.download("brand.bin") == b"F"

        await control_a.close()
        await control_b.close()

    async def test_stor_accept_timeout_is_425_and_file_unchanged(self):
        control = await self.start_session()
        await epsv(control)
        await control.send("STOR colors.bin\r\n")
        await control.code(150)
        await control.code(425)
        await control.send("ABOR\r\n")
        await control.code(225)
        assert await self.download("colors.bin") == FILES["colors.bin"]
        await control.close()

    async def test_upload_receive_failure_leaves_no_change(self):
        self.server.close()
        await self.server.wait_closed()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=0.25,
            hooks=ForcedSendFailureHooks(),
        )
        self.port = self.server.sockets[0].getsockname()[1]

        control = await self.start_session()
        data_sock = await self.start_upload(control, "colors.bin")
        await asyncio.get_running_loop().sock_sendall(data_sock, b"boom")
        await control.code(426)
        data_sock.close()
        assert await self.download("colors.bin") == FILES["colors.bin"]
        await control.close()

    async def test_upload_completion_and_abor_race_success_wins(self):
        await self.upload_race(success_wins=True)

    async def test_upload_completion_and_abor_race_abort_wins(self):
        await self.upload_race(success_wins=False)

    async def upload_race(self, success_wins: bool):
        self.server.close()
        await self.server.wait_closed()
        hooks = CompletionRaceHooks()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=1,
            hooks=hooks,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        control = await self.start_session()

        payload = b"race-payload"
        data_sock = await self.start_upload(control, "colors.bin")
        await self.send_eof(data_sock, payload)

        await hooks.finish_reached.wait()
        await control.send("ABOR\r\n")
        await hooks.abort_at_decision.wait()

        if success_wins:
            hooks.finish_allowed.set()
            await control.code(226)
            hooks.abort_allowed.set()
            await control.code(225)
            assert await self.download("colors.bin") == payload
        else:
            hooks.abort_allowed.set()
            # Give ABOR its state-claim opportunity while completion remains
            # parked immediately before its own claim.
            await asyncio.sleep(0.02)
            hooks.finish_allowed.set()
            await control.code(426)
            await control.code(226)
            await asyncio.sleep(0.02)
            # The canceled upload publishes nothing and says nothing more.
            assert control.replies.empty()
            assert await self.download("colors.bin") == FILES["colors.bin"]
            # A later accepted transfer gets exactly one terminal reply.
            data_sock = await self.start_upload(control, "colors.bin")
            await self.send_eof(data_sock, b"after-race")
            await control.code(226)
            assert await self.download("colors.bin") == b"after-race"

        await control.close()

    async def test_stor_requires_type_i_and_a_name(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        control = FtpControl(reader, writer)
        await control.code(220)
        await login(control)

        await control.send("STOR x.bin\r\n")
        await control.code(504)
        await control.send("TYPE I\r\n")
        await control.code(200)
        await control.send("STOR\r\n")
        await control.code(501)
        await control.send("STOR bad name\r\n")
        await control.code(501)
        await control.close()


if __name__ == "__main__":
    unittest.main()
