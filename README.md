# Read-Only FTP Demo

A tiny, dependency-free read-only FTP server demonstrating correct concurrency
between the **control connection** and **data connections**. A blocked data
send never blocks the control reader, so `ABOR`, `NOOP`, and `QUIT` remain
usable while a download is in flight.

## Run

```bash
docker compose up --build
```

Connect (any FTP client in binary/passive mode works; account is
`demo` / `demo-pass`):

```bash
python3 - <<'PY'
import ftplib
ftp = ftplib.FTP()
ftp.connect("127.0.0.1", 2121)
ftp.login("demo", "demo-pass")
with open("patterns.bin", "wb") as fh:
    ftp.retrbinary("RETR patterns.bin", fh.write)
ftp.quit()
PY
```

## Fixed demo content

Built into the process — there is no served directory and nothing can be
written:

| File           | Size      |
|----------------|-----------|
| `colors.bin`   | 4,096 B   |
| `counter.bin`  | 16,384 B  |
| `patterns.bin` | 32,768 B (the 32 KiB maximum) |

Account: `demo` / `demo-pass`.

## Supported command set

`USER`, `PASS`, `TYPE I`, `EPSV`, `REST`, `RETR`, `ABOR`, `NOOP`, `QUIT`.

Everything else is rejected with `502`. Upload (`STOR`, `APPE`, ...), active
mode (`PORT`/`EPRT`), and directory operations (`LIST`, `NLST`, `CWD`, `PWD`,
`MKD`, `DELE`, ...) are intentionally unsupported. `TYPE` accepts only `I`.

## Concurrency and protocol rules

* Control commands are read incrementally and framed only on `CRLF`; a command
  split across TCP segments is reassembled. Control reading and data sending
  advance independently (separate asyncio tasks).
* Each `EPSV` opens a fresh passive listener and **replaces** the previous
  one; any connection queued on the old listener but not yet accepted is
  drained and closed, so it can never be mistaken for a later connection.
* Each accepted `RETR` exclusively owns its listener, data socket, file
  cursor, terminal state, and terminal reply. Those objects are never shared
  with a later transfer.
* `REST offset` is consumed only by the **next accepted** `RETR`. A rejected
  `RETR` (missing file, offset past EOF, no listener) does not consume it.
* During an active transfer, every command except `ABOR`, `NOOP`, and `QUIT`
  is explicitly rejected with `503`.
* No data connection accepted within the timeout ⇒ `425`. A data failure while
  sending ⇒ `426`.
* `ABOR`:
  * active transfer: close the data side, then in order `426` then `226`;
  * already finished, or no active transfer: `225`.
* Completion and abort race for a single terminal state under a lock. Exactly
  one conclusion is emitted. A stale `accept`, `drain`, or completion callback
  from an old transfer cannot affect or answer a new one — a terminal reply is
  written only if the transfer is still the session's current one. The client
  can therefore determine which transfer ended solely from control replies.

## Configuration

Environment variables (all optional):

| Variable             | Default          | Meaning                          |
|----------------------|------------------|----------------------------------|
| `FTP_HOST`           | `0.0.0.0`       | Bind address                     |
| `FTP_PORT`           | `2121`           | Control port                     |
| `FTP_PASSIVE_PORTS`  | `0` (ephemeral)  | `port` or `start-end`            |
| `FTP_ACCEPT_TIMEOUT` | `10`             | Seconds to wait for data connect |

## Tests

Real dual-TCP tests with controllable send barriers:

```bash
python3 -m unittest -v
```

They cover abort while the data side is slow-reading, zero-byte `REST` and
offset resume, the end-of-transfer completion race in both orders,
replacement/late arrival of an old listener, `425`/`426` handling, and
rejection of unsupported commands.


## 被动二进制上传
新增 STOR filename，继续使用固定账户、TYPE I、EPSV 和原有控制状态。服务器最多保存 8 个文件、每个最多 32768 字节。文件名仅为 1..80 个 ASCII 字母数字及 ._- 且首字符为字母数字。无 REST 时上传完整替换，REST k 时必须已有文件，保存原内容 [0,k) 后接收到 EOF 的新字节，原来的后缀不保留；REST 0 有效。失败或被拒绝的 STOR 不消费 REST/EPSV，仅接受后独占这些状态。上传内容在成功终态前不对其他会话或 RETR 可见；ABOR、控制连接断开、超限、接收异常或文件已经被另一会话更新时不修改文件。每次成功发布都产生新版本，即使文件字节未变；多个接受到同版本的上传只能有一个成功发布，成功以 226 为准，传输失败 426，接受超时 425，中止回复仍按 426/226 顺序。上传、RETR 和控制会话共享文件存储，旧传输回调不得发布新文件或影响新的传输。
