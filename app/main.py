import argparse
import base64
from dataclasses import dataclass
import random
import socket
import string
import time
from threading import Thread
from typing import Callable, Literal, NamedTuple
class Token(NamedTuple):
    type: str
    data: bytes
@dataclass
class Replica:
    port: int
    conn: socket.socket
@dataclass
class Context:
    cmd_handlers: dict[str, Callable[["Context", "ConnContext", list[bytes]], bytes]]
    store: dict[bytes, tuple[bytes, int | None]]
    role: Literal[b"master", b"slave"]
    replication_id: bytes
    replication_offset: int
    replicas: dict[int, Replica]
@dataclass
class ConnContext:
    id: int
    conn: socket.socket
def send_to_replicas(ctx: Context, cmd: list[bytes]):
    for r in ctx.replicas.values():
        r.conn.send(encode_arr(cmd))
def handle_ping(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    return b"+PONG\r\n"
def handle_echo(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    if len(cmd) < 2:
        return encode_err(b"Need 1 arg")
    return encode_bulkstring(cmd[1])
def handle_set(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    if len(cmd) < 3:
        return encode_err(b"Need 2 args")
    expiry = None
    if len(cmd) >= 5:
        if to_str(cmd[3]).lower() == "px":
            expiry = time.time_ns() + int(cmd[4]) * 1_000_000
    ctx.store[cmd[1]] = (cmd[2], expiry)
    send_to_replicas(ctx, cmd)
    return encode_ok()
def handle_get(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    if len(cmd) < 2:
        return encode_err(b"Need 1 arg")
    value, expiry = ctx.store.get(cmd[1], (None, None))
    if expiry and time.time_ns() > expiry:
        ctx.store.pop(cmd[1])
        value = None
    return encode_bulkstring(value)
def socket_connect(host: str, port: int) -> socket.socket:
    conn = socket.socket()
    conn.connect((host, port))
    return conn
def handle_replconf(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    if len(cmd) < 3:
        return encode_err(b"Need 2 args")
    if cmd[1] == b"listening-port":
        port = int(cmd[2])
        ctx.replicas[cctx.id] = Replica(port, cctx.conn)
        return encode_ok()
    elif cmd[1] == b"capa":
        if cmd[2] != b"psync2":
            return encode_err(b"Err only supports psync2")
        return encode_ok()
    return encode_err(b"Err invalid replconf cmd")
def handle_psync(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    if len(cmd) < 3:
        return encode_err(b"Need 2 args")
    if cmd[1] == b"?":
        empty = base64.decodebytes(
            b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
        )
        return (
            encode_string(
                b"FULLRESYNC "
                + ctx.replication_id
                + b" "
                + to_bytes(ctx.replication_offset)
            )
            + encode_bulkstring(empty)[:-2]
        )
    return encode_err(b"Err invalid psync cmd")
def encode_info_field(field: bytes, value: bytes):
    return field + b":" + value + b"\r\n"
def info_replication(ctx: Context) -> bytes:
    res = b"# Replication\r\n"
    res += encode_info_field(b"role", ctx.role)
    if ctx.role == b"master":
        res += encode_info_field(b"connected_slaves", to_bytes(len(ctx.replicas)))
    res += encode_info_field(b"master_replid", ctx.replication_id)
    res += encode_info_field(b"master_repl_offset", to_bytes(ctx.replication_offset))
    return res
def handle_info(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    section = None
    if len(cmd) >= 2:
        section = cmd[1]
    res = b""
    if section is None or section == b"replication":
        res += info_replication(ctx)
    return encode_bulkstring(res)
def handle_unknown(ctx: Context, cctx: ConnContext, cmd: list[bytes]) -> bytes:
    return b"-Unknown command\r\n"
def encode_bulkstring(in_str: bytes | None) -> bytes:
    if in_str is None:
        return b"$-1\r\n"
    return b"$" + to_bytes(len(in_str)) + b"\r\n" + in_str + b"\r\n"
def to_bytes(input: str | int) -> bytes:
    if isinstance(input, int):
        input = str(input)
    return bytes(input, "utf-8")
def to_str(in_str: bytes) -> str:
    return str(in_str, "utf-8")
def encode_ok():
    return b"+OK\r\n"
def encode_arr(arr: list[bytes]):
    res = b"*" + to_bytes(len(arr)) + b"\r\n"
    for e in arr:
        res += encode_bulkstring(e)
    return res
def encode_string(in_str: bytes):
    return b"+" + in_str + b"\r\n"
def encode_err(in_str: bytes):
    return b"-" + in_str + b"\r\n"
def send_err(conn: socket.socket, error: bytes):
    print(error)
    conn.send(encode_err(error))
def get_token(
    conn: socket.socket,
    buf: bytes,
    fixed_size: int | None = None,
    fixed_type: str | None = None,
) -> tuple[Token, bytes]:
    while True:
        if fixed_size:
            assert fixed_type
            if len(buf) >= fixed_size:
                skip_len = 0
                if buf[fixed_size : fixed_size + 2] == b"\r\n":
                    skip_len = 2
                return (
                    Token(fixed_type, buf[:fixed_size]),
                    buf[fixed_size + skip_len :],
                )
        else:
            cmd_end = buf.find(b"\r\n")
            if cmd_end != -1:
                res = (Token(chr(buf[0]), buf[1:cmd_end]), buf[cmd_end + 2 :])
                if chr(buf[0]) in "$!=":
                    res = get_token(conn, res[1], int(res[0].data), res[0].type)
                return res
        recv_buf = conn.recv(1024)
        buf += recv_buf
        print(f"Recv {buf}")
        if not recv_buf:
            raise ConnectionError
def generate_rid() -> bytes:
    return to_bytes(
        "".join(random.choices(string.ascii_lowercase + string.digits, k=40))
    )
def sync_loop(m_conn: socket.socket, ctx: Context, port: int):
    buf = b""
    with m_conn:
        m_conn.send(encode_arr([b"ping"]))
        token, buf = get_token(m_conn, buf)
        if token != Token("+", b"PONG"):
            print("Sync err: didn't get PONG")
            return
        m_conn.send(encode_arr([b"replconf", b"listening-port", to_bytes(port)]))
        token, buf = get_token(m_conn, buf)
        if token != Token("+", b"OK"):
            print("Sync err: didn't get OK for listening port")
            return
        m_conn.send(encode_arr([b"replconf", b"capa", b"psync2"]))
        token, buf = get_token(m_conn, buf)
        if token != Token("+", b"OK"):
            print("Sync err: didn't get OK for capa")
            return
        m_conn.send(encode_arr([b"psync", b"?", b"-1"]))
        token, buf = get_token(m_conn, buf)
        resp_arr = token.data.split(b" ")
        if resp_arr[0] != b"FULLRESYNC":
            print("Sync err: didn't get FULLRESYNC for psync")
            return
        ctx.replication_id = resp_arr[1]
        ctx.replication_offset = int(resp_arr[2])
        token, buf = get_token(m_conn, buf)
        if token.type != "$":
            print("Sync err: didn't get RDB for psync")
            return
        client_loop(m_conn, ctx, True, buf)


def client_loop(
    conn: socket.socket, ctx: Context, from_master: bool = False, prev_buf: bytes = b""
):
    print(f"Client loop start {conn}")
    cctx = ConnContext(conn.fileno(), conn)
    with conn:
        buf = prev_buf
        while True:
            try:
                cmd: list[bytes] = []
                token, buf = get_token(conn, buf)
                if token.type != "*":
                    send_err(conn, b"Expected array for cmd")
                    continue
                arr_len = int(token.data)
                if arr_len < 1:
                    send_err(conn, b"Init arr needs 1+ elements")
                    continue
                for _ in range(arr_len):
                    token, buf = get_token(conn, buf)
                    cmd.append(token.data)
                print(f"Got command: {cmd}")
                res = ctx.cmd_handlers.get(to_str(cmd[0]).lower(), handle_unknown)(
                    ctx, cctx, cmd
                )
                if not from_master:
                    conn.send(res)
            except (ConnectionError, AssertionError):
                break
    if cctx.id in ctx.replicas:
        ctx.replicas[cctx.id].conn.close()
        ctx.replicas.pop(cctx.id)
    print(f"Client loop stop {conn}")


def main():
    print("Server start!")
    ctx = Context(
        cmd_handlers={
            "ping": handle_ping,
            "echo": handle_echo,
            "set": handle_set,
            "get": handle_get,
            "info": handle_info,
            "replconf": handle_replconf,
            "psync": handle_psync,
        },
        store={},
        role=b"master",
        replication_id=generate_rid(),
        replication_offset=0,
        replicas={},
    )
    parser = argparse.ArgumentParser(description="GVK Redis")
    parser.add_argument("--port", default=6379, type=int, help="Server port")
    parser.add_argument("--replicaof", nargs="+", help="Replica server host and port")
    args = parser.parse_args()
    m_conn = None
    if args.replicaof:
        ctx.role = b"slave"
        replicaof = args.replicaof[0].split()
        m_conn = socket_connect(replicaof[0], int(replicaof[1]))
        Thread(target=lambda: sync_loop(m_conn, ctx, args.port), daemon=True).start()
    server_socket = socket.create_server(("localhost", args.port))
    while True:
        conn, _ = server_socket.accept()  # wait for client
        print(f"Got connection from {conn}")
        Thread(target=lambda: client_loop(conn, ctx), daemon=True).start()
if __name__ == "__main__":
    Thread(target=main, daemon=True).start()
    while True:
        time.sleep(10)
