import socket
import os
from threading import Thread
from time import time
from argparse import ArgumentParser
import itertools
import random

data = {}
expiry_time = {}
rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
replicas: list[socket.socket] = []


class _Settings:
    def __init__(self, port, replicaof):
        self.port = port
        self.replicaof = replicaof and tuple(replicaof)
        self.replid = random.randbytes(20).hex()
        self.repl_offset = 0
    @property
    def role(self):
        return "slave" if self.replicaof else "master"


def _parse_resp(it):
    ty = next(it)
    if ty in b"+-":  # simple string and error
        s = bytearray()
        while (c := next(it)) != ord(b"\r"):
            s.append(c)
        # FIXME: don't assert
        assert next(it) == ord(b"\n")
        if ty == ord(b"+"):
            return bytes(s)
        else:
            return Exception(s)
    elif ty in b"$!=":  # bulk string, bulk error, verbatim string
        length_digits = bytearray([next(it)])
        while (c := next(it)) != ord(b"\r"):
            length_digits.append(c)
        assert next(it) == ord(b"\n")
        if ty == b"=":
            encoding = bytes([next(it), next(it), next(it)])
            assert next(it) == ord(b":")
        else:
            encoding = None
        length = int(length_digits)
        if length == -1:
            return None
        data = bytes(itertools.islice(it, length))
        assert next(it) == ord(b"\r")
        assert next(it) == ord(b"\n")
        if ty == ord(b"$"):
            return data
        elif ty == ord(b"="):
            assert encoding in (b"txt", b"mkd")
            return data
        else:
            return Exception(data)
    elif (
        ty == ord(b":") or ty == 40
    ):  # int and big int (40 == lparen, hack to fix vim indentation)
        sign = next(it)
        digits = bytearray()
        if sign not in b"+-":
            digits.append(sign)
            sign = ord(b"+")
        while (c := next(it)) != ord(b"\r"):
            digits.append(c)
        assert next(it) == ord(b"\n")
        val = int(digits)
        if sign == ord(b"-"):
            val = -val
        return val
    elif ty in b"*~>":  # array, set, and push
        length_digits = bytearray([next(it)])
        while (c := next(it)) != ord(b"\r"):
            length_digits.append(c)
        assert next(it) == ord(b"\n")
        length = int(length_digits)
        if length == -1:
            return None
        elements = (_parse_resp(it) for _ in range(length))
        if ty == ord(b"~"):
            return set(elements)
        else:
            return list(elements)
    elif ty == ord(b"%"):  # map
        length_digits = bytearray([next(it)])
        while (c := next(it)) != ord(b"\r"):
            length_digits.append(c)
        assert next(it) == ord(b"\n")
        length = int(length_digits)
        if length == -1:
            return None
        return {_parse_resp(it): _parse_resp(it) for _ in range(length)}
    elif ty == ord(b"_"):  # null
        assert next(it) == ord(b"\r")
        assert next(it) == ord(b"\n")
        return None
    elif ty == ord(b"#"):  # bool
        v = next(it)
        assert v in b"tf"
        assert next(it) == ord(b"\r")
        assert next(it) == ord(b"\n")
        return v == ord(b"t")
    elif ty == ord(b","):  # float
        integral_digits = bytearray([next(it)])
        while (c := next(it)) not in b"\r.eE":
            integral_digits.append(c)
        if c == ord(b"."):
            fractional_digits = bytearray([next(it)])
            while (c := next(it)) not in b"\reE":
                fractional_digits.append(c)
            integral_digits.append(ord(b"."))
            integral_digits.extend(fractional_digits)
        if c in b"eE":
            exponent_digits = next(it)
            while (c := next(it)) != ord(b"\r"):
                exponent_digits += c
            integral_digits.append(ord(b"e"))
            integral_digits.extend(exponent_digits)
        assert next(it) == ord(b"\n")
        return float(integral_digits)


def _encode_resp(value, resp3=False, is_array=False) -> bytes:
    if isinstance(value, (bytes, bytearray, str)):
        binary_data = value.encode() if isinstance(value, str) else value
        # FIXME: 2n
        is_simple = b"\n" not in binary_data and b"\r" not in binary_data
        if is_simple:
            return b"".join([b"+", binary_data, b"\r\n"])
        else:
            return b"".join(
                [
                    b"$",
                    str(len(binary_data)).encode(),
                    b"\r\n",
                    binary_data,
                    b"\r\n",
                ]
            )
    elif isinstance(value, int):
        is_big = value.bit_length() > 64
        if is_big:
            ty = (
                chr(40).encode()  # lparen without breaking vim indent
                if resp3
                else b"+"
            )
            return b"".join(
                [
                    ty,
                    str(value).encode(),
                    b"\r\n",
                ]
            )
        else:
            return b"".join([b":", str(value).encode(), b"\r\n"])
    elif isinstance(value, (list, set)):
        ty = b"*" if isinstance(value, list) else b"~"
        return b"".join(
            itertools.chain(
                [ty, str(len(value)).encode(), b"\r\n"],
                (_encode_resp(el, resp3) for el in value),
            )
        )
    elif isinstance(value, dict):
        ty = b"%" if resp3 else b"*"
        return b"".join(
            itertools.chain(
                [ty], (_encode_resp(x, resp3) for kv in value.items() for x in kv)
            )
        )
    elif value is None:
        return b"_\r\n" if resp3 else b"*-1\r\n" if is_array else b"$-1\r\n"
    elif isinstance(value, bool):
        return b"#t\r\n" if value else b"#f\r\n"
    elif isinstance(value, float):
        # FIXME: don't depend on python's behaviour
        return b"".join([b",", str(value).encode(), b"\r\n"])
    else:
        raise NotImplementedError(type(value))


class Connection(Thread):
    def __init__(self, socket, address, settings: _Settings):
        super().__init__()
        self.sock = socket
        self.addr = address
        self.settings = settings

        print(f"Connection from {address}")
        self.start()

    def run(self):
        while True:
            request = self.sock.recv(1024)
            if request:
                parsed_req = self.parse_req(request)
                print(f"Request: {parsed_req}")
                self.parse_command(parsed_req)

    def parse_req(self, req):
        return req.decode().split("\r\n")[2:-1:2]

    def parse_command(self, req):
        match req[0].lower():
            case "ping":
                print("PONG!")
                self.sock.send(_encode_resp("PONG"))
            case "echo":
                print(f"ECHOING! {req[1]}")
                self.sock.send(_encode_resp(req[1]))
            case "set":
                data[req[1]] = req[2]
                if len(req) > 3 and req[3].lower() == "px":
                    additional_time = int(req[4])
                    expiry_time[req[1]] = additional_time + (time() * 1000)
                self.sock.send(_encode_resp("OK"))
                for rep in replicas:
                    rep.sendall(_encode_resp(req))
            case "get":
                if req[1] in data and (
                    (req[1] in expiry_time and expiry_time[req[1]] >= time() * 1000)
                    or req[1] not in expiry_time
                ):
                    self.sock.send(_encode_resp(data[req[1]]))
                else:
                    self.sock.send("$-1\r\n".encode())
            case "info":
                if req[1].lower() == "replication":
                    self.sock.send(
                        _encode_resp(
                            "\n".join(
                                [
                                    "# Replication",
                                    f"role:{role}",
                                    f"master_replid:{self.settings.replid}",
                                    f"master_repl_offset:{self.settings.repl_offset}",
                                ]
                            )
                        )
                    )
            case "replconf":
                self.sock.send(_encode_resp("OK"))
            case "psync":
                replicas.append(self.sock)
                self.sock.send(_encode_resp(f"FULLRESYNC {self.settings.replid} 0"))
                bres = bytes.fromhex(rdb)
                self.sock.send(f"${len(bres)}\r\n".encode() + bres)

def main(settings: _Settings):
    print("STARTED!")
    server_socket = socket.create_server(("localhost", settings.port), reuse_port=True)
    server_socket.listen(0)
    while True:
        client, client_addr = server_socket.accept()  # wait for client
        Connection(client, client_addr, settings)



if __name__ == "__main__":
    parser = ArgumentParser("A Redis server written in Python")
    parser.add_argument("--port", type=int, default=6379)
    class _ReplicaOf:
        i = -1
        def __call__(self, value):
            self.i += 1
            return value if self.i == 0 else int(value)
        def __repr__(self):
            return "replicaof"
    parser.add_argument("--replicaof", nargs="+", type=_ReplicaOf())
    args = parser.parse_args()
    role = "master" if not args.replicaof else "slave"
    if args.replicaof and len(args.replicaof[0].split()) == 2:
        master_info = args.replicaof[0].split()
        master_host = master_info[0]
        master_port = int(master_info[1])
        master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_sock.connect((master_host, master_port))
        master_sock.send(_encode_resp(["PING"]))
        master_sock.recv(1024).decode()
        master_sock.send(_encode_resp(["REPLCONF", "listening-port", str(args.port)]))
        master_sock.recv(1024).decode()
        master_sock.send(_encode_resp(["REPLCONF", "capa", "psync2"]))
        master_sock.recv(1024).decode()
        master_sock.send(_encode_resp(["PSYNC", "?", "-1"]))
        master_sock.recv(1024).decode()

    main(_Settings(**vars(args)))
