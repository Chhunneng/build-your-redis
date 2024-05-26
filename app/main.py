import socket
import os
from threading import Thread
from time import time
from argparse import ArgumentParser

data = {}
expiry_time = {}


class Connection(Thread):
    def __init__(self, socket, address):
        super().__init__()
        self.sock = socket
        self.addr = address

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
                self.sock.send("+PONG\r\n".encode())
            case "echo":
                print(f"ECHOING! {req[1]}")
                self.sock.send(f"+{req[1]}\r\n".encode())
            case "set":
                data[req[1]] = req[2]
                if len(req) > 3 and req[3].lower() == "px":
                    additional_time = int(req[4])
                    expiry_time[req[1]] = additional_time + (time() * 1000)
                self.sock.send("+OK\r\n".encode())
            case "get":
                if req[1] in data and (
                    (req[1] in expiry_time and expiry_time[req[1]] >= time() * 1000)
                    or req[1] not in expiry_time
                ):
                    self.sock.send(f"+{data[req[1]]}\r\n".encode())
                else:
                    self.sock.send("$-1\r\n".encode())
            case "info":
                if req[1].lower() == "replication":
                    result = f"role:{role}\r\n"
                    result += f"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
                    result += f"master_repl_offset:0\r\n"
                    self.sock.send(f"${len(result)}\r\n{result}\r\n".encode())


def main():
    print("STARTED!")
    server_socket = socket.create_server(
        ("localhost", args.port), reuse_port=True
    )
    while True:
        client, client_addr = server_socket.accept()  # wait for client
        Connection(client, client_addr)


if __name__ == "__main__":
    parser = ArgumentParser("A Redis server written in Python")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", nargs="+", help="Replica server host and port")
    args = parser.parse_args()
    role = "master" if not args.replicaof else "slave"
    if args.replicaof and len(args.replicaof[0].split()) == 2:
        master_info = args.replicaof[0].split()
        master_host = master_info[0]
        master_port = int(master_info[1])
        master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_sock.connect((master_host, master_port))
        master_sock.send("*1\r\n$4\r\nping\r\n".encode())
        master_sock.recv(1024).decode()
        master_sock.send(
            f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{args.port}\r\n*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode()
        )
    main()
