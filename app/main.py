import socket
import os
from threading import *
from time import time


class Connection(Thread):
    def __init__(self, socket, address):
        super().__init__()
        self.sock = socket
        self.addr = address
        self.data = {}
        self.expiry_time = {}
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
                self.data[req[1]] = req[2]
                if len(req) > 3 and req[3].lower() == "px":
                    additional_time = int(req[4])
                    self.expiry_time[req[1]] = additional_time + (time() * 1000)
                self.sock.send("+OK\r\n".encode())
            case "get":
                if req[1] in self.data and (
                    (
                        req[1] in self.expiry_time
                        and self.expiry_time[req[1]] >= time() * 1000
                    )
                    or req[1] not in self.expiry_time
                ):
                    self.sock.send(f"+{self.data[req[1]]}\r\n".encode())
                else:
                    self.sock.send("$-1\r\n".encode())


def main():
    print("STARTED!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        client, client_addr = server_socket.accept()  # wait for client
        Connection(client, client_addr)


if __name__ == "__main__":
    main()
