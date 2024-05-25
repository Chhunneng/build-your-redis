import socket


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept() # wait for client
    client, _ = server_socket.accept()
    # client.send(b"+PONG\r\n")
    while True:
        request = client.recv(512)
        data = request.decode()
        if "ping" in data.lower():
            client.send("+PONG\r\n".encode())


if __name__ == "__main__":
    main()
