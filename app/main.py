import socket
import threading

def connect(connection: socket.socket) -> None:
    with connection:
        connected: bool = True
        while connected:
            command: str = connection.recv(1024).decode()
            connected = bool(command)
            if "ping" in command.lower():
                response = "+PONG\r\n"
            connection.sendall(response.encode())


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, address = server_socket.accept()
        print(f"accepted connection - {address}")
        thread: threading.Thread = threading.Thread(target=connect, args=[connection])
        thread.start()

if __name__ == "__main__":
    main()
