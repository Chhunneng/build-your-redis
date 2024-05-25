import socket
import threading

def handle_connection(conn, addr):
    while True:
        request = conn.recv(512)
        if not request:
            break
        data = request.decode()
        if "ping" in data.lower():
            response = "+PONG\r\n"
            conn.send(response.encode())
    # conn.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Received a connection from {client_address}")
        handle_connection(client_socket, client_address)
        threading.Thread(
            target=handle_connection, args=[client_socket, client_address]
        ).start()


if __name__ == "__main__":
    main()
