import socket  # noqa: F401
import select


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    socket_list = [server_socket]
    clients = {}

    while True:
        read_sockets, _, exception_sockets = select.select(socket_list, [], socket_list)

        for notified_socket in read_sockets:
            # new client socket:
            if notified_socket == server_socket:
                client_socket, client_address = server_socket.accept()
                client_socket.setblocking(False)
                socket_list.append(client_socket)
                clients[client_socket] = client_address

            else:
                data = notified_socket.recv(1024)
                if not data:
                    socket_list.remove(notified_socket)
                    del clients[notified_socket]
                    notified_socket.close()
                    continue
                notified_socket.sendall(b"+PONG\r\n")

        for notified_socket in exception_sockets:
            socket_list.remove(notified_socket)
            del clients[notified_socket]
            notified_socket.close()

if __name__ == "__main__":
    main()
