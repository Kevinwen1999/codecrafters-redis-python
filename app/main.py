import socket  # noqa: F401
import select
from app.redisParser import RedisParser


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    socket_list = [server_socket]
    clients = {}
    database = {}
    parser = RedisParser()

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
                content = parser.parse(data)
                if type(content) is list:
                    print(content)
                    if content[0].lower() == "echo":
                        notified_socket.sendall(str.encode(parser.to_resp_string(content[1])))
                    elif content[0].lower() == "ping":
                        notified_socket.sendall(b"+PONG\r\n")
                    elif content[0].lower() == "set":
                        database[content[1]] = content[2]
                        notified_socket.sendall(str.encode(parser.to_resp_string("OK")))
                    elif content[0].lower() == "get":
                        keyName = content[1]
                        if keyName in database.keys():
                            notified_socket.sendall(str.encode(parser.to_resp_string(database[keyName])))
                        else:
                            notified_socket.sendall(str.encode(parser.to_resp_null()))
                    

        for notified_socket in exception_sockets:
            socket_list.remove(notified_socket)
            del clients[notified_socket]
            notified_socket.close()

if __name__ == "__main__":
    main()
