import socket  # noqa: F401
import select
from datetime import datetime, timedelta, MAXYEAR
import math
from app.redisParser import RedisParser
from app.rdbReader import RDBParser
import argparse
import threading

infinite_time = datetime(MAXYEAR - 1, 1, 1, 23, 59, 59, 999999)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Parse Redis file arguments")

    # Define arguments
    parser.add_argument(
        '--dir', 
        type=str, 
        required=False,
        help="Directory where Redis files are stored"
    )
    
    parser.add_argument(
        '--dbfilename', 
        type=str, 
        required=False,
        help="Name of the Redis database file"
    )

    parser.add_argument(
        '--port', 
        type=int, 
        required=False,
        help="Number of port"
    )

    parser.add_argument(
        '--replicaof', 
        type=str, 
        required=False,
        help="Number of port"
    )

    # Parse the arguments
    args = parser.parse_args()

    # Access the arguments
    directory = args.dir
    dbfilename = args.dbfilename
    port_number = args.port
    replicaOption = args.replicaof
    master_host = None
    master_port = None
    current_role = "master"
    replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    replication_offset = 0

    
    print(f"Directory: {directory}")
    print(f"DB Filename: {dbfilename}")

    directory = "x" if directory is None else directory
    dbfilename = "x" if dbfilename is None else dbfilename
    port_number = 6379 if port_number is None else port_number

    parser = RedisParser()



    def handle_replica(master_host, master_port):
        """
        Connects the replica to the master and sends periodic PING commands.
        """
        try:
            # Create a socket connection to the master
            master_socket = socket.create_connection((master_host, master_port))
            print(f"Connected to master at {master_host}:{master_port}")

            # Send initial PING
            master_socket.sendall(b"*1\r\n$4\r\nPING\r\n")
            response = master_socket.recv(1024)
            print(f"Response from master: {response.decode().strip()}")

            master_socket.sendall(str.encode(parser.to_resp_array(['REPLCONF', 'listening-port', str(port_number)])))
            response = master_socket.recv(1024)
            print(f"Response from master: {response.decode().strip()}")

            master_socket.sendall(str.encode(parser.to_resp_array(['REPLCONF', 'capa', 'psync2'])))
            response = master_socket.recv(1024)
            print(f"Response from master: {response.decode().strip()}")

            master_socket.sendall(str.encode(parser.to_resp_array(['PSYNC', '?', '-1'])))
            response = master_socket.recv(1024)
            print(f"Response from master: {response.decode().strip()}")


            """ # Periodically send PING commands
            while True:
                # Send PING command every 10 seconds
                threading.Event().wait(10)
                master_socket.sendall(b"*1\r\n$4\r\nPING\r\n")
                response = master_socket.recv(1024)
                print(f"Response from master: {response.decode().strip()}") """

        except Exception as e:
            print(f"Error connecting to master: {e}")



    if replicaOption is not None:
        master_host, master_port = args.replicaof.split()
        master_port = int(master_port)
        current_role = "slave"

        # Start a new thread to handle the replica connection
        replica_thread = threading.Thread(target=handle_replica, args=(master_host, master_port))
        replica_thread.daemon = True  # Daemon thread to stop when the main program exits
        replica_thread.start()

    dbReader = RDBParser(directory + '/' + dbfilename)
    dbReader.parse()

    server_socket = socket.create_server(("localhost", port_number), reuse_port=True)
    server_socket.setblocking(False)
    socket_list = [server_socket]
    clients = {}
    database = {}
    for db in dbReader.get_databases():
        print(f"Database Index: {db['index']}")
        for entry in db['hash_table']:
            key = entry.get('key')
            value = entry.get('value')
            expire = entry.get('expire')
            database[key] = [value, expire, expire]
    

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
                        expire_time = infinite_time
                        current_time = datetime.now()
                        print(current_time)
                        if len(content) == 5 and content[3].lower() == 'px':
                            expire_time = datetime.now() + timedelta(microseconds=int(content[4]))
                        print(expire_time)
                        database[content[1]] = [content[2], expire_time]
                        notified_socket.sendall(str.encode(parser.to_resp_string("OK")))
                    elif content[0].lower() == "get":
                        keyName = content[1]
                        current_time = datetime.now()
                        print(current_time)
                        print(database[keyName][1])
                        print(current_time - database[keyName][1])
                        if keyName in database.keys() and (current_time <= database[keyName][1] + timedelta(milliseconds=100)):
                            notified_socket.sendall(str.encode(parser.to_resp_string(database[keyName][0])))
                        else:
                            notified_socket.sendall(str.encode(parser.to_resp_null()))
                    elif content[0].lower() == 'config':
                        if content[2].lower() == 'dir':
                            notified_socket.sendall(str.encode(parser.to_resp_array(['dir', directory])))
                        elif content[2].lower() == 'dbfilename':
                            notified_socket.sendall(str.encode(parser.to_resp_array(['dbfilename', dbfilename])))
                    elif content[0].lower() == 'keys':
                        notified_socket.sendall(str.encode(parser.to_resp_array(database.keys())))
                    elif content[0].lower() == 'info':
                        if content[1].lower() == 'replication':
                            response = "role:" + current_role + "\n"
                            if (current_role == "master"):
                                response += "master_replid:" + replication_id + "\n"
                                response += "master_repl_offset:" + str(replication_offset) + "\n"
                            notified_socket.sendall(str.encode(parser.to_resp_string(response)))
                    elif content[0].lower() == 'replconf':
                        notified_socket.sendall(str.encode(parser.to_resp_string("OK")))
                    elif content[0].lower() == 'psync':
                        notified_socket.sendall(str.encode(parser.to_resp_simple_string("FULLRESYNC <REPL_ID> 0")))

                    

        for notified_socket in exception_sockets:
            socket_list.remove(notified_socket)
            del clients[notified_socket]
            notified_socket.close()

if __name__ == "__main__":
    main()
