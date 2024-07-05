import socket
import threading
import json
import re
import time
import os

chunk_size = 300
class Node:
    def __init__(self, host, port):
        self.address = ('192.168.1.33', 5004)
        self.peers = [('192.168.1.33', 5000), ('192.168.1.33', 5002), ('192.168.1.33', 5003), ('192.168.1.33', 5001)]
        self.leader_addr = None
        self.role = 'worker'
        self.votes = list()
        self.voted = False
        self.time_delta = 2
        self.start_time = None
        self.worker_connections = list()
        self.client_connections = list()
        self.pending_tasks = list()  # Cola de tareas pendientes
        self.completed_tasks = list()  # Cola de tareas completadas

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.address)
        server_socket.listen()
        # print(f"Líder escuchando en {self.address}")

        threading.Thread(target=self.handle_peer_connections).start()
        threading.Thread(target=self.handle_in_connections, args=(server_socket,)).start()

    def handle_peer_connections(self):
        peers = self.peers.copy()
        peer_sockets = list()
        num_connections = len(peers)
        while num_connections > 0:
            for peer in peers:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    peer_socket.connect(peer)
                    peer_sockets.append(peer_socket)
                    peers.remove(peer)
                    num_connections -= 1

                    # print(f"Conectado a {peer}")
                    registration_message = json.dumps({"type": "reg-worker", "cpus": os.cpu_count()})
                    peer_socket.sendall(registration_message.encode('utf-8'))
                except ConnectionRefusedError:
                    continue

        for peer in peer_sockets:
            threading.Thread(target=self.json_handler, args=(peer,)).start()

        threading.Thread(target=self.heartbeat, args=(peer_sockets,)).start()

    def heartbeat(self, peers):
        self.start_time = time.time()
        while True:
            if self.role == 'leader':
                while True:
                    msg = json.dumps({"type": "heartbeat", "leader": self.address})
                    for peer in peers:
                        peer.sendall(msg.encode('utf-8'))
                    time.sleep(0.1)

            elif self.role == 'worker':
                while time.time() - self.start_time < self.time_delta:
                    time.sleep(0.1)
                    continue
                self.role = 'candidate'
                msg = json.dumps({"type": "req-vote"})
                for peer in peers:
                    try:
                        peer.sendall(msg.encode('utf-8'))
                    except OSError:
                        peers.remove(peer)
                        continue
                time.sleep(0.1)
            elif self.role == 'candidate':
                num_votes = len(self.votes)
                if num_votes > len(self.peers) // 2:
                    self.role = 'leader'
                    # print("IM LEADER NOW")
                    self.votes = list()

    def handle_in_connections(self, server_socket):
        while True:
            conn_socket, addr = server_socket.accept()
            threading.Thread(target=self.json_handler, args=(conn_socket,)).start()

    def json_handler(self, conn_socket):
        socket_alive = True
        while socket_alive:
            # print("Esperando mensaje...\n")
            msg = ''
            while True:
                try:
                    msg_part = conn_socket.recv(1024).decode('utf-8')
                    msg += msg_part
                except ConnectionResetError:
                    # print("Desconectado...\n")
                    conn_socket.close()
                    socket_alive = False
                    break
                if not msg:
                    # print("Desconectado...\n")
                    conn_socket.close()
                    socket_alive = False
                    break
                try:
                    json_msg = json.loads(msg.strip())
                    # print(json_msg)
                    # print()
                    match json_msg.get("type"):
                        case "heartbeat":
                            self.start_time = time.time()
                            if self.role != 'worker':
                                self.role = 'worker'
                            self.leader_addr = json_msg.get("leader")
                            self.voted = False
                            print(json_msg)
                        case "req-vote":
                            if not self.voted:
                                self.voted = True
                                msg = json.dumps({"type": "re-vote"})
                                conn_socket.sendall(msg.encode('utf-8'))
                        case "re-vote":
                            if self.role == 'candidate':
                                self.votes.append(json_msg)
                        case "reg-worker":
                            self.register_worker(conn_socket, json_msg)
                        case "reg-client":
                            self.register_client(conn_socket, json_msg)
                        case "send-file":
                            self.receive_file(conn_socket, json_msg)
                        case "req-count_words":
                            self.delegate(conn_socket, json_msg, "count_words")
                        case "req-find_keyword":
                            self.delegate(conn_socket, json_msg, "find_keyword")
                        case "req-count_keyword":
                            self.delegate(conn_socket, json_msg, "count_keyword")
                        case "count_words":
                            self.count_words(conn_socket, json_msg)
                        case "find_keyword":
                            self.find_keyword(conn_socket, json_msg)
                        case "count_keyword":
                            self.count_keyword(conn_socket, json_msg)
                        case "re-count_words":
                            self.receive_task(conn_socket, json_msg)
                        case "re-find_keyword":
                            self.receive_task(conn_socket, json_msg)
                        case "re-count_keyword":
                            self.receive_task(conn_socket, json_msg)

                except ValueError:
                    # print("Falta completar el mensaje...")
                    continue
                break
    def register_client(self, client_socket, msg):
        if self.role == 'leader':
            msg = json.dumps({"type": "reg-client-confirmation", "status": True, "leader": self.address})
            client_socket.sendall(msg.encode('utf-8'))

            self.client_connections.append(client_socket)
            # print(f"Clientes: {self.client_connections}")
            # print(f"Num. Clientes: {len(self.client_connections)}")
        else:
            msg = json.dumps({"type": "reg-client-confirmation", "status": False, "leader": self.leader_addr})
            client_socket.sendall(msg.encode('utf-8'))

    def register_worker(self, worker_socket, msg):
        cpus = msg.get("cpus")
        self.worker_connections.append((worker_socket, cpus))
        # print(f"Trabajadores: {self.worker_connections}")
        # print(f"Num. trabajdores: {len(self.worker_connections)}")

    def receive_file(self, server_socket, msg):
        file_path = f"./{msg.get("name")}"
        with open(file_path, 'wb') as f:
            # print("Recibiendo texto...")
            while True:
                chunk = server_socket.recv(1024)
                # print(chunk.decode('utf-8'))
                if b'END_OF_FILE' in chunk:
                    f.write(chunk[:-len(b'END_OF_FILE')])
                    break
                f.write(chunk)
        # print("Texto recibido!")

    def delegate(self, server_socket, msg, command):
        # print("Delegandooo")
        file_name = msg.get("name")
        keyword = msg.get("keyword")

        num_tasks = 0

        with open(file_name, 'rb') as file:
            if len(self.worker_connections) > 0:
                while True:
                    empty_chunk = False
                    for worker in self.worker_connections:
                        # print(f"Trying with worker {worker} from {self.worker_connections}")
                        try:
                            str(worker[0].getpeername())
                        except OSError:
                            # print("Found missing socket, removing...")
                            self.worker_connections.remove(worker)
                            # print("Removed it!")
                            continue

                        # print("Reading fileeee")
                        file_chunk = file.read(chunk_size * worker[1]).decode('utf-8')
                        if file_chunk:
                            msg = json.dumps({"type": command, "text": file_chunk, "keyword": keyword,
                                              "client": str(server_socket.getpeername())})
                            task = (str(server_socket.getpeername()), str(worker[0].getpeername()), msg)
                            self.pending_tasks.append(task)
                            num_tasks += 1
                            worker[0].sendall(msg.encode('utf-8'))
                            time.sleep(0.1)
                        else:
                            empty_chunk = True
                            break

                    if empty_chunk:
                        break

        # print("Finalizado con el envio del delego")
        # print("Esperando resultados...")
        # print(f"Numero de tasks = {num_tasks}")
        # print(f"PENDING TASKS = {self.pending_tasks}")
        result = 0
        while num_tasks > 0:
            for compl_task in self.completed_tasks:
                if compl_task[0] == str(server_socket.getpeername()):

                    if type(compl_task[2]) is bool:
                        result = bool(result) or compl_task[2]
                        num_tasks -= 1
                    elif type(compl_task[2]) is int:
                        result += compl_task[2]
                        num_tasks -= 1
                    self.completed_tasks.remove(compl_task)

        msg = json.dumps({"type": command, "rpta": result})
        server_socket.sendall(msg.encode('utf-8'))

    def receive_task(self, server_socket, msg):
        # print("Receiveing task result")
        client = msg.get("client")
        worker = msg.get("worker")
        rpta = None

        match msg.get("type"):
            case "re-count_words":
                rpta = msg.get("count")
            case "re-find_keyword":
                rpta = msg.get("found")
            case "re-count_keyword":
                rpta = msg.get("count")

        self.completed_tasks.append((client, worker, rpta))
        # print(f"Completed tasks: {self.completed_tasks}")
        return rpta

    def count_words(self, server_socket, msg):
        text = msg.get("text")
        client = msg.get("client")
        words = re.findall(r'\b\w+\b', text)
        count = len(words)

        response = json.dumps({"type": "re-count_words", "count": count, "text": text, "client": client,
                               "worker": str(server_socket.getsockname())})
        server_socket.sendall(response.encode('utf-8'))

    def find_keyword(self, server_socket, msg):
        keyword = msg.get("keyword")
        text = msg.get("text")
        client = msg.get("client")
        found = keyword in text

        response = json.dumps(
            {"type": "re-find_keyword", "keyword": keyword, "found": found, "text": text, "client": client,
             "worker": str(server_socket.getsockname())})
        server_socket.sendall(response.encode('utf-8'))

    def count_keyword(self, server_socket, msg):
        keyword = msg.get("keyword")
        text = msg.get("text")
        client = msg.get("client")
        words = re.findall(r'\b\w+\b', text)
        count = 0

        for word in words:
            if word == keyword:
                count += 1

        response = json.dumps(
            {"type": "re-count_keyword", "keyword": keyword, "count": count, "text": text, "client": client,
             "worker": str(server_socket.getsockname())})
        server_socket.sendall(response.encode('utf-8'))

if __name__ == "__main__":
    leader = Node('127.0.0.1', 5000)
    leader.start()