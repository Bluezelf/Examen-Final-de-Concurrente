import socket
import threading
import json

# Nodo Líder
class LeaderNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.workers = {}  # Diccionario para guardar los nodos trabajadores
        self.tasks = []  # Cola de tareas pendientes

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Líder escuchando en {self.host}:{self.port}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Conexión recibida de {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket, addr)).start()

    def handle_client(self, client_socket, addr):
        try:
            message = client_socket.recv(1024).decode('utf-8')
            task = json.loads(message)
            if task.get("type") == "register":
                self.register_worker(client_socket, addr, task)
            else:
                self.assign_task(task)
        except json.JSONDecodeError as e:
            print(f"Error al manejar la conexión de {addr}: {e}")
        except Exception as e:
            print(f"Error al manejar la conexión de {addr}: {e}")
        finally:
            client_socket.close()

    def assign_task(self, task):
        if self.workers:
            # Asignar tarea al primer trabajador disponible
            worker_addr, worker_socket = next(iter(self.workers.items()))
            worker_socket.send(json.dumps(task).encode('utf-8'))
        else:
            print("No hay trabajadores disponibles. Añadiendo tarea a la cola.")
            self.tasks.append(task)

    def register_worker(self, worker_socket, addr, task):
        worker_id = (task["host"], task["port"])
        self.workers[worker_id] = worker_socket
        print(f"Trabajador registrado: {worker_id}")

    def handle_worker_failure(self, worker_addr):
        if worker_addr in self.workers:
            del self.workers[worker_addr]
            print(f"Trabajador {worker_addr} eliminado debido a fallo.")
            # Reasignar tareas pendientes
            if self.tasks:
                for task in self.tasks:
                    self.assign_task(task)
                self.tasks = []

if __name__ == "__main__":
    leader = LeaderNode('127.0.0.1', 5000)
    leader.start()
