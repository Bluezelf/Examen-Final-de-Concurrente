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
            full_message = ''
            while True:
                message_part = client_socket.recv(1024).decode('utf-8')
                if not message_part:
                    break
                full_message += message_part
            print(f"Mensaje completo recibido: {full_message}")
            task = json.loads(full_message)
            if task.get("type") == "register":
                self.register_worker(client_socket, addr, task)
            else:
                self.assign_task(task)
        except json.JSONDecodeError as e:
            print(f"Error de JSON al manejar la conexión de {addr}: {e}")
        except Exception as e:
            print(f"Error al manejar la conexión de {addr}: {e}")
        finally:
            client_socket.close()

    def assign_task(self, task):
        if self.workers:
            task_assigned = False
            for worker_addr, worker_socket in list(self.workers.items()):
                if worker_socket:  # Verificar que el socket está activo y no cerrado
                    try:
                        # Intentar enviar la tarea al trabajador
                        worker_socket.send(json.dumps(task).encode('utf-8'))
                        print(f"Tarea enviada a {worker_addr}")
                        task_assigned = True
                        break  # Salir del loop si la tarea se asignó correctamente
                    except Exception as e:
                        print(f"Error al enviar tarea a {worker_addr}: {e}")
                        self.attempt_reconnect(worker_addr, task)
                        break  # Salir del loop después de intentar reconectar
                else:
                    print(f"Socket no disponible para {worker_addr}")
                    # Marcar este trabajador como inactivo y eliminarlo de la lista
                    del self.workers[worker_addr]

            if not task_assigned:
                print("No hay trabajadores disponibles o conexión fallida. Añadiendo tarea a la cola.")
                self.tasks.append(task)
        else:
            print("No hay trabajadores disponibles. Añadiendo tarea a la cola.")
            self.tasks.append(task)

    def attempt_reconnect(self, worker_addr, task):
        try:
            # Cerrar el socket viejo si aún está abierto
            if worker_addr in self.workers and self.workers[worker_addr]:
                self.workers[worker_addr].close()

            # Intentar reconectar
            new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            new_socket.connect(worker_addr)
            self.workers[worker_addr] = new_socket
            print(f"Reconectado con éxito a {worker_addr}")

            # Reintentar enviar la tarea después de la reconexión
            new_socket.send(json.dumps(task).encode('utf-8'))
            print(f"Tarea reenviada con éxito a {worker_addr}")
        except Exception as e:
            print(f"No se pudo reconectar con {worker_addr}: {e}")
            # Eliminar el trabajador del diccionario si la reconexión falla
            if worker_addr in self.workers:
                del self.workers[worker_addr]

    def register_worker(self, worker_socket, addr, task):
        worker_id = (task["host"], task["port"])
        self.workers[worker_id] = worker_socket
        print(f"Trabajador registrado: {worker_id}, Total trabajadores: {len(self.workers)}")

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
