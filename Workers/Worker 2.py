import socket
import threading
import json

# Nodo Trabajador
class WorkerNode:
    def __init__(self, host, port, leader_host, leader_port):
        self.host = host
        self.port = port
        self.leader_host = leader_host
        self.leader_port = leader_port

    def start(self):
        worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        worker_socket.bind((self.host, self.port))
        worker_socket.listen(1)
        print(f"Trabajador escuchando en {self.host}:{self.port}")

        self.register_with_leader()

        while True:
            client_socket, addr = worker_socket.accept()
            print(f"Tarea recibida de {addr}")
            threading.Thread(target=self.handle_task, args=(client_socket,)).start()

    def register_with_leader(self):
        leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        leader_socket.connect((self.leader_host, self.leader_port))
        registration_message = json.dumps({"type": "register", "host": self.host, "port": self.port})
        leader_socket.sendall(registration_message.encode('utf-8'))
        leader_socket.close()

    def handle_task(self, client_socket):
        try:
            task = client_socket.recv(1024).decode('utf-8')
            task_data = json.loads(task)

            # Procesar la tarea
            result = self.process_task(task_data)

            # Enviar el resultado de vuelta al líder
            result_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result_socket.connect((self.leader_host, self.leader_port))
            result_message = json.dumps({"type": "result", "result": result})
            result_socket.sendall(result_message.encode('utf-8'))
            result_socket.close()
        except json.JSONDecodeError as e:
            print(f"Error al manejar la tarea: {e}")
        except Exception as e:
            print(f"Error al manejar la tarea: {e}")
        finally:
            client_socket.close()

    def process_task(self, task):
        # Implementar la lógica de procesamiento de tareas aquí
        if task["type"] == "word_count":
            return self.count_words(task["text"], task.get("word"))
        elif task["type"] == "keyword_search":
            return self.search_keywords(task["text"], task["keywords"])
        elif task["type"] == "repeated_keyword_search":
            return self.search_repeated_keywords(task["text"], task["keyword"], task["count"])

    def count_words(self, text, word=None):
        if word:
            return text.lower().split().count(word.lower())
        return len(text.split())

    def search_keywords(self, text, keywords):
        result = {}
        for keyword in keywords:
            result[keyword] = text.lower().split().count(keyword.lower())
        return result

    def search_repeated_keywords(self, text, keyword, count):
        words = text.lower().split()
        return words.count(keyword.lower()) >= count

if __name__ == "__main__":
    worker = WorkerNode('127.0.0.1', 5002, '127.0.0.1', 5000)
    worker.start()
