import socket
import json
import os


# Cliente
class Client:
    def __init__(self, leader_host, leader_port):
        self.leader_host = leader_host
        self.leader_port = leader_port

    def send_task(self, task):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.leader_host, self.leader_port))
        client_socket.sendall(json.dumps(task).encode('utf-8'))
        client_socket.close()

    def send_file_task(self, file_path, task_type, additional_params={}):
        if not os.path.exists(file_path):
            print(f"Error: El archivo {file_path} no existe.")
            print(f"Directorio actual de trabajo: {os.getcwd()}")
            return

        with open(file_path, 'r') as file:
            text = file.read()

        task = {
            "type": task_type,
            "text": text
        }
        task.update(additional_params)

        self.send_task(task)


if __name__ == "__main__":
    client = Client('127.0.0.1', 5000)

    # Ejemplo de tarea: Contar una palabra específica en el archivo de texto
    client.send_file_task('Mitesto.txt', 'word_count', {"word": "test"})

    # Ejemplo de tarea: Buscar palabras clave en el archivo de texto
    client.send_file_task('Mitesto.txt', 'keyword_search', {"keywords": ["example", "test"]})

    # Ejemplo de tarea: Buscar palabras clave repetidas n veces en el archivo de texto
    client.send_file_task('Mitesto.txt', 'repeated_keyword_search', {"keyword": "example", "count": 3})
