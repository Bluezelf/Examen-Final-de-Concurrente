import socket
import json
import os
import time

class Client:
    def __init__(self, leader_host, leader_port):
        self.leader_address = ('192.168.1.33', 5000)

    def start(self):
        while True:
            leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_socket.connect(self.leader_address)

            # print(f"Conectado a {self.leader_address}")
            registration_message = json.dumps({"type": "reg-client", "host": "waaa", "port": "weee"})
            # print(f"Enviando mensaje de registro al líder: {registration_message}")
            leader_socket.sendall(registration_message.encode('utf-8'))

            m = leader_socket.recv(1024).decode('utf-8')
            conf_msg = json.loads(m)
            if conf_msg.get("status"):
                # print("Found leader")
                break
            else:
                self.leader_address = tuple(conf_msg.get("leader"))

        self.command_handler(leader_socket)

    def command_handler(self, leader_socket):
        while True:
            print("===== CLIENTE =====")
            print("Escoger uno de los siguientes comandos: ")
            print("\t[1] Contar palabras.")
            print("\t[2] Encontrar palabra clave.")
            print("\t[3] Contar palabra clave.")
            command = input("==> ")

            match int(command):
                case 1:
                    file_name = self.send_file(leader_socket)
                    time.sleep(0.1)
                    msg = json.dumps({"type": "req-count_words", "name": file_name, "keyword": None})
                    # print(f"Enviando mensaje: {msg}")
                    leader_socket.sendall(msg.encode('utf-8'))
                    self.wait_response(leader_socket)

                case 2:
                    print("\tPalabra clave a buscar: ")
                    keyword = input()
                    file_name = self.send_file(leader_socket)
                    time.sleep(0.1)
                    msg = json.dumps({"type": "req-find_keyword", "name": file_name, "keyword": keyword})
                    # print(f"Enviando mensaje: {msg}")
                    leader_socket.sendall(msg.encode('utf-8'))
                    self.wait_response(leader_socket)

                case 3:
                    print("\tPalabra clave a contar: ")
                    keyword = input()
                    file_name = self.send_file(leader_socket)
                    time.sleep(0.1)
                    msg = json.dumps({"type": "req-count_keyword", "name": file_name, "keyword": keyword})
                    # print(f"Enviando mensaje: {msg}")
                    leader_socket.sendall(msg.encode('utf-8'))
                    self.wait_response(leader_socket)


            print()

    def wait_response(self, leader_socket):
        # print("waiting for response...")
        m = leader_socket.recv(1024).decode('utf-8')
        msg = json.loads(m)
        # print(msg)
        match msg.get("type"):
            case "count_words":
                print(f"Hay {msg.get("rpta")} palabras!")
            case "find_keyword":
                if msg.get("rpta"):
                    print("La palabra si se encuentra en el texto!")
                else:
                    print("La palabra no se encuentra en el texto...")

            case "count_keyword":
                print(f"La palabra aparece {msg.get("rpta")} veces!")

    def send_file(self, leader_socket):
        # print("Sending file to leader")

        files = [file for file in os.listdir() if '.txt' in file]
        print("Archivos disponibles: ")
        for i, f in enumerate(files):
            print(f"\t[{i}] {f}")
        file_idx = input("Archivo a mandar: ")
        file_name = files[int(file_idx)]

        # Aviso de que se mandara un archivo
        msg = json.dumps({"type": "send-file", "name": file_name})
        leader_socket.sendall(msg.encode('utf-8'))

        with open(file_name, 'rb') as file:
            # print("Enviando texto...")
            while chunk := file.read(1024):
                leader_socket.sendall(chunk)
        leader_socket.sendall(b'END_OF_FILE')
        # print("Texto enviado!")

        return file_name

if __name__ == "__main__":
    client = Client('127.0.0.1', 5000)
    client.start()
