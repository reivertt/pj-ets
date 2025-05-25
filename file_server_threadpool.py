from socket import *
import socket
import threading
import logging
import time
import sys
from concurrent.futures import ThreadPoolExecutor

from file_protocol import  FileProtocol
fp = FileProtocol()

def handle_client_connection(connection, client_addr):
    logging.warning("")
    try:
        while True:
            data_received = ""
            while True:
                data = connection.recv(1024)
                if data:
                    data_received += data.decode()
                    if "\r\n\r\n" in data_received:
                        break
            
            if data_received:
                d = data_received.strip()
                logging.info(f"Menerima dari {client_addr}: {d}")
                hasil = fp.proses_string(d)
                hasil += "\r\n\r\n"
                connection.sendall(hasil.encode())
            else:
                break
        connection.close()

    except Exception as e:
        print()

class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=6667, max_workers=5):
        super().__init__
        self.ipinfo=(ipaddress,port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.my_socket.settimeout(1.0)
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        threading.Thread.__init__(self)

    def run(self):
        logging.warning(f"server berjalan di ip address {self.ipinfo}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        self.running = True

        while self.running:
            try:
                connection, client_addr = self.my_socket.accept()
                self.executor.submit(handle_client_connection, connection, client_addr)
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logging.error(f"Error: {e}")
                break
                
            logging.warning("Server mematikan thread pool")
            self.executor.shutdown(wait=True)
            logging.warning("Thread pool dimatikan")
            self.my_socket.close()
            logging.warning("Socket server ditutup")

    def stop(self):
        logging.warning("Stopping")
        self.running = False


def main():
    svr = Server(ipaddress='0.0.0.0',port=6667)
    svr.start()


if __name__ == "__main__":
    main()

