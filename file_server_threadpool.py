from socket import *
import socket
import threading
import logging
import time
import sys
from concurrent.futures import ThreadPoolExecutor

from file_protocol import  FileProtocol
fp = FileProtocol()

MAX_WORKERS = 50

def handle_client_connection(connection, client_addr):
    logging.warning(f"connected to {client_addr}")
    try:
        while True:
            start_time = time.time()
            data_received = []
            size = 0
            while True:
                data = connection.recv(4096)
                if data:
                    size += len(data)
                    data_received.append(data)
                    logging.debug(f"Processing size: {size}")
                    if b"\r\n\r\n" in b"".join(data_received):
                        break
            if data_received:
                end_time = time.time()
                duration = end_time - start_time
                data_received_total = b"".join(data_received).decode()
                d = data_received_total.strip()
                logging.warning(f"Time Processed: {duration}\nBits Processed: {size}")
                logging.warning(f"Throughput: {size / duration if duration > 0 else float('inf')}")
                hasil = fp.proses_string(d)
                hasil += "\r\n\r\n"
                connection.sendall(hasil.encode())
            else:
                break
        connection.close()

    except Exception as e:
        print()

class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=6667, max_workers=MAX_WORKERS):
        super().__init__
        self.ipinfo=(ipaddress,port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.my_socket.settimeout(1.0)
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        threading.Thread.__init__(self)

    def run(self):
        try:
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
        except Exception as e:
            logging.critical(f"Server run failed to start or bind/listen: {e}", exc_info=True)
            self.running = False # Ensure it's set to false if startup failed
        finally:
            logging.warning("Server_run loop selesai. Memulai shutdown...")
            
            logging.warning("Mematikan thread pool executor...")
            self.executor.shutdown(wait=True) # Wait for all current tasks to complete
            logging.warning("Thread pool executor dimatikan.")
            
            if self.my_socket:
                logging.warning("Menutup socket server...")
                self.my_socket.close()
                logging.warning("Socket server ditutup.")
            logging.warning("Server thread selesai.")


    def stop(self):
        logging.warning("Stopping")
        self.running = False


def main():
    svr = Server(ipaddress='0.0.0.0',port=6667)
    svr.start()

    try:
        while svr.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.warning("Keyboard interrupt diterima! Mengirim sinyal stop ke server...")
    finally:
        if svr.is_alive():
            svr.stop()  
            svr.join() 
        logging.warning("Program server utama selesai.")



if __name__ == "__main__":
    main()

