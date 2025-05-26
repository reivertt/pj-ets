from socket import *
import socket
import threading
import logging
import time
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing # Untuk mendapatkan CPU count

from file_protocol import  FileProtocol
fp = FileProtocol()

MAX_WORKERS = 50

def process_command(command_str):
    return fp.proses_string(command_str)

def handle_client_io(connection, client_addr, process_pool):
    logging.info(f"connected to {client_addr}")
    try:
        data_received = []
        start_time = time.time()
        size = 0
        while True:
            data = connection.recv(4096)
            if data:
                size += len(data)
                data_received.append(data)
                logging.debug(f"Processing size: {size}")
                if b"\r\n\r\n" in b"".join(data_received):
                    break
            else:
                break
        
        if not data_received:
            logging.warning(f"Error: Tidak ada data dari {client_addr}")
            return
        
        d = b"".join(data_received).decode().strip()
        logging.info(f"Mengirim {d[:100]}{'...' if len(d) > 100 else ''} ke ProcessPool")
        end_time = time.time()
        duration = end_time - start_time
        
        logging.warning(f"Time Processed: {duration}\nBits Processed: {size}")
        logging.warning(f"Throughput: {size / duration if duration > 0 else float('inf')}")
        
        future = process_pool.submit(process_command, d)
        hasil = future.result()
        hasil += "\r\n\r\n"
        connection.sendall(hasil.encode())
        
    except socket.timeout:
        logging.warning(f"Socket timeout dari {client_addr}")
    except ConnectionResetError:
        logging.warning(f"Koneksi dari {client_addr} direset oleh peer.")
    except Exception as e:
        logging.error(f"Error pada koneksi {client_addr}: {e}")
    finally:
        logging.info(f"Menutup koneksi I/O dari {client_addr}")
        connection.close()

class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=6667, max_workers=None, max_io_threads=MAX_WORKERS):
        super().__init__()
        self.ipinfo=(ipaddress,port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.my_socket.settimeout(1.0)
        self.running = False

        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
        
        self.process_pool = ProcessPoolExecutor(max_workers=max_workers)
        logging.info(f"ProcessPoolExecutor dimulai dengan max_workers={max_workers}")

        self.io_executor = ThreadPoolExecutor(max_workers=max_io_threads)
        logging.info(f"ThreadPoolExecutor (untuk I/O) dimulai dengan max_workers={max_io_threads}")

    def run(self):
        try:
            logging.warning(f"server berjalan di ip address {self.ipinfo}")
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(10)
            self.running = True

            while self.running:
                try:
                    connection, client_addr = self.my_socket.accept()
                    
                    self.io_executor.submit(handle_client_io, connection, client_addr, self.process_pool)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        logging.error(f"Error: {e}")
                    break

        except Exception as e:
            logging.critical(f"Server run failed to start or bind/listen: {e}", exc_info=True)
            self.running = False
        finally:
            logging.warning("Mematikan I/O thread pool executor...")
            if hasattr(self, 'io_executor') and self.io_executor:
                self.io_executor.shutdown(wait=True)
            logging.warning("I/O thread pool executor dimatikan.")

            logging.warning("Mematikan process pool executor...")
            if hasattr(self, 'process_pool') and self.process_pool:
                self.process_pool.shutdown(wait=True)
            logging.warning("Process pool executor dimatikan.")


    def stop(self):
        logging.warning("Stopping")
        self.running = False


def main():


    svr = Server(ipaddress='0.0.0.0', port=6667)
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
    # print(multiprocessing.cpu_count())
    main()

