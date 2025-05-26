import socket
import json
import base64
import logging
import os
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
SERVER_HOST = '172.16.16.101'
SERVER_PORT = 6667
NUM_CLIENT_WORKERS = 1
BASE_DUMMY_FILENAME = "donalbebek" # test
# BASE_DUMMY_FILENAME = "Deep_Learning_Notes_-_COSE474(03)" # 5mb
# BASE_DUMMY_FILENAME = "IFDPInfrastructure" # 10mb
# BASE_DUMMY_FILENAME = "preprocessed_df_assignment" # 20mb
EXTENSION = ".jpg" # csv or pdf
DOWNLOAD_DIR = "downloads/"
LOG_LEVEL = logging.INFO
ACTION = "GET" # POST or GET

stats = {
    "connections_attempted": 0,
    "connections_successful": 0,
    "connections_failed": 0,
    "ops_attempted": 0,
    "ops_successful": 0,
    "ops_failed": 0,
    "post_ok": 0,
    "get_ok": 0,
    "bits_processed": 0,
    "errors": []
}
stats_lock = threading.Lock()

def send_command_to_server(command_str, server_address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(None) 
    connection_success = False
    try:
        with stats_lock:
            stats["connections_attempted"] += 1
        sock.connect(server_address)
        connection_success = True
        with stats_lock:
            stats["connections_successful"] += 1
        
        command_str += "\r\n\r\n"
        sock.sendall(command_str.encode())
        
        data_received_parts = []
        current_response_size = 0
        while True:
            try:
                part = sock.recv(4096)
                if not part: break
                data_received_parts.append(part)
                current_response_size += len(part)
                logging.debug(f"Processing chunks size: {current_response_size}")
                if b"\r\n\r\n" in b"".join(data_received_parts): break
                
            except socket.timeout:
                logging.debug("Timeout waiting for server data part.")
                if not data_received_parts: return False, "Timeout with no data"
                break 
        
        if not data_received_parts:
            return False, "No data from server"

        if ACTION == "GET":
            stats['bits_processed'] = current_response_size
        elif ACTION == "POST":
            stats['bits_processed'] = len(command_str)     

        data_received = b"".join(data_received_parts)
        data_received_str = data_received.decode('utf-8', errors='ignore')
        clean_data_str = data_received_str.split("\r\n\r\n", 1)[0]
        
        hasil = json.loads(clean_data_str)
        return True, hasil
    except socket.timeout:
        logging.debug(f"Overall timeout connecting/communicating with {server_address}.")
        if not connection_success:
            with stats_lock:
                stats["connections_failed"] += 1
        return False, "Overall socket timeout"
    except ConnectionRefusedError:
        logging.debug(f"Connection refused by {server_address}.")
        with stats_lock:
            stats["connections_failed"] += 1
        return False, "Connection refused"
    except json.JSONDecodeError as e:
        logging.debug(f"JSON Decode Error: {e}. Raw: '{clean_data_str[:100]}'")
        return False, f"JSON Decode Error: {e}"
    except Exception as e:
        logging.debug(f"Generic error in send_command: {e}")
        if not connection_success: # If connect() didn't happen or failed before this.
             with stats_lock:
                stats["connections_failed"] += 1 # Count as connection fail if very early
        return False, str(e)
    finally:
        sock.close()

def perform_operation(worker_id, action, server_address):   
    filename_for_ops = f"{BASE_DUMMY_FILENAME}_w{worker_id}_{int(time.time_ns())}{EXTENSION}"
    basefile = f"{BASE_DUMMY_FILENAME}{EXTENSION}"

    op_success = False
    command_str = ""
    op_type = action.lower()

    with stats_lock:
        stats["ops_attempted"] += 1

    if action == "POST":
        current_post_filename = filename_for_ops
        fp = open(f"files/{basefile}", 'rb')
        encoded_content = base64.b64encode(fp.read()).decode()
        fp.close()        
        command_str = f"POST {current_post_filename} {encoded_content}"
        op_type = f"post_{current_post_filename}" # More specific log
    elif action == "GET":
        command_str = f"GET {basefile}"

    logging.debug(f"Worker {worker_id}: Action {action}, Cmd: {command_str[:70]}...")
    status, response = send_command_to_server(command_str, server_address)

    if status and response.get('status') == 'OK':
        op_success = True
        with stats_lock:
            stats["ops_successful"] += 1
            if action == "POST": stats["post_ok"] += 1
            elif action == "GET":
                stats["get_ok"] += 1
                if DOWNLOAD_DIR and response.get('data_namafile') and response.get('data_file') is not None:
                    try:
                        if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
                        dl_filename = os.path.join(DOWNLOAD_DIR, f"w{worker_id}_{response['data_namafile']}_{int(time.time_ns())}")
                        file_bytes = base64.b64decode(response['data_file'])
                        with open(dl_filename, 'wb') as f: f.write(file_bytes) # Disable for pure speed test
                    except Exception as e_dl:
                        logging.warning(f"Worker {worker_id}: Failed to save downloaded file: {e_dl}")

        logging.debug(f"Worker {worker_id} ({op_type}): SUCCESS")
    else:
        with stats_lock:
            stats["ops_failed"] += 1
            error_detail = f"W{worker_id} ({op_type}): FAILED. Server Response: {response}"
            stats["errors"].append(error_detail)
        logging.info(error_detail) 
    
    return op_success

def client_worker_task(worker_id, server_address):
    """Task for a single client worker thread."""
    logging.info(f"Worker {worker_id}: Starting {ACTION} to {server_address}")
    success = perform_operation(worker_id, ACTION, server_address)

    if not success:
        logging.warning(f"Worker {worker_id}: Finished, {ACTION} operation FAILED.")
    else:
        logging.info(f"Worker {worker_id}: Finished, {ACTION} operation SUCCEEDED.")
    return success

def main_stress_test():
    logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    server_address = (SERVER_HOST, SERVER_PORT)
    
    if DOWNLOAD_DIR and not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logging.info(f"Created download directory: {DOWNLOAD_DIR}")

    logging.info(f"Starting stress test: {NUM_CLIENT_WORKERS} workers "
                 f"against server {server_address}")
    
    start_time = time.time()
    
    total_successes = 0
    
    with ThreadPoolExecutor(max_workers=NUM_CLIENT_WORKERS) as executor:
        futures = [executor.submit(client_worker_task, i, server_address) 
                   for i in range(NUM_CLIENT_WORKERS)]
        
        for future in as_completed(futures):
            try:
                total_successes += future.result()
            except Exception as e:
                logging.error(f"Error in worker future: {e}")

    end_time = time.time()
    duration = end_time - start_time
    
    throughput = stats['bits_processed'] / duration if duration > 0 else float('inf')

    logging.info("--- Stress Test Summary ---")
    logging.info(f"Total duration: {duration:.2f} seconds")
    logging.info(f"Connections: {stats['connections_successful']} successful / {stats['connections_attempted']} attempted / {stats['connections_failed']} failed")
    logging.info(f"Operations: {stats['ops_successful']} successful / {stats['ops_attempted']} attempted / {stats['ops_failed']} failed")
    logging.info(f"Bits processed: {stats['bits_processed']:.2f}")
    logging.info(f"Throughput (b/s): {throughput:.2f}")
    logging.info(f"  POST OK: {stats['post_ok']}")
    logging.info(f"  GET OK: {stats['get_ok']}")

    if stats["errors"]:
        logging.warning(f"\n--- Top {min(10, len(stats['errors']))} Errors ---")
        for i, err in enumerate(stats["errors"][:10]):
            logging.warning(f"{i+1}. {err}")
        if len(stats["errors"]) > 10:
            logging.warning(f"...and {len(stats['errors']) - 10} more errors.")
    else:
        logging.info("No operational errors recorded in stats (debug logs may have more).")

if __name__ == '__main__':
    main_stress_test()