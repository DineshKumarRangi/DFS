import socket
import threading
import os
import sys

NODE_NAME = sys.argv[1] if len(sys.argv) > 1 else "unnamed_node"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9001

STORAGE_PATH = os.path.join("storage", NODE_NAME)
os.makedirs(STORAGE_PATH, exist_ok=True)

def handle_client(conn):
    try:
        header = conn.recv(1024).decode().strip()
        parts = header.split('|')
        if parts[0] == "upload":
            filename = parts[1]
            filesize = int(parts[2])
            filepath = os.path.join(STORAGE_PATH, filename)

            received = b""
            while len(received) < filesize:
                data = conn.recv(min(4096, filesize - len(received)))
                if not data:
                    break
                received += data

            with open(filepath, 'wb') as f:
                f.write(received)

            conn.send(b"OK")

        elif parts[0] == "download":
            filename = parts[1]
            filepath = os.path.join(STORAGE_PATH, filename)

            if not os.path.exists(filepath):
                conn.send(b"NOTFOUND".ljust(1024, b' '))
                return

            with open(filepath, 'rb') as f:
                content = f.read()

            conn.sendall(str(len(content)).encode().ljust(1024, b' '))
            conn.sendall(content)

    except Exception as e:
        print(f"[{NODE_NAME}] Error: {e}")
    finally:
        conn.close()

def run_node():
    sock = socket.socket()
    sock.bind(('localhost', PORT))
    sock.listen(5)
    print(f"[{NODE_NAME}] Datanode running on port {PORT}...")

    while True:
        conn, _ = sock.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

run_node()


