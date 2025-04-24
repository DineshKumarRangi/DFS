import time, threading, socket
from config import DATANODES, REPLICATION_FACTOR

node_status = {node: True for node in DATANODES}
file_table = {}  # filename: [node1, node2]

def listen_heartbeats():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('localhost', 6000))
    while True:
        msg, addr = sock.recvfrom(1024)
        node = msg.decode()
        node_status[node] = True

def monitor_nodes():
    while True:
        time.sleep(5)
        for node in node_status:
            if not node_status[node]:
                print(f"[ALERT] {node} seems down!")
                # Trigger replication here (not shown)
            node_status[node] = False

if __name__ == "__main__":
    threading.Thread(target=listen_heartbeats).start()
    threading.Thread(target=monitor_nodes).start()
    print("Master is running...")
