import socket 
from time import sleep  
from config import DATANODES as all_nodes, REPLICATION_FACTOR as copies  


def send_to_node(port, msg, retries=1):
    for attempt in range(retries + 1):
        try:
            s = socket.socket()  
            s.settimeout(1.5)
            s.connect(('127.0.0.1', port))
            s.send(msg.encode('ascii'))  
            return s
        except:
            if attempt == retries:
                raise
            sleep(0.3 * (attempt + 1))  
    return None


def upload_file(file_name, data):
    stored = 0
    nodes_used = []
    
   
    target_nodes = list(all_nodes.items())[:copies]  
    
    for name, port in target_nodes:
        msg = f"save|{file_name}|{data}"  
        try:
            sock = send_to_node(port, msg, retries=2)
            if sock:
                print(f"[+] {name} accepted {file_name}")
                stored += 1
                nodes_used.append(name)
                sock.close()  
        except Exception as e:
            print(f"[-] {name} failed: {str(e)[:50]}...")  
    
    if stored < copies:
        print(f"Warning: Only {stored}/{copies} copies saved!")


def download_file(file_name):
   
    for name, port in all_nodes.items():
        try:
            sock = send_to_node(port, f"get|{file_name}")
            reply = sock.recv(1024 * 10).decode()  
            sock.close()
            
            if reply == "NOT_FOUND":
                continue
            else:
                print(f"Got {file_name} from {name}")
                return reply
        except:
            pass  
    
    print("File not found anywhere. Sad!")
    return None


if __name__ == "__main__":
    
    upload_file("todo.txt", "Buy milk")
    download_file("todo.txt")  
