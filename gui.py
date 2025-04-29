import tkinter as tk
from tkinter import filedialog, simpledialog, ttk
import socket
import threading
import os


REPLICATION_FACTOR = 2
DATANODES = {
    "node1": 5001,
    "node2": 5002,
    "node3": 5003,
}

class DistributedFSApp:
    def __init__(self, master):
        self.master = master
        self.master.title("Distributed File System")
        self.master.geometry("500x400")

        self.upload_btn = tk.Button(master, text="Upload File", command=self.upload_file)
        self.upload_btn.pack(pady=10)

        self.download_btn = tk.Button(master, text="Download File", command=self.download_file)
        self.download_btn.pack(pady=10)

        self.progress = ttk.Progressbar(master, length=400, mode='determinate')
        self.progress.pack(pady=10)

        self.log_box = tk.Text(master, height=15, width=60)
        self.log_box.pack(pady=10)

    def log(self, message):
        self.log_box.insert(tk.END, message + "\n")
        self.log_box.see(tk.END)

    def upload_file(self):
        threading.Thread(target=self._upload_worker).start()

    def _upload_worker(self):
        file_path = filedialog.askopenfilename()
        if not file_path:
            self.log("No file selected.")
            return

        filename = os.path.basename(file_path)
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
        except Exception as e:
            self.log(f"Failed to read file: {e}")
            return

        self.progress['value'] = 0
        step = 100 / REPLICATION_FACTOR
        nodes = list(DATANODES.items())[:REPLICATION_FACTOR]

        for i, (name, port) in enumerate(nodes):
            try:
                sock = socket.socket()
                sock.connect(('localhost', port))

                header = f"upload|{filename}|{len(content)}"
                sock.sendall(header.encode().ljust(1024, b' '))
                sock.sendall(content)
                ack = sock.recv(1024)
                if ack:
                    self.log(f"[{name}] Uploaded {filename}")
            except Exception as e:
                self.log(f"[{name}] Failed to upload {filename}: {e}")
            finally:
                self.progress['value'] += step
                sock.close()

    def download_file(self):
        filename = simpledialog.askstring("File Name", "Enter the file name to download:")
        if not filename:
            self.log("No file name entered.")
            return

        save_path = filedialog.asksaveasfilename(defaultextension=".*", title="Save As")
        if not save_path:
            self.log("Download cancelled.")
            return

        threading.Thread(target=self._download_file_worker, args=(filename, save_path)).start()

    def _download_file_worker(self, filename, save_path):
        for name, port in DATANODES.items():
            try:
                sock = socket.socket()
                sock.connect(('localhost', port))
                request = f"download|{filename}"
                sock.sendall(request.encode().ljust(1024, b' '))

                header = sock.recv(1024).decode().strip()
                if header.startswith("NOTFOUND"):
                    self.log(f"[{name}] File not found.")
                    sock.close()
                    continue

                filesize = int(header)
                received = b""
                while len(received) < filesize:
                    chunk = sock.recv(min(4096, filesize - len(received)))
                    if not chunk:
                        break
                    received += chunk

                with open(save_path, 'wb') as f:
                    f.write(received)

                self.log(f"[{name}] Downloaded and saved as: {save_path}")
                break

            except Exception as e:
                self.log(f"[{name}] Download failed: {e}")
            finally:
                sock.close()

if __name__ == "__main__":
    root = tk.Tk()
    app = DistributedFSApp(root)
    root.mainloop()

