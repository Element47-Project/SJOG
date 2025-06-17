import json
import os
import tkinter as tk
from tkinter import ttk, simpledialog, messagebox
from tkinterdnd2 import TkinterDnD, DND_FILES
from uploader import save_dropped_files

DATA_DIR = "Data"
CONFIG_PATH = "config.json"
LOG_PATH = 'log.log'


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


class SJOGUploaderGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SJOG Data Uploader")
        self.root.geometry("750x650")

        self.config = load_config()
        self.selected_location = tk.StringVar()
        self.file_paths = []

        self.conn = None
        self.cursor = None
        self.table_dict = None

        self.setup_widgets()
        self.refresh_location_list()

    def setup_widgets(self):
        frame = ttk.Frame(self.root)
        frame.pack(pady=10)

        ttk.Label(frame, text="Select Location:").pack(side="left", padx=5)
        self.location_dropdown = ttk.Combobox(frame, textvariable=self.selected_location, state="readonly", width=30)
        self.location_dropdown.pack(side="left")
        self.location_dropdown.bind("<<ComboboxSelected>>", self.load_existing_files)

        ttk.Button(frame, text="＋", width=3, command=self.add_location).pack(side="left", padx=2)
        ttk.Button(frame, text="－", width=3, command=self.delete_location).pack(side="left", padx=2)

        ttk.Label(self.root, text="Location File Area (drop files here or view existing):").pack(pady=10)
        self.drop_frame = tk.Frame(self.root, bd=2, relief="groove", bg="#f0f8ff")
        self.drop_frame.pack(pady=5, padx=10, fill="both", expand=False)

        self.file_listbox = tk.Listbox(self.drop_frame, height=10, width=90)
        self.file_listbox.pack(padx=10, pady=10, fill="both", expand=True)
        self.file_listbox.drop_target_register(DND_FILES)
        self.file_listbox.dnd_bind("<<Drop>>", self.handle_drop)

        ttk.Button(self.root, text="Upload All Files", command=self.upload_files).pack(pady=10)

        self.status_text = tk.Text(self.root, height=10, width=90, state='disabled')
        self.status_text.pack(pady=10)

    def refresh_location_list(self):
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
        locations = list(self.config.get("sites", {}).keys())
        self.location_dropdown['values'] = locations
        if locations:
            default = self.config.get("default_site", locations[0])
            self.selected_location.set(default)
            # 不再默认加载文件列表
        else:
            self.file_listbox.delete(0, tk.END)

    def add_location(self):
        new_location = simpledialog.askstring("Add Location", "Enter new location name:")
        if new_location:
            if new_location in self.config.get("sites", {}):
                messagebox.showinfo("Info", "This location already exists.")
                return
            path = os.path.join(DATA_DIR, new_location)
            os.makedirs(path, exist_ok=True)
            self.config["sites"][new_location] = {
                "gas": {}, "electricity": {}, "water": {}, "waste": {}
            }
            self.selected_location.set(new_location)
            self.save_config()
            self.refresh_location_list()
            self.log_status(f"✅ Created new location: {new_location}")

    def delete_location(self):
        location = self.selected_location.get()
        if not location:
            return
        path = os.path.join(DATA_DIR, location)
        if messagebox.askyesno("Delete Location",
                               f"Are you sure you want to delete location '{location}' and all its files?"):
            try:
                for fname in os.listdir(path):
                    os.remove(os.path.join(path, fname))
                os.rmdir(path)
                self.config["sites"].pop(location, None)
                self.selected_location.set("")
                self.save_config()
                self.refresh_location_list()
                self.log_status(f"🗑️ Deleted location: {location}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete location: {e}")

    def load_existing_files(self, event=None):
        self.file_listbox.delete(0, tk.END)
        location = self.selected_location.get()
        location_path = os.path.join(DATA_DIR, location)
        if os.path.exists(location_path):
            for fname in os.listdir(location_path):
                fpath = os.path.join(location_path, fname)
                if os.path.isfile(fpath):
                    self.file_listbox.insert(tk.END, fname)

    def handle_drop(self, event):
        location = self.selected_location.get()
        if not location:
            self.log_status("⚠️ Please select a location first.")
            return

        location_path = os.path.join(DATA_DIR, location)
        os.makedirs(location_path, exist_ok=True)

        files = self.root.tk.splitlist(event.data)
        success, failed = save_dropped_files(files, location_path)
        for f in success:
            self.log_status(f"📥 Dropped and saved: {os.path.basename(f)}")
        for f, err in failed:
            self.log_status(f"❌ Failed to save {os.path.basename(f)}: {err}")
        self.load_existing_files()

    def log_status(self, message):
        self.status_text.configure(state='normal')
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.configure(state='disabled')
        self.status_text.see(tk.END)

    def init_db_connection(self):
        self.log_status("⏳ Connecting to database...")
        try:
            from SJOGUploader import connect_to_db, get_all_table_primary_keys, CONNECTION_STRING
            self.conn, self.cursor = connect_to_db(CONNECTION_STRING)
            self.table_dict, _ = get_all_table_primary_keys(self.cursor)
            self.log_status("✅ Database connection established.")
        except Exception as e:
            self.log_status(f"❌ Failed to connect to database: {e}")

    def upload_files(self):
        if self.conn is None:
            self.init_db_connection()

        location = self.selected_location.get()
        location_path = os.path.join(DATA_DIR, location)

        if not location or not os.path.exists(location_path):
            self.log_status("⚠️ Please select a valid location.")
            return

        try:
            from SJOGUploader import process_files_in_directory
            process_files_in_directory(location_path, self.cursor, self.table_dict)
            self.log_status(f"✅ Uploaded files for location: {location}")
        except Exception as e:
            self.log_status(f"❌ Upload failed: {e}")

        self.load_existing_files()

    def save_config(self):
        with open(CONFIG_PATH, "w") as f:
            json.dump(self.config, f, indent=2)


if __name__ == "__main__":
    root = TkinterDnD.Tk()
    app = SJOGUploaderGUI(root)
    root.mainloop()
