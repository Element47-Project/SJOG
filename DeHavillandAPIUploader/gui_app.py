import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime, timedelta
import threading
import logging
import os
import subprocess
from dotenv import load_dotenv
from db_connector import AzureConnector
from processor import DataProcessor, clean_data, DataFilling
import sys


def resource_path(relative_path):
    """兼容 PyInstaller 打包后的路径"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


# Load environment variables
load_dotenv(resource_path("config.env"))
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("PASSWORD")

log_file = "process_log.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=log_file, filemode='w')


class DeHavilandApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DeHavilland Data Processing Tool")
        self.root.state('zoomed')
        self.latest_csv_file = resource_path(
            os.path.join("assets", f"DHdata_processed_{datetime.now().strftime('%Y%m%d')}.csv"))

        main_frame = tk.Frame(root)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        control_frame = tk.Frame(main_frame)
        control_frame.pack(fill='x', pady=5)

        tk.Label(control_frame, text="Start Date (YYYY-MM-DD):").grid(row=0, column=0, padx=5)
        self.start_entry = tk.Entry(control_frame, width=12)
        self.start_entry.insert(0, (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
        self.start_entry.grid(row=0, column=1, padx=5)

        tk.Label(control_frame, text="End Date (YYYY-MM-DD):").grid(row=0, column=2, padx=5)
        self.end_entry = tk.Entry(control_frame, width=12)
        self.end_entry.insert(0, datetime.now().strftime('%Y-%m-%d'))
        self.end_entry.grid(row=0, column=3, padx=5)

        tk.Button(control_frame, text="Run Processing", command=self.run_process_thread).grid(row=0, column=4, padx=5)
        tk.Button(control_frame, text="Open CSV", command=self.open_csv_file).grid(row=0, column=5, padx=5)
        tk.Button(control_frame, text="Upload File", command=self.upload_latest_file).grid(row=0, column=6, padx=5)

        self.log_text = tk.Text(main_frame, height=12, bg="#f8f8f8")
        self.log_text.pack(fill='both', expand=True)

        class TextRedirector:
            def __init__(self, text_widget):
                self.text_widget = text_widget

            def write(self, msg):
                self.text_widget.insert(tk.END, msg)
                self.text_widget.see(tk.END)

            def flush(self):
                pass

        sys.stdout = TextRedirector(self.log_text)
        sys.stderr = TextRedirector(self.log_text)

    def run_process_thread(self):
        threading.Thread(target=self.run_process).start()

    def run_process(self):
        try:
            start_time = self.start_entry.get() + ' 00:00:00'
            end_time = self.end_entry.get() + ' 00:00:00'

            print("Connecting to Azure SQL database...")
            connector = AzureConnector()
            raw_data = connector.fetch_data(start_time, end_time)

            if raw_data.empty:
                print("No data fetched from the database.")
                return

            print("Formatting raw data...")
            raw_data.drop_duplicates(subset=['DateTime', 'Meter'], inplace=True)

            processor = DataProcessor({
                'Grid meter': '142812', '101': '142813', '102': '142814', '103': '142815',
                '201': '142816', '202': '142825', '203': '142817', '301': '142818', '302': '142819', '303': '142820',
                'Commercial': '142821', 'Common Area Lights': '142822', 'Common Area': '142823',
                'SOLAR AND BATTERY DB': '142824'
            })
            formatted = processor.format_data(raw_data)
            cleaned = clean_data(formatted)

            print("Interpolating missing intervals...")
            filler = DataFilling(cleaned)
            filler.identify_gaps()
            final = filler.clean_data()

            if 'import_diff' in final.columns:
                final.drop(columns=['import_diff', 'export_diff', 'time_diff'], inplace=True, errors='ignore')

            final.to_csv(self.latest_csv_file, index=False)
            print(f"Data saved to file: {self.latest_csv_file}")

        except Exception as e:
            messagebox.showerror("Error", str(e))

    def open_csv_file(self):
        if self.latest_csv_file and os.path.exists(self.latest_csv_file):
            subprocess.Popen(['start', '', self.latest_csv_file], shell=True)
        else:
            messagebox.showwarning("Warning", "No CSV file to open.")

    def upload_latest_file(self):
        # 打开文件选择器，默认路径是 assets 文件夹
        file_path = filedialog.askopenfilename(
            title="Select CSV File to Upload",
            initialdir=os.path.abspath("assets"),
            filetypes=[("CSV files", "*.csv")]
        )

        if not file_path:
            return  # 用户取消选择

        from uploader import upload_and_notify  # 放在函数内部导入，避免循环引用问题

        try:
            start_time = self.start_entry.get()
            end_time = self.end_entry.get()
            upload_and_notify(file_path, start_time, end_time)
            messagebox.showinfo("Upload Complete", f"File uploaded successfully: {os.path.basename(file_path)}")
        except Exception as e:
            messagebox.showerror("Upload Error", str(e))


if __name__ == '__main__':
    root = tk.Tk()
    app = DeHavilandApp(root)
    root.mainloop()
