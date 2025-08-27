import pandas as pd
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from ssl import TLSVersion


class PowerLedgerUploader:
    def __init__(self, file_path):
        self.auth_url = "https://sandbox-auth.powerledger.io/oauth/token"
        self.upload_url = "https://sandbox-readings-secure.powerledger.io/api/1.2/readings"
        self.username = "dh_meter_2@powerledger.io"
        self.password = "P@ssword123"
        self.client_id = "readings-api"
        self.file_path = file_path
        self.batch_size = 999
        self.access_token = None
        self.session = self._create_session()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _create_session(self):
        """Creates a session with TLSv1.2 enforced."""
        class SSLAdapter(HTTPAdapter):
            def __init__(self, **kwargs):
                self.ssl_context = create_urllib3_context(ssl_minimum_version=TLSVersion.TLSv1_2)
                super().__init__(**kwargs)

        session = requests.Session()
        session.mount("https://", SSLAdapter())
        return session

    def authenticate(self):
        """Authenticates and retrieves the access token."""
        auth_data = {
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
            "client_id": self.client_id,
        }

        try:
            response = self.session.post(self.auth_url, data=auth_data, verify=False)
            if response.status_code == 200:
                self.access_token = response.json().get("access_token")
                print("Successfully obtained access token:", self.access_token)
            else:
                raise Exception(f"Failed to obtain token: {response.status_code} {response.text}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Authentication request failed: {e}")

    def read_csv_file(self):
        """Reads the CSV file and returns the data as a DataFrame."""
        try:
            data_csv = pd.read_csv(self.file_path)
            print(f"File {self.file_path} read successfully.")
            return data_csv
        except Exception as e:
            raise Exception(f"Failed to read CSV file: {e}")

    def upload_batch(self, batch_data):
        """Uploads a batch of data to the API."""
        data = {"readings": batch_data}
        try:
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            }
            response = self.session.post(self.upload_url, headers=headers, json=data, verify=False)
            if response.status_code == 202:
                print("Batch uploaded successfully:", response.json())
            else:
                raise Exception(f"Batch upload failed: {response.status_code} {response.text}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Batch upload request failed: {e}")

    def process_and_upload(self):
        """Processes the CSV data and uploads it in batches."""
        data_csv = self.read_csv_file()
        all_readings = []

        # Build readings data list
        for _, row in data_csv.iterrows():
            try:
                reading = {
                    "meterId": str(row["meterUid"]),
                    "import": str(row["import"]),
                    "export": str(row["export"]),
                    "timeStamp": row["timeStamp"],
                    "isEstimate": bool(row.get("isEstimate", False)),
                    "energyUnit": row.get("energyUnit", "Wh"),
                }
                all_readings.append(reading)
            except KeyError as e:
                raise Exception(f"Missing required field: {e}")

        # Upload data in batches
        for i in range(0, len(all_readings), self.batch_size):
            batch = all_readings[i:i + self.batch_size]
            print(f"\nUploading batch {i // self.batch_size + 1}: {len(batch)} records.")
            self.upload_batch(batch)
            print("Batch upload completed.")

        print("All batches uploaded successfully.")
