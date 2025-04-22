import os
import time


def clean_old_logs(directory, days=30):
    # Get the current time
    now = time.time()
    # Calculate the cutoff time
    cutoff = now - (days * 86400)

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        # Check if the file is a log file and is older than the cutoff time
        if os.path.isfile(file_path) and filename.endswith('.log'):
            file_mtime = os.path.getmtime(file_path)
            if file_mtime < cutoff:
                print(f"Deleting {file_path}")
                os.remove(file_path)


# Specify the directory to clean
log_directory = '/path/to/log/directory'
clean_old_logs(log_directory)
