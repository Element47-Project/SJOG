import os
import sys
import pandas as pd
from log_parser import parse_nginx_log
from report_generator import generate_html_report

def main(log_dir, output_file):
    # Collect all log files in the specified directory
    log_files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith('.log')]

    # Parse logs and collect error entries
    error_entries = []
    for log_file in log_files:
        error_entries.extend(parse_nginx_log(log_file))

    # Create a DataFrame from the error entries
    df = pd.DataFrame([entry.split() for entry in error_entries], columns=[
        'ip', 'identity', 'user', 'timestamp', 'request', 'status_code', 'size', 'referer', 'user_agent'
    ])
    df['status_code'] = df['status_code'].astype(int)

    # Generate HTML report
    html_report = generate_html_report(df)

    # Save the report to the output file
    with open(output_file, 'w') as file:
        file.write(html_report)

if __name__ == "__main__":
    log_directory = sys.argv[1]
    output_file = sys.argv[2]
    main(log_directory, output_file)