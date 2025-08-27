import pandas as pd
import matplotlib.pyplot as plt

def generate_html_report(df):
    # Generate status code statistics
    status_counts = df['status_code'].value_counts()

    # Plot the status code statistics
    plt.figure(figsize=(10, 6))
    status_counts.plot(kind='bar')
    plt.title('Status Code Statistics')
    plt.xlabel('Status Code')
    plt.ylabel('Count')
    plt.savefig('status_code_stats.png')
    plt.close()

    # Convert DataFrame to HTML
    html_table = df.to_html()

    # Create HTML report
    html_report = f"""
    <html>
    <head>
        <title>DataFrame Report</title>
    </head>
    <body>
        <h1>Status Code Statistics</h1>
        <img src="status_code_stats.png" alt="Status Code Statistics">
        <h2>Data</h2>
        {html_table}
    </body>
    </html>
    """
    return html_report
