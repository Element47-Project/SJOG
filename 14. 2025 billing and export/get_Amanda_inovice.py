import os
import pdfplumber
import pandas as pd
import re


# Custom function to extract data from second page and parse title
def extract_second_page_data_with_title(pdf_path):
    data = []
    title = os.path.basename(pdf_path)  # Get filename as title
    # Extract cluster, year, month from title
    title_pattern = r"([A-Za-z0-9]+)(\d{4})(\d{2})"
    match = re.search(title_pattern, title)
    if match:
        cluster, year, month = match.groups()
        # Simplify cluster name pattern
        # Convert WTCH01A, WTCH01B, WTCH02A etc. to 1A, 1B, 2A etc.
        cluster_pattern = r"WTCH0(\d+[A-Z])"
        cluster_match = re.search(cluster_pattern, cluster)
        if cluster_match:
            cluster = cluster_match.group(1)  # Keep only number and letter part
    else:
        cluster, year, month = None, None, None

    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) > 1:  # Ensure at least two pages exist
            page = pdf.pages[1]  # Get second page
            text = page.extract_text()

            # Custom extraction logic
            # Match descriptions with brackets and regular descriptions
            pattern_full = r"(On Peak Energy Charge|Off Peak Energy Charge|Daily Supply Charge|Essential System Services|Solar On Peak Feedback \(Credit Applied\)|Solar Off Peak Feedback \(Credit Applied\)|Western Power Network AA5 Tariff Increase)\s*([\d,\.]+\s*(?:kWh|Days))?\s*([\d,\.]+\s*\$\/(?:kWh|Day))?\s*[-­\$]?([\d,\.]+)"

            # Match short descriptions like Sub Total and Plus : GST
            pattern_short = r"(Sub Total|Plus : GST|Total This NMI)\s*\$?\s*([-­\d,\.]+)"

            # Match all full description table rows
            matches_full = re.findall(pattern_full, text)
            # Match short description data
            matches_short = re.findall(pattern_short, text)

            # Process full description matches
            for match in matches_full:
                description, usage, rate, amount = match
                # Special handling for Solar feedback items
                if "Solar" in description and "Feedback" in description:
                    # Ensure amount is negative
                    amount = amount.replace("­", "-")  # Replace special minus character
                    if not amount.startswith("-"):
                        amount = f"-{amount}"
                data.append([cluster, year, month, description, usage, rate, amount])

            # Process short description matches
            for match in matches_short:
                description, amount = match
                # Ensure negative numbers display correctly
                amount = amount.replace("­", "-")  # Replace special minus character
                data.append([cluster, year, month, description, None, None, amount])

    return data


# Process multiple PDF files' second page content and save to CSV
def process_multiple_pdfs_with_parsed_title(pdf_folder, output_csv):
    all_data = []
    for filename in os.listdir(pdf_folder):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(pdf_folder, filename)
            second_page_data = extract_second_page_data_with_title(pdf_path)
            all_data.extend(second_page_data)

    # Convert data to DataFrame
    df = pd.DataFrame(all_data, columns=["Cluster", "Year", "Month", "Description", "Usage", "Rate", "Amount"])

    # Data cleaning and transformation
    # 1. Remove currency symbols and spaces from Amount column, convert to numeric
    df['Amount'] = df['Amount'].str.replace('$', '').str.replace(',', '').str.strip().astype(float)

    # 2. Extract kWh values from Usage column
    df['Usage_Value'] = df['Usage'].str.extract(r'([\d,\.]+)').replace(',', '', regex=True).astype(float)

    # 3. Pivot the data to convert Description to columns
    pivot_df = df.pivot_table(
        index=['Cluster', 'Year', 'Month'],
        columns='Description',
        values=['Amount', 'Usage_Value'],
        aggfunc='first'
    ).reset_index()

    # Flatten column names
    pivot_df.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in pivot_df.columns]

    # 4. Rename columns for clarity
    column_mapping = {
        'Amount_Sub Total': 'Sub_Total',
        'Amount_Total This NMI': 'Total_incl_GST'
    }
    pivot_df.rename(columns=column_mapping, inplace=True)

    # 5. Calculate consumption and export kWh
    pivot_df['Consumption_kWh'] = (
            pivot_df['Usage_Value_On Peak Energy Charge'] +
            pivot_df['Usage_Value_Off Peak Energy Charge']
    ).round(2)

    pivot_df['Export_kWh'] = (
            pivot_df['Usage_Value_Solar On Peak Feedback (Credit Applied)'] +
            pivot_df['Usage_Value_Solar Off Peak Feedback (Credit Applied)']
    ).round(2)

    # 6. Calculate combined energy consumption and solar feedback with GST
    pivot_df['Amanda_consumption_incl_GST'] = (
            (pivot_df['Amount_On Peak Energy Charge'] + pivot_df['Amount_Off Peak Energy Charge']) * 1.1).round(2)
    pivot_df['Amanda_export_buyback_incl_GST'] = ((pivot_df['Amount_Solar On Peak Feedback (Credit Applied)'] +
                                                   pivot_df[
                                                       'Amount_Solar Off Peak Feedback (Credit Applied)']) * 1.1).round(
        2)

    # 7. Calculate Daily Supply and Other Fees separately including GST
    pivot_df['Daily_Supply_incl_GST'] = (pivot_df['Amount_Daily Supply Charge'] * 1.1).round(2)

    pivot_df['Other_Fees_incl_GST'] = ((pivot_df['Sub_Total'] - (
            pivot_df['Amount_On Peak Energy Charge'] +
            pivot_df['Amount_Off Peak Energy Charge'] +
            pivot_df['Amount_Solar On Peak Feedback (Credit Applied)'] +
            pivot_df['Amount_Solar Off Peak Feedback (Credit Applied)'] +
            pivot_df['Amount_Daily Supply Charge']
    )) * 1.1).round(2)

    # 8. Select and reorder columns
    final_columns = [
        'Cluster', 'Year', 'Month',
        'Consumption_kWh',
        'Export_kWh',
        'Amanda_consumption_incl_GST',
        'Amanda_export_buyback_incl_GST',
        'Daily_Supply_incl_GST',
        'Other_Fees_incl_GST',
        'Total_incl_GST'
    ]
    pivot_df = pivot_df[final_columns]

    # 9. Sort by cluster, year, month
    pivot_df.sort_values(['Cluster', 'Year', 'Month'], inplace=True)

    # Save to CSV
    pivot_df.to_csv(output_csv, index=False)
    print(f"Data has been saved to {output_csv}")


# Set PDF folder path and output file path
pdf_folder = "Amanda invoice 2025"  # Replace with path to folder containing PDF files
output_csv = "output.csv"  # Replace with path for output CSV

# Process multiple PDF files
process_multiple_pdfs_with_parsed_title(pdf_folder, output_csv)
