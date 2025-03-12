# Project Name

## Overview
This project processes meter data, cleans it, calculates consumption based on tariffs, and generates invoices for each meter. The data is then uploaded to a database.

## Project Structure
project_name/ 
├── main.py # Main program 
├── utils/ # Utility functions 
├── models/ # Model definitions 
├── data/ # Datasets 
├── requirements.txt # Dependencies 
└── README.md # Project documentation

## Setup
1. Clone the repository:
    ```sh
    git clone <repository_url>
    cd project_name
    ```

2. Create a virtual environment and activate it:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. Install the dependencies:
    ```sh
    pip install -r requirements.txt
    ```

4. Set up environment variables:
    Create a `.env` file in the root directory and add the following variables:
    ```env
    AZURE_SQL_SERVER=<your_sql_server>
    AZURE_SQL_DB_NAME=<your_db_name>
    AZURE_SQL_USERNAME=<your_username>
    AZURE_SQL_PASSWORD=<your_password>
    FILE_ADDRESS=<your_file_address>
    ```

## Usage
Run the main script to process data:
```sh
python main.py