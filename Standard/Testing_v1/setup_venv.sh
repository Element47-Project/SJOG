#!/bin/bash

# Check if Python3 and virtualenv are installed
if ! command -v python3 &> /dev/null
then
    echo "Python3 could not be found, please install it first."
    exit
fi

# Create a virtual environment named 'venv'
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Check if requirements.txt exists
if [ ! -f requirements.txt ]; then
    echo "requirements.txt not found!"
    deactivate
    exit
fi

# Install packages from requirements.txt
pip install -r requirements.txt

echo "Virtual environment setup and packages installed."

# Deactivate the virtual environment
deactivate
