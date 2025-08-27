#!/bin/bash

# Define variables
APP_NAME="flaskapp"
APP_DIR="/Users/qi/Documents/GitHub/SATEC-HTML/SATEC/flaskapp"  # --- Replace with the path to your Flask app
VENV_DIR="${APP_DIR}/venv"  
APACHE_CONF_DIR="/etc/apache2/sites-available"
APACHE_CONF_FILE="${APACHE_CONF_DIR}/${APP_NAME}.conf"
REQUIREMENTS_FILE="${APP_DIR}/requirements.txt"

# Step 1: Install mod_wsgi if not already installed
echo "Installing mod_wsgi for Python..."
sudo apt update
sudo apt install -y apache2

# Step 2: Create a virtual environment if it does not exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating a virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Step 3: Activate the virtual environment and install requirements
echo "Activating virtual environment and installing requirements..."
source "${VENV_DIR}/bin/activate"
if [ -f "$REQUIREMENTS_FILE" ]; then
    pip install -r "$REQUIREMENTS_FILE"
else
    echo "No requirements.txt found, installing Flask..."
    pip install flask
fi

# Step 4: Create or update the Apache configuration
echo "Creating Apache configuration..."
sudo bash -c "cat > $APACHE_CONF_FILE" << EOL
<VirtualHost *:80>
    ServerAdmin webmaster@localhost
    ServerName 127.0.0.1  # Replace with your server's IP or domain

    # Proxy to the Flask app
    ProxyPass "/flaskapp" "http://localhost:5001/flaskapp"
    ProxyPassReverse "/flaskapp" "http://localhost:5001/flaskapp"

    # Serve static files directly
    Alias /flaskapp/static/ /root/SATEC/EM133XM_HMI/static/
    <Directory "/root/SATEC/EM133XM_HMI/static">
        Require all granted
    </Directory>


    ErrorLog \${APACHE_LOG_DIR}/${APP_NAME}_error.log
    CustomLog \${APACHE_LOG_DIR}/${APP_NAME}_access.log combined
</VirtualHost>
EOL

# Step 5: Create the WSGI file for the Flask app
echo "Creating WSGI file..."
cat > "${APP_DIR}/app.wsgi" << EOL
import sys
import os
from app import app as application  # Adjust 'app' if your Flask instance has a different name

# Add the application directory to the sys.path
sys.path.insert(0, '${APP_DIR}')

# Activate the virtual environment
activate_this = '${VENV_DIR}/bin/activate_this.py'
with open(activate_this) as file_:
    exec(file_.read(), {'__file__': activate_this})
EOL

# Step 6: Enable the new Apache site and restart Apache
echo "Enabling Apache site and restarting Apache..."
sudo a2ensite "$APP_NAME"
sudo systemctl restart apache2

# Step 7: Provide user feedback
echo "Setup complete!"
echo "Your Flask app is now running at http://127.0.0.1 (or your server's IP)."
echo "If you encounter issues, check the logs in /var/log/apache2/."
