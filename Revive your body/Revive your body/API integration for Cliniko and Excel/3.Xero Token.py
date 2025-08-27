import requests
import base64

# Your Xero API credentials
client_id = "5788DC519D6742ECB9920FAB4D5211E7"
client_secret = "qcslZQILXmBPCGWBh5wIwDv4YDH9XlRTjpETHe-LSFEC_Rvs"
redirect_uri = "https://developer.com.au"

# The authorization code from the redirect URL
authorization_code = "iaAJzM24ICkfpucKMRrwEou2vA-bkZXgXdRRCQFoDEs"

# Xero's token endpoint
token_url = "https://identity.xero.com/connect/token"

# Encode client_id and client_secret in Base64 for the Authorization header
auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

# Prepare the request headers and data
headers = {
    "Authorization": f"Basic {auth_header}",
    "Content-Type": "application/x-www-form-urlencoded"
}

data = {
    "grant_type": "authorization_code",
    "code": authorization_code,
    "redirect_uri": redirect_uri
}

# Make the request to exchange the authorization code for tokens
response = requests.post(token_url, headers=headers, data=data)

# Check the response
if response.status_code == 200:
    tokens = response.json()
    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]
    print("Access Token:", access_token)
    print("Refresh Token:", refresh_token)
else:
    print("Failed to obtain tokens. Status code:", response.status_code)
    print("Response:", response.text)
