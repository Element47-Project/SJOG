import requests

# 你的 Client ID 和 Client Secret
client_id = "0DCE76AAA50A465EBB8AF41A9086CA4E"  # 替换为你的 Client ID
client_secret = "fWWc2vrrazdXPYmbrxgMOtX4bmZoYpyiZflW-an2dMZl6P6r"  # 替换为你的 Client Secret
refresh_token = "wVWWSwziglgCgpgjVd30Wm8hAmW8zGtZmMrZwA8_jMg"  # 你的 Refresh Token

# Xero 的 Token 端点
token_url = "https://identity.xero.com/connect/token"

# 请求数据
data = {
    "grant_type": "refresh_token",
    "client_id": client_id,
    "client_secret": client_secret,
    "refresh_token": refresh_token
}

# 发送 POST 请求
response = requests.post(token_url, data=data)

# 检查响应
if response.status_code == 200:
    # 解析返回的 JSON 数据
    tokens = response.json()
    access_token = tokens["access_token"]
    new_refresh_token = tokens["refresh_token"]  # 新的 Refresh Token
    print("✅ 新的 Access Token:", access_token)
    print("✅ 新的 Refresh Token:", new_refresh_token)
else:
    print("❌ 获取新的 Access Token 失败:", response.status_code, response.json())