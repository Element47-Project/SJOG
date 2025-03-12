#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# 检查 Python3 是否安装
if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: Python3 is not installed. Please install Python3 first." >&2
  exit 1
fi

# 检查 Python3 的 venv 模块是否可用
if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "Error: Python venv module is not available. Please ensure you have the correct Python version." >&2
  exit 1
fi

# 定义虚拟环境目录
VENV_DIR="venv"

# 如果虚拟环境不存在则创建
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment in '$VENV_DIR'..."
  python3 -m venv "$VENV_DIR"
else
  echo "Virtual environment '$VENV_DIR' already exists."
fi

# 激活虚拟环境
source "$VENV_DIR/bin/activate"

# 检查 requirements.txt 是否存在
if [ ! -f requirements.txt ]; then
  echo "Error: requirements.txt not found. Please provide the dependencies file." >&2
  deactivate
  exit 1
fi

# 升级 pip 并安装依赖
echo "Upgrading pip and installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Virtual environment setup complete."

# 可选：完成后自动退出虚拟环境
deactivate
