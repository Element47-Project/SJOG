"""写一个Python函数，解析Nginx日志文件，提取状态码为404和500的日志条目"""


def parse_nginx_log(log_file_path):
    # 1. 读取 Nginx 日志文件
    with open(log_file_path, 'r') as file:
        log_lines = file.readlines()

    # 2. 定义状态码列表
    status_codes = [404, 500]

    # 3. 解析并提取符合要求的日志条目
    error_log_entries = []
    for line in log_lines:
        # 3.1 按空格分隔日志行
        parts = line.split()

        # 3.2 若状态码为 404 或者 500，提取并添加到列表
        if len(parts) >= 9 and int(parts[8]) in status_codes:
            error_log_entries.append(line.strip())

    # 4. 返回符合要求的日志条目
    return error_log_entries
