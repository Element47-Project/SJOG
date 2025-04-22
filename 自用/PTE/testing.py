from datetime import datetime


def ddmmyyyy_to_timestamp(date_str):
    date_obj = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
    perth_offset_seconds = 8 * 3600
    return int(date_obj.timestamp()) + perth_offset_seconds


def timestamp_to_32bit(timestamp):
    high_bits = (timestamp >> 16) & 0xFFFF
    low_bits = timestamp & 0xFFFF
    return [low_bits, high_bits]


date_time = "25/02/2025 00:00:00"
st_timestamp = ddmmyyyy_to_timestamp(date_time)
start_bits = timestamp_to_32bit(st_timestamp)
print(st_timestamp)
print(start_bits)
