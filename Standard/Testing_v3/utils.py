from datetime import datetime


def ddmmyyyy_to_timestamp(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        print(f"Error: Wrong Format：{ValueError}")
        return None
    perth_offset_seconds = 8 * 3600
    timestamp = int(date_obj.timestamp()) + perth_offset_seconds
    return timestamp


def timestamp_to_32bit(timestamp):
    if timestamp is None:
        return None
    high_bits = (timestamp >> 16) & 0xFFFF
    low_bits = timestamp & 0xFFFF
    return [low_bits, high_bits]
