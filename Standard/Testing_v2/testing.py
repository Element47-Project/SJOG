from datetime import datetime


def ddmmyyyy_to_timestamp(date_str):
    """transfer str format data DD/MM/YYYY to timestamp and set to +8 time zone
    """
    print(f"ddmmyyyy_to_timestamp{date_str}")
    formats = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"]
    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            perth_offset_seconds = 8 * 3600
            timestamp = int(date_obj.timestamp()) + perth_offset_seconds
            return timestamp
        except ValueError:
            print(f"错误:解析失败")
            return None


def timestamp_to_32bit(timestamp=int):
    """transfer timestamp(int) to 2*16bit value to write into register
    """
    if timestamp is None:
        return None

    # Split into high and low 16-bit parts
    high_bits = (timestamp >> 16) & 0xFFFF  # High 16 bits
    low_bits = timestamp & 0xFFFF  # Low 16 bits
    return [low_bits, high_bits]


a = "2025-02-18 10:35:00"
b = "26/02/2025 17:25:00"
c = "10/08/2024 10:55"

print(ddmmyyyy_to_timestamp(a))
print(ddmmyyyy_to_timestamp(b))
print(ddmmyyyy_to_timestamp(c))
