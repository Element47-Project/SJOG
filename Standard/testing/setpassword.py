from pymodbus.client import ModbusTcpClient
from datetime import datetime
import logging
# Enable logging for detailed debug output
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)

def ddmmyyyy_to_timestamp(date_str = str):
    """transfer str format data DD/MM/YYYY to timestamp and set to +8 time zone
    """

    # Parse the date from string in DD/MM/YYYY format
    date_obj = datetime.strptime(date_str, "%d/%m/%Y")
    
    # Convert to timestamp (seconds since epoch)
    perth_offset_seconds = 8 * 3600  # UTC+8 is 8 hours ahead of UTC
    timestamp = int(date_obj.timestamp()) + perth_offset_seconds  # Convert the timestamp to an integer

    return timestamp

def timestamp_to_32bit(timestamp=int):
        """transfer timestamp(int) to 2*16bit value to write into register
        """

      # Split into high and low 16-bit parts
        high_bits = (timestamp >> 16) & 0xFFFF  # High 16 bits
        low_bits = timestamp & 0xFFFF          # Low 16 bits
        return [low_bits,high_bits]

# Replace with your meter's IP and port
client = ModbusTcpClient('10.173.200.7', port=502, timeout=20)

timestamp1 = ddmmyyyy_to_timestamp('28/10/2024')
timestamp2 = ddmmyyyy_to_timestamp('28/10/2024')
timebit1 = timestamp_to_32bit(timestamp1)
timebit2 = timestamp_to_32bit(timestamp2)

client.write_registers(63128,timebit1,2) #start
# client.write_registers(63128,timebit2,2) #endtime

client.write_register(63120,7,2)
response = client.read_holding_registers(63120, 1, 2)
print(response) ## should be 7

client.write_register(63120,11,2)

client.write_register(64944, 9,2)
client.write_register(64945, 1,2)
response = client.read_holding_registers(64960,36,2)
print([(i, response.registers[i]) for i in range(36)])  # Changed to list comprehension with tuples

response = client.read_holding_registers(63152,8,2)
print(response.registers)

# Close the connection
client.close()
