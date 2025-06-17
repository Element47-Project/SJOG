# Import pymodbus, config and other modules
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian
from datetime import datetime, timezone
import os
import sys
import time
from pymodbus.client import ModbusSerialClient
import platform
import re
import subprocess
from config import map_133

METER_MODELNAME = "EM133XM"
METER_SNO = ""
METER_FIRMWARE = ""
METER_BOOT = ""
METER_CTRATIO = ""
METER_SUBNET = ""
METER_GATEWAY = ""


def initialize(ip_address, node_address, port_no=203):
    """Initialize a Modbus client with dynamic parameters."""
    client = ModbusSerialClient(method='rtu', port=f'socket://{ip_address}:{port_no}', timeout=20)
    return client, ip_address, node_address


class DataLogger:
    def __init__(self, ip, node, port_no, start_time=None, end_time=None):
        self.ip = ip
        self.node = node
        self.client, _, _ = initialize(ip, node, port_no)
        self.TwoDArray = []
        self.start_time = start_time
        self.end_time = end_time

    def GetDataMatrix(self):
        return self.TwoDArray

    def ReadDatalogger(self, DataLogNo, node):
        self.TwoDArray = []
        self.end_time = datetime.now()
        self.log_id = DataLogNo
        self.node = node
        if self.log_id < 1 or self.log_id > 16:
            return
        try:
            if self.client.connect():
                # Clear transfer buffer
                self.client.write_register(address=63120, value=1, slave=self.node)

                # Request file info
                self.client.write_register(address=64944, value=9, slave=self.node)
                self.client.write_register(address=64945, value=self.log_id, slave=self.node)

                # Read file info
                FileInfoBlock = self.client.read_holding_registers(address=64960, count=36, slave=self.node)
                if FileInfoBlock.isError():
                    print(f"[ERROR] Failed to read FileInFo: {FileInfoBlock}")
                    self.client.close()
                    return

                if not hasattr(FileInfoBlock, "registers") or len(FileInfoBlock.registers) < 36:
                    print("[DEBUG] Invalid FileInFo")
                    return

                # Total no of records in the data file
                TotalRecordNo = FileInfoBlock.getRegister(8)

                # First record no
                FirstRecordNo = FileInfoBlock.getRegister(12)

                # Last record no
                LastRecordNo = FileInfoBlock.getRegister(13)

                # Current record no pointed by meter
                CurrentRecordNo = FileInfoBlock.getRegister(10)

                # Size of data buffer. 8 for data log.
                FileResponseBlock = self.client.read_holding_registers(address=63152, count=8, slave=self.node)

                # No of records in the block
                BlockRecordNo = FileResponseBlock.getRegister(4)

                # No of words in each record. 40 for data log
                RecordSize = FileResponseBlock.getRegister(5)

                # Set the file position to the oldest record
                self.client.write_register(address=63120, value=5, slave=self.node)
                self.client.write_register(address=63121, value=self.log_id, slave=self.node)

                # Initial
                self.client.write_register(address=63120, value=1, slave=self.node)

                if self.start_time:
                    st_timestamp = ddmmyyyy_to_timestamp(self.start_time)
                    start_bits = timestamp_to_32bit(st_timestamp)

                    self.client.write_register(address=63120, value=7, slave=self.node)
                    self.client.write_registers(address=63128, values=start_bits, slave=self.node)
                    self.client.write_register(address=63120, value=11, slave=self.node)
                    response = self.client.read_holding_registers(address=63160, count=1, slave=self.node)
                    if response.isError():
                        print("Error reading response for start time")
                        return None
                    else:
                        FileInfoBlock = self.client.read_holding_registers(address=64960, count=36, slave=self.node)
                        StartTimeToEnd = FileInfoBlock.getRegister(9)
                        print(f"Found Number of Record from start time till last = {StartTimeToEnd}")
                else:
                    StartTimeToEnd = TotalRecordNo

                TimeRangeRecordNo = StartTimeToEnd
                print(f'Number of Records fall in time range = {TimeRangeRecordNo}')
                ReadRecordNo = 1
                update_interval = 100
                timer_start = time.time()

                while ReadRecordNo <= TimeRangeRecordNo:
                    DataBufferRegister = 63160  # reset value when going back to for next data block
                    for i in range(0, BlockRecordNo):
                        # Read current record
                        ReadRegSet = self.client.read_holding_registers(address=DataBufferRegister, count=RecordSize,
                                                                        slave=self.node)

                        if ReadRegSet.isError():
                            print(f"Error reading record {ReadRecordNo}, skipping")
                            continue

                        ReadRegArray = [ReadRegSet.getRegister(j) for j in range(0, RecordSize)]

                        # Convert unix datetimestamp to local time. Adjust daylight savings
                        TimeCal = ReadRegArray[3] * 65536 + ReadRegArray[2]
                        dt_object = datetime.fromtimestamp(TimeCal, tz=timezone.utc).strftime('%d/%m/%Y %H:%M:%S ')

                        if TimeCal == 0:
                            continue

                        if ReadRegSet.getRegister(9) > 32767:
                            Para1 = (-65536 + ReadRegSet.getRegister(8)) / 10
                        else:
                            Para1 = ((ReadRegSet.getRegister(9) * 65536) + ReadRegSet.getRegister(8)) / 10

                        if ReadRegSet.getRegister(11) > 32767:
                            Para2 = (-65536 + ReadRegSet.getRegister(10)) / 10
                        else:
                            Para2 = ((ReadRegSet.getRegister(11) * 65536) + ReadRegSet.getRegister(10)) / 10

                        if ReadRegSet.getRegister(13) > 32767:
                            Para3 = (-65536 + ReadRegSet.getRegister(12)) / 10
                        else:
                            Para3 = ((ReadRegSet.getRegister(13) * 65536) + ReadRegSet.getRegister(12)) / 10

                        if ReadRegSet.getRegister(15) > 32767:
                            Para4 = (-65536 + ReadRegSet.getRegister(14)) / 10
                        else:
                            Para4 = ((ReadRegSet.getRegister(15) * 65536) + ReadRegSet.getRegister(14)) / 10

                        if ReadRegSet.getRegister(17) > 32767:
                            Para5 = (-65536 + ReadRegSet.getRegister(16)) / 1000
                        else:
                            Para5 = ((ReadRegSet.getRegister(17) * 65536) + ReadRegSet.getRegister(16)) / 1000

                        if ReadRegSet.getRegister(19) > 32767:
                            Para6 = (-65536 + ReadRegSet.getRegister(18)) / 1000
                        else:
                            Para6 = ((ReadRegSet.getRegister(19) * 65536) + ReadRegSet.getRegister(18)) / 1000

                        CustomDataArray = [str(ReadRecordNo), str(dt_object), Para1, Para2, Para3, Para4, Para5, Para6]
                        self.TwoDArray.insert(ReadRecordNo, CustomDataArray)
                        DataBufferRegister += RecordSize
                        ReadRecordNo += 1

                        if ReadRecordNo % update_interval == 0:
                            elasped_time = time.time() - timer_start
                            estimated_time = elasped_time * (TimeRangeRecordNo / ReadRecordNo)
                            print(f'Progress: {ReadRecordNo}/{TimeRangeRecordNo} '
                                  f'({ReadRecordNo / TimeRangeRecordNo * 100:.1f}%)')
                            print(f'estimated time remaining: {estimated_time - elasped_time:.2f}s')

                    # Clear the buffer for next read
                    self.client.write_register(address=63120, value=1, slave=self.node)

                if TimeRangeRecordNo != 0:
                    total_used_time = time.time() - timer_start
                    print(f'Data log finished, used time {total_used_time}')

            else:
                self.TwoDArray = []

        except Exception as e:
            print(f"Exception in ReadDatalogger: {e}")

        finally:
            try:
                self.client.close()
                print("Closed Client")
            except Exception as e:
                print(f"Close Issue: {e}")
            finally:
                return self.TwoDArray



def ddmmyyyy_to_timestamp(date_str):
    """transfer str format data DD/MM/YYYY to timestamp and set to +8 time zone
    """
    try:
        date_obj = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        print(f"错误:格式不对：{ValueError}")
        return None
    perth_offset_seconds = 8 * 3600  # UTC+8 is 8 hours ahead of UTC
    timestamp = int(date_obj.timestamp()) + perth_offset_seconds  # Convert the timestamp to an integer

    return timestamp


def timestamp_to_32bit(timestamp=int):
    """transfer timestamp(int) to 2*16bit value to write into register
    """
    if timestamp is None:
        return None

    # Split into high and low 16-bit parts
    high_bits = (timestamp >> 16) & 0xFFFF  # High 16 bits
    low_bits = timestamp & 0xFFFF  # Low 16 bits
    return [low_bits, high_bits]
