from pymodbus.client import ModbusTcpClient
from datetime import datetime, timezone
import time


def initialize(ip_address, node_address, port_no=203):
    client = ModbusTcpClient(host=ip_address, port=port_no, timeout=20)
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
        self.log_id = DataLogNo
        self.node = node
        if self.log_id < 1 or self.log_id > 16:
            return

        try:
            if self.client.connect():
                self.client.write_register(address=63120, value=1, slave=self.node)
                self.client.write_register(address=64944, value=9, slave=self.node)
                self.client.write_register(address=64945, value=self.log_id, slave=self.node)
                FileInfoBlock = self.client.read_holding_registers(address=64960, count=36, slave=self.node)
                TotalRecordNo = FileInfoBlock.getRegister(8)
                FirstRecordNo = FileInfoBlock.getRegister(12)
                LastRecordNo = FileInfoBlock.getRegister(13)
                CurrentRecordNo = FileInfoBlock.getRegister(10)
                FileResponseBlock = self.client.read_holding_registers(address=63152, count=8, slave=self.node)
                BlockRecordNo = FileResponseBlock.getRegister(4)
                RecordSize = FileResponseBlock.getRegister(5)
                self.client.write_register(address=63120, value=5, slave=self.node)
                self.client.write_register(address=63121, value=self.log_id, slave=self.node)
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

                if self.end_time:
                    ed_timestamp = ddmmyyyy_to_timestamp(self.end_time)
                    end_bits = timestamp_to_32bit(ed_timestamp)
                    print(f"end time {self.end_time}")
                    self.client.write_register(address=63120, value=7, slave=self.node)
                    self.client.write_registers(address=63128, values=end_bits, slave=self.node)
                    self.client.write_register(address=63120, value=11, slave=self.node)
                    response = self.client.read_holding_registers(address=63160, count=1, slave=self.node)
                    if response.isError():
                        print("Error reading response for end time")
                        EndTimeToEnd = 0
                    else:
                        FileInfoBlock = self.client.read_holding_registers(address=64960, count=36, slave=self.node)
                        EndTimeToEnd = FileInfoBlock.getRegister(9)
                        print(f"Found Number of Record from end time till last = {EndTimeToEnd}")
                else:
                    EndTimeToEnd = 0

                TimeRangeRecordNo = StartTimeToEnd - EndTimeToEnd
                print(f'Number of Records fall in time range = {TimeRangeRecordNo}')
                ReadRecordNo = 1
                update_interval = 20
                timer_start = time.time()

                while ReadRecordNo <= TimeRangeRecordNo:
                    DataBufferRegister = 63160
                    for i in range(0, BlockRecordNo):
                        ReadRegSet = self.client.read_holding_registers(address=DataBufferRegister, count=RecordSize,
                                                                        slave=self.node)
                        ReadRegArray = [ReadRegSet.getRegister(j) for j in range(0, RecordSize)]
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
                            print(
                                f'Progress: {ReadRecordNo}/{TimeRangeRecordNo} ({ReadRecordNo / TimeRangeRecordNo * 100:.1f}%)')
                            print(f'estimated time remaining: {estimated_time - elasped_time:.2f}s')

                    self.client.write_register(address=63120, value=1, slave=self.node)

                if TimeRangeRecordNo != 0:
                    total_used_time = time.time() - timer_start
                    print(f'Data log finished, used time {total_used_time}')

            else:
                self.TwoDArray = []
            return

        except Exception as e:
            print(f"Exception in ReadDatalogger: {e}")

        finally:
            return self.TwoDArray


def ddmmyyyy_to_timestamp(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        print(f"错误:格式不对：{ValueError}")
        return None
    perth_offset_seconds = 8 * 3600
    timestamp = int(date_obj.timestamp()) + perth_offset_seconds
    return timestamp


def timestamp_to_32bit(timestamp=int):
    if timestamp is None:
        return None
    high_bits = (timestamp >> 16) & 0xFFFF
    low_bits = timestamp & 0xFFFF
    return [low_bits, high_bits]
