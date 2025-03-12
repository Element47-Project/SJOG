# EM133XM_HMI_Library Module
# Import pymodbus, config and other modules
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian
from datetime import datetime, timezone
import os
import sys
import time
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
    client = ModbusTcpClient(host=ip_address, port=port_no, timeout=20)
    return client, ip_address, node_address


# Network settings for HMI and meter
# class ModbusNetworkConfiguration:
#     def __init__(self, interface='utun4'):
#         self.interface = interface
#         self.system = platform.system()
#         self.GetHMINetworkSettings()
#         self.GetHMIGatewaySettings()
#         self.GetMeterinfo()
#
#     # Read current network settings
#     def GetHMINetworkSettings(self):
#         try:
#             # Get IP and subnet mask
#             if self.system == "Darwin":
#                 output = subprocess.check_output(f"ifconfig {self.interface}").decode()
#                 ip_pattern = r"inet (\d+\.\d+\.\d+\.\d+) netmask (0x[a-fA-F0-9]+)"
#             else:  # Linux
#                 output = subprocess.check_output(f"ip addr show {self.interface}").decode()
#                 ip_pattern = r"inet (\d+\.\d+\.\d+\.\d+)/(\d+)"
#
#             ip_match = re.search(ip_pattern, output)
#             if ip_match:
#                 ip_address = ip_match.group(1)
#                 if self.system == "Darwin":
#                     subnet_mask_hex = ip_match.group(2)
#                     subnet_mask = '.'.join([str(int(subnet_mask_hex[i:i + 2], 16)) for i in range(2, 10, 2)])
#                 else:  # Linux
#                     cidr = int(ip_match.group(2))
#                     subnet_mask = '.'.join([str((0xffffffff << (32 - cidr) >> i) & 0xff) for i in [24, 16, 8, 0]])
#
#                 self.HMIIPAddress = ip_address
#                 self.HMISubnet = subnet_mask
#             else:
#                 return 'Error: Could not find IP address and subnet mask'
#         except:
#             self.HMIIPAddress = "0.0.0.0"
#             self.HMISubnet = "0.0.0.0"
#
#     def GetHMIGatewaySettings(self):
#         try:
#             if self.system == "Darwin":
#                 output = subprocess.check_output("netstat -nr | grep default", shell=True).decode()
#                 gateway_pattern = r"default\s+(\d+\.\d+\.\d+\.\d+)"
#             else:  # Linux
#                 output = subprocess.check_output("ip route | grep default", shell=True).decode()
#                 gateway_pattern = r"default via (\d+\.\d+\.\d+\.\d+)"
#
#             gateway_match = re.search(gateway_pattern, output)
#             if gateway_match:
#                 gateway = gateway_match.group(1)
#                 self.HMIGateway = gateway
#             else:
#                 return 'Error: Could not find gateway'
#         except:
#             self.HMIGateway = "0.0.0.0"
#
#     def GetMeterinfo(self):
#         global client
#         global MeterIPAddress
#         global MeterNodeAddress
#         global METER_SNO, METER_FIRMWARE, METER_BOOT, METER_CTRATIO, METER_SUBNET, METER_GATEWAY
#
#         if not client.connect():
#             print('connection failed!!')
#             global METER_SNO, METER_FIRMWARE, METER_BOOT, METER_CTRATIO, METER_MODELNAME
#             METER_SNO = ""
#             METER_FIRMWARE = ""
#             METER_BOOT = ""
#             METER_CTRATIO = ""
#             METER_MODELNAME = ""
#
#             return
#
#         if client.connect():
#             try:
#                 print('connection successful')
#                 Meter_Subnet_Raw = client.read_holding_registers(address=46578, count=2, slave=MeterNodeAddress)
#                 Meter_Subnet_oct1 = Meter_Subnet_Raw.getRegister(0) & 255
#                 Meter_Subnet_oct2 = Meter_Subnet_Raw.getRegister(0) >> 8
#                 Meter_Subnet_oct3 = Meter_Subnet_Raw.getRegister(1) & 255
#                 Meter_Subnet_oct4 = Meter_Subnet_Raw.getRegister(1) >> 8
#
#                 METER_SUBNET = str(Meter_Subnet_oct1) + "." + str(Meter_Subnet_oct2) + "." + str(
#                     Meter_Subnet_oct3) + "." + str(Meter_Subnet_oct4)
#                 Meter_Gateway_Raw = client.read_holding_registers(address=46580, count=2, slave=MeterNodeAddress)
#
#                 Meter_Gateway_oct1 = Meter_Gateway_Raw.getRegister(0) & 255
#                 Meter_Gateway_oct2 = Meter_Gateway_Raw.getRegister(0) >> 8
#                 Meter_Gateway_oct3 = Meter_Gateway_Raw.getRegister(1) & 255
#                 Meter_Gateway_oct4 = Meter_Gateway_Raw.getRegister(1) >> 8
#
#                 METER_GATEWAY = str(Meter_Gateway_oct1) + "." + str(Meter_Gateway_oct2) + "." + str(
#                     Meter_Gateway_oct3) + "." + str(Meter_Gateway_oct4)
#
#                 # Read meter details
#                 Meter_Model_Raw = client.read_holding_registers(address=46080, count=31, slave=MeterNodeAddress)
#
#                 # Serial no
#                 METER_SNO = (Meter_Model_Raw.getRegister(1) * 65536) + Meter_Model_Raw.getRegister(0)
#
#                 # Model name.
#                 Model_Name = ''  # Clear the data before join
#                 for x in range(4, 12):
#                     if len(hex(Meter_Model_Raw.getRegister(x))) > 3:  # Blank register
#                         Model_Name = Model_Name + self.CHR16Read(Meter_Model_Raw.getRegister(x))
#
#                 METER_MODELNAME = Model_Name
#
#                 METER_FIRMWARE = 'V' + str(Meter_Model_Raw.getRegister(20) / 100) + "." + str(
#                     Meter_Model_Raw.getRegister(21))
#
#                 METER_BOOT = 'V' + str(Meter_Model_Raw.getRegister(24) / 100) + "." + str(
#                     Meter_Model_Raw.getRegister(25))
#
#                 # Get defined scales used in 16 bit reads.
#                 HighRawScale = client.read_holding_registers(address=241, count=1, slave=MeterNodeAddress).getRegister(
#                     0)
#                 LowRawScale = client.read_holding_registers(address=240, count=1, slave=MeterNodeAddress).getRegister(0)
#                 RawScaleRange = HighRawScale - LowRawScale
#                 VoltageScale = client.read_holding_registers(address=242, count=1, slave=MeterNodeAddress).getRegister(
#                     0)
#                 CurrentScale = client.read_holding_registers(address=243, count=1, slave=MeterNodeAddress).getRegister(
#                     0)
#
#                 # Get CT and PT Ratios
#                 CTprimaryCurrent = client.read_holding_registers(address=2306, count=1,
#                                                                  slave=MeterNodeAddress).getRegister(0)
#                 PTratio = client.read_holding_registers(address=2305, count=1, slave=MeterNodeAddress).getRegister(
#                     0) * 0.1
#
#                 METER_CTRATIO = CTprimaryCurrent
#
#             except:
#                 pass
#
#     # Display on HMI as strings
#     def GetHMIIPAddressStr(self):
#         self.GetHMINetworkSettings()
#         return str(self.HMIIPAddress)
#
#     def GetHMISubnetStr(self):
#         self.GetHMINetworkSettings()
#         return str(self.HMISubnet)
#
#     def GetHMIGatewayStr(self):
#         self.GetHMIGatewaySettings()
#         return str(self.HMIGateway)
#
#     def GetMeterIPAddressStr(self):
#         global MeterIPAddress
#         return str(MeterIPAddress)
#
#     def GetMeterNodeAddressStr(self):
#         global MeterNodeAddress
#         return str(MeterNodeAddress)
#
#     def GetMeterInfoStr(self):
#         global METER_SUBNET, METER_GATEWAY, METER_CTRATIO, METER_SNO
#         global METER_FIRMWARE, METER_BOOT, METER_MODELNAME
#         meterinfo = [METER_SUBNET, METER_GATEWAY, METER_CTRATIO,
#                      METER_SNO, METER_FIRMWARE, METER_BOOT, METER_MODELNAME]
#         return meterinfo
#
#     # Split into octets for user inputs
#     def GetHMIIPAddressArray(self):
#         self.GetHMINetworkSettings()
#         return self.HMIIPAddress.split(".")
#
#     def GetHMISubnetArray(self):
#         self.GetHMINetworkSettings()
#         return self.HMISubnet.split(".")
#
#     def GetHMIGatewayArray(self):
#         self.GetHMIGatewaySettings()
#         return self.HMIGateway.split(".")
#
#     def GetMeterIPAddressArray(self):
#         global MeterIPAddress
#         return MeterIPAddress.split(".")
#
#     # Validate IP address
#     def validIPAddress(self, IP=str):
#
#         def isIPv4(s):
#             try:
#                 return str(int(s)) == s and 0 <= int(s) <= 255
#             except:
#                 return False
#
#         def isIPv6(s):
#             if len(s) > 4:
#                 return False
#             try:
#                 return int(s, 16) >= 0 and s[0] != '-'
#             except:
#                 return False
#
#         if IP.count(".") == 3 and all(isIPv4(i) for i in IP.split(".")):
#             return True  # IPv4
#         if IP.count(":") == 7 and all(isIPv6(i) for i in IP.split(":")):
#             return True  # "IPv6"
#         return False
#
#     # Validate modbus address
#     def validModbusAddress(Self, node=str):
#         if node.count(".") > 0:
#             return False
#         if int(node) >= 1 and int(node) <= 247:
#             return True
#         else:
#             return False
#
#     # Set the network as per user inputs
#     def SetNetwork(self, HMIip, HMIsub, HMIgway):
#         if self.validIPAddress(HMIip) and self.validIPAddress(HMIsub) and self.validIPAddress(HMIgway):
#
#             # Set HMI address and subnet
#             if os.system('sudo ethtool eth0 | grep \'Link detected: yes\'') == 0:
#
#                 os.system('sudo ifconfig eth0 down')
#                 os.system('sudo ifconfig eth0 ' + HMIip)
#                 os.system('sudo ifconfig eth0 netmask ' + HMIsub)
#                 os.system('sudo ifconfig eth0 up')
#
#                 # Setting gateway address to Pi takes bit of time, thats why time delays are introduced.
#                 timeout = time.time() + 15
#                 while not os.popen('route | grep default | grep eth0').read().split():
#                     if time.time() > timeout:
#                         # print("Timeout: Eth0 connection down")
#                         return False
#
#                 os.system('sudo route del default eth0')
#                 os.system('sudo route add default gw ' + HMIgway + ' metric 202 eth0')
#                 os.system('sudo ifconfig eth0 up')
#
#                 timeout = time.time() + 15
#
#                 while not os.popen('ifconfig eth0 | grep -w inet').read().split():
#                     if time.time() > timeout:
#                         # print("Timeout: Eth0 connection down")
#                         return False
#
#                 global client
#                 client = ModbusTcpClient(host=MeterIPAddress, port=502)
#             return False
#         return False
#
#     # Set the network as per user inputs
#     def SetNetwork2(self, Metip, Metnode):
#         if self.validIPAddress(Metip) and self.validModbusAddress(Metnode):
#             global MeterIPAddress
#             MeterIPAddress = Metip
#             print(MeterIPAddress)
#             global MeterNodeAddress
#             MeterNodeAddress = int(Metnode)
#             print(MeterNodeAddress)
#         return False
#
#     def CHR16Read(self, i):
#         # Each modbus 16bit word has two ASCII chars and the bytes are transferred in reverse order.
#         self.ReadInt = i
#         if len(hex(self.ReadInt)) == 4:  # One char in the register
#             return chr(
#                 int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[2] + hex(self.ReadInt)[3]), 16))
#         if len(hex(self.ReadInt)) == 6:  # Two chars in the register.
#             Char1 = chr(
#                 int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[4] + hex(self.ReadInt)[5]), 16))
#             Char2 = chr(
#                 int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[2] + hex(self.ReadInt)[3]), 16))
#             return Char1 + Char2
#
#     def CloseProgram(self):
#         global client
#         client.close()
#         # Undo comment if to shudown Pi on close
#         # os.system("sudo shutdown -h now")
#         sys.exit()


# Read logged data from data logs

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
                # Clear transfer buffer
                self.client.write_register(address=63120, value=1, slave=self.node)

                # Request file info
                self.client.write_register(address=64944, value=9, slave=self.node)
                self.client.write_register(address=64945, value=self.log_id, slave=self.node)

                # Read file info
                FileInfoBlock = self.client.read_holding_registers(address=64960, count=36, slave=self.node)

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
                # if TimeRangeRecordNo > 2000:
                #     EndTimeToEnd = StartTimeToEnd - 2000
                ReadRecordNo = 1
                update_interval = 20
                timer_start = time.time()

                while ReadRecordNo <= TimeRangeRecordNo:
                    DataBufferRegister = 63160  # reset value when going back to for next data block
                    for i in range(0, BlockRecordNo):
                        # Read current record
                        ReadRegSet = self.client.read_holding_registers(address=DataBufferRegister, count=RecordSize,
                                                                   slave=self.node)
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
            return self.TwoDArray


# # Read logged data from event log
# class EventLogger:
#     def __init__(self, ip, node):
#         self.ip = ip
#         self.node = node
#         self.client, _, _ = initialize(ip, node)
#         self.LogFileID = 0  # FileID for event log is always 0
#         self.TwoDArray = []
#
#     def GetDataMatrix(self):
#         # print("Length of Eventlog Array:"+ str(len(self.TwoDArray)))
#         # print (self.TwoDArray)
#         return self.TwoDArray
#
#     def ReadEventlogger(self):
#         if self.client.connect():
#
#             # Clear Buffer
#             self.client.write_register(address=63120, value=1, slave=self.node)
#             self.client.write_register(address=63121, value=self.LogFileID, slave=self.node)  # Not Working
#
#             # Request file info
#             self.client.write_register(address=64944, value=9, slave=self.node)
#             rq = self.client.write_register(address=64945, value=self.LogFileID, slave=self.node)
#             if rq.isError():
#                 print("rq error")
#                 return 0
#
#             # Read file info
#             FileInfoBlock = self.client.read_holding_registers(address=64960, count=36, slave=self.node)
#
#             # Total no of records in the data file
#             TotalRecordNo = FileInfoBlock.getRegister(8)
#
#             # First record no
#             FirstRecordNo = FileInfoBlock.getRegister(12)
#
#             # Last record no
#             LastRecordNo = FileInfoBlock.getRegister(13)
#
#             # Current record no pointed by meter
#             CurrentRecordNo = FileInfoBlock.getRegister(10)
#
#             # Size of data buffer
#             FileResponseBlock = self.client.read_holding_registers(address=63152, count=8, slave=self.node)
#
#             # No of words in each record. 12 for event log
#             RecordSize = FileResponseBlock.getRegister(5)
#
#             # Set the position at the first record
#             self.client.write_register(address=63120, value=5, slave=self.node)
#             self.client.write_register(address=63121, value=self.LogFileID, slave=self.node)
#
#             ReadRecordNo = 1
#             while ReadRecordNo <= TotalRecordNo:
#                 DataBufferRegister = 63160  # reset value when going back to for next data block
#                 for i in range(0, FileResponseBlock.getRegister(4)):
#
#                     if ReadRecordNo <= TotalRecordNo:
#                         # Read current reacord data
#                         ReadRegSet = self.client.read_holding_registers(address=DataBufferRegister, count=RecordSize,
#                                                                    slave=self.node)
#                         # Store the record in an array. All 16 parameters are read
#                         ReadRegArray = [ReadRegSet.getRegister(j) for j in range(0, RecordSize)]
#
#                         # Convert unix datetimestamp to local time. Adjust daylight savings
#                         TimeCal = ReadRegArray[3] * 65536 + ReadRegArray[2]
#                         dt_object = datetime.fromtimestamp(TimeCal, tz=timezone.utc).strftime('%d/%m/%Y %H:%M:%S ')
#
#                         # Copy required columns into another array
#                         CustomDataArray = [str(ReadRecordNo), str(dt_object),
#                                            self.GetEventCause(ReadRegSet.getRegister(7)),
#                                            self.GetEventSource(ReadRegSet.getRegister(7)),
#                                            self.GetEventEffect(ReadRegSet.getRegister(8)), '', '', '', '']
#
#                         # print(ReadRegArray)
#                         # print(CustomDataArray)
#                         self.TwoDArray.insert(ReadRecordNo, CustomDataArray)
#                         DataBufferRegister = DataBufferRegister + RecordSize
#                         ReadRecordNo = ReadRecordNo + 1
#                 self.client.write_register(address=63120, value=1, slave=self.node)
#                 self.client.write_register(address=63121, value=self.LogFileID, slave=self.node)
#         else:
#             self.TwoDArray = [['0'], ['0'], ['0'], ['0'], ['0'], ['0'], ['0'], ['0'], ['0']]
#
#     def GetEventCause(self, i):
#
#         self.ReadInt = i
#
#         if len(hex(self.ReadInt)) <= 3:  # Blank register
#             return ''
#         else:
#             hexstr = str(hex(self.ReadInt)[0]) + str(hex(self.ReadInt)[1]) + str(hex(self.ReadInt)[2]) + str(
#                 hex(self.ReadInt)[3])
#
#         if hexstr == '0x5b':
#             return 'COMM'
#         elif hexstr == '0x5c':
#             return 'Front Panel'
#         elif hexstr == '0x5d':
#             return 'Selfcheck'
#         elif hexstr == '0x62':
#             return 'Hardware'
#         elif hexstr == '0x63':
#             return 'External'
#         else:
#             return ''
#
#     def GetEventSource(self, i):
#
#         self.ReadInt = i
#         if len(hex(self.ReadInt)) <= 3:  # Blank register
#             return ''
#         else:
#             hexstr = str(hex(self.ReadInt))
#             hexstr1 = str(hex(self.ReadInt)[0]) + str(hex(self.ReadInt)[1]) + str(hex(self.ReadInt)[2]) + str(
#                 hex(self.ReadInt)[3])
#             hexstr2 = str(hex(self.ReadInt)[0]) + str(hex(self.ReadInt)[1]) + str(hex(self.ReadInt)[4]) + str(
#                 hex(self.ReadInt)[5])
#
#         if hexstr1 == '0x62' or hexstr1 == '0x63':
#             if hexstr == '0x6202':
#                 return 'RAM Error'
#             if hexstr == '0x6203':
#                 return 'HW WDOG Reset'
#             if hexstr == '0x6204':
#                 return 'Sampling Fault'
#             if hexstr == '0x6205':
#                 return 'CPU Exception'
#             if hexstr == '0x6207':
#                 return 'SW WDOG Reset'
#             if hexstr == '0x620d':
#                 return 'Low Battery'
#             if hexstr == '0x620f':
#                 return 'EEPROM Fault'
#             if hexstr == '0x6300':
#                 return 'Power Down'
#             if hexstr == '0x6308':
#                 return 'Power Up'
#             if hexstr == '0x6309':
#                 return 'External Reset'
#
#         if hexstr1 != '0x62' and hexstr1 != '0x63':
#
#             if hexstr2 == '0x03':
#                 return '123 memory'
#             if hexstr2 == '0x04':
#                 return 'Factory Setup'
#             if hexstr2 == '0x05':
#                 return 'Password Setup'
#             if hexstr2 == '0x06':
#                 return 'Basic Setup'
#             if hexstr2 == '0x07':
#                 return 'Comms Setup'
#             if hexstr2 == '0x08':
#                 return 'Real Time Clock'
#             if hexstr2 == '0x09':
#                 return 'Digital Inputs Setup'
#             if hexstr2 == '0x0a':
#                 return 'Pulse Counters Setup'
#             if hexstr2 == '0x0b':
#                 return 'AO Setup'
#             if hexstr2 == '0x0e':
#                 return 'TImers Setup'
#             if hexstr2 == '0x10':
#                 return 'Setpoints Setup'
#             if hexstr2 == '0x11':
#                 return 'Pulsing Setup'
#             if hexstr2 == '0x12':
#                 return 'User Rigester Map Setup'
#             if hexstr2 == '0x14':
#                 return 'Datalog Setup'
#             if hexstr2 == '0x15':
#                 return 'Memory Setup'
#             if hexstr2 == '0x16':
#                 return 'TOU Registers Setup'
#             if hexstr2 == '0x18':
#                 return 'TOU Daily Setup'
#             if hexstr2 == '0x19':
#                 return 'TOU Calender Setup'
#             if hexstr2 == '0x1b':
#                 return 'RO Setup'
#             if hexstr2 == '0x1c':
#                 return 'User Selectable Options Setup'
#             if hexstr2 == '0x1f':
#                 return 'DNP3.0 Class 0 Map'
#             if hexstr2 == '0x20':
#                 return 'DNP3.0 Options Setup'
#             if hexstr2 == '0x21':
#                 return 'DNP3.0 Events Setup'
#             if hexstr2 == '0x22':
#                 return 'DNP3.0 Event Setpoints'
#             if hexstr2 == '0x23':
#                 return 'Calibration Registers'
#             if hexstr2 == '0x24':
#                 return 'Date/Time Setup'
#             if hexstr2 == '0x25':
#                 return 'Net Setup'
#             if hexstr2 == '0x30':
#                 return 'IEC60870-5 Setup'
#             if hexstr2 == '0x41':
#                 return 'Test Mode'
#             if hexstr2 == '0x4e':
#                 return 'Firmware Downloaded'
#         else:
#             return ''
#
#     def GetEventEffect(self, i):
#
#         self.ReadInt = i
#         if len(hex(self.ReadInt)) <= 3:  # Blank register
#             return ''
#         else:
#             hexstr = str(hex(self.ReadInt))
#             hexstr1 = str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[2] + hex(self.ReadInt)[3])
#             ID = int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[4] + hex(self.ReadInt)[5]), 16)
#
#         if hexstr == '0x6000':
#             return 'Cleard Energy'
#         elif hexstr == '0x6100':
#             return 'Cleared Max Demand'
#         elif hexstr == '0x6101':
#             return 'Cleared Power Max Demand'
#         elif hexstr == '0x6102':
#             return 'Cleared VOlt/Amp max Demand'
#         elif hexstr == '0x6200':
#             return 'Cleared TOU Energy'
#         elif hexstr == '0x6300':
#             return 'Cleared TOU Max Demand'
#         elif hexstr == '0x6400':
#             return 'Cleared All Counters'
#         elif hexstr1 == '0x64':
#             return 'Cleared Counter ' + str(ID)
#         elif hexstr == '0x6500':
#             return 'Cleared Min/max'
#         elif hexstr1 == '0x6a':
#             return 'Cleared Log ' + str(ID)
#         elif hexstr == '0x6b06':
#             return 'Cleared Communications Counters'
#         elif hexstr1 == '0xf1':
#             return 'Cleared Setpoint ' + str(ID)
#         elif hexstr == '0xf200':
#             return 'Setup 123 Cleared'
#         elif hexstr == '0xf300':
#             return 'Setup Reset'
#         elif hexstr == '0xf400':
#             return 'Setup Changed'
#         elif hexstr == '0xf500':
#             return 'RTC Set'
#         elif hexstr == '0xf600':
#             return 'Enabled'
#         elif hexstr == '0xf700':
#             return 'Disabled'
#         elif hexstr1 == '0xfa':
#             return 'Sucessful'
#         elif hexstr == '0xfb00':
#             return 'No Change'
#         else:
#             return ''


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
