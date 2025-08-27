# EM133XM_HMI_Library Module
# Import pymodbus, config and other modules
from pymodbus.client import ModbusTcpClient
from datetime import datetime, timedelta
import os
import sys
import time

METER_MODELNAME = "EM133XM"
METER_SNO = ""
METER_FIRMWARE = ""
METER_BOOT = ""
METER_CTRATIO = ""
METER_SUBNET = ""
METER_GATEWAY = ""

# Meter default addresses
MeterIPAddress = '10.173.200.1'  # IP address
MeterNodeAddress = 2  # Modbus slave address

client = ModbusTcpClient(host=MeterIPAddress, port=205)


# Network settings for HMI and meter
class ModbusNetworkConfiguration:
    def __init__(self):
        pass

    # Read current network settings
    def GetHMINetworkSettings(self):
        try:
            ifconfigStr = os.popen('ifconfig eth0 | grep -w inet').read().split()
            self.HMIIPAddress = ifconfigStr[1]
            self.HMISubnet = ifconfigStr[3]
        except:
            self.HMIIPAddress = "0.0.0.0"
            self.HMISubnet = "0.0.0.0"

    def GetHMIGatewaySettings(self):
        try:
            ifconfigStr = os.popen('route | grep default | grep eth0').read().split()
            self.HMIGateway = ifconfigStr[1]
        except:
            self.HMIGateway = "0.0.0.0"

    # Display on HMI as strings
    def GetHMIIPAddressStr(self):
        self.GetHMINetworkSettings()
        return str(self.HMIIPAddress)

    def GetHMISubnetStr(self):
        self.GetHMINetworkSettings()
        return str(self.HMISubnet)

    def GetHMIGatewayStr(self):
        self.GetHMIGatewaySettings()
        return str(self.HMIGateway)

    def GetMeterIPAddressStr(self):
        global MeterIPAddress
        return str(MeterIPAddress)

    def GetMeterNodeAddressStr(self):
        global MeterNodeAddress
        return str(MeterNodeAddress)

    # Split into octets for user inputs
    def GetHMIIPAddressArray(self):
        self.GetHMINetworkSettings()
        return self.HMIIPAddress.split(".")

    def GetHMISubnetArray(self):
        self.GetHMINetworkSettings()
        return self.HMISubnet.split(".")

    def GetHMIGatewayArray(self):
        self.GetHMIGatewaySettings()
        return self.HMIGateway.split(".")

    def GetMeterIPAddressArray(self):
        global MeterIPAddress
        return MeterIPAddress.split(".")

    # Validate IP address
    def validIPAddress(self, IP):

        def isIPv4(s):
            try:
                return str(int(s)) == s and 0 <= int(s) <= 255
            except:
                return False

        def isIPv6(s):
            if len(s) > 4:
                return False
            try:
                return int(s, 16) >= 0 and s[0] != '-'
            except:
                return False

        if IP.count(".") == 3 and all(isIPv4(i) for i in IP.split(".")):
            return True  # IPv4
        if IP.count(":") == 7 and all(isIPv6(i) for i in IP.split(":")):
            return True  # "IPv6"
        return False

    # Validate modbus address
    def validModbusAddress(Self, node):
        if node.count(".") > 0:
            return False
        if int(node) >= 1 and int(node) <= 247:
            return True
        else:
            return False

    # Set the network as per user inputs
    def SetNetwork(self, HMIip, HMIsub, HMIgway, Metip, Metnode):
        if self.validIPAddress(HMIip) and self.validIPAddress(HMIsub) and self.validIPAddress(
                HMIgway) and self.validIPAddress(Metip) and self.validModbusAddress(Metnode):

            # Set HMI address and subnet
            if os.system('sudo ethtool eth0 | grep \'Link detected: yes\'') == 0:

                os.system('sudo ifconfig eth0 down')
                os.system('sudo ifconfig eth0 ' + HMIip)
                os.system('sudo ifconfig eth0 netmask ' + HMIsub)
                os.system('sudo ifconfig eth0 up')

                # Setting gateway address to Pi takes bit of time, thats why time delays are introduced.
                timeout = time.time() + 15
                while not os.popen('route | grep default | grep eth0').read().split():
                    if time.time() > timeout:
                        # print("Timeout: Eth0 connection down")
                        return False

                os.system('sudo route del default eth0')
                os.system('sudo route add default gw ' + HMIgway + ' metric 202 eth0')
                os.system('sudo ifconfig eth0 up')

                timeout = time.time() + 15

                while not os.popen('ifconfig eth0 | grep -w inet').read().split():
                    if time.time() > timeout:
                        # print("Timeout: Eth0 connection down")
                        return False

                global MeterIPAddress
                MeterIPAddress = Metip
                global MeterNodeAddress
                MeterNodeAddress = int(Metnode)

                global client
                client = ModbusTcpClient(host=MeterIPAddress, port=502)

            return False
        return False

    def CloseProgram(self):
        global client
        client.close()
        # Undo comment if to shudown Pi on close
        # os.system("sudo shutdown -h now")
        sys.exit()


# Read real time measurement from meter
class RealTimeMeasurement:
    def __init__(self):
        self.SetDefaultValues()
        self.ReadData()

        # Read data

    def ReadData(self):
        global client
        global MeterIPAddress
        global MeterNodeAddress

        if not client.connect():
            self.SetDefaultValues()
            return

        if client.connect():
            try:

                # Read meter network details
                Meter_Subnet_Raw = client.read_holding_registers(46578, 2, unit=MeterNodeAddress)
                Meter_Subnet_oct1 = Meter_Subnet_Raw.getRegister(0) & 255
                Meter_Subnet_oct2 = Meter_Subnet_Raw.getRegister(0) >> 8
                Meter_Subnet_oct3 = Meter_Subnet_Raw.getRegister(1) & 255
                Meter_Subnet_oct4 = Meter_Subnet_Raw.getRegister(1) >> 8

                global METER_SUBNET

                METER_SUBNET = str(Meter_Subnet_oct1) + "." + str(Meter_Subnet_oct2) + "." + str(
                    Meter_Subnet_oct3) + "." + str(Meter_Subnet_oct4)

                Meter_Gateway_Raw = client.read_holding_registers(46580, 2, unit=MeterNodeAddress)

                Meter_Gateway_oct1 = Meter_Gateway_Raw.getRegister(0) & 255
                Meter_Gateway_oct2 = Meter_Gateway_Raw.getRegister(0) >> 8
                Meter_Gateway_oct3 = Meter_Gateway_Raw.getRegister(1) & 255
                Meter_Gateway_oct4 = Meter_Gateway_Raw.getRegister(1) >> 8

                global METER_GATEWAY

                METER_GATEWAY = str(Meter_Gateway_oct1) + "." + str(Meter_Gateway_oct2) + "." + str(
                    Meter_Gateway_oct3) + "." + str(Meter_Gateway_oct4)

                # Read meter details
                Meter_Model_Raw = client.read_holding_registers(46080, 31, unit=MeterNodeAddress)

                # Serial no
                self.SNO = (Meter_Model_Raw.getRegister(1) * 65536) + Meter_Model_Raw.getRegister(0)

                global METER_SNO
                METER_SNO = self.SNO

                # Model name.
                self.Model_Name = ''  # Clear the data before join
                for x in range(4, 12):
                    if len(hex(Meter_Model_Raw.getRegister(x))) > 3:  # Blank register
                        self.Model_Name = self.Model_Name + self.CHR16Read(Meter_Model_Raw.getRegister(x))

                global METER_MODELNAME
                METER_MODELNAME = self.Model_Name

                self.FirmWare = 'V' + str(Meter_Model_Raw.getRegister(20) / 100) + "." + str(
                    Meter_Model_Raw.getRegister(21))

                global METER_FIRMWARE
                METER_FIRMWARE = self.FirmWare

                self.Boot = 'V' + str(Meter_Model_Raw.getRegister(24) / 100) + "." + str(
                    Meter_Model_Raw.getRegister(25))

                global METER_BOOT
                METER_BOOT = self.Boot

                # Get defined scales used in 16 bit reads.
                HighRawScale = client.read_holding_registers(241, 1, unit=MeterNodeAddress).getRegister(0)
                LowRawScale = client.read_holding_registers(240, 1, unit=MeterNodeAddress).getRegister(0)
                RawScaleRange = HighRawScale - LowRawScale
                VoltageScale = client.read_holding_registers(242, 1, unit=MeterNodeAddress).getRegister(0)
                CurrentScale = client.read_holding_registers(243, 1, unit=MeterNodeAddress).getRegister(0)

                # Get CT and PT Ratios
                self.CTprimaryCurrent = client.read_holding_registers(2306, 1, unit=MeterNodeAddress).getRegister(0)
                PTratio = client.read_holding_registers(2305, 1, unit=MeterNodeAddress).getRegister(0) * 0.1

                global METER_CTRATIO
                METER_CTRATIO = self.CTprimaryCurrent

                # Calculate eng scale max and min
                Pmax = (VoltageScale * PTratio) * (self.CTprimaryCurrent * 2) * 2
                Vmax = VoltageScale * PTratio
                Imax = self.CTprimaryCurrent * 2

                # Get defined data resolution - 0=Low, 1=High
                DeviceResolution = client.read_holding_registers(2390, 1, unit=MeterNodeAddress).getRegister(0)

                # set default values for data resolution
                U1Factor = 1
                U2Factor = 1
                U3Factor = 1
                # High resolution
                if DeviceResolution == 1:
                    U2Factor = 100
                    if PTratio > 1.0:
                        U1Factor = 1
                        U3Factor = 1
                    else:
                        U1Factor = 10
                        U3Factor = 1000

                # Low resolution
                if DeviceResolution == 0:
                    U1Factor = 1
                    U2Factor = 1
                    U3Factor = 1

                # Read 1-second hhase values
                raw_data1 = client.read_holding_registers(13952, 65, unit=MeterNodeAddress)

                self.V1Eu = ((raw_data1.getRegister(1) * 65536) + raw_data1.getRegister(0)) / U1Factor
                self.V2Eu = ((raw_data1.getRegister(3) * 65536) + raw_data1.getRegister(2)) / U1Factor
                self.V3Eu = ((raw_data1.getRegister(5) * 65536) + raw_data1.getRegister(4)) / U1Factor
                self.I1Eu = ((raw_data1.getRegister(7) * 65536) + raw_data1.getRegister(6)) / U2Factor
                self.I2Eu = ((raw_data1.getRegister(9) * 65536) + raw_data1.getRegister(8)) / U2Factor
                self.I3Eu = ((raw_data1.getRegister(11) * 65536) + raw_data1.getRegister(10)) / U2Factor

                if raw_data1.getRegister(13) > 32767:
                    self.kW1Eu = (-65536 + raw_data1.getRegister(12)) / U3Factor
                else:
                    self.kW1Eu = ((raw_data1.getRegister(13) * 65536) + raw_data1.getRegister(12)) / U3Factor

                if raw_data1.getRegister(15) > 32767:
                    self.kW2Eu = (-65536 + raw_data1.getRegister(14)) / U3Factor
                else:
                    self.kW2Eu = ((raw_data1.getRegister(15) * 65536) + raw_data1.getRegister(14)) / U3Factor

                if raw_data1.getRegister(17) > 32767:
                    self.kW3Eu = (-65536 + raw_data1.getRegister(16)) / U3Factor
                else:
                    self.kW3Eu = ((raw_data1.getRegister(17) * 65536) + raw_data1.getRegister(16)) / U3Factor

                if raw_data1.getRegister(19) > 32767:
                    self.kvar1Eu = (-65536 + raw_data1.getRegister(18)) / U3Factor
                else:
                    self.kvar1Eu = ((raw_data1.getRegister(19) * 65536) + raw_data1.getRegister(18)) / U3Factor

                if raw_data1.getRegister(21) > 32767:
                    self.kvar2Eu = (-65536 + raw_data1.getRegister(20)) / U3Factor
                else:
                    self.kvar2Eu = ((raw_data1.getRegister(21) * 65536) + raw_data1.getRegister(20)) / U3Factor

                if raw_data1.getRegister(23) > 32767:
                    self.kvar3Eu = (-65536 + raw_data1.getRegister(22)) / U3Factor
                else:
                    self.kvar3Eu = ((raw_data1.getRegister(23) * 65536) + raw_data1.getRegister(22)) / U3Factor

                self.kVA1Eu = ((raw_data1.getRegister(25) * 65536) + raw_data1.getRegister(24)) / U3Factor
                self.kVA2Eu = ((raw_data1.getRegister(27) * 65536) + raw_data1.getRegister(26)) / U3Factor
                self.kVA3Eu = ((raw_data1.getRegister(29) * 65536) + raw_data1.getRegister(28)) / U3Factor

                if raw_data1.getRegister(31) > 32767:
                    self.PF1Eu = (-65536 + raw_data1.getRegister(30)) / 1000
                else:
                    self.PF1Eu = ((raw_data1.getRegister(31) * 65536) + raw_data1.getRegister(30)) / 1000

                if raw_data1.getRegister(33) > 32767:
                    self.PF2Eu = (-65536 + raw_data1.getRegister(32)) / 1000
                else:
                    self.PF2Eu = ((raw_data1.getRegister(33) * 65536) + raw_data1.getRegister(32)) * 1000

                if raw_data1.getRegister(35) > 32767:
                    self.PF3Eu = (-65536 + raw_data1.getRegister(34)) * 1000
                else:
                    self.PF3Eu = ((raw_data1.getRegister(35) * 65536) + raw_data1.getRegister(34)) * 1000

                self.V1THDEu = ((raw_data1.getRegister(37) * 65536) + raw_data1.getRegister(36)) / 10
                self.V2THDEu = ((raw_data1.getRegister(39) * 65536) + raw_data1.getRegister(38)) / 10
                self.V3THDEu = ((raw_data1.getRegister(41) * 65536) + raw_data1.getRegister(40)) / 10

                self.I1THDEu = ((raw_data1.getRegister(43) * 65536) + raw_data1.getRegister(42)) / 10
                self.I2THDEu = ((raw_data1.getRegister(45) * 65536) + raw_data1.getRegister(44)) / 10
                self.I3THDEu = ((raw_data1.getRegister(47) * 65536) + raw_data1.getRegister(46)) / 10

                self.I1TDDEu = ((raw_data1.getRegister(55) * 65536) + raw_data1.getRegister(54)) / 10
                self.I2TDDEu = ((raw_data1.getRegister(57) * 65536) + raw_data1.getRegister(56)) / 10
                self.I3TDDEu = ((raw_data1.getRegister(59) * 65536) + raw_data1.getRegister(58)) / 10

                # Read 1-second Total Values
                raw_data2 = client.read_holding_registers(14336, 26, unit=MeterNodeAddress)

                if raw_data2.getRegister(1) > 32767:
                    self.TotkWEu = (-65536 + raw_data2.getRegister(0)) / U3Factor
                else:
                    self.TotkWEu = ((raw_data2.getRegister(1) * 65536) + raw_data2.getRegister(0)) / U3Factor

                if raw_data2.getRegister(3) > 32767:
                    self.TotkvarEu = (-65536 + raw_data2.getRegister(2)) / U3Factor
                else:
                    self.TotkvarEu = ((raw_data2.getRegister(3) * 65536) + raw_data2.getRegister(2)) / U3Factor

                self.TotkVAEu = ((raw_data2.getRegister(5) * 65536) + raw_data2.getRegister(4)) / U3Factor

                if raw_data2.getRegister(7) > 32767:
                    self.TotPFEu = (-65536 + raw_data2.getRegister(6)) / U3Factor
                else:
                    self.TotPFEu = ((raw_data2.getRegister(7) * 65536) + raw_data2.getRegister(6)) / U3Factor

                self.TotVEu = ((raw_data2.getRegister(21) * 65536) + raw_data2.getRegister(20)) / U1Factor

                ## Read 1-second auxiliary values
                raw_data3 = client.read_holding_registers(14466, 4, unit=MeterNodeAddress)

                self.INEu = ((raw_data3.getRegister(1) * 65536) + raw_data3.getRegister(0)) / U2Factor
                self.FreqEu = ((raw_data3.getRegister(3) * 65536) + raw_data3.getRegister(2)) / 100

                raw_data4 = client.read_holding_registers(7324, 7, unit=MeterNodeAddress)

                self.V1AngEu = ((raw_data4.getRegister(0) * 360) / 9999) - 180
                self.V2AngEu = ((raw_data4.getRegister(1) * 360) / 9999) - 180
                self.V3AngEu = ((raw_data4.getRegister(2) * 360) / 9999) - 180
                self.I1AngEu = ((raw_data4.getRegister(4) * 360) / 9999) - 180
                self.I2AngEu = ((raw_data4.getRegister(5) * 360) / 9999) - 180
                self.I3AngEu = ((raw_data4.getRegister(6) * 360) / 9999) - 180

            except:
                pass

        else:

            self.SetDefaultValues()

    # Convert rawdata into ASCII chars
    def CHR16Read(self, i):
        # Each modbus 16bit word has two ASCII chars and the bytes are transferred in reverse order.
        self.ReadInt = i
        if len(hex(self.ReadInt)) == 4:  # One char in the register
            return chr(
                int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[2] + hex(self.ReadInt)[3]), 16))
        if len(hex(self.ReadInt)) == 6:  # Two chars in the register.
            Char1 = chr(
                int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[4] + hex(self.ReadInt)[5]), 16))
            Char2 = chr(
                int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[2] + hex(self.ReadInt)[3]), 16))
            return Char1 + Char2

    # Set default values
    def SetDefaultValues(self):
        self.V1Eu = 0
        self.V2Eu = 0
        self.V3Eu = 0
        self.I1Eu = 0
        self.I2Eu = 0
        self.I3Eu = 0
        self.kW1Eu = 0
        self.kW2Eu = 0
        self.kW3Eu = 0
        self.kvar1Eu = 0
        self.kvar2Eu = 0
        self.kvar3Eu = 0
        self.kVA1Eu = 0
        self.kVA2Eu = 0
        self.kVA3Eu = 0
        self.PF1Eu = 0
        self.PF2Eu = 0
        self.PF3Eu = 0
        self.V1THDEu = 0
        self.V2THDEu = 0
        self.V3THDEu = 0
        self.I1THDEu = 0
        self.I2THDEu = 0
        self.I3THDEu = 0
        self.I1TDDEu = 0
        self.I2TDDEu = 0
        self.I3TDDEu = 0
        self.V1AngEu = 0
        self.V2AngEu = 0
        self.V3AngEu = 0
        self.I1AngEu = 0
        self.I2AngEu = 0
        self.I3AngEu = 0
        self.TotkWEu = 0
        self.TotkvarEu = 0
        self.TotkVAEu = 0
        self.TotPFEu = 0
        self.INEu = 0
        self.TotVEu = 0
        self.TotFreqEu = 0
        self.FreqEu = 0
        self.Model_Name = ''
        self.SNO = 0
        self.FirmWare = ''
        self.Boot = ''
        self.CTprimaryCurrent = 0

    # Pack data into Array using dictionary for easy use
    def DataArray(self):

        self.RealTime_Data = {}
        self.RealTime_Data['V1Eu'] = str(self.V1Eu)
        self.RealTime_Data['V2Eu'] = str(self.V2Eu)
        self.RealTime_Data['V2Eu'] = str(self.V2Eu)
        self.RealTime_Data['V3Eu'] = str(self.V3Eu)
        self.RealTime_Data['I1Eu'] = str(self.I1Eu)
        self.RealTime_Data['I2Eu'] = str(self.I2Eu)
        self.RealTime_Data['I3Eu'] = str(self.I3Eu)
        self.RealTime_Data['kW1Eu'] = str(self.kW1Eu)
        self.RealTime_Data['kW2Eu'] = str(self.kW2Eu)
        self.RealTime_Data['kW3Eu'] = str(self.kW3Eu)
        self.RealTime_Data['kvar1Eu'] = str(self.kvar1Eu)
        self.RealTime_Data['kvar2Eu'] = str(self.kvar2Eu)
        self.RealTime_Data['kvar3Eu'] = str(self.kvar3Eu)
        self.RealTime_Data['kVA1Eu'] = str(self.kVA1Eu)
        self.RealTime_Data['kVA2Eu'] = str(self.kVA2Eu)
        self.RealTime_Data['kVA3Eu'] = str(self.kVA3Eu)
        self.RealTime_Data['PF1Eu'] = str(self.PF1Eu)
        self.RealTime_Data['PF2Eu'] = str(self.PF2Eu)
        self.RealTime_Data['PF3Eu'] = str(self.PF3Eu)
        self.RealTime_Data['V1THDEu'] = str(self.V1THDEu)
        self.RealTime_Data['V2THDEu'] = str(self.V2THDEu)
        self.RealTime_Data['V3THDEu'] = str(self.V3THDEu)
        self.RealTime_Data['I1THDEu'] = str(self.I1THDEu)
        self.RealTime_Data['I2THDEu'] = str(self.I2THDEu)
        self.RealTime_Data['I3THDEu'] = str(self.I3THDEu)
        self.RealTime_Data['I1TDDEu'] = str(self.I1TDDEu)
        self.RealTime_Data['I2TDDEu'] = str(self.I2TDDEu)
        self.RealTime_Data['I3TDDEu'] = str(self.I3TDDEu)
        self.RealTime_Data['V1AngEu'] = str("%.3f" % self.V1AngEu)
        self.RealTime_Data['V2AngEu'] = str("%.3f" % self.V2AngEu)
        self.RealTime_Data['V3AngEu'] = str("%.3f" % self.V3AngEu)
        self.RealTime_Data['I1AngEu'] = str("%.3f" % self.I1AngEu)
        self.RealTime_Data['I2AngEu'] = str("%.3f" % self.I2AngEu)
        self.RealTime_Data['I3AngEu'] = str("%.3f" % self.I3AngEu)
        self.RealTime_Data['TotkWEu'] = str(self.TotkWEu)
        self.RealTime_Data['TotkvarEu'] = str(self.TotkvarEu)
        self.RealTime_Data['TotkVAEu'] = str(self.TotkVAEu)
        self.RealTime_Data['TotPFEu'] = str(self.TotPFEu)
        self.RealTime_Data['TotVEu'] = str(self.TotVEu)
        self.RealTime_Data['INEu'] = str(self.INEu)
        self.RealTime_Data['FreqEu'] = str(self.FreqEu)
        return self.RealTime_Data


# Read logged data from data logs		
class DataLogger:
    def __init__(self):
        self.LogFileID = 1
        self.TwoDArray = []

    def GetDataMatrix(self):
        return self.TwoDArray

    def ReadDatalogger(self, DataLogNo):
        self.TwoDArray = []
        self.LogFileID = DataLogNo
        if self.LogFileID < 1 or self.LogFileID > 16:  # Filed ID = 0 is for eventlog, use other function
            return

        global client
        if client.connect():

            # Clear transfer buffer
            client.write_register(63120, 1, unit=MeterNodeAddress)

            # Request file info
            client.write_register(64944, 9, unit=MeterNodeAddress)
            client.write_register(64945, self.LogFileID, unit=MeterNodeAddress)

            # Read file info
            FileInfoBlock = client.read_holding_registers(64960, 36, unit=MeterNodeAddress)

            # Total no of records in the data file
            TotalRecordNo = FileInfoBlock.getRegister(8)

            # First record no
            FirstRecordNo = FileInfoBlock.getRegister(12)

            # Last record no
            LastRecordNo = FileInfoBlock.getRegister(13)

            # Current record no pointed by meter
            CurrentRecordNo = FileInfoBlock.getRegister(10)

            # Size of data buffer. 8 for data log.
            FileResponseBlock = client.read_holding_registers(63152, 8, unit=MeterNodeAddress)

            # No of records in the block
            BlockRecordNo = FileResponseBlock.getRegister(4)

            # No of words in each record. 40 for data log
            RecordSize = FileResponseBlock.getRegister(5)

            # Set the file position to the oldest record
            client.write_register(63120, 5, unit=MeterNodeAddress)
            client.write_register(63121, self.LogFileID, unit=MeterNodeAddress)

            ReadRecordNo = 1
            while ReadRecordNo <= TotalRecordNo:
                print("I am inside the while loop")
                DataBufferRegister = 63160  # reset value when going back to for next data block
                for i in range(0, BlockRecordNo):
                    if ReadRecordNo <= TotalRecordNo:
                        # Read current record
                        ReadRegSet = client.read_holding_registers(DataBufferRegister, RecordSize,
                                                                   unit=MeterNodeAddress)
                        # Store the record in an array. All 16 parameters are read
                        ReadRegArray = [ReadRegSet.getRegister(j) for j in range(0, RecordSize)]

                        # Convert unix datetimestamp to local time. Adjust daylight savings
                        TimeCal = ReadRegArray[3] * 65536 + ReadRegArray[2]
                        dt_object = datetime.utcfromtimestamp(TimeCal).strftime('%d/%m/%Y %H:%M:%S ')

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

                        # Copy required columns into another array. Adjust scaling as required.
                        CustomDataArray = [str(ReadRecordNo), str(dt_object), Para1, Para2, Para3, Para4, Para5, Para6]
                        self.TwoDArray.insert(ReadRecordNo, CustomDataArray)
                        DataBufferRegister = DataBufferRegister + RecordSize
                        ReadRecordNo = ReadRecordNo + 1

                # Clear the buffer for next read
                client.write_register(63120, 1, unit=MeterNodeAddress)

        else:
            self.TwoDArray = []


# Read logged data from event log
class EventLogger:
    def __init__(self):
        self.LogFileID = 0  # FileID for event log is always 0
        self.TwoDArray = []

    def GetDataMatrix(self):
        # print("Length of Eventlog Array:"+ str(len(self.TwoDArray)))
        # print (self.TwoDArray)
        return self.TwoDArray

    def ReadEventlogger(self):
        global client
        if client.connect():

            # Clear Buffer
            client.write_register(63120, 1, unit=MeterNodeAddress)
            client.write_register(63121, self.LogFileID, unit=MeterNodeAddress)  # Not Working

            # Request file info
            client.write_register(64944, 9, unit=MeterNodeAddress)
            rq = client.write_register(64945, self.LogFileID, unit=MeterNodeAddress)
            if rq.isError():
                print("rq error")
                return 0

            # Read file info
            FileInfoBlock = client.read_holding_registers(64960, 36, unit=MeterNodeAddress)

            # Total no of records in the data file
            TotalRecordNo = FileInfoBlock.getRegister(8)

            # First record no
            FirstRecordNo = FileInfoBlock.getRegister(12)

            # Last record no
            LastRecordNo = FileInfoBlock.getRegister(13)

            # Current record no pointed by meter
            CurrentRecordNo = FileInfoBlock.getRegister(10)

            # Size of data buffer
            FileResponseBlock = client.read_holding_registers(63152, 8, unit=MeterNodeAddress)

            # No of words in each record. 12 for event log
            RecordSize = FileResponseBlock.getRegister(5)

            # Set the position at the first record
            client.write_register(63120, 5, unit=MeterNodeAddress)
            client.write_register(63121, self.LogFileID, unit=MeterNodeAddress)

            ReadRecordNo = 1
            while ReadRecordNo <= TotalRecordNo:
                DataBufferRegister = 63160  # reset value when going back to for next data block
                for i in range(0, FileResponseBlock.getRegister(4)):

                    if ReadRecordNo <= TotalRecordNo:
                        # Read current reacord data
                        ReadRegSet = client.read_holding_registers(DataBufferRegister, RecordSize,
                                                                   unit=MeterNodeAddress)
                        # Store the record in an array. All 16 parameters are read
                        ReadRegArray = [ReadRegSet.getRegister(j) for j in range(0, RecordSize)]

                        # Convert unix datetimestamp to local time. Adjust daylight savings
                        TimeCal = ReadRegArray[3] * 65536 + ReadRegArray[2]
                        dt_object = datetime.utcfromtimestamp(TimeCal).strftime('%d/%m/%Y %H:%M:%S ')

                        # Copy required columns into another array
                        CustomDataArray = [str(ReadRecordNo), str(dt_object),
                                           self.GetEventCause(ReadRegSet.getRegister(7)),
                                           self.GetEventSource(ReadRegSet.getRegister(7)),
                                           self.GetEventEffect(ReadRegSet.getRegister(8)), '', '', '', '']

                        # print(ReadRegArray)
                        # print(CustomDataArray)
                        self.TwoDArray.insert(ReadRecordNo, CustomDataArray)
                        DataBufferRegister = DataBufferRegister + RecordSize
                        ReadRecordNo = ReadRecordNo + 1
                client.write_register(63120, 1, unit=MeterNodeAddress)
                client.write_register(63121, self.LogFileID, unit=MeterNodeAddress)
        else:
            self.TwoDArray = [['0'], ['0'], ['0'], ['0'], ['0'], ['0'], ['0'], ['0'], ['0']]

    def GetEventCause(self, i):

        self.ReadInt = i

        if len(hex(self.ReadInt)) <= 3:  # Blank register
            return ''
        else:
            hexstr = str(hex(self.ReadInt)[0]) + str(hex(self.ReadInt)[1]) + str(hex(self.ReadInt)[2]) + str(
                hex(self.ReadInt)[3])

        if hexstr == '0x5b':
            return 'COMM'
        elif hexstr == '0x5c':
            return 'Front Panel'
        elif hexstr == '0x5d':
            return 'Selfcheck'
        elif hexstr == '0x62':
            return 'Hardware'
        elif hexstr == '0x63':
            return 'External'
        else:
            return ''

    def GetEventSource(self, i):

        self.ReadInt = i
        if len(hex(self.ReadInt)) <= 3:  # Blank register
            return ''
        else:
            hexstr = str(hex(self.ReadInt))
            hexstr1 = str(hex(self.ReadInt)[0]) + str(hex(self.ReadInt)[1]) + str(hex(self.ReadInt)[2]) + str(
                hex(self.ReadInt)[3])
            hexstr2 = str(hex(self.ReadInt)[0]) + str(hex(self.ReadInt)[1]) + str(hex(self.ReadInt)[4]) + str(
                hex(self.ReadInt)[5])

        if hexstr1 == '0x62' or hexstr1 == '0x63':
            if hexstr == '0x6202':
                return 'RAM Error'
            if hexstr == '0x6203':
                return 'HW WDOG Reset'
            if hexstr == '0x6204':
                return 'Sampling Fault'
            if hexstr == '0x6205':
                return 'CPU Exception'
            if hexstr == '0x6207':
                return 'SW WDOG Reset'
            if hexstr == '0x620d':
                return 'Low Battery'
            if hexstr == '0x620f':
                return 'EEPROM Fault'
            if hexstr == '0x6300':
                return 'Power Down'
            if hexstr == '0x6308':
                return 'Power Up'
            if hexstr == '0x6309':
                return 'External Reset'

        if hexstr1 != '0x62' and hexstr1 != '0x63':

            if hexstr2 == '0x03':
                return '123 memory'
            if hexstr2 == '0x04':
                return 'Factory Setup'
            if hexstr2 == '0x05':
                return 'Password Setup'
            if hexstr2 == '0x06':
                return 'Basic Setup'
            if hexstr2 == '0x07':
                return 'Comms Setup'
            if hexstr2 == '0x08':
                return 'Real Time Clock'
            if hexstr2 == '0x09':
                return 'Digital Inputs Setup'
            if hexstr2 == '0x0a':
                return 'Pulse Counters Setup'
            if hexstr2 == '0x0b':
                return 'AO Setup'
            if hexstr2 == '0x0e':
                return 'TImers Setup'
            if hexstr2 == '0x10':
                return 'Setpoints Setup'
            if hexstr2 == '0x11':
                return 'Pulsing Setup'
            if hexstr2 == '0x12':
                return 'User Rigester Map Setup'
            if hexstr2 == '0x14':
                return 'Datalog Setup'
            if hexstr2 == '0x15':
                return 'Memory Setup'
            if hexstr2 == '0x16':
                return 'TOU Registers Setup'
            if hexstr2 == '0x18':
                return 'TOU Daily Setup'
            if hexstr2 == '0x19':
                return 'TOU Calender Setup'
            if hexstr2 == '0x1b':
                return 'RO Setup'
            if hexstr2 == '0x1c':
                return 'User Selectable Options Setup'
            if hexstr2 == '0x1f':
                return 'DNP3.0 Class 0 Map'
            if hexstr2 == '0x20':
                return 'DNP3.0 Options Setup'
            if hexstr2 == '0x21':
                return 'DNP3.0 Events Setup'
            if hexstr2 == '0x22':
                return 'DNP3.0 Event Setpoints'
            if hexstr2 == '0x23':
                return 'Calibration Registers'
            if hexstr2 == '0x24':
                return 'Date/Time Setup'
            if hexstr2 == '0x25':
                return 'Net Setup'
            if hexstr2 == '0x30':
                return 'IEC60870-5 Setup'
            if hexstr2 == '0x41':
                return 'Test Mode'
            if hexstr2 == '0x4e':
                return 'Firmware Downloaded'
        else:
            return ''

    def GetEventEffect(self, i):

        self.ReadInt = i
        if len(hex(self.ReadInt)) <= 3:  # Blank register
            return ''
        else:
            hexstr = str(hex(self.ReadInt))
            hexstr1 = str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[2] + hex(self.ReadInt)[3])
            ID = int(str(hex(self.ReadInt)[0] + hex(self.ReadInt)[1] + hex(self.ReadInt)[4] + hex(self.ReadInt)[5]), 16)

        if hexstr == '0x6000':
            return 'Cleard Energy'
        elif hexstr == '0x6100':
            return 'Cleared Max Demand'
        elif hexstr == '0x6101':
            return 'Cleared Power Max Demand'
        elif hexstr == '0x6102':
            return 'Cleared VOlt/Amp max Demand'
        elif hexstr == '0x6200':
            return 'Cleared TOU Energy'
        elif hexstr == '0x6300':
            return 'Cleared TOU Max Demand'
        elif hexstr == '0x6400':
            return 'Cleared All Counters'
        elif hexstr1 == '0x64':
            return 'Cleared Counter ' + str(ID)
        elif hexstr == '0x6500':
            return 'Cleared Min/max'
        elif hexstr1 == '0x6a':
            return 'Cleared Log ' + str(ID)
        elif hexstr == '0x6b06':
            return 'Cleared Communications Counters'
        elif hexstr1 == '0xf1':
            return 'Cleared Setpoint ' + str(ID)
        elif hexstr == '0xf200':
            return 'Setup 123 Cleared'
        elif hexstr == '0xf300':
            return 'Setup Reset'
        elif hexstr == '0xf400':
            return 'Setup Changed'
        elif hexstr == '0xf500':
            return 'RTC Set'
        elif hexstr == '0xf600':
            return 'Enabled'
        elif hexstr == '0xf700':
            return 'Disabled'
        elif hexstr1 == '0xfa':
            return 'Sucessful'
        elif hexstr == '0xfb00':
            return 'No Change'
        else:
            return ''


# Collect billing data
class Billing():
    def __init__(self, *args, **kwargs):
        pass

    # Read datalogs and process the billing data
    def UpdateBillData(self):

        # Return default values when client is disconnected
        global client
        if not client.connect():
            self.Line1_CurMon = [0, 0, 0, 0, 0, 0]
            self.Line2_CurMon = [0, 0, 0, 0, 0, 0]
            self.Line3_CurMon = [0, 0, 0, 0, 0, 0]
            self.Phase_CurMon = [0, 0, 0, 0, 0, 0]
            self.Line1_PrevMon = [0, 0, 0, 0, 0, 0]
            self.Line2_PrevMon = [0, 0, 0, 0, 0, 0]
            self.Line3_PrevMon = [0, 0, 0, 0, 0, 0]
            self.Phase_PrevMon = [0, 0, 0, 0, 0, 0]
            return

        self.DataLoggerInstance = DataLogger()

        self.DataLoggerInstance.ReadDatalogger(1)
        self.TwoDArray_Log_1 = self.DataLoggerInstance.GetDataMatrix()
        self.DataLoggerInstance.ReadDatalogger(2)
        self.TwoDArray_Log_2 = self.DataLoggerInstance.GetDataMatrix()
        self.DataLoggerInstance.ReadDatalogger(3)
        self.TwoDArray_Log_3 = self.DataLoggerInstance.GetDataMatrix()

        self.TodayMonth = datetime.today().month

        self.Line1_CurMon = self.GetData(self.TodayMonth, self.TwoDArray_Log_1)
        self.Line1_PrevMon = self.GetData(self.TodayMonth - 1, self.TwoDArray_Log_1)

        self.Line2_CurMon = self.GetData(self.TodayMonth, self.TwoDArray_Log_2)
        self.Line2_PrevMon = self.GetData(self.TodayMonth - 1, self.TwoDArray_Log_2)

        self.Line3_CurMon = self.GetData(self.TodayMonth, self.TwoDArray_Log_3)
        self.Line3_PrevMon = self.GetData(self.TodayMonth - 1, self.TwoDArray_Log_3)

    def getLine1CurMon(self):
        return self.Line1_CurMon

    def getLine1PrevMon(self):
        return self.Line1_PrevMon

    def getLine2CurMon(self):
        return self.Line2_CurMon

    def getLine2PrevMon(self):
        return self.Line2_PrevMon

    def getLine3CurMon(self):
        return self.Line3_CurMon

    def getLine3PrevMon(self):
        return self.Line3_PrevMon

    def getPhaseCurMon(self):

        self.Phase_CurMon = [0, 0, 0, 0, 0, 0]

        for i in range(0, len(self.Line1_CurMon)):
            self.Phase_CurMon.append(self.Line1_CurMon[i] + self.Line2_CurMon[i] + self.Line3_CurMon[i])
        return self.Phase_CurMon

    def getPhasePrevMon(self):

        self.Phase_PrevMon = [0, 0, 0, 0, 0, 0]

        for i in range(0, len(self.Line1_PrevMon)):
            self.Phase_PrevMon.append(self.Line1_PrevMon[i] + self.Line2_PrevMon[i] + self.Line3_PrevMon[i])
        return self.Phase_PrevMon

    def GetData(self, monthVar, LogDataArray):

        EmptyDataArray = [0, 0, 0, 0, 0, 0]

        try:
            FirstOccurrence = self.GetRecordData(monthVar, LogDataArray)
            LastOccurrence = self.GetRecordData(monthVar, reversed(LogDataArray))

            return [float(LastOccurrence[3]) - float(FirstOccurrence[3]),
                    float(LastOccurrence[4]) - float(FirstOccurrence[4]),
                    float(LastOccurrence[5]) - float(FirstOccurrence[5]),
                    self.GetMaxReading(monthVar, LogDataArray, 6),
                    self.GetMaxReading(monthVar, LogDataArray, 8),
                    float(LastOccurrence[2]) - float(FirstOccurrence[2])]

        except:
            return EmptyDataArray

    def GetRecordData(self, forMonth, DataLog):

        for Record in DataLog:
            if datetime.strptime(Record[1], '%d/%m/%Y %H:%M:%S ').month == forMonth:
                return Record

    def GetMaxReading(self, forMonth, DataLog, index):
        TempArray = []
        for Record in DataLog:
            if datetime.strptime(Record[1], '%d/%m/%Y %H:%M:%S ').month == forMonth:
                TempArray.append(float(Record[index]))

        return max(TempArray)
