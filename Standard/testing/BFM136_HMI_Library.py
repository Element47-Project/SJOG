# BFM136_HMI_Library Module
# Import pymodbus, config and other modules
from pymodbus.client import ModbusTcpClient
from datetime import datetime, timezone
import os
import sys
import time
import platform
import subprocess
import re


METER_MODELNAME = "BFM136"
METER_SNO = ""
METER_FIRMWARE = ""
METER_BOOT = ""
METER_CTRATIO = ""
METER_SUBNET = ""
METER_GATEWAY = ""

LINE_1 = True
LINE_2 = True
LINE_3 = True


# Meter default addresses
MeterIPAddress = '192.168.1.203' # IP address
MeterNodeAddress = 2 # Modbus slave address

#Network interface
INTERFACE = 'eth0'

client = ModbusTcpClient(host=MeterIPAddress, port=502) 


# Network settings for HMI and meter
class ModbusNetworkConfiguration:
        def __init__(self,interface = INTERFACE):
                self.interface = interface 
                self.system = platform.system()
                self.GetHMINetworkSettings()
                self.GetHMIGatewaySettings()
                self.GetMeterinfo()

        
        # Read current network settings
        def GetHMINetworkSettings(self):
                try:
                        # Get IP and subnet mask
                        if self.system == "Darwin":
                                output = subprocess.check_output(f"ifconfig {self.interface}", shell=True).decode()
                                ip_pattern = r"inet (\d+\.\d+\.\d+\.\d+) netmask (0x[a-fA-F0-9]+)"
                        else:  # Linux
                                output = subprocess.check_output(f"ip addr show {self.interface}", shell=True).decode()
                                ip_pattern = r"inet (\d+\.\d+\.\d+\.\d+)/(\d+)"

                        ip_match = re.search(ip_pattern, output)
                        if ip_match:
                                ip_address = ip_match.group(1)
                                if self.system == "Darwin":
                                        subnet_mask_hex = ip_match.group(2)
                                        subnet_mask = '.'.join([str(int(subnet_mask_hex[i:i+2], 16)) for i in range(2, 10, 2)])
                                else:  # Linux
                                        cidr = int(ip_match.group(2))
                                        subnet_mask = '.'.join([str((0xffffffff << (32 - cidr) >> i) & 0xff) for i in [24, 16, 8, 0]])
                        
                                self.HMIIPAddress = ip_address
                                self.HMISubnet = subnet_mask
                        else:
                                return 'Error: Could not find IP address and subnet mask'
                except: 
                        self.HMIIPAddress = "0.0.0.0"
                        self.HMISubnet = "0.0.0.0"

        def GetHMIGatewaySettings(self):
                try:
                        if self.system == "Darwin":
                                output = subprocess.check_output("netstat -nr | grep default", shell=True).decode()
                                gateway_pattern = r"default\s+(\d+\.\d+\.\d+\.\d+)"
                        else:  # Linux
                                output = subprocess.check_output("ip route | grep default", shell=True).decode()
                                gateway_pattern = r"default via (\d+\.\d+\.\d+\.\d+)"

                        gateway_match = re.search(gateway_pattern, output)
                        if gateway_match:
                                gateway = gateway_match.group(1)
                                self.HMIGateway = gateway
                        else:
                              return 'Error: Could not find gateway'
                except:
                        self.HMIGateway = "0.0.0.0"

        def GetMeterinfo(self):
            global client 
            global MeterIPAddress 
            global MeterNodeAddress
            global METER_SNO, METER_FIRMWARE, METER_BOOT, METER_CTRATIO, METER_SUBNET, METER_GATEWAY
                
            if client.connect():
                try:
                        # Read meter network details
                        Meter_Subnet_Raw = client.read_holding_registers(46578, 2, slave=MeterNodeAddress)
                        Meter_Subnet_oct1 = Meter_Subnet_Raw.getRegister(0) & 255
                        Meter_Subnet_oct2 = Meter_Subnet_Raw.getRegister(0) >> 8
                        Meter_Subnet_oct3 = Meter_Subnet_Raw.getRegister(1) & 255
                        Meter_Subnet_oct4 = Meter_Subnet_Raw.getRegister(1) >> 8
                        
                        global METER_SUBNET
                        
                        METER_SUBNET = str(Meter_Subnet_oct1)+"."+ str(Meter_Subnet_oct2)+"."+str(Meter_Subnet_oct3)+"."+str(Meter_Subnet_oct4)                       
                                
                        Meter_Gateway_Raw = client.read_holding_registers(46580, 2, slave=MeterNodeAddress)
                        
                        Meter_Gateway_oct1 = Meter_Gateway_Raw.getRegister(0) & 255
                        Meter_Gateway_oct2 = Meter_Gateway_Raw.getRegister(0) >> 8
                        Meter_Gateway_oct3 = Meter_Gateway_Raw.getRegister(1) & 255
                        Meter_Gateway_oct4 = Meter_Gateway_Raw.getRegister(1) >> 8
                        
                        global METER_GATEWAY
                        
                        METER_GATEWAY = str(Meter_Gateway_oct1)+"."+ str(Meter_Gateway_oct2)+"."+str(Meter_Gateway_oct3)+"."+str(Meter_Gateway_oct4)                       
                        
                        # Read meter details
                        Meter_Model_Raw = client.read_holding_registers(46080, 26, slave=MeterNodeAddress)
                        
                        # Serial no
                        global  METER_SNO
                        METER_SNO =  (Meter_Model_Raw.getRegister(1) * 65536) + Meter_Model_Raw.getRegister(0)
                        
                        # Firmware and Boot
                        global  METER_FIRMWARE
                        METER_FIRMWARE =  'V'+str(Meter_Model_Raw.getRegister(20)/100)+"."+str(Meter_Model_Raw.getRegister(21))
                        
                        # Boot
                        global  METER_BOOT
                        METER_BOOT = 'V'+str(Meter_Model_Raw.getRegister(24)/100)+"."+str(Meter_Model_Raw.getRegister(25))                                                          
                        
                        # CT Ratio                     
                        global  METER_CTRATIO
                        METER_CTRATIO = client.read_holding_registers(46213, 1, slave=MeterNodeAddress).getRegister(0)
                                                        
                except:
                        METER_MODELNAME = "BFM136"
                        METER_SNO = ""
                        METER_FIRMWARE = ""
                        METER_BOOT = ""
                        METER_CTRATIO = ""
                        METER_SUBNET = ""
                        METER_GATEWAY = ""

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
        def GetMeterInfoStr(self):
                global METER_SUBNET, METER_GATEWAY, METER_CTRATIO, METER_SNO
                global METER_FIRMWARE, METER_BOOT, METER_MODELNAME
                meterinfo = [METER_SUBNET, METER_GATEWAY, METER_CTRATIO,
                        METER_SNO,METER_FIRMWARE,METER_BOOT,METER_MODELNAME]
                return meterinfo
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
        def validIPAddress(self,IP):

                def isIPv4(s):
                        try: return str(int(s)) == s and 0 <= int(s) <= 255
                        except: return False
            
                def isIPv6(s):
                        if len(s) > 4:
                                return False
                        try : return int(s, 16) >= 0 and s[0] != '-'
                        except: return False

                if IP.count(".") == 3 and all(isIPv4(i) for i in IP.split(".")):
                        return True #IPv4
                if IP.count(":") == 7 and all(isIPv6(i) for i in IP.split(":")):
                        return True #"IPv6"
                return False
				
        # Validate modbus address
        def validModbusAddress(Self,node):
                if node.count (".") > 0:
                        return False
                if int(node)  >= 1 and int(node)  <= 247:
                        return True
                else:
                        return False
                
        # Set the network as per user inputs
        def SetNetwork(self, HMIip, HMIsub, HMIgway):
                if self.validIPAddress(HMIip) and self.validIPAddress(HMIsub) and self.validIPAddress(HMIgway):
                        
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
                                                #print("Timeout: Eth0 connection down")
                                                return False

                                os.system('sudo route del default eth0')
                                os.system('sudo route add default gw ' + HMIgway + ' metric 202 eth0')
                                os.system('sudo ifconfig eth0 up')  

                                timeout = time.time() + 15 

                                while not os.popen('ifconfig eth0 | grep -w inet').read().split():
                                        if time.time() > timeout:
                                                #print("Timeout: Eth0 connection down")
                                                return False
                                
                                global client 
                                client = ModbusTcpClient(host=MeterIPAddress, port=502)
                        return False
                return False
        # Set the network as per user inputs
        def SetNetwork2(self, Metip, Metnode):
                if self.validIPAddress(Metip) and self.validModbusAddress(Metnode):
                        global MeterIPAddress
                        MeterIPAddress = Metip
                        global MeterNodeAddress
                        MeterNodeAddress = int(Metnode)
                return False 
        def CloseProgram(self):
                global client
                client.close()
                #Undo comment if to shudown Pi on close
                #os.system("sudo shutdown -h now")
                sys.exit()
                
# Read real Time Measurements from meter
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
                                Meter_Subnet_Raw = client.read_holding_registers(46578, 2, slave=MeterNodeAddress)
                                Meter_Subnet_oct1 = Meter_Subnet_Raw.getRegister(0) & 255
                                Meter_Subnet_oct2 = Meter_Subnet_Raw.getRegister(0) >> 8
                                Meter_Subnet_oct3 = Meter_Subnet_Raw.getRegister(1) & 255
                                Meter_Subnet_oct4 = Meter_Subnet_Raw.getRegister(1) >> 8
                                
                                global METER_SUBNET
                                
                                METER_SUBNET = str(Meter_Subnet_oct1)+"."+ str(Meter_Subnet_oct2)+"."+str(Meter_Subnet_oct3)+"."+str(Meter_Subnet_oct4)                       
                                 
                                Meter_Gateway_Raw = client.read_holding_registers(46580, 2, slave=MeterNodeAddress)
                                
                                Meter_Gateway_oct1 = Meter_Gateway_Raw.getRegister(0) & 255
                                Meter_Gateway_oct2 = Meter_Gateway_Raw.getRegister(0) >> 8
                                Meter_Gateway_oct3 = Meter_Gateway_Raw.getRegister(1) & 255
                                Meter_Gateway_oct4 = Meter_Gateway_Raw.getRegister(1) >> 8
                                
                                global METER_GATEWAY
                                
                                METER_GATEWAY = str(Meter_Gateway_oct1)+"."+ str(Meter_Gateway_oct2)+"."+str(Meter_Gateway_oct3)+"."+str(Meter_Gateway_oct4)                       
                                
                                # Read meter details
                                Meter_Model_Raw = client.read_holding_registers(46080, 26, slave=MeterNodeAddress)
                                
                                # Serial no
                                self.SNO = (Meter_Model_Raw.getRegister(1) * 65536) + Meter_Model_Raw.getRegister(0)
                                global  METER_SNO
                                METER_SNO = self.SNO
                                
                                # Model name. 
                                self.Model_Name = '' # Clear the data before join
                                for x in range(4,12):
                                    if len(hex(Meter_Model_Raw.getRegister(x))) > 3:# Blank register
                                       self.Model_Name = self.Model_Name + self.CHR16Read(Meter_Model_Raw.getRegister(x))
                                global METER_MODELNAME
                                METER_MODELNAME = self.Model_Name
                                
                                # Firmware and Boot
                                self.FirmWare = 'V'+str(Meter_Model_Raw.getRegister(20)/100)+"."+str(Meter_Model_Raw.getRegister(21))
                                global  METER_FIRMWARE
                                METER_FIRMWARE = self.FirmWare
                                
                                # Boot
                                self.Boot = 'V'+str(Meter_Model_Raw.getRegister(24)/100)+"."+str(Meter_Model_Raw.getRegister(25))
                                global  METER_BOOT
                                METER_BOOT = self.Boot                                                          
                                
                                # CT Ratio                     
                                self.CTprimaryCurrent = client.read_holding_registers(46213, 1, slave=MeterNodeAddress).getRegister(0)
                                global  METER_CTRATIO
                                METER_CTRATIO = self.CTprimaryCurrent
                                
                                # PT Ratio
                                PTratio = client.read_holding_registers(46209, 1, slave=MeterNodeAddress).getRegister(0) * 0.1
                                     
                                #Get defined data units
                                U1Factor = 10
                                U2Factor = 100
                                U3Factor = 1000
                                
                                # Read 1-second hhase values
                                raw_data1 = client.read_holding_registers(13952, 36 , slave=MeterNodeAddress)
                                				
                                self.V1Eu = ((raw_data1.getRegister(1) * 65536 ) + raw_data1.getRegister(0)) / U1Factor
                                self.V2Eu = ((raw_data1.getRegister(3) * 65536 ) + raw_data1.getRegister(2)) / U1Factor
                                self.V3Eu = ((raw_data1.getRegister(5) * 65536 ) + raw_data1.getRegister(4)) / U1Factor
                                self.I1Eu = ((raw_data1.getRegister(7) * 65536 ) + raw_data1.getRegister(6)) / U2Factor
                                self.I2Eu = ((raw_data1.getRegister(9) * 65536 ) + raw_data1.getRegister(8)) / U2Factor
                                self.I3Eu = ((raw_data1.getRegister(11) * 65536 ) + raw_data1.getRegister(10)) / U2Factor
                                
                                if raw_data1.getRegister(13) > 32767:
                                        self.kW1Eu = (-65536 + raw_data1.getRegister(12)) / U3Factor
                                else:
                                        self.kW1Eu = ((raw_data1.getRegister(13) * 65536 ) + raw_data1.getRegister(12)) / U3Factor
                                        
                                if raw_data1.getRegister(15) > 32767:
                                        self.kW2Eu = (-65536 + raw_data1.getRegister(14)) / U3Factor
                                else:
                                        self.kW2Eu = ((raw_data1.getRegister(15) * 65536 ) + raw_data1.getRegister(14)) / U3Factor
                                
                                if raw_data1.getRegister(17) > 32767:
                                        self.kW3Eu = (-65536 + raw_data1.getRegister(16)) / U3Factor
                                else:
                                        self.kW3Eu = ((raw_data1.getRegister(17) * 65536 ) + raw_data1.getRegister(16)) / U3Factor
                                
                                if raw_data1.getRegister(19) > 32767:
                                        self.kvar1Eu = (-65536 + raw_data1.getRegister(18)) / U3Factor
                                else:
                                        self.kvar1Eu = ((raw_data1.getRegister(19) * 65536 ) + raw_data1.getRegister(18)) / U3Factor
                                        
                                if raw_data1.getRegister(21) > 32767:
                                        self.kvar2Eu = (-65536 + raw_data1.getRegister(20)) / U3Factor
                                else:
                                        self.kvar2Eu = ((raw_data1.getRegister(21) * 65536 ) + raw_data1.getRegister(20)) / U3Factor
                                
                                if raw_data1.getRegister(23) > 32767:
                                        self.kvar3Eu = (-65536 + raw_data1.getRegister(22)) / U3Factor
                                else:
                                        self.kvar3Eu = ((raw_data1.getRegister(23) * 65536 ) + raw_data1.getRegister(22)) / U3Factor
                                
                                self.kVA1Eu = ((raw_data1.getRegister(25) * 65536 ) + raw_data1.getRegister(24)) / U3Factor
                                self.kVA2Eu = ((raw_data1.getRegister(27) * 65536 ) + raw_data1.getRegister(26)) / U3Factor
                                self.kVA3Eu = ((raw_data1.getRegister(29) * 65536 ) + raw_data1.getRegister(28)) / U3Factor
                                
                                if raw_data1.getRegister(31) > 32767:
                                        self.PF1Eu = (-65536 + raw_data1.getRegister(30)) /1000
                                else:
                                        self.PF1Eu = ((raw_data1.getRegister(31) * 65536 ) + raw_data1.getRegister(30)) /1000
                                
                                if raw_data1.getRegister(33) > 32767:
                                        self.PF2Eu = (-65536 + raw_data1.getRegister(32)) /1000
                                else:
                                        self.PF2Eu = ((raw_data1.getRegister(33) * 65536 ) + raw_data1.getRegister(32)) /1000
                                
                                if raw_data1.getRegister(35) > 32767:
                                        self.PF3Eu = (-65536 + raw_data1.getRegister(34))  /1000
                                else:
                                        self.PF3Eu = ((raw_data1.getRegister(35) * 65536 ) + raw_data1.getRegister(34)) /1000
                                
                                # Read 1-second Total Values
                                raw_data2 = client.read_holding_registers(14336, 8 , slave=MeterNodeAddress)
                                
                                if raw_data2.getRegister(1) > 32767:
                                        self.TotkWEu = (-65536 + raw_data2.getRegister(0)) / U3Factor
                                else:
                                        self.TotkWEu = ((raw_data2.getRegister(1) * 65536 ) + raw_data2.getRegister(0)) / U3Factor
                                
                                if raw_data2.getRegister(3) > 32767:
                                        self.TotkvarEu = (-65536 + raw_data2.getRegister(2)) / U3Factor
                                else:
                                        self.TotkvarEu = ((raw_data2.getRegister(3) * 65536 ) + raw_data2.getRegister(2)) / U3Factor
                                        
                                
                                self.TotkVAEu = ((raw_data2.getRegister(5) * 65536 ) + raw_data2.getRegister(4)) / U3Factor
                                
                                if raw_data2.getRegister(7) > 32767:
                                        self.TotPFEu = (-65536 + raw_data2.getRegister(6)) / U3Factor
                                else:
                                        self.TotPFEu = ((raw_data2.getRegister(7) * 65536 ) + raw_data2.getRegister(6)) / U3Factor
                                
                                
                                ## Read 1-second auxiliary values
                                raw_data3 = client.read_holding_registers(14466, 4 , slave=MeterNodeAddress)
                                
                                self.INEu = ((raw_data3.getRegister(1) * 65536 ) + raw_data3.getRegister(0)) / U2Factor
                                self.FreqEu = ((raw_data3.getRegister(3) * 65536 ) + raw_data3.getRegister(2)) /100
                                
                                raw_data4 = client.read_holding_registers(4648, 7 , slave=MeterNodeAddress)
                                
                                self.V1AngEu = ((raw_data4.getRegister(0) * 360)/9999)- 180
                                self.V2AngEu = ((raw_data4.getRegister(1) * 360)/9999)- 180
                                self.V3AngEu = ((raw_data4.getRegister(2) * 360)/9999)- 180
                                self.I1AngEu = ((raw_data4.getRegister(4) * 360)/9999)- 180
                                self.I2AngEu = ((raw_data4.getRegister(5) * 360)/9999)- 180
                                self.I3AngEu = ((raw_data4.getRegister(6) * 360)/9999)- 180
                                
                                raw_data5 = client.read_holding_registers(14208, 2 , slave=MeterNodeAddress)
                                self.TotVEu = ((raw_data5.getRegister(1) * 65536 ) + raw_data5.getRegister(0)) / U1Factor
                        except:
                                pass
                                
                else:
                        self.SetDefaultValues()
        
        # Convert rawdata into ASCII chars
        def CHR16Read(self,i):
		# Each modbus 16bit word has two ASCII chars and the bytes are transferred in reverse order.
                self.ReadInt = i
                if len(hex(self.ReadInt)) == 4:# One char in the register
                        return chr(int(str(hex(self.ReadInt)[0]+hex(self.ReadInt)[1]+hex(self.ReadInt)[2]+hex(self.ReadInt)[3]),16))
                if len(hex(self.ReadInt)) == 6:# Two chars in the register.
                        Char1 = chr(int(str(hex(self.ReadInt)[0]+hex(self.ReadInt)[1]+hex(self.ReadInt)[4]+hex(self.ReadInt)[5]),16))
                        Char2 = chr(int(str(hex(self.ReadInt)[0]+hex(self.ReadInt)[1]+hex(self.ReadInt)[2]+hex(self.ReadInt)[3]),16))
                        return Char1+Char2
        # Set default values
        def SetDefaultValLine1(self):
                self.V1Eu = 0
                self.I1Eu = 0
                self.kW1Eu = 0
                self.kvar1Eu = 0
                self.kVA1Eu = 0
                self.PF1Eu = 0
                self.V1AngEu = 0
                self.I1AngEu = 0
                self.V1THDEu = 0
                self.I1THDEu = 0
                self.I1TDDEu = 0
#                self.FreqEu = 0
                self.I1Eu = 0
        def SetDefaultValLine2(self):
                self.V2Eu = 0
                self.I2Eu = 0 
                self.kW2Eu = 0
                self.kvar2Eu = 0
                self.kVA2Eu = 0
                self.PF2Eu = 0
                self.V2AngEu = 0
                self.I2AngEu = 0
                self.V2THDEu = 0
                self.I2THDEu = 0
                self.I2TDDEu = 0
 #               self.FreqEu = 0
                self.I2Eu = 0
        def SetDefaultValLine3(self):
                self.V3Eu = 0
                self.I3Eu = 0
                self.kW3Eu = 0
                self.kvar3Eu = 0
                self.kVA3Eu = 0
                self.PF3Eu = 0
                self.V3AngEu = 0
                self.I3AngEu = 0
                self.V3THDEu = 0
                self.I3THDEu = 0
                self.I3TDDEu = 0
  #              self.FreqEu = 0
                self.I3Eu = 0

        #Set default values
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
            self.CTprimaryCurrent =0
            

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
        def __init__(self, start_time = None, end_time = None):
                self.LogFileID = 1
                self.TwoDArray = []
                self.start_time = start_time
                self.end_time = end_time

        def GetDataMatrix(self):
                return self.TwoDArray
        def ReadDatalogger(self, DataLogNo):
                self.TwoDArray = []
                self.LogFileID = DataLogNo
                if self.LogFileID < 1 or self.LogFileID > 16: # Filed ID = 0 is for eventlog, use other function
                   return 

                global client
                if client.connect():

                        #Clear transfer buffer before read
                        client.write_register(63120, 1, slave=MeterNodeAddress)
                        #client.write_register(63121, self.LogFileID, slave=MeterNodeAddress)
                                                
                        #Request file info 
                        client.write_register(64944, 9, slave=MeterNodeAddress)
                        client.write_register(64945, self.LogFileID, slave=MeterNodeAddress)
                        
                        #Read file info
                        FileInfoBlock = client.read_holding_registers(64960, 36, slave=MeterNodeAddress)
                        
                        #Total no of records in the data file
                        TotalRecordNo = FileInfoBlock.getRegister(8)

                        #First record no
                        FirstRecordNo = FileInfoBlock.getRegister(12)

                        #Last record no
                        LastRecordNo = FileInfoBlock.getRegister(13)
                        
                        #Current record no pointed by meter
                        CurrentRecordNo = FileInfoBlock.getRegister(10)
 
                        #Size of data buffer. 8 for data log.
                        FileResponseBlock = client.read_holding_registers(63152, 8, slave=MeterNodeAddress)

                        #No of records in the block
                        BlockRecordNo = FileResponseBlock.getRegister(4)

                        #No of words in each record. 20 for data log
                        RecordSize = FileResponseBlock.getRegister(5)
                   
                        #Set the file position to the oldest record
                        client.write_register(63120, 5, slave=MeterNodeAddress)                        
                        client.write_register(63121, self.LogFileID, slave=MeterNodeAddress)
                        

                        #if start and end time are provided, perform find operation
                        if self.start_time and self.end_time: 
                                print(f'get start time {self.start_time} and end time {self.end_time}')
                                # clear transfer buffer
                                client.write_register(address=63120, value=1, slave=MeterNodeAddress)
                                
                                # Convert to timestamp (seconds since epoch)
                                st_timestamp = ddmmyyyy_to_timestamp(self.start_time)
                                ed_timestamp = ddmmyyyy_to_timestamp(self.end_time)

                                if ed_timestamp >= st_timestamp:
                                        # Split into high and low 16-bit parts
                                        start_bits = timestamp_to_32bit(st_timestamp)
                                        end_bits = timestamp_to_32bit(ed_timestamp)
                                else:
                                        start_bits = timestamp_to_32bit(ed_timestamp)
                                        end_bits = timestamp_to_32bit(st_timestamp)

                                #  Set the file function to 7 (Find)
                                client.write_register(address=63120, value=7, slave=MeterNodeAddress)

                                #  Set the end timestamp (split into two 16-bit registers)
                                client.write_registers(address=63128, values=end_bits, slave=MeterNodeAddress)  

                                #  Perform Find Operation
                                client.write_register(address=63120, value=11, slave=MeterNodeAddress)

                                response = client.read_holding_registers(address=63160, count=1, slave=MeterNodeAddress)
                                if response.isError():
                                        print(f"Error reading response: {response}")
                                        
                                else:
                                        FileInfoBlock = client.read_holding_registers(address=64960, count=36, slave=MeterNodeAddress)
                                        EndTimeToEnd = FileInfoBlock.getRegister(9)
                                        print(f'Found Number of Record from End time till last = {EndTimeToEnd}')

                                #   Set the file function to 7 (Find)
                                client.write_register(address=63120, value=7, slave=MeterNodeAddress)

                                #   Set the start timestamp (split into two 16-bit registers)
                                client.write_registers(address=63128, values=start_bits, slave=MeterNodeAddress)  # Low part of start time

                                #   Perform Find Operation
                                client.write_register(address=63120, value=11, slave=MeterNodeAddress)

                                response = client.read_holding_registers(address=63160, count=1, slave=MeterNodeAddress)
                                if response.isError():
                                        print(f"Error reading response: {response}")
                                        return None

                                else:
                                        FileInfoBlock = client.read_holding_registers(address=64960, count=36, slave=MeterNodeAddress)
                                        StartTimeToEnd = FileInfoBlock.getRegister(9)
                                        print(f'Found Number of Record from start time till last = {StartTimeToEnd}')

                        elif self.start_time and not self.end_time:
                                print(f'start time {self.start_time} and no end time')
                                # clear transfer buffer
                                client.write_register(address=63120, value=1, slave=MeterNodeAddress)
                                
                                # Convert to timestamp (seconds since epoch)
                                st_timestamp = ddmmyyyy_to_timestamp(self.start_time)
                                start_bits = timestamp_to_32bit(st_timestamp)

                                #   Set the file function to 7 (Find)
                                client.write_register(address=63120, value=7, slave=MeterNodeAddress)

                                #   Set the start timestamp (split into two 16-bit registers)
                                client.write_registers(address=63128, values=start_bits, slave=MeterNodeAddress)  # Low part of start time

                                #   Perform Find Operation
                                client.write_register(address=63120, value=11, slave=MeterNodeAddress)

                                response = client.read_holding_registers(address=63160, count=1, slave=MeterNodeAddress)
                                if response.isError():
                                        print(f"Error reading response: {response}")
                                        return None

                                else:
                                        FileInfoBlock = client.read_holding_registers(address=64960, count=36, slave=MeterNodeAddress)
                                        StartTimeToEnd = FileInfoBlock.getRegister(9)
                                        print(f'Found Number of Record from start time till last = {StartTimeToEnd}')
                                        EndTimeToEnd = 0

                        elif not self.start_time and self.end_time:
                                print(f'no start time and end time {self.end_time}')
                                # clear transfer buffer
                                client.write_register(address=63120, value=1, slave=MeterNodeAddress)
                                
                                # Convert to timestamp (seconds since epoch)
                                ed_timestamp = ddmmyyyy_to_timestamp(self.end_time)
                                end_bits = timestamp_to_32bit(ed_timestamp)

                                #   Set the file function to 7 (Find)
                                client.write_register(address=63120, value=7, slave=MeterNodeAddress)

                                #   Set the start timestamp (split into two 16-bit registers)
                                client.write_registers(address=63128, values=end_bits, slave=MeterNodeAddress)  # Low part of start time

                                #   Perform Find Operation
                                client.write_register(address=63120, value=11, slave=MeterNodeAddress)

                                response = client.read_holding_registers(address=63160, count=1, slave=MeterNodeAddress)
                                if response.isError():
                                        print(f"Error reading response: {response}")
                                        return None

                                else:
                                        FileInfoBlock = client.read_holding_registers(address=64960, count=36, slave=MeterNodeAddress)
                                        EndTimeToEnd = FileInfoBlock.getRegister(9)
                                        print(f'Found Number of Record from start time till last = {EndTimeToEnd}')
                                        StartTimeToEnd = TotalRecordNo
                                        #Set the file position to the oldest record
                                        client.write_register(address=63120, value=5, slave=MeterNodeAddress)

                              
                        else:
                                print('no start time no end time')
                                #Set the file position to the oldest record
                                client.write_register(address=63120, value=5, slave=MeterNodeAddress)
                                StartTimeToEnd = TotalRecordNo
                                EndTimeToEnd = 0

                        TimeRangeRecordNo = StartTimeToEnd - EndTimeToEnd
                        print(f'Number of Records fall in time range = {TimeRangeRecordNo}')


                        ReadRecordNo = 1
                        update_interval = 200
                        timer_start = time.time()                        
                        while ReadRecordNo <= TotalRecordNo :
                                DataBufferRegister = 63160 # reset value when going back to for next data block
                                for i in range(0, BlockRecordNo):
                                        #Read current record
                                        ReadRegSet = client.read_holding_registers(DataBufferRegister, RecordSize , slave=MeterNodeAddress)
                                        #Store the record in an array. All 16 parameters are read
                                        ReadRegArray = [ReadRegSet.getRegister(j) for j in range (0, RecordSize)]
                                        
                                        #Convert unix datetimestamp to local time. Adjust daylight savings
                                        TimeCal = ReadRegArray[3] * 65536 + ReadRegArray[2]
                                        dt_object = datetime.fromtimestamp(TimeCal, tz=timezone.utc).strftime('%d/%m/%Y %H:%M:%S ')

                                        if TimeCal == 0 :
                                                continue

                                        if ReadRegSet.getRegister(9) > 32767:                                                      
                                                Para1 = (-65536 + ReadRegSet.getRegister(8)) / 10
                                        else:
                                                Para1 = ((ReadRegSet.getRegister(9) * 65536 ) + ReadRegSet.getRegister(8)) / 10

                                        if ReadRegSet.getRegister(11) > 32767:                                                      
                                                Para2 = (-65536 + ReadRegSet.getRegister(10)) / 10
                                        else:
                                                Para2 = ((ReadRegSet.getRegister(11) * 65536 ) + ReadRegSet.getRegister(10)) / 10


                                        if ReadRegSet.getRegister(13) > 32767:                                                      
                                                Para3 = (-65536 + ReadRegSet.getRegister(12)) / 10
                                        else:
                                                Para3 = ((ReadRegSet.getRegister(13) * 65536 ) + ReadRegSet.getRegister(12)) / 10

                                        if ReadRegSet.getRegister(15) > 32767:                                                      
                                                Para4 = (-65536 + ReadRegSet.getRegister(14)) / 10
                                        else:
                                                Para4 = ((ReadRegSet.getRegister(15) * 65536 ) + ReadRegSet.getRegister(14)) / 10

                                        if ReadRegSet.getRegister(17) > 32767:                                                      
                                                Para5 = (-65536 + ReadRegSet.getRegister(16)) / 1000
                                        else:
                                                Para5 = ((ReadRegSet.getRegister(17) * 65536 ) + ReadRegSet.getRegister(16)) / 1000
                                                
                                        if ReadRegSet.getRegister(19) > 32767:                                                      
                                                Para6 = (-65536 + ReadRegSet.getRegister(18)) / 1000
                                        else:
                                                Para6 = ((ReadRegSet.getRegister(19) * 65536 ) + ReadRegSet.getRegister(18)) / 1000
                                                                                        
                                        #Copy required columns into another array. Adjust scaling as required.
                                        CustomDataArray = [ str(ReadRecordNo),str(dt_object),Para1,Para2,Para3,Para4,Para5,Para6]
                                        self.TwoDArray.insert(ReadRecordNo, CustomDataArray)
                                        DataBufferRegister = DataBufferRegister + RecordSize 
                                        ReadRecordNo = ReadRecordNo + 1

                                        if ReadRecordNo % update_interval == 0:
                                                elasped_time = time.time() - timer_start
                                                estimated_time = elasped_time * (TimeRangeRecordNo / ReadRecordNo)
                                                print(f'Progress: {ReadRecordNo}/{TimeRangeRecordNo} ({ReadRecordNo/TimeRangeRecordNo*100:.1f}%)')
                                                print(f'estimated time remaining: {estimated_time - elasped_time:.2f}s')


                                #Clear the buffer for next read                
                                client.write_register(63120, 1, slave=MeterNodeAddress)
                                #client.write_register(63121, self.LogFileID, slave=MeterNodeAddress)

                        if TimeRangeRecordNo !=0:
                                total_used_time = time.time() - timer_start
                                print(f'123 log finished, used time {total_used_time}')

                else:
                        self.TwoDArray = []

#Read logged data from event log		
class EventLogger:
        def __init__(self):
                self.LogFileID = 0 # FileID for event log is always 0
                self.TwoDArray = []
        def GetDataMatrix(self):
                #print("Length of Eventlog Array:"+ str(len(self.TwoDArray)))
                #print (self.TwoDArray)
                return self.TwoDArray
        def ReadEventlogger(self):
                global client
                if client.connect():

                        #Clear Buffer
                        client.write_register(63120, 1, slave=MeterNodeAddress)
                        #client.write_register(63121, self.LogFileID , slave=MeterNodeAddress)#Not Working
                        
                        #Request file info
                        client.write_register(64944, 9, slave=MeterNodeAddress)
                        client.write_register(64945, self.LogFileID, slave=MeterNodeAddress)
                        
                        #Read file info
                        FileInfoBlock = client.read_holding_registers(64960, 36, slave=MeterNodeAddress)
                        
                        #Total no of records in the data file
                        TotalRecordNo = FileInfoBlock.getRegister(8)

                        #First record no
                        FirstRecordNo = FileInfoBlock.getRegister(12)

                        #Last record no
                        LastRecordNo = FileInfoBlock.getRegister(13)
                        
                        #Current record no pointed by meter
                        CurrentRecordNo = FileInfoBlock.getRegister(10)
                        
                        #Size of data buffer
                        FileResponseBlock = client.read_holding_registers(63152, 8, slave=MeterNodeAddress)

                        #No of words in each record. 12 for event log
                        RecordSize = FileResponseBlock.getRegister(5)
   
                        #Set the position at the first record
                        client.write_register(63120, 5, slave=MeterNodeAddress)
                        client.write_register(63121, self.LogFileID, slave=MeterNodeAddress)

                        ReadRecordNo = 1
                        while ReadRecordNo <= TotalRecordNo :
                                DataBufferRegister = 63160 # reset value when going back to for next data block
                                for i in range(0, FileResponseBlock.getRegister(4)):

                                        if ReadRecordNo <= TotalRecordNo:
                                                #Read current reacord data
                                                ReadRegSet = client.read_holding_registers(DataBufferRegister, RecordSize , slave=MeterNodeAddress)
						#Store the record in an array. All 16 parameters are read
                                                ReadRegArray = [ReadRegSet.getRegister(j) for j in range (0, RecordSize)]
                                                
						#Convert unix datetimestamp to local time. Adjust daylight savings
                                                TimeCal = ReadRegArray[3] * 65536 + ReadRegArray[2]
                                                dt_object = datetime.utcfromtimestamp(TimeCal).strftime('%d/%m/%Y %H:%M:%S ')
  												
						#Copy required columns into another array
                                                CustomDataArray = [str(ReadRecordNo),str(dt_object), self.GetEventCause(ReadRegSet.getRegister(7)),self.GetEventSource(ReadRegSet.getRegister(7)),self.GetEventEffect(ReadRegSet.getRegister(8)),'','','','']
                                                                     
                                                #print(ReadRegArray)
                                                #print(CustomDataArray)
                                                self.TwoDArray.insert(ReadRecordNo, CustomDataArray)
                                                DataBufferRegister = DataBufferRegister + RecordSize 
                                                ReadRecordNo = ReadRecordNo + 1
                                client.write_register(63120, 1, slave=MeterNodeAddress)
                                #client.write_register(63121, self.LogFileID, slave=MeterNodeAddress)
                else:
                        self.TwoDArray = []

        def GetEventCause(self, i):

            self.ReadInt = i

            if len(hex(self.ReadInt)) <= 3:# Blank register
                return ''
            else:
                hexstr = str(hex(self.ReadInt)[0])+str(hex(self.ReadInt)[1])+str(hex(self.ReadInt)[2])+str(hex(self.ReadInt)[3])
           
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
            if len(hex(self.ReadInt)) <= 3:# Blank register
                return ''
            else:
                hexstr = str(hex(self.ReadInt))
                hexstr1 = str(hex(self.ReadInt)[0])+str(hex(self.ReadInt)[1])+str(hex(self.ReadInt)[2])+str(hex(self.ReadInt)[3])
                hexstr2 = str(hex(self.ReadInt)[0])+str(hex(self.ReadInt)[1])+str(hex(self.ReadInt)[4])+str(hex(self.ReadInt)[5])

            if hexstr1 == '0x62' or hexstr1 == '0x63' :
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

            if hexstr1 != '0x62' and hexstr1 != '0x63' :

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
            if len(hex(self.ReadInt)) <= 3:# Blank register
                return ''
            else:
                hexstr = str(hex(self.ReadInt))
                hexstr1 = str(hex(self.ReadInt)[0]+ hex(self.ReadInt)[1] + hex(self.ReadInt)[2] + hex(self.ReadInt)[3])
                ID = int(str(hex(self.ReadInt)[0]+ hex(self.ReadInt)[1] + hex(self.ReadInt)[4] + hex(self.ReadInt)[5]),16)
                              
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

#Collect monthly billing data
class Billing():
   def __init__(self, *args, **kwargs):
           pass

   #Read datalog 1 and process the data
   def UpdateBillData(self):

       # Return default values when client is disconnected       
       global client
       if not client.connect():
               self.CurMonBillData = [0,0,0,0,0,0]
               self.PreMonBillData = [0,0,0,0,0,0]
               return

       self.DataLoggerInstance = DataLogger()
       
       self.DataLoggerInstance.ReadDatalogger(1)
       self.DataLog1Array = self.DataLoggerInstance.GetDataMatrix()

       self.TodayMonth = datetime.today().month

       self.CurMonBillData = self.GetData(self.TodayMonth, self.DataLog1Array)
       self.PreMonBillData = self.GetData(self.TodayMonth-1, self.DataLog1Array)

   #Return current month data to display
   def getCurMonBillData(self):
           return self.CurMonBillData

   #Return previous month data to display
   def getPreMonBillData(self):
           return self.PreMonBillData

   #Find the first and last records for current month and previous month
   def GetData(self, monthVar, LogDataArray):
             
       EmptyDataArray =[0,0,0,0,0,0]

       try:
               FirstRecord = self.GetRecordData(monthVar, LogDataArray)
               LastRecord = self.GetRecordData(monthVar, reversed(LogDataArray))

               return [float(LastRecord[3])-float(FirstRecord[3]),
               float(LastRecord[4])-float(FirstRecord[4]),
               float(LastRecord[5])-float(FirstRecord[5]),
               self.GetMaxReading(monthVar, LogDataArray, 6),
               self.GetMaxReading(monthVar, LogDataArray, 7),
               float(LastRecord[2])-float(FirstRecord[2])]
       except:
               return EmptyDataArray
                

   #Get record info from datalog                    
   def GetRecordData(self, forMonth, DataLog):
       for Record in DataLog:
           if datetime.strptime(Record[1], '%d/%m/%Y %H:%M:%S ').month == forMonth :
               return Record

   #get maximum values from the data
   def GetMaxReading(self, forMonth, DataLog, index):
       TempArray = []
       for Record in DataLog:
           if datetime.strptime(Record[1], '%d/%m/%Y %H:%M:%S ').month == forMonth :
                   TempArray.append(float(Record[index]))
       return max(TempArray)

#List the submeter assignments here
def GetNodeConnection(Node):

        if not Node:
                return
        global LINE_1
        global LINE_2
        global LINE_3
        global MeterNodeAddress
        MeterNodeAddress = int(Node)

        if int(Node) == 1:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 2:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 3:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True

        if int(Node) == 4:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 5:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 6:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True

        if int(Node) == 7:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 8:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 9:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True

        if int(Node) == 10:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 11:
                LINE_1 = True
                LINE_2 = True
                LINE_3 = True
        if int(Node) == 12:
                LINE_1 = False
                LINE_2 = True
                LINE_3 = False




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
