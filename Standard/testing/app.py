from flask import Flask, render_template, jsonify, request, redirect
import EM133XM_HMI_Library as HMI_133Library
import BFM136_HMI_Library as HMI_136Library
from datetime import datetime

# import time
METER = 'EM133XM'
# METER = 'BFM136'

app = Flask(__name__, static_url_path='/flaskapp/static')
Meter_Model = ''


@app.route('/flaskapp')
def home():
    # Call the function from your library script to fetch data for billing
    billing_data = 'home page'
    return render_template('home.html', data=billing_data)


# Route to render the engineering page (HTML)
@app.route('/flaskapp/engineering')
def engineering():
    return render_template('engineering.html')


# Route to serve real-time data as JSON
@app.route('/flaskapp/get_real_time_data')
def get_real_time_data():
    if METER == 'BFM136':
        RealTimeData = HMI_136Library.RealTimeMeasurement()
    else:
        RealTimeData = HMI_133Library.RealTimeMeasurement()

    RealData = RealTimeData.DataArray()

    real_data = {
        'line1': {
            'V': RealData['V1Eu'],
            'I': RealData['I1Eu'],
            'KW': RealData['kW1Eu'],
            'KVAR': RealData['kvar1Eu'],
            'KVA': RealData['kVA1Eu'],
            'PF': RealData['PF1Eu'],
            'V_ANGLE': RealData['V1AngEu'],
            'I_ANGLE': RealData['I1AngEu'],
            'V_THD': RealData['V1THDEu'],
            'I_THD': RealData['I1THDEu'],
            'I_TDD': RealData['I1TDDEu'],
            'FREQ': RealData['FreqEu']
        },
        'line2': {
            'V': RealData['V2Eu'],
            'I': RealData['I2Eu'],
            'KW': RealData['kW2Eu'],
            'KVAR': RealData['kvar2Eu'],
            'KVA': RealData['kVA2Eu'],
            'PF': RealData['PF2Eu'],
            'V_ANGLE': RealData['V2AngEu'],
            'I_ANGLE': RealData['I2AngEu'],
            'V_THD': RealData['V2THDEu'],
            'I_THD': RealData['I2THDEu'],
            'I_TDD': RealData['I2TDDEu'],
            'FREQ': RealData['FreqEu']
        },
        'line3': {
            'V': RealData['V3Eu'],
            'I': RealData['I3Eu'],
            'KW': RealData['kW3Eu'],
            'KVAR': RealData['kvar3Eu'],
            'KVA': RealData['kVA3Eu'],
            'PF': RealData['PF3Eu'],
            'V_ANGLE': RealData['V3AngEu'],
            'I_ANGLE': RealData['I3AngEu'],
            'V_THD': RealData['V3THDEu'],
            'I_THD': RealData['I3THDEu'],
            'I_TDD': RealData['I3TDDEu'],
            'FREQ': RealData['FreqEu']
        },
        'total_values': {
            'V_TOTAL': RealData['TotVEu'],
            'I1': RealData['I1Eu'],
            'I2': RealData['I2Eu'],
            'I3': RealData['I3Eu'],
            'KW_TOTAL': RealData['TotkWEu'],
            'KVAR_TOTAL': RealData['TotkvarEu'],
            'KVA_TOTAL': RealData['TotkVAEu'],
            'PF_TOTAL': RealData['TotPFEu'],
            'IN': RealData['INEu'],
            'FREQ': RealData['FreqEu']
        }
    }

    return jsonify(real_data)


@app.route('/flaskapp/history')
def history():
    return render_template('history.html')


# Route to get data logger information
@app.route('/flaskapp/datalogger/<int:DataLoggerNo>')
def Datalogger(DataLoggerNo):
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    format_start = format_date(start_date)
    format_end = format_date(end_date)
    DspLogName = "DATALOG" + str(DataLoggerNo)

    try:
        if METER == 'BFM136':
            DataLoggerInstance = HMI_136Library.DataLogger(start_time=format_start, end_time=format_end)
            modbus_config = HMI_136Library.ModbusNetworkConfiguration()
        else:
            DataLoggerInstance = HMI_133Library.DataLogger(start_time=format_start, end_time=format_end)
            modbus_config = HMI_133Library.ModbusNetworkConfiguration()

        DataLoggerInstance.ReadDatalogger(DataLoggerNo)
        TwoDArray = DataLoggerInstance.GetDataMatrix()
        print(TwoDArray)
        IP = modbus_config.GetMeterIPAddressStr()
        node = modbus_config.GetMeterNodeAddressStr()

        response = {
            'log_name': DspLogName,
            'ipaddress': IP,
            'nodeno': node,
            'data': TwoDArray
        }
    except Exception as e:
        print(f"Error fetching DataLogger {DataLoggerNo}: {str(e)}")
        response = {
            'log_name': DspLogName,
            'ipaddress': "Unknown",
            'nodeno': "Unknown",
            'data': []
        }

    return jsonify(response)


# Route to get event logger information
@app.route('/flaskapp/eventlogger')
def Eventlogger():
    DspLogName = "EVENT LOG"

    # Create DataLogger instance and retrieve the data
    if METER == 'BFM136':
        EventLoggerInstance = HMI_136Library.EventLogger()
    else:
        EventLoggerInstance = HMI_133Library.EventLogger()

    EventLoggerInstance.ReadEventlogger()
    TwoDArray = EventLoggerInstance.GetDataMatrix()

    response = {
        'log_name': DspLogName,
        'data': TwoDArray
    }

    return jsonify(response)


# Function to retrieve network configuration details
def get_network_config(modbus_config):
    hmi_ip = modbus_config.GetHMIIPAddressStr()
    hmi_subnet = modbus_config.GetHMISubnetStr()
    hmi_gateway = modbus_config.GetHMIGatewayStr()
    meter_ip = modbus_config.GetMeterIPAddressStr()
    meter_node = modbus_config.GetMeterNodeAddressStr()
    meter_info = modbus_config.GetMeterInfoStr()
    meter_subnet = meter_info[0]
    meter_gateway = meter_info[1]
    ct_ratio = meter_info[2]
    serial_no = meter_info[3]
    firmware = meter_info[4]
    boot = meter_info[5]
    meter_name = meter_info[6]

    return {
        'hmi_ip': hmi_ip,
        'hmi_subnet': hmi_subnet,
        'hmi_gateway': hmi_gateway,
        'meter_ip': meter_ip,
        'meter_node': meter_node,
        'meter_subnet': meter_subnet,
        'meter_gateway': meter_gateway,
        'ct_ratio': ct_ratio,
        'serial_no': serial_no,
        'firmware': firmware,
        'boot': boot,
        'model_name': meter_name
    }


@app.route('/flaskapp/settings', methods=['GET', 'POST'])
def settings():
    if METER == 'BFM136':
        modbus_config = HMI_136Library.ModbusNetworkConfiguration()
    else:
        modbus_config = HMI_133Library.ModbusNetworkConfiguration()

    if request.method == 'POST':
        # Retrieve parent and child from the request
        data = request.get_json()
        new_ip = data.get('parent')
        new_node = data.get('child')[5:]
        print(new_ip, new_node)
        modbus_config.SetNetwork2(Metip=new_ip, Metnode=new_node)
        network_config = get_network_config(modbus_config)

        return jsonify(network_config)

    # Handle GET request to render the full settings page
    network_config = get_network_config(modbus_config)

    return render_template('settings.html', **network_config)


def format_date(date_str):
    if date_str:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj.strftime('%d/%m/%Y')
    return None


@app.route('/')
def redirect_to_flaskapp():
    return redirect('/flaskapp')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
