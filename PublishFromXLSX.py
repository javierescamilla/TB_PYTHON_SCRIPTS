from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import pandas as pd
import time
import datetime

# Position of the column in consumptionFile
DATE_COLUMN = 0
CONSUMPTION_COLUMN = 7
MONEY_COLUMN = 8

XLSXFile = pd.read_excel("MYeBOXReports/AGUA_HELADA_S1.xlsx", names=["Date", "ignore", "ignore2", "ignore3", "ignore4", "ignore5", "ignore6", "Consumption", "money"], sheet_name='Export')

client = TBDeviceMqttClient("34.231.242.52", "uHoHupnBNJWVeZj4uCeE")
client.connect()
results = []
result = True

for index in range(0, len(XLSXFile.index)):

    timestamp = int(time.mktime(datetime.datetime.strptime(str(XLSXFile.values[index,DATE_COLUMN]), "%Y-%m-%d %H:%M:%S").timetuple()))
    power = 0
    consumption = XLSXFile.values[index, CONSUMPTION_COLUMN]
    telemetry_with_ts = {"ts": timestamp*1000, "values": {"kWh": consumption, "kW":power}}
    results.append(client.send_telemetry(telemetry_with_ts))

for tmp_result in results:
    result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
print("Result", str(result))
client.disconnect()