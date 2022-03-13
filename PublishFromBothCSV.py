from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import pandas as pd
import time
import datetime

CSVFile_1 = pd.read_csv("MYeBOXReports/Torres de enfriamiento sol1.csv", sep=';', skiprows=range(0,2), names=["Date", "Instant power", "Consumption"])
CSVFile_2 = pd.read_csv("MYeBOXReports/General beneficio seco.csv", sep=';', skiprows=range(0,2), names=["Date", "Instant power", "Consumption"])

client = TBDeviceMqttClient("34.231.242.52", "sLJTEs1QMEa9QFOcD2xF")
client.connect()
results = []
result = True

for i in range(0, len(CSVFile_1.index)):
    for j in range(0, len(CSVFile_1.columns)):
        if (j == 0):
            unixTime = time.mktime(datetime.datetime.strptime(CSVFile_1.values[i,j], "%m/%d/%y %I:%M:%S %p").timetuple())
        elif (j == 1):
            consumption = CSVFile_2.values[i,j] - CSVFile_1.values[i,j]
        elif (j == 2):
            instantPower = CSVFile_2.values[i,j] - CSVFile_1.values[i,j]

    print(unixTime, consumption, instantPower)
    telemetry_with_ts = {"ts": unixTime*1000, "values": {"kWh": consumption, "kW":instantPower}}
    results.append(client.send_telemetry(telemetry_with_ts))

for tmp_result in results:
    result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
print("Result", str(result))
client.disconnect()