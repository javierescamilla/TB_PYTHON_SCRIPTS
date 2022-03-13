from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import pandas as pd
import time
import datetime

CSVFile = pd.read_csv("MYeBOXReports/chillerdecaf_0.csv", sep=';', skiprows=range(0,2), names=["Date", "Instant power", "Consumption"])
client = TBDeviceMqttClient("34.231.242.52", "TfkIPYYazkGS7zpENhkh")
client.connect()
results = []
result = True

# "%-d/%-m/%y %-I:%M:%S %p"
for index, row in CSVFile.iterrows():
    unixTime = time.mktime(datetime.datetime.strptime(row['Date'], "%m/%d/%y %I:%M:%S %p").timetuple())
    instantPower = row['Instant power']
    consumption = row['Consumption']
    telemetry_with_ts = {"ts": unixTime*1000, "values": {"kWh": consumption, "kW":instantPower}}
    results.append(client.send_telemetry(telemetry_with_ts))

    #print("Time: ", unixTime, "Power: ", instantPower, "kWh: ", consumption)

for tmp_result in results:
    result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
print("Result", str(result))
client.disconnect()