from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import pandas as pd
import time
import datetime
"Current L1", "Current L2", "Current L3"
CSVFile = pd.read_csv("MYeBOXReports/demoschettinoespejo.csv", sep=';', skiprows=range(0,2), names=["Date", "Consumption", "Current L1", "Current L2", "Current L3", "Instant power", "PF", "Voltage L1", "Voltage L2", "Voltage L3"])
client = TBDeviceMqttClient("34.231.242.52", "b3kcu7IU2yRcbkMUwfRV")
client.connect()
results = []
result = True

# "%-d/%-m/%y %-I:%M:%S %p"
for index, row in CSVFile.iterrows():
    unixTime = time.mktime(datetime.datetime.strptime(row['Date'], "%m/%d/%y %I:%M:%S %p").timetuple())
    telemetry_with_ts = {"ts": unixTime*1000, "values": {"kWh": row['Consumption'], "kW":row['Instant power'], "voltage_l1":row["Voltage L1"], "voltage_l2":row["Voltage L2"], "voltage_l3":row["Voltage L3"], "current_l1":row["Current L1"], "current_l2":row["Current L2"], "current_l3":row["Current L3"], "power_factor":row["PF"]}}
    results.append(client.send_telemetry(telemetry_with_ts))

for tmp_result in results:
    result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
print("Result", str(result))
client.disconnect()