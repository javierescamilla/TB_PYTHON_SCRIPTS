from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import pandas as pd
import time
from datetime import datetime
import math

fiveMinutesInMs = 300000
oneDayInMs = 86400
df = pd.read_excel('Production/BitacoraSopladorBeneficio.xlsx', names=["Date", "Inicio", "Fin", "Toneladas"], sheet_name='Sol 1')
df_2 = pd.read_excel('Production/BitacoraSopladorBeneficio.xlsx', names=["Date", "Inicio", "Fin", "Toneladas"], sheet_name='Sol 2')
CSVFile = pd.read_csv("MYeBOXReports/Soplador beneficio seco.csv", sep=';', skiprows=range(0,2), names=["timestamp", "Instant power", "Consumption"])
prodDf = pd.DataFrame(columns=['timestamp', 'ton_sol1'])
prodDf_2 = pd.DataFrame(columns=['timestamp', 'ton_sol2'])

client = TBDeviceMqttClient("34.231.242.52", "spTJtL2WYtvyCtoTBwo4")
client.connect()
results = []
result = True

def strTimeToMsInteger(time):
    hours, minutes, seconds = (["0", "0"] + time.split(":"))[-3:]
    hours = int(hours)
    minutes =  int(minutes)
    return int(3600000 * hours + 60000 * minutes)/1000

currentIndex = 0

for index, row in df.iterrows():
    strInitTime = row['Inicio'].strftime("%H:%M:%S %p")
    strEndTime = row['Fin'].strftime("%H:%M:%S %p")
    unixTime = time.mktime(row['Date'].timetuple())
    strInitTime = strTimeToMsInteger(strInitTime) + unixTime
    strEndTime = strTimeToMsInteger(strEndTime) + unixTime
    
    if (strEndTime >= strInitTime):
        timeTaken = strEndTime - strInitTime
    elif(strEndTime < strInitTime):
        # This means that strEndTime cross to the next day.
        strEndTime += oneDayInMs
        timeTaken = strEndTime - strInitTime

    timeTaken = strEndTime - strInitTime

    fiveMinutesIntervalsTaken = timeTaken*1000 / fiveMinutesInMs

    for index in range(int(fiveMinutesIntervalsTaken)):
        prodDf.loc[currentIndex + index, 'timestamp'] = (strInitTime + (fiveMinutesInMs/1000) * index)
        # Conversion to int

        prodDf.loc[currentIndex + index, 'ton_sol1'] = row['Toneladas'] / fiveMinutesIntervalsTaken
        currentIndex = currentIndex + 1

for index, row in df_2.iterrows():
    strInitTime = row['Inicio'].strftime("%H:%M:%S %p")
    strEndTime = row['Fin'].strftime("%H:%M:%S %p")
    unixTime = time.mktime(row['Date'].timetuple())
    strInitTime = strTimeToMsInteger(strInitTime) + unixTime
    strEndTime = strTimeToMsInteger(strEndTime) + unixTime
    
    if (strEndTime >= strInitTime):
        timeTaken = strEndTime - strInitTime
    elif(strEndTime < strInitTime):
        # This means that strEndTime cross to the next day.
        strEndTime += oneDayInMs
        timeTaken = strEndTime - strInitTime

    timeTaken = strEndTime - strInitTime

    fiveMinutesIntervalsTaken = timeTaken*1000 / fiveMinutesInMs

    for index in range(int(fiveMinutesIntervalsTaken)):
        prodDf_2.loc[currentIndex + index, 'timestamp'] = (strInitTime + (fiveMinutesInMs/1000) * index)
        # Conversion to int

        prodDf_2.loc[currentIndex + index, 'ton_sol2'] = row['Toneladas'] / fiveMinutesIntervalsTaken
        currentIndex = currentIndex + 1

for index, row in CSVFile.iterrows():
    CSVFile.loc[index ,'timestamp'] = (time.mktime(datetime.strptime(row['timestamp'], "%m/%d/%y %I:%M:%S %p").timetuple()))


resultDataFrame = pd.merge(CSVFile, prodDf, on='timestamp', how='outer')
fullResultDataFrame = pd.merge(resultDataFrame, prodDf_2, on='timestamp', how='outer')
fullResultDataFrame.to_csv("results/AllDetails.csv", index=False)

for index, row in fullResultDataFrame.iterrows():
    unixTime = row['timestamp']
    instantPower = row['Instant power']
    consumption = row['Consumption']
    tons_sol_1 = row['ton_sol1']
    tons_sol_2 = row['ton_sol2']

    # Solo kWh
    if(not math.isnan(instantPower) and math.isnan(tons_sol_1) and math.isnan(tons_sol_2)):
        print(consumption)
    # kWh y ton1
    elif(not math.isnan(instantPower) and not math.isnan(tons_sol_1) and math.isnan(tons_sol_2)):
        ide_sol1 = consumption * tons_sol_1
        print(consumption, tons_sol_1, ide_sol1)
    # kWh y ton2
    elif(not math.isnan(instantPower) and math.isnan(tons_sol_1) and not math.isnan(tons_sol_2)):
        ide_sol2 = consumption * tons_sol_2
        print(consumption, tons_sol_2, ide_sol2)
    # Solo ton1
    elif(math.isnan(instantPower) and not math.isnan(tons_sol_1) and math.isnan(tons_sol_2)):
        print(tons_sol_1)
    # Solo ton2
    elif(math.isnan(instantPower) and math.isnan(tons_sol_1) and not math.isnan(tons_sol_2)):
        print(tons_sol_2)

    #telemetry_with_ts = {"ts": unixTime*1000, "values": {"kWh": consumption, "kW":instantPower, "ton_s1":tons_sol_1, "ton_s2":tons_sol_2}}
    #results.append(client.send_telemetry(telemetry_with_ts))

# for tmp_result in results:
#     result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
# print("Result", str(result))
# client.disconnect()