from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
from datetime import datetime
import pandas as pd
import time
import math

# Position of the column in consumptionFile
DATE_COLUMN = 0
CONSUMPTION_COLUMN = 7
MONEY_COLUMN = 8

# Position of the column in productionFile
TON_SOL_1 = 1
TON_SOL_2 = 2
TON_TOTAL = 3

# Production relation
toSol1 = True
toSol2 = False

# Settings
publishTelemetry = True
storeTelemetry = True

# TB settings
client = TBDeviceMqttClient("34.231.242.52", "RfjHlGa4mTaGaZkCN6k0")
client.connect()
results = []
result = True

consumptionFile = pd.read_excel("MYeBOXReports/TorreSol1.xlsx",names=["Date", "consumption_base", "consumption_inter", "consumption_peak", "money_base", "money_intermediate", "money_peak", "consumption", "money"], sheet_name='Export')
productionFile = pd.read_excel('Production/prodSopladorBeneficio.xlsx',names=["Date", "Ton_sol1", "Ton_sol2", "Ton_total"], sheet_name='SopladorBeneficio')
newDfConsumption = pd.DataFrame(columns=['timestamp', 'consumption', 'money'])

if(toSol1 and toSol2):
    newDfProduction = pd.DataFrame(columns=['timestamp', 'ton_sol_1', 'ton_sol_2', 'ton_total'])
elif(toSol1):
    newDfProduction = pd.DataFrame(columns=['timestamp', 'ton_total'])
elif(toSol2):
    newDfProduction = pd.DataFrame(columns=['timestamp', 'ton_total'])

consumption = 0
money = 0
currentIndex = 0

# Convert five minutal production df to daily production df

for i in range(0, len(consumptionFile.index)-1):

    if(consumptionFile.values[i,DATE_COLUMN].day == consumptionFile.values[i+1,DATE_COLUMN].day):
        consumption = consumptionFile.values[i,CONSUMPTION_COLUMN] + consumption
        money = consumptionFile.values[i,MONEY_COLUMN] + consumption
    else:
        fullDate = datetime.strptime(str(consumptionFile.values[i,DATE_COLUMN]), "%Y-%m-%d %H:%M:%S")
        date = str(fullDate.year) + "-" + str(fullDate.month) + "-" + str(fullDate.day)
        newDfConsumption.loc[currentIndex, 'timestamp'] = int(time.mktime(datetime.strptime(date, "%Y-%m-%d").timetuple()))
        newDfConsumption.loc[currentIndex, 'consumption'] = consumption
        newDfConsumption.loc[currentIndex, 'money'] = money
        currentIndex = currentIndex +1
        consumption = 0

if(consumption != 0):
    fullDate = datetime.strptime(str(consumptionFile.iloc[-1]['Date']), "%Y-%m-%d %H:%M:%S")
    date = str(fullDate.year) + "-" + str(fullDate.month) + "-" + str(fullDate.day)
    newDfConsumption.loc[currentIndex, 'timestamp'] = int(time.mktime(datetime.strptime(date, "%Y-%m-%d").timetuple()))
    newDfConsumption.loc[currentIndex, 'consumption'] = consumption
    newDfConsumption.loc[currentIndex, 'money'] = money
    consumption = 0
    money = 0

# Create a new df of production just with the needed values

for index in range(len(productionFile.index)):
    if(toSol1 and toSol2):
        newDfProduction.loc[index, 'timestamp'] = int(time.mktime(datetime.strptime(str(productionFile.values[index,0]), "%Y-%m-%d %H:%M:%S").timetuple()))
        newDfProduction.loc[index, 'ton_total'] = productionFile.values[index,TON_TOTAL]
        newDfProduction.loc[index, 'ton_sol_1'] = productionFile.values[index,TON_SOL_1]
        newDfProduction.loc[index, 'ton_sol_2'] = productionFile.values[index,TON_SOL_2]
    elif(toSol1):
        newDfProduction.loc[index, 'timestamp'] = int(time.mktime(datetime.strptime(str(productionFile.values[index,0]), "%Y-%m-%d %H:%M:%S").timetuple()))
        newDfProduction.loc[index, 'ton_total'] = productionFile.values[index,TON_SOL_1]
    elif(toSol2):
        newDfProduction.loc[index, 'timestamp'] = int(time.mktime(datetime.strptime(str(productionFile.values[index,0]), "%Y-%m-%d %H:%M:%S").timetuple()))
        newDfProduction.loc[index, 'ton_total'] = productionFile.values[index,TON_SOL_2]

# Merge new production and consumption files and store them in a xlsx

resultDataFrame = pd.merge(newDfProduction, newDfConsumption, on='timestamp', how='outer')

# Calculate the performance

def calculatePerformanceIndex(df, plant_1="", plant_2=""):
    if(plant_1 != "" and plant_2 != ""):
        # Calculate for both (Solubles 1 and 2)
        df['ide_sol_1'] = ""
        df['ide_sol_2'] = ""
        df['ide'] = ""
        df['ige_sol_1'] = ""
        df['ige_sol_2'] = ""
        df['ige'] = ""

        for index, row in df.iterrows():
            if(not math.isnan(row['consumption']) and not math.isnan(row['ton_sol_1']) and not math.isnan(row['ton_sol_2'])):
                if(row['ton_sol_1'] > 0):
                    df.loc[index, 'ide_sol_1'] = row['consumption'] / row['ton_sol_1']
                if(row['ton_sol_2'] > 0):
                    df.loc[index, 'ide_sol_2'] = row['consumption'] / row['ton_sol_2']
                if(row['ton_total'] > 0):
                    df.loc[index, 'ide'] = row['consumption'] / row['ton_total']
            if(not math.isnan(row['money'])):
                if(row['ton_sol_1'] > 0):
                    df.loc[index, 'ige_sol_1'] = row['money'] / row['ton_sol_1']
                if(row['ton_sol_2'] > 0):
                    df.loc[index, 'ige_sol_2'] = row['money'] / row['ton_sol_2']
                if(row['ton_total'] > 0):
                    df.loc[index, 'ige'] = row['money'] / row['ton_total']

    elif(plant_1 != "" or plant_2 != ""):
        # Calculate just for one solubles plant
        df["ide"] = ""
        df['ige'] = ""

        for index, row in df.iterrows():
            if(not math.isnan(row['consumption']) and not math.isnan(row['ton_total'])):
                if(row['ton_total'] > 0):
                    df.loc[index, 'ide'] = row['consumption'] / row['ton_total']
            if(not math.isnan(row['money'])):
                if(row['ton_total'] > 0):
                    df.loc[index, 'ige'] = row['money'] / row['ton_total']

if(toSol1 and toSol2):
    calculatePerformanceIndex(resultDataFrame, "sol1", "sol2")
elif(toSol1):
    calculatePerformanceIndex(resultDataFrame, "sol1")
elif(toSol2):
    calculatePerformanceIndex(resultDataFrame, "sol2")

if(storeTelemetry):
    resultDataFrame.to_excel("results/AllDetails.xlsx", index=False)

if(publishTelemetry):
    for index, row in resultDataFrame.iterrows():
        
        # Both production lines
        if(toSol1 and toSol2):

            unixTime = row['timestamp']
            consumption = row['consumption']
            money = row['money']

            tons = row['ton_total']
            tons_sol_1 = row['ton_sol_1']
            tons_sol_2 =  row['ton_sol_2']
            ige = row['ige']
            ige_sol_1 = row['ige_sol_1']
            ige_sol_2 = row['ige_sol_2']
            ide = row['ide']
            ide_sol_1 = row['ide_sol_1']
            ide_sol_2 = row['ide_sol_2']

            # Just tons
            if(not math.isnan(tons) and math.isnan(consumption)):
                telemetry_with_ts = {"ts": unixTime*1000, "values": {"prod_sol":tons, "prod_sol_1":tons_sol_1, "prod_sol_2": tons_sol_2}}
                results.append(client.send_telemetry(telemetry_with_ts))
            # Tons and consumption
            if(not math.isnan(tons) and not math.isnan(consumption)):
                if(tons_sol_1 > 0 and tons_sol_2 > 0):
                    telemetry_with_ts = {"ts": unixTime*1000, "values": {"prod_sol":tons, "prod_sol_1":tons_sol_1, "prod_sol_2": tons_sol_2, "kWh_sol": consumption, "money_sol": money, "ide_sol_1": ide_sol_1, "ide_sol_2": ide_sol_2, "ide_sol": ide, "ige_sol_1": ige_sol_1, "ige_sol_2": ige_sol_2, "ige_sol": ige}}
                elif(tons_sol_1 <= 0 and tons_sol_2 > 0):
                    telemetry_with_ts = {"ts": unixTime*1000, "values": {"prod_sol":tons, "prod_sol_1":tons_sol_1, "prod_sol_2": tons_sol_2, "kWh_sol": consumption, "money_sol": money, "ide_sol_2": ide_sol_2, "ide_sol": ide, "ige_sol_2": ige_sol_2, "ige_sol": ige}}
                elif(tons_sol_2 <= 0 and tons_sol_1 > 0):
                    telemetry_with_ts = {"ts": unixTime*1000, "values": {"prod_sol":tons, "prod_sol_1":tons_sol_1, "prod_sol_2": tons_sol_2, "kWh_sol": consumption, "money_sol": money, "ide_sol_1": ide_sol_1, "ide_sol": ide, "ige_sol_1": ige_sol_1, "ige_sol": ige}}
                else:
                    telemetry_with_ts = {"ts": unixTime*1000, "values": {"prod_sol":tons, "prod_sol_1":tons_sol_1, "prod_sol_2": tons_sol_2, "kWh_sol": consumption, "money_sol": money }}

                results.append(client.send_telemetry(telemetry_with_ts))

        # Just one production line
        elif(toSol1 or toSol2):

            unixTime = row['timestamp']
            consumption = row['consumption']
            money = row['money']
            tons = row['ton_total']
            ige = row['ige']
            ide = row['ide']

            # Just tons
            if(not math.isnan(tons) and math.isnan(consumption)):
                telemetry_with_ts = {"ts": unixTime*1000, "values": {"prod_sol":tons}}
                results.append(client.send_telemetry(telemetry_with_ts))
            # Tons and consumption
            if(not math.isnan(tons) and not math.isnan(consumption)):
                if(tons > 0):
                    print(unixTime, consumption, money, tons, ide, ige)
                    telemetry_with_ts = {"ts": unixTime*1000, "values": {"kWh_sol": consumption, "money_sol":money, "prod_sol":tons, "ide_sol": ide, "ige_sol": ige}}
                else:
                    print(unixTime, consumption, money, tons)
                    telemetry_with_ts = {"ts": unixTime*1000, "values": {"kWh_sol": consumption, "money_sol":money, "prod_sol":tons}}
                results.append(client.send_telemetry(telemetry_with_ts))

    for tmp_result in results:
        result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
    print("Result", str(result))
    client.disconnect()