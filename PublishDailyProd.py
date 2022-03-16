import pandas as pd
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
from datetime import datetime
import time
import math

df = pd.read_excel('Production/prodSopladorBeneficio.xlsx', names=["Date", "Sol1", "Sol2", "kWh_sol1", "kWh_sol2", "pesos_sol1", "pesos_sol2", "ide_sol1", "ide_sol2", "ige_sol1", "ige_sol2"], sheet_name='SopladorBeneficio')

client = TBDeviceMqttClient("34.231.242.52", "MGfRWtWI70En2cLwfmWM")
client.connect()
results = []
result = True

def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

for index, row in df.iterrows():
    timestamp = time.mktime(datetime.strptime(str(row['Date']), "%Y-%m-%d %H:%M:%S").timetuple())
    prod_sol_1 = (row['Sol1'])
    prod_sol_2 = (row['Sol2'])
    kWh_sol_1 = (row['kWh_sol1'])
    kWh_sol_2 = (row['kWh_sol2'])
    pesos_sol_1 = (row['pesos_sol1'])
    pesos_sol_2 = (row['pesos_sol2'])
    ide_sol_1 = (row['ide_sol1'])
    ide_sol_2 = (row['ide_sol2'])
    ige_sol_1 = (row['ige_sol1'])
    ige_sol_2 = (row['ige_sol2'])

    if(prod_sol_2 == 0):
        #print(int(timestamp), truncate(prod_sol_1,3), truncate(prod_sol_2,3), truncate(kWh_sol_1,3), truncate(kWh_sol_2,3), truncate(pesos_sol_1,3), truncate(pesos_sol_2,3), truncate(ide_sol_1,3), truncate(ige_sol_1,3))
        telemetry_with_ts = {"ts": int(timestamp)*1000, "values": {"prod_sol_1": truncate(prod_sol_1,3), "prod_sol_2":truncate(prod_sol_2,3), "kWh_sol_1":truncate(kWh_sol_1,3), "kWh_sol_2":truncate(kWh_sol_2,3), "pesos_sol_1":truncate(pesos_sol_1,3), "ide_sol_1":truncate(ide_sol_1,3), "ige_sol_1":truncate(ige_sol_1,3)}}
    
    # Solo sol2 SOL1 TIENE 0 PRODUCCION
    elif(prod_sol_1 == 0):
        #print(int(timestamp), truncate(prod_sol_1,3), truncate(prod_sol_2,3), truncate(kWh_sol_1,3), truncate(kWh_sol_2,3), truncate(pesos_sol_1,3), truncate(pesos_sol_2,3), truncate(ide_sol_2,3), truncate(ige_sol_2,3))
        telemetry_with_ts = {"ts": int(timestamp)*1000, "values": {"prod_sol_1": truncate(prod_sol_1,3), "prod_sol_2":truncate(prod_sol_2,3), "kWh_sol_1":truncate(kWh_sol_1,3), "kWh_sol_2":truncate(kWh_sol_2,3), "pesos_sol_2":truncate(pesos_sol_2,3), "ide_sol_2":truncate(ide_sol_2,3), "ige_sol_2":truncate(ige_sol_2,3)}}
    
    # Solo full HAY LECTURAS DEL ANALIZADOR Y DE PRODUCCION
    elif(not math.isnan(prod_sol_1) and not math.isnan(prod_sol_2) and not math.isnan(kWh_sol_1) and not math.isnan(kWh_sol_2)):
        #print(int(timestamp), truncate(prod_sol_1,3), truncate(prod_sol_2,3), truncate(kWh_sol_1,3), truncate(kWh_sol_2,3), truncate(pesos_sol_1,3), truncate(pesos_sol_2,3), truncate(ide_sol_1,3), truncate(ide_sol_2,3), truncate(ige_sol_1,3), truncate(ige_sol_2,3))
        telemetry_with_ts = {"ts": int(timestamp)*1000, "values": {"prod_sol_1": truncate(prod_sol_1,3), "prod_sol_2":truncate(prod_sol_2,3), "kWh_sol_1":truncate(kWh_sol_1,3), "kWh_sol_2":truncate(kWh_sol_2,3), "pesos_sol_1":truncate(pesos_sol_1,3), "pesos_sol_2":truncate(pesos_sol_2,3), "ide_sol_1":truncate(ide_sol_1,3), "ide_sol_2":truncate(ide_sol_2,3), "ige_sol_1":truncate(ige_sol_1,3), "ige_sol_2":truncate(ige_sol_2,3)}}
    
    # Solo sol1 y sol2 NO HAY LECTURAS DEL ANALIZADOR SOLO PROD
    if(not math.isnan(prod_sol_1) and not math.isnan(prod_sol_2) and math.isnan(kWh_sol_1) and math.isnan(kWh_sol_2)):
        #print(int(timestamp), truncate(prod_sol_1,3), truncate(prod_sol_2,3))
        telemetry_with_ts = {"ts": int(timestamp)*1000, "values": {"prod_sol_1": truncate(prod_sol_1,3), "prod_sol_2":truncate(prod_sol_2,3)}}

    results.append(client.send_telemetry(telemetry_with_ts))

for tmp_result in results:
    result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
print("Result", str(result))
client.disconnect()