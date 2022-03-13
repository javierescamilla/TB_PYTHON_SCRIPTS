from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import random
import math

START_DATE = 1641016800000 # 1 de Enero 2022
one_day = 86400000         # 1 dia en mss
five_minutes_in_ms = 300000
NUMBER_OF_DECIMALS = 2

# MODIFICAR:
baseCurrent = 310
ammount_of_readings = 12 * 24 # 1 dia de emulacion
#ammount_of_readings = 1
OFFSET_IN_DAYS = one_day * 0  # Dias de offset con respecto a enero 1

# NO TOCAR
baseVoltage = 460
basePf = 0.95
baseKWh = 1230

# MODIFICAR:
baseCurrentMin = baseCurrent * -0.05
baseCurrentMax = baseCurrent * 0.05

# NO TOCAR
baseVoltageMin = baseVoltage * -0.01
baseVoltageMax = baseVoltage * 0.01
basePfMin = -0.01
basePfMax = 0.02

client = TBDeviceMqttClient("34.231.242.52", "IYhHPunxPWqzxdCv0ODW")
telemetry_with_ts = {"ts": START_DATE + OFFSET_IN_DAYS, "values": {"current_l1": baseCurrent, "current_l2": baseCurrent, "current_l3": baseCurrent,  "voltage_l1": baseVoltage, "voltage_l2": baseVoltage, "voltage_l3": baseVoltage, "power_factor": basePf, "kWh_raw": baseKWh}}

client.max_inflight_messages_set(ammount_of_readings)
client.connect()
results = []
result = True

def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

for i in range(0, ammount_of_readings):

    telemetry_with_ts["values"]["current_l1"] = truncate(baseCurrent + random.uniform(baseCurrentMin, baseCurrentMax),NUMBER_OF_DECIMALS)
    telemetry_with_ts["values"]["current_l2"] = truncate(baseCurrent + random.uniform(baseCurrentMin, baseCurrentMax),NUMBER_OF_DECIMALS)
    telemetry_with_ts["values"]["current_l3"] = truncate(baseCurrent + random.uniform(baseCurrentMin, baseCurrentMax),NUMBER_OF_DECIMALS)
    averageCurrent = (telemetry_with_ts["values"]["current_l1"] + telemetry_with_ts["values"]["current_l2"] + telemetry_with_ts["values"]["current_l3"]) / 3

    telemetry_with_ts["values"]["voltage_l1"] = truncate(baseVoltage + random.uniform(baseVoltageMin, baseVoltageMax),NUMBER_OF_DECIMALS)
    telemetry_with_ts["values"]["voltage_l2"] = truncate(baseVoltage + random.uniform(baseVoltageMin, baseVoltageMax),NUMBER_OF_DECIMALS)
    telemetry_with_ts["values"]["voltage_l3"] = truncate(baseVoltage + random.uniform(baseVoltageMin, baseVoltageMax),NUMBER_OF_DECIMALS)
    averageVoltage = (telemetry_with_ts["values"]["voltage_l1"] + telemetry_with_ts["values"]["voltage_l2"] + telemetry_with_ts["values"]["voltage_l3"]) / 3

    telemetry_with_ts["values"]["power_factor"] = truncate(basePf + random.uniform(basePfMin, basePfMax),NUMBER_OF_DECIMALS)

    telemetry_with_ts["values"]["kW"] = truncate((averageVoltage * averageCurrent * telemetry_with_ts["values"]["power_factor"] * 1.73) / 1000, NUMBER_OF_DECIMALS)
    kWh_increment = telemetry_with_ts["values"]["kW"] / 12
    telemetry_with_ts["values"]["kWh_raw"] += truncate(kWh_increment, NUMBER_OF_DECIMALS)
    
    telemetry_with_ts["ts"] += five_minutes_in_ms 
    results.append(client.send_telemetry(telemetry_with_ts))

for tmp_result in results:
    result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS
    
print("Result", str(result))
client.disconnect()