import requests
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import json
import re
import time
import schedule

# API Endpoint (China Region)
BASE_URL = ""

# Credentials (From Northbound API Management)
USERNAME = ""
PASSWORD = ""

# InfluxDB Information
INFLUXDB_ADDRESS = 'localhost'
INFLUXDB_PORT = 8086
INFLUXDB_USER = ''     
INFLUXDB_PASSWORD = ''
INFLUXDB_DATABASE = 'OANDM'

# Connect InfluxDB
influx_client = InfluxDBClient(INFLUXDB_ADDRESS, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASSWORD, INFLUXDB_DATABASE)

# Initial InfluxDB
def init_influxdb(inf_db):
    databases = influx_client.get_list_database()
    # Check Database , if don't have a database , System create a database from input parameter (inf_db)
    if not any(db['name'] == inf_db for db in databases):
        influx_client.create_database(inf_db)
        print(f"Create Database {inf_db} : Success")
    # Switch Database
    influx_client.switch_database(inf_db)
    print(f"Switch Database {inf_db} : Success")
    
# Function GET String Inverter From InfluxDB
def query_get_string_inverter():
    device_1_list = []
    device_38_list = []
    query_1 = f"SELECT id as devId , devName , plant_name , plant_code FROM device_plant where devTypeId = '1'"
    result_1 = influx_client.query(query_1)    
    # Result
    for point in result_1.get_points():
        device_1_list.append({"devId":point["devId"],"plant_code":re.sub(r"/.*", "", point["plant_code"]),"plant_name":point["plant_name"]})
    query_38 = f"SELECT id as devId , devName , plant_name , plant_code FROM device_plant where devTypeId = '38'"
    result_38 = influx_client.query(query_38)    
    # Result
    for point in result_38.get_points():
        device_38_list.append({"devId":point["devId"],"plant_code":re.sub(r"/.*", "", point["plant_code"]),"plant_name":point["plant_name"]})
    return device_1_list , device_38_list
    
# Function GET Power Meter From InfluxDB
def query_get_power_meter():
    device_17_list = []
    device_47_list = []
    query_17 = f"SELECT id as devId , devName , plant_name , plant_code FROM device_plant where devTypeId = '17'"
    result_17 = influx_client.query(query_17)    
    # Result
    for point in result_17.get_points():
        device_17_list.append({"devId":point["devId"],"plant_code":re.sub(r"/.*", "", point["plant_code"]),"plant_name":point["plant_name"]})
    query_47 = f"SELECT id as devId , devName , plant_name , plant_code FROM device_plant where devTypeId = '47'"
    result_47 = influx_client.query(query_47)    
    # Result
    for point in result_47.get_points():
        device_47_list.append({"devId":point["devId"],"plant_code":re.sub(r"/.*", "", point["plant_code"]),"plant_name":point["plant_name"]})
    return device_17_list , device_47_list

# Function GET Battery From InfluxDB
def query_get_battery():
    device_39_list = []
    query_39 = f"SELECT id as devId , devName , plant_name , plant_code FROM device_plant where devTypeId = '39'"
    result_39 = influx_client.query(query_39)    
    # Result
    for point in result_39.get_points():
        device_39_list.append({"devId":point["devId"],"plant_code":re.sub(r"/.*", "", point["plant_code"]),"plant_name":point["plant_name"]})    
    return device_39_list

# Function GET EMI From InfluxDB
def query_get_emi():
    device_10_list = []
    query_10 = f"SELECT id as devId , devName , plant_name , plant_code FROM device_plant where devTypeId = '10'"
    result_10 = influx_client.query(query_10)    
    # Result
    for point in result_10.get_points():
        device_10_list.append({"devId":point["devId"],"plant_code":re.sub(r"/.*", "", point["plant_code"]),"plant_name":point["plant_name"]})    
    return device_10_list
 
# Function GET String Inverter From InfluxDB
def query_get_device_name(devId):
    where_clause = f"id = '{devId}'"
    query = f"SELECT devName , plant_code , plant_name FROM device_plant WHERE {where_clause}"
    result = influx_client.query(query)
    device_name = ""
    plant_code = ""
    plant_name = ""
    # Result
    for point in result.get_points():
        device_name = point['devName']
        plant_code = re.sub(r"/.*", "", point['plant_code'])
        plant_name = point['plant_name']
    # print(device_name,plant_code)
    return device_name , plant_code , plant_name

# Function GET XSRF-Token
def get_token():
    url = f"{BASE_URL}/thirdData/login"
    headers = {"Content-Type": "application/json"}
    payload = {"userName": USERNAME, "systemCode": PASSWORD}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print (data)
        cookies = session.cookies.get("XSRF-TOKEN")
        print("🍪 Cookies XSRF-TOKEN :", cookies)
        return cookies
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# V.2 Function GET Real KPI Device Inverter , Call Every 5 Minutes
def get_real_kpi_device_inverter(xsrf):
    url = f"{BASE_URL}/thirdData/getDevRealKpi"
    device_t1_data , device_t38_data = query_get_string_inverter()
    join_t1_devId = ",".join(str(item["devId"]) for item in device_t1_data)
    # print(join_t1_devId)
    join_t38_devId = ",".join(str(item["devId"]) for item in device_t38_data)
    # print(join_t38_devId)
    # return    
    for loop_device_type in range(0,2):
        if loop_device_type == 0:
            devTypeId = "1"
            devId = join_t1_devId
        if loop_device_type == 1:
            devTypeId = "38"
            devId = join_t38_devId            
        headers = {"Content-Type": "application/json","xsrf-token":xsrf}
        payload = {"devIds" : devId,"devTypeId":devTypeId}
        # payload = {"devIds" : "1000000053771302","devTypeId":"1"}
        session = requests.Session() 
        response = session.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # print (data)
            # return
            try:
                for dat in range(0,len(data["data"])):
                    # print(data["data"][dat]["devId"])
                    device_name , plant_code , plant_name = query_get_device_name(data["data"][dat]["devId"])
                    # print(device_name,plant_code,plant_name)
                    
                    # Check Key In Data
                    if "inverter_state" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["inverter_state"] == None :
                            inverter_state = 0
                        elif data["data"][dat]["dataItemMap"]["inverter_state"] != None :
                            inverter_state = data["data"][dat]["dataItemMap"]["inverter_state"]                          
                    if "ab_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["ab_u"] == None :
                            ab_u = 0
                        elif data["data"][dat]["dataItemMap"]["ab_u"] != None :
                            ab_u = data["data"][dat]["dataItemMap"]["ab_u"]                        
                    if "bc_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["bc_u"] == None :
                            bc_u = 0
                        elif data["data"][dat]["dataItemMap"]["bc_u"] != None :
                            bc_u = data["data"][dat]["dataItemMap"]["bc_u"]                        
                    if "ca_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["ca_u"] == None :
                            ca_u = 0
                        elif data["data"][dat]["dataItemMap"]["ca_u"] != None :
                            ca_u = data["data"][dat]["dataItemMap"]["ca_u"]                        
                    if "a_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["a_u"] == None :
                            a_u = 0
                        elif data["data"][dat]["dataItemMap"]["a_u"] != None :
                            a_u = data["data"][dat]["dataItemMap"]["a_u"]                        
                    if "b_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["b_u"] == None :
                            b_u = 0
                        elif data["data"][dat]["dataItemMap"]["b_u"] != None :
                            b_u = data["data"][dat]["dataItemMap"]["b_u"]                        
                    if "c_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["c_u"] == None :
                            c_u = 0
                        elif data["data"][dat]["dataItemMap"]["c_u"] != None :
                            c_u = data["data"][dat]["dataItemMap"]["c_u"]                        
                    if "a_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["a_i"] == None :
                            a_i = 0
                        elif data["data"][dat]["dataItemMap"]["a_i"] != None :
                            a_i = data["data"][dat]["dataItemMap"]["a_i"]                        
                    if "b_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["b_i"] == None :
                            b_i = 0
                        elif data["data"][dat]["dataItemMap"]["b_i"] != None :
                            b_i = data["data"][dat]["dataItemMap"]["b_i"]                        
                    if "c_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["c_i"] == None :
                            c_i = 0
                        elif data["data"][dat]["dataItemMap"]["c_i"] != None :
                            c_i = data["data"][dat]["dataItemMap"]["c_i"]                        
                    if "efficiency" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["efficiency"] == None :
                            efficiency = 0
                        elif data["data"][dat]["dataItemMap"]["efficiency"] != None :
                            efficiency = data["data"][dat]["dataItemMap"]["efficiency"]                        
                    if "temperature" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["temperature"] == None :
                            temperature = 0
                        elif data["data"][dat]["dataItemMap"]["temperature"] != None :
                            temperature = data["data"][dat]["dataItemMap"]["temperature"]
                    if "power_factor" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["power_factor"] == None :
                            power_factor = 0
                        elif data["data"][dat]["dataItemMap"]["power_factor"] != None :
                            power_factor = data["data"][dat]["dataItemMap"]["power_factor"]                        
                    if "elec_freq" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["elec_freq"] == None :
                            elec_freq = 0
                        elif data["data"][dat]["dataItemMap"]["elec_freq"] != None :
                            elec_freq = data["data"][dat]["dataItemMap"]["elec_freq"]                        
                    if "active_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["active_power"] == None :
                            active_power = 0
                        elif data["data"][dat]["dataItemMap"]["active_power"] != None :
                            active_power = data["data"][dat]["dataItemMap"]["active_power"]                        
                    if "reactive_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["reactive_power"] == None :
                            reactive_power = 0
                        elif data["data"][dat]["dataItemMap"]["reactive_power"] != None :
                            reactive_power = data["data"][dat]["dataItemMap"]["reactive_power"]                        
                    if "day_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["day_cap"] == None :
                            day_cap = 0
                        elif data["data"][dat]["dataItemMap"]["day_cap"] != None :
                            day_cap = data["data"][dat]["dataItemMap"]["day_cap"]                        
                    if "mppt_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_power"] == None :
                            mppt_power = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_power"] != None :
                            mppt_power = data["data"][dat]["dataItemMap"]["mppt_power"]
                            
                    # Check PV_U
                    if "pv1_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv1_u"] == None :
                            pv1_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv1_u"] != None :
                            pv1_u = data["data"][dat]["dataItemMap"]["pv1_u"]                        
                    if "pv2_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv2_u"] == None :
                            pv2_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv2_u"] != None :
                            pv2_u = data["data"][dat]["dataItemMap"]["pv2_u"]                        
                    if "pv3_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv3_u"] == None :
                            pv3_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv3_u"] != None :
                            pv3_u = data["data"][dat]["dataItemMap"]["pv3_u"]                        
                    if "pv4_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv4_u"] == None :
                            pv4_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv4_u"] != None :
                            pv4_u = data["data"][dat]["dataItemMap"]["pv4_u"]                        
                    if "pv5_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv5_u"] == None :
                            pv5_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv5_u"] != None :
                            pv5_u = data["data"][dat]["dataItemMap"]["pv5_u"]                        
                    if "pv6_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv6_u"] == None :
                            pv6_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv6_u"] != None :
                            pv6_u = data["data"][dat]["dataItemMap"]["pv6_u"]                        
                    if "pv7_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv7_u"] == None :
                            pv7_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv7_u"] != None :
                            pv7_u = data["data"][dat]["dataItemMap"]["pv7_u"]                        
                    if "pv8_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv8_u"] == None :
                            pv8_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv8_u"] != None :
                            pv8_u = data["data"][dat]["dataItemMap"]["pv8_u"]                
                    if "pv9_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv9_u"] == None :
                            pv9_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv9_u"] != None :
                            pv9_u = data["data"][dat]["dataItemMap"]["pv9_u"]                        
                    if "pv10_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv10_u"] == None :
                            pv10_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv10_u"] != None :
                            pv10_u = data["data"][dat]["dataItemMap"]["pv10_u"]                        
                    if "pv11_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv11_u"] == None :
                            pv11_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv11_u"] != None :
                            pv11_u = data["data"][dat]["dataItemMap"]["pv11_u"]                        
                    if "pv12_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv12_u"] == None :
                            pv12_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv12_u"] != None :
                            pv12_u = data["data"][dat]["dataItemMap"]["pv12_u"]                        
                    if "pv13_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv13_u"] == None :
                            pv13_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv13_u"] != None :
                            pv13_u = data["data"][dat]["dataItemMap"]["pv13_u"]                        
                    if "pv14_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv14_u"] == None :
                            pv14_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv14_u"] != None :
                            pv14_u = data["data"][dat]["dataItemMap"]["pv14_u"]                        
                    if "pv15_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv15_u"] == None :
                            pv15_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv15_u"] != None :
                            pv15_u = data["data"][dat]["dataItemMap"]["pv15_u"]                        
                    if "pv16_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv16_u"] == None :
                            pv16_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv16_u"] != None :
                            pv16_u = data["data"][dat]["dataItemMap"]["pv16_u"]                        
                    if "pv17_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv17_u"] == None :
                            pv17_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv17_u"] != None :
                            pv17_u = data["data"][dat]["dataItemMap"]["pv17_u"]                        
                    if "pv18_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv18_u"] == None :
                            pv18_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv18_u"] != None :
                            pv18_u = data["data"][dat]["dataItemMap"]["pv18_u"]                        
                    if "pv19_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv19_u"] == None :
                            pv19_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv19_u"] != None :
                            pv19_u = data["data"][dat]["dataItemMap"]["pv19_u"]                        
                    if "pv20_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv20_u"] == None :
                            pv20_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv20_u"] != None :
                            pv20_u = data["data"][dat]["dataItemMap"]["pv20_u"]                        
                    if "pv21_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv21_u"] == None :
                            pv21_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv21_u"] != None :
                            pv21_u = data["data"][dat]["dataItemMap"]["pv21_u"]                        
                    if "pv22_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv22_u"] == None :
                            pv22_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv22_u"] != None :
                            pv22_u = data["data"][dat]["dataItemMap"]["pv22_u"]                        
                    if "pv23_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv23_u"] == None :
                            pv23_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv23_u"] != None :
                            pv23_u = data["data"][dat]["dataItemMap"]["pv23_u"]                        
                    if "pv24_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv24_u"] == None :
                            pv24_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv24_u"] != None :
                            pv24_u = data["data"][dat]["dataItemMap"]["pv24_u"]                        
                    if "pv25_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv25_u"] == None :
                            pv25_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv25_u"] != None :
                            pv25_u = data["data"][dat]["dataItemMap"]["pv25_u"]                        
                    if "pv26_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv26_u"] == None :
                            pv26_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv26_u"] != None :
                            pv26_u = data["data"][dat]["dataItemMap"]["pv26_u"]                        
                    if "pv27_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv27_u"] == None :
                            pv27_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv27_u"] != None :
                            pv27_u = data["data"][dat]["dataItemMap"]["pv27_u"]                        
                    if "pv28_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv28_u"] == None :
                            pv28_u = 0
                        elif data["data"][dat]["dataItemMap"]["pv28_u"] != None :
                            pv28_u = data["data"][dat]["dataItemMap"]["pv28_u"]
                            
                    # Check PV_I
                    if "pv1_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv1_i"] == None :
                            pv1_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv1_i"] != None :
                            pv1_i = data["data"][dat]["dataItemMap"]["pv1_i"]                        
                    if "pv2_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv2_i"] == None :
                            pv2_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv2_i"] != None :
                            pv2_i = data["data"][dat]["dataItemMap"]["pv2_i"]                        
                    if "pv3_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv3_i"] == None :
                            pv3_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv3_i"] != None :
                            pv3_i = data["data"][dat]["dataItemMap"]["pv3_i"]                        
                    if "pv4_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv4_i"] == None :
                            pv4_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv4_i"] != None :
                            pv4_i = data["data"][dat]["dataItemMap"]["pv4_i"]                        
                    if "pv5_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv5_i"] == None :
                            pv5_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv5_i"] != None :
                            pv5_i = data["data"][dat]["dataItemMap"]["pv5_i"]                        
                    if "pv6_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv6_i"] == None :
                            pv6_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv6_i"] != None :
                            pv6_i = data["data"][dat]["dataItemMap"]["pv6_i"]                        
                    if "pv7_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv7_i"] == None :
                            pv7_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv7_i"] != None :
                            pv7_i = data["data"][dat]["dataItemMap"]["pv7_i"]                        
                    if "pv8_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv8_i"] == None :
                            pv8_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv8_i"] != None :
                            pv8_i = data["data"][dat]["dataItemMap"]["pv8_i"]                
                    if "pv9_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv9_i"] == None :
                            pv9_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv9_i"] != None :
                            pv9_i = data["data"][dat]["dataItemMap"]["pv9_i"]                        
                    if "pv10_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv10_i"] == None :
                            pv10_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv10_i"] != None :
                            pv10_i = data["data"][dat]["dataItemMap"]["pv10_i"]                        
                    if "pv11_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv11_i"] == None :
                            pv11_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv11_i"] != None :
                            pv11_i = data["data"][dat]["dataItemMap"]["pv11_i"]                        
                    if "pv12_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv12_i"] == None :
                            pv12_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv12_i"] != None :
                            pv12_i = data["data"][dat]["dataItemMap"]["pv12_i"]                        
                    if "pv13_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv13_i"] == None :
                            pv13_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv13_i"] != None :
                            pv13_i = data["data"][dat]["dataItemMap"]["pv13_i"]                        
                    if "pv14_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv14_i"] == None :
                            pv14_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv14_i"] != None :
                            pv14_i = data["data"][dat]["dataItemMap"]["pv14_i"]                        
                    if "pv15_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv15_i"] == None :
                            pv15_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv15_i"] != None :
                            pv15_i = data["data"][dat]["dataItemMap"]["pv15_i"]                        
                    if "pv16_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv16_i"] == None :
                            pv16_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv16_i"] != None :
                            pv16_i = data["data"][dat]["dataItemMap"]["pv16_i"]                        
                    if "pv17_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv17_i"] == None :
                            pv17_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv17_i"] != None :
                            pv17_i = data["data"][dat]["dataItemMap"]["pv17_i"]                        
                    if "pv18_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv18_i"] == None :
                            pv18_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv18_i"] != None :
                            pv18_i = data["data"][dat]["dataItemMap"]["pv18_i"]                        
                    if "pv19_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv19_i"] == None :
                            pv19_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv19_i"] != None :
                            pv19_i = data["data"][dat]["dataItemMap"]["pv19_i"]                        
                    if "pv20_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv20_i"] == None :
                            pv20_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv20_i"] != None :
                            pv20_i = data["data"][dat]["dataItemMap"]["pv20_i"]                        
                    if "pv21_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv21_i"] == None :
                            pv21_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv21_i"] != None :
                            pv21_i = data["data"][dat]["dataItemMap"]["pv21_i"]                        
                    if "pv22_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv22_i"] == None :
                            pv22_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv22_i"] != None :
                            pv22_i = data["data"][dat]["dataItemMap"]["pv22_i"]                        
                    if "pv23_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv23_i"] == None :
                            pv23_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv23_i"] != None :
                            pv23_i = data["data"][dat]["dataItemMap"]["pv23_i"]                        
                    if "pv24_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv24_i"] == None :
                            pv24_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv24_i"] != None :
                            pv24_i = data["data"][dat]["dataItemMap"]["pv24_i"]                        
                    if "pv25_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv25_i"] == None :
                            pv25_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv25_i"] != None :
                            pv25_i = data["data"][dat]["dataItemMap"]["pv25_i"]                        
                    if "pv26_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv26_i"] == None :
                            pv26_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv26_i"] != None :
                            pv26_i = data["data"][dat]["dataItemMap"]["pv26_i"]                        
                    if "pv27_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv27_i"] == None :
                            pv27_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv27_i"] != None :
                            pv27_i = data["data"][dat]["dataItemMap"]["pv27_i"]                        
                    if "pv28_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv28_i"] == None :
                            pv28_i = 0
                        elif data["data"][dat]["dataItemMap"]["pv28_i"] != None :
                            pv28_i = data["data"][dat]["dataItemMap"]["pv28_i"]
                            
                    # Check MPPT
                    if "total_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["total_cap"] == None :
                            total_cap = 0
                        elif data["data"][dat]["dataItemMap"]["total_cap"] != None :
                            total_cap = data["data"][dat]["dataItemMap"]["total_cap"]                        
                    if "open_time" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["open_time"] == None :
                            open_time = 0
                        elif data["data"][dat]["dataItemMap"]["open_time"] != None :
                            open_time = data["data"][dat]["dataItemMap"]["open_time"]                        
                    if "close_time" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["close_time"] == None :
                            close_time = 0
                        elif data["data"][dat]["dataItemMap"]["close_time"] != None :
                            close_time = data["data"][dat]["dataItemMap"]["close_time"]                        
                    if "mppt_total_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_total_cap"] == None :
                            mppt_total_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_total_cap"] != None :
                            mppt_total_cap = data["data"][dat]["dataItemMap"]["mppt_total_cap"]                        
                    if "mppt_1_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_1_cap"] == None :
                            mppt_1_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_1_cap"] != None :
                            mppt_1_cap = data["data"][dat]["dataItemMap"]["mppt_1_cap"]                        
                    if "mppt_2_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_2_cap"] == None :
                            mppt_2_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_2_cap"] != None :
                            mppt_2_cap = data["data"][dat]["dataItemMap"]["mppt_2_cap"]                        
                    if "mppt_3_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_3_cap"] == None :
                            mppt_3_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_3_cap"] != None :
                            mppt_3_cap = data["data"][dat]["dataItemMap"]["mppt_3_cap"]                        
                    if "mppt_4_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_4_cap"] == None :
                            mppt_4_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_4_cap"] != None :
                            mppt_4_cap = data["data"][dat]["dataItemMap"]["mppt_4_cap"]
                    if "mppt_5_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_5_cap"] == None :
                            mppt_5_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_5_cap"] != None :
                            mppt_5_cap = data["data"][dat]["dataItemMap"]["mppt_5_cap"]                        
                    if "mppt_6_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_6_cap"] == None :
                            mppt_6_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_6_cap"] != None :
                            mppt_6_cap = data["data"][dat]["dataItemMap"]["mppt_6_cap"]                        
                    if "mppt_7_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_7_cap"] == None :
                            mppt_7_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_7_cap"] != None :
                            mppt_7_cap = data["data"][dat]["dataItemMap"]["mppt_7_cap"]                        
                    if "mppt_8_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_8_cap"] == None :
                            mppt_8_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_8_cap"] != None :
                            mppt_8_cap = data["data"][dat]["dataItemMap"]["mppt_8_cap"] 
                    if "mppt_9_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_9_cap"] == None :
                            mppt_9_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_9_cap"] != None :
                            mppt_9_cap = data["data"][dat]["dataItemMap"]["mppt_9_cap"]                        
                    if "mppt_10_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["mppt_10_cap"] == None :
                            mppt_10_cap = 0
                        elif data["data"][dat]["dataItemMap"]["mppt_10_cap"] != None :
                            mppt_10_cap = data["data"][dat]["dataItemMap"]["mppt_10_cap"]
                    if "run_state" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["run_state"] == None :
                            run_state = 0
                        elif data["data"][dat]["dataItemMap"]["run_state"] != None :
                            run_state = data["data"][dat]["dataItemMap"]["run_state"]

                            
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "realtime_device_inverter", # Measurement of Sensor
                            "tags": {
                                "plant_code" : plant_code+"/getDevRealKpi",  # Tags of sensor
                                "plant_name" : plant_name,
                                "device_name" : device_name,
                                "device_id" : str(data["data"][dat]["devId"])
                            },
                            "fields": {
                                "devTypeId" : devTypeId,
                                "inverter_state" : float(inverter_state),
                                "ab_u" : float(ab_u),
                                "bc_u" : float(bc_u),
                                "ca_u" : float(ca_u),
                                "a_u" : float(a_u),
                                "b_u" : float(b_u),
                                "c_u" : float(c_u),
                                "a_i" : float(a_i),
                                "b_i" : float(b_i),
                                "c_i" : float(c_i),
                                "efficiency" : float(efficiency),
                                "temperature" : float(temperature),
                                "power_factor" : float(power_factor),
                                "elec_freq" : float(elec_freq),
                                "active_power": float(active_power),
                                "reactive_power" : float(reactive_power),
                                "day_cap" : float(day_cap),
                                "mppt_power" : float(mppt_power),
                                "pv1_u" : float(pv1_u),
                                "pv2_u" : float(pv2_u),
                                "pv3_u" : float(pv3_u),
                                "pv4_u" : float(pv4_u),
                                "pv5_u" : float(pv5_u),
                                "pv6_u" : float(pv6_u),
                                "pv7_u" : float(pv7_u),
                                "pv8_u" : float(pv8_u),
                                "pv9_u" : float(pv9_u),
                                "pv10_u" : float(pv10_u),
                                "pv11_u" : float(pv11_u),
                                "pv12_u" : float(pv12_u),
                                "pv13_u" : float(pv13_u),
                                "pv14_u" : float(pv14_u),
                                "pv15_u" : float(pv15_u),
                                "pv16_u" : float(pv16_u),
                                "pv17_u" : float(pv17_u),
                                "pv18_u" : float(pv18_u),
                                "pv19_u" : float(pv19_u),
                                "pv20_u" : float(pv20_u),
                                "pv21_u" : float(pv21_u),
                                "pv22_u" : float(pv22_u),
                                "pv23_u" : float(pv23_u),
                                "pv24_u" : float(pv24_u),
                                "pv25_u" : float(pv25_u),
                                "pv26_u" : float(pv26_u),
                                "pv27_u" : float(pv27_u),
                                "pv28_u" : float(pv28_u),
                                "pv1_i" : float(pv1_i),
                                "pv2_i" : float(pv2_i),
                                "pv3_i" : float(pv3_i),
                                "pv4_i" : float(pv4_i),
                                "pv5_i" : float(pv5_i),
                                "pv6_i" : float(pv6_i),
                                "pv7_i" : float(pv7_i),
                                "pv8_i" : float(pv8_i),
                                "pv9_i" : float(pv9_i),
                                "pv10_i" : float(pv10_i),
                                "pv11_i" : float(pv11_i),
                                "pv12_i" : float(pv12_i),
                                "pv13_i" : float(pv13_i),
                                "pv14_i" : float(pv14_i),
                                "pv15_i" : float(pv15_i),
                                "pv16_i" : float(pv16_i),
                                "pv17_i" : float(pv17_i),
                                "pv18_i" : float(pv18_i),
                                "pv19_i" : float(pv19_i),
                                "pv20_i" : float(pv20_i),
                                "pv21_i" : float(pv21_i),
                                "pv22_i" : float(pv22_i),
                                "pv23_i" : float(pv23_i),
                                "pv24_i" : float(pv24_i),
                                "pv25_i" : float(pv25_i),
                                "pv26_i" : float(pv26_i),
                                "pv27_i" : float(pv27_i),
                                "pv28_i" : float(pv28_i),
                                "total_cap" : float(total_cap),
                                "open_time" : str(open_time),
                                "close_time" : str(close_time),
                                "mppt_total_cap" : float(mppt_total_cap),
                                "mppt_1_cap" : float(mppt_1_cap),
                                "mppt_2_cap" : float(mppt_2_cap),
                                "mppt_3_cap" : float(mppt_3_cap),
                                "mppt_4_cap" : float(mppt_4_cap),
                                "mppt_5_cap" : float(mppt_5_cap),
                                "mppt_6_cap" : float(mppt_6_cap),
                                "mppt_7_cap" : float(mppt_7_cap),
                                "mppt_8_cap" : float(mppt_8_cap),
                                "mppt_9_cap" : float(mppt_9_cap),
                                "mppt_10_cap" : float(mppt_10_cap),
                                "run_state" : float(run_state)
                            }
                        }
                    ]          
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement realtime_device_inverter Success : Device Type " , devTypeId)
            except Exception as e:
                print("Error processing message: Get Device Real KPI Inverter ", e)
        else:
            print("❌ HTTP Error:", response.status_code, response.text)
            
        time.sleep(5)

# V.2 Function GET Real KPI Device Grid Meter , Call Every 5 Minutes
def get_real_kpi_device_power_meter(xsrf):
    url = f"{BASE_URL}/thirdData/getDevRealKpi"
    device_t17_data , device_t47_data = query_get_power_meter()
    join_t17_devId = ",".join(str(item["devId"]) for item in device_t17_data)
    join_t47_devId = ",".join(str(item["devId"]) for item in device_t47_data)
    # print(join_t1_devId)
    # return
    for loop_device_type in range(0,2):
        if loop_device_type == 0:
            devTypeId = "17"
            devId = join_t17_devId
        if loop_device_type == 1:
            devTypeId = "47"
            devId = join_t47_devId
        headers = {"Content-Type": "application/json","xsrf-token":xsrf}
        payload = {"devIds" : devId,"devTypeId":devTypeId}
        session = requests.Session() 
        response = session.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # print (data)
            # return
            try:
                for dat in range(0,len(data["data"])):
                    # print(data["data"][dat]["devId"])
                    device_name , plant_code , plant_name = query_get_device_name(data["data"][dat]["devId"])
                    # print(device_name,plant_code,plant_name)
                    
                    # Check Key In Data
                    if "ab_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["ab_u"] == None :
                            ab_u = 0
                        elif data["data"][dat]["dataItemMap"]["ab_u"] != None :
                            ab_u = data["data"][dat]["dataItemMap"]["ab_u"]                        
                    if "bc_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["bc_u"] == None :
                            bc_u = 0
                        elif data["data"][dat]["dataItemMap"]["bc_u"] != None :
                            bc_u = data["data"][dat]["dataItemMap"]["bc_u"]                        
                    if "ca_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["ca_u"] == None :
                            ca_u = 0
                        elif data["data"][dat]["dataItemMap"]["ca_u"] != None :
                            ca_u = data["data"][dat]["dataItemMap"]["ca_u"]                        
                    if "a_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["a_u"] == None :
                            a_u = 0
                        elif data["data"][dat]["dataItemMap"]["a_u"] != None :
                            a_u = data["data"][dat]["dataItemMap"]["a_u"]                        
                    if "b_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["b_u"] == None :
                            b_u = 0
                        elif data["data"][dat]["dataItemMap"]["b_u"] != None :
                            b_u = data["data"][dat]["dataItemMap"]["b_u"]                        
                    if "c_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["c_u"] == None :
                            c_u = 0
                        elif data["data"][dat]["dataItemMap"]["c_u"] != None :
                            c_u = data["data"][dat]["dataItemMap"]["c_u"]                        
                    if "a_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["a_i"] == None :
                            a_i = 0
                        elif data["data"][dat]["dataItemMap"]["a_i"] != None :
                            a_i = data["data"][dat]["dataItemMap"]["a_i"]                        
                    if "b_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["b_i"] == None :
                            b_i = 0
                        elif data["data"][dat]["dataItemMap"]["b_i"] != None :
                            b_i = data["data"][dat]["dataItemMap"]["b_i"]                        
                    if "c_i" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["c_i"] == None :
                            c_i = 0
                        elif data["data"][dat]["dataItemMap"]["c_i"] != None :
                            c_i = data["data"][dat]["dataItemMap"]["c_i"]
                    if "active_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["active_power"] == None :
                            active_power = 0
                        elif data["data"][dat]["dataItemMap"]["active_power"] != None :
                            active_power = data["data"][dat]["dataItemMap"]["active_power"]
                    if "power_factor" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["power_factor"] == None :
                            power_factor = 0
                        elif data["data"][dat]["dataItemMap"]["power_factor"] != None :
                            power_factor = data["data"][dat]["dataItemMap"]["power_factor"]
                    if "reactive_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["reactive_power"] == None :
                            reactive_power = 0
                        elif data["data"][dat]["dataItemMap"]["reactive_power"] != None :
                            reactive_power = data["data"][dat]["dataItemMap"]["reactive_power"]                        
                    if "grid_frequency" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["grid_frequency"] == None :
                            grid_frequency = 0
                        elif data["data"][dat]["dataItemMap"]["grid_frequency"] != None :
                            grid_frequency = data["data"][dat]["dataItemMap"]["grid_frequency"]
                    if "active_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["active_cap"] == None :
                            active_cap = 0
                        elif data["data"][dat]["dataItemMap"]["active_cap"] != None :
                            active_cap = data["data"][dat]["dataItemMap"]["active_cap"]
                            
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "realtime_device_grid", # Measurement of Sensor
                            "tags": {
                                "plant_code" : plant_code+"/getDevRealKpi",  # Tags of sensor
                                "plant_name" : plant_name,
                                "device_name" : device_name,
                                "device_id" : str(data["data"][dat]["devId"])
                            },
                            "fields": {
                                "devTypeId" : devTypeId,
                                "ab_u" : float(ab_u),
                                "bc_u" : float(bc_u),
                                "ca_u" : float(ca_u),
                                "a_u" : float(a_u),
                                "b_u" : float(b_u),
                                "c_u" : float(c_u),
                                "a_i" : float(a_i),
                                "b_i" : float(b_i),
                                "c_i" : float(c_i),
                                "active_power": float(active_power),
                                "power_factor" : float(power_factor),                            
                                "reactive_power" : float(reactive_power),
                                "grid_frequency" : float(grid_frequency),
                                "active_cap" : float(active_cap)
                            }
                        }
                    ]          
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement realtime_device_grid Success : Device Type " , devTypeId)
            except Exception as e:
                print("Error processing message: Get Device Real KPI Power Meter", e)
        else:
            print("❌ HTTP Error:", response.status_code, response.text)
            
        time.sleep(5)

# V.2 Function GET Real KPI Device Grid Meter , Call Every 5 Minutes
def get_real_kpi_device_battery(xsrf):
    url = f"{BASE_URL}/thirdData/getDevRealKpi"
    device_t39_data = query_get_battery()
    join_t39_devId = ",".join(str(item["devId"]) for item in device_t39_data)
    # print(join_t1_devId)
    # return
    for loop_device_type in range(0,1):
        if loop_device_type == 0:
            devTypeId = "39"
            devId = join_t39_devId
        headers = {"Content-Type": "application/json","xsrf-token":xsrf}
        payload = {"devIds" : devId,"devTypeId":devTypeId}
        session = requests.Session() 
        response = session.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # print (data)
            # return
            try:
                for dat in range(0,len(data["data"])):
                    # print(data["data"][dat]["devId"])
                    device_name , plant_code , plant_name = query_get_device_name(data["data"][dat]["devId"])
                    # print(device_name,plant_code,plant_name)
                    
                    # Check Key In Data
                    if "battery_status" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["battery_status"] == None :
                            battery_status = 0
                        elif data["data"][dat]["dataItemMap"]["battery_status"] != None :
                            battery_status = data["data"][dat]["dataItemMap"]["battery_status"]                        
                    if "max_charge_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["max_charge_power"] == None :
                            max_charge_power = 0
                        elif data["data"][dat]["dataItemMap"]["max_charge_power"] != None :
                            max_charge_power = data["data"][dat]["dataItemMap"]["max_charge_power"]                        
                    if "max_discharge_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["max_discharge_power"] == None :
                            max_discharge_power = 0
                        elif data["data"][dat]["dataItemMap"]["max_discharge_power"] != None :
                            max_discharge_power = data["data"][dat]["dataItemMap"]["max_discharge_power"]                        
                    if "ch_discharge_power" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["ch_discharge_power"] == None :
                            ch_discharge_power = 0
                        elif data["data"][dat]["dataItemMap"]["ch_discharge_power"] != None :
                            ch_discharge_power = data["data"][dat]["dataItemMap"]["ch_discharge_power"]                        
                    if "busbar_u" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["busbar_u"] == None :
                            busbar_u = 0
                        elif data["data"][dat]["dataItemMap"]["busbar_u"] != None :
                            busbar_u = data["data"][dat]["dataItemMap"]["busbar_u"]                        
                    if "battery_soc" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["battery_soc"] == None :
                            battery_soc = 0
                        elif data["data"][dat]["dataItemMap"]["battery_soc"] != None :
                            battery_soc = data["data"][dat]["dataItemMap"]["battery_soc"]                        
                    if "battery_soh" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["battery_soh"] == None :
                            battery_soh = 0
                        elif data["data"][dat]["dataItemMap"]["battery_soh"] != None :
                            battery_soh = data["data"][dat]["dataItemMap"]["battery_soh"]                        
                    if "ch_discharge_model" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["ch_discharge_model"] == None :
                            ch_discharge_model = 0
                        elif data["data"][dat]["dataItemMap"]["ch_discharge_model"] != None :
                            ch_discharge_model = data["data"][dat]["dataItemMap"]["ch_discharge_model"]                        
                    if "charge_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["charge_cap"] == None :
                            charge_cap = 0
                        elif data["data"][dat]["dataItemMap"]["charge_cap"] != None :
                            charge_cap = data["data"][dat]["dataItemMap"]["charge_cap"]
                    if "discharge_cap" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["discharge_cap"] == None :
                            discharge_cap = 0
                        elif data["data"][dat]["dataItemMap"]["discharge_cap"] != None :
                            discharge_cap = data["data"][dat]["dataItemMap"]["discharge_cap"]
                    if "run_state" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["run_state"] == None :
                            run_state = 0
                        elif data["data"][dat]["dataItemMap"]["run_state"] != None :
                            run_state = data["data"][dat]["dataItemMap"]["run_state"]
                            
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "realtime_device_battery", # Measurement of Sensor
                            "tags": {
                                "plant_code" : plant_code+"/getDevRealKpi",  # Tags of sensor
                                "plant_name" : plant_name,
                                "device_name" : device_name,
                                "device_id" : str(data["data"][dat]["devId"])
                            },
                            "fields": {
                                "devTypeId" : devTypeId,
                                "battery_status" : float(battery_status),
                                "max_charge_power" : float(max_charge_power),
                                "max_discharge_power" : float(max_discharge_power),
                                "ch_discharge_power" : float(ch_discharge_power),
                                "busbar_u" : float(busbar_u),
                                "battery_soc" : float(battery_soc),
                                "battery_soh" : float(battery_soh),
                                "ch_discharge_model" : float(ch_discharge_model),
                                "charge_cap" : float(charge_cap),
                                "discharge_cap": float(discharge_cap),
                                "run_state" : float(run_state)
                            }
                        }
                    ]          
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement realtime_device_battery Success : Device Type " , devTypeId)
            except Exception as e:
                print("Error processing message: Get Device Real KPI Battery", e)
        else:
            print("❌ HTTP Error:", response.status_code, response.text)
            
        time.sleep(5)

# V.2 Function GET Real KPI Device Grid Meter , Call Every 5 Minutes
def get_real_kpi_device_emi(xsrf):
    url = f"{BASE_URL}/thirdData/getDevRealKpi"
    device_t10_data = query_get_emi()
    join_t10_devId = ",".join(str(item["devId"]) for item in device_t10_data)
    # print(join_t1_devId)
    # return
    for loop_device_type in range(0,1):
        if loop_device_type == 0:
            devTypeId = "10"
            devId = join_t10_devId
        headers = {"Content-Type": "application/json","xsrf-token":xsrf}
        payload = {"devIds" : devId,"devTypeId":devTypeId}
        session = requests.Session() 
        response = session.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # print (data)
            # return
            try:
                for dat in range(0,len(data["data"])):
                    # print(data["data"][dat]["devId"])
                    device_name , plant_code , plant_name = query_get_device_name(data["data"][dat]["devId"])
                    # print(device_name,plant_code,plant_name)
                    
                    # Check Key In Data
                    if "temperature" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["temperature"] == None :
                            temperature = 0
                        elif data["data"][dat]["dataItemMap"]["temperature"] != None :
                            temperature = data["data"][dat]["dataItemMap"]["temperature"]                        
                    if "pv_temperature" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["pv_temperature"] == None :
                            pv_temperature = 0
                        elif data["data"][dat]["dataItemMap"]["pv_temperature"] != None :
                            pv_temperature = data["data"][dat]["dataItemMap"]["pv_temperature"]                        
                    if "wind_speed" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["wind_speed"] == None :
                            wind_speed = 0
                        elif data["data"][dat]["dataItemMap"]["wind_speed"] != None :
                            wind_speed = data["data"][dat]["dataItemMap"]["wind_speed"]                        
                    if "wind_direction" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["wind_direction"] == None :
                            wind_direction = 0
                        elif data["data"][dat]["dataItemMap"]["wind_direction"] != None :
                            wind_direction = data["data"][dat]["dataItemMap"]["wind_direction"]                        
                    if "radiant_total" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["radiant_total"] == None :
                            radiant_total = 0
                        elif data["data"][dat]["dataItemMap"]["radiant_total"] != None :
                            radiant_total = data["data"][dat]["dataItemMap"]["radiant_total"]                        
                    if "radiant_line" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["radiant_line"] == None :
                            radiant_line = 0
                        elif data["data"][dat]["dataItemMap"]["radiant_line"] != None :
                            radiant_line = data["data"][dat]["dataItemMap"]["radiant_line"]                        
                    if "horiz_radiant_line" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["horiz_radiant_line"] == None :
                            horiz_radiant_line = 0
                        elif data["data"][dat]["dataItemMap"]["horiz_radiant_line"] != None :
                            horiz_radiant_line = data["data"][dat]["dataItemMap"]["horiz_radiant_line"]                        
                    if "horiz_radiant_total" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["horiz_radiant_total"] == None :
                            horiz_radiant_total = 0
                        elif data["data"][dat]["dataItemMap"]["horiz_radiant_total"] != None :
                            horiz_radiant_total = data["data"][dat]["dataItemMap"]["horiz_radiant_total"]
                    if "run_state" in data["data"][dat]["dataItemMap"]:
                        if data["data"][dat]["dataItemMap"]["run_state"] == None :
                            run_state = 0
                        elif data["data"][dat]["dataItemMap"]["run_state"] != None :
                            run_state = data["data"][dat]["dataItemMap"]["run_state"]
                            
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "realtime_device_emi", # Measurement of Sensor
                            "tags": {
                                "plant_code" : plant_code+"/getDevRealKpi",  # Tags of sensor
                                "plant_name" : plant_name,
                                "device_name" : device_name,
                                "device_id" : str(data["data"][dat]["devId"])
                            },
                            "fields": {
                                "devTypeId" : devTypeId,
                                "temperature" : str(temperature),
                                "pv_temperature" : str(pv_temperature),
                                "wind_speed" : str(wind_speed),
                                "wind_direction" : str(wind_direction),
                                "radiant_total" : str(radiant_total),
                                "radiant_line" : str(radiant_line),
                                "horiz_radiant_line" : str(horiz_radiant_line),
                                "horiz_radiant_total" : str(horiz_radiant_total),
                                "run_state" : float(run_state)
                            }
                        }
                    ]          
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement realtime_device_emi Success : Device Type " , devTypeId)
            except Exception as e:
                print("Error processing message: Get Device Real KPI Battery", e)
        else:
            print("❌ HTTP Error:", response.status_code, response.text)
            
        time.sleep(5)

# def call_api():
    # init_influxdb(INFLUXDB_DATABASE)
    # token = get_token()
    # time.sleep(5)
    # get_real_kpi_device_inverter(token)

# # Main System
# schedule.every(5).minutes.do(call_api)
# while True:
    # schedule.run_pending()
    # time.sleep(1)

if __name__ == '__main__':
    init_influxdb(INFLUXDB_DATABASE)
    token = get_token()
    time.sleep(5)
    # get_real_kpi_device_inverter(token)
    # time.sleep(5)
    # get_real_kpi_device_power_meter(token)
    # get_real_kpi_device_battery(token)
    get_real_kpi_device_emi(token)
    
# grid active_power id_6060 meter
# load (inverter id_203 active_power) + (grid active_power id_6060)
# batt t39 (charge+dis)+(inverter1)/1000
# PV inverter(str1)+(charge+discharge)