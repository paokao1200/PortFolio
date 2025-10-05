import requests
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import json
import re
import time

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

# Function GET Plant Code From InfluxDB
def query_get_plant_code():
    customer = []
    # Get Value From Tag Key Plant Code
    plant_code_query = 'SHOW TAG VALUES ON "OANDM" FROM "information_plant" WITH KEY = "plant_code"'
    plant_code_result = influx_client.query(plant_code_query)    
    # Get plant_code
    for point in plant_code_result.get_points():
        original_code = point["value"]
        cleaned_code = re.sub(r"/.*", "", original_code)  # ตัดข้อความหลัง "/"
        customer.append(cleaned_code)
    print(customer)
    return customer
    
# Function GET Plant Name From InfluxDB
def query_get_plant_name(plant_code):
    where_clause = f"plant_code = '{plant_code}/getStationList'"
    query = f"SELECT DISTINCT(plantName) as plantName FROM information_plant WHERE {where_clause}"
    result = influx_client.query(query)
    plant_name = ""
    # Result
    for point in result.get_points():
        plan_name = point['plantName']
    return plan_name

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

# Function GET Station List
def get_station_list_old(xsrf):
    url = f"{BASE_URL}/thirdData/getStationList" #/station  : measurement information_plant 
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    stationList = []
    if response.status_code == 200:
        data = response.json()
        # print(data)
        try:
            for dat in range(0,len(data["data"])):
                stationList.append(data["data"][dat]["stationCode"])
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "realtime_plant", # Measurement of Sensor
                        "tags": {
                            "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                            "plant_name" : "test"
                        },
                        "fields": {
                            "total_income": data["data"][dat]["dataItemMap"]["total_income"],
                            "total_power" : data["data"][dat]["dataItemMap"]["total_power"],
                            "day_on_grid_energy" : data["data"][dat]["dataItemMap"]["day_on_grid_energy"],
                            "day_power": data["data"][dat]["dataItemMap"]["day_power"],
                            "day_use_energy" : data["data"][dat]["dataItemMap"]["day_use_energy"],
                            "day_income" : data["data"][dat]["dataItemMap"]["day_income"],
                            "real_health_state" : data["data"][dat]["dataItemMap"]["real_health_state"],
                            "month_power" : data["data"][dat]["dataItemMap"]["month_power"]
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement realtime_plant Success")
        except Exception as e:
            print("Error processing message: ", data["failCode"],data["data"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)
    print (stationList)
    return stationList

# Function GET Station List
def get_station_list(xsrf):
    url = f"{BASE_URL}/thirdData/stations" #/station  : measurement information_plant 
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"pageNo" : 1}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    stationList = []
    if response.status_code == 200:
        data = response.json()
        # print(data)
        try:
            for dat in range(0,len(data["data"]["list"])):
                stationList.append(data["data"]["list"][dat]["plantCode"])
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "information_plant", # Measurement of Sensor
                        "tags": {
                            "plant_code" : data["data"]["list"][dat]["plantCode"]+"/getStationList",  # Tags of sensor
                        },
                        "fields": {
                            "capacity": data["data"]["list"][dat]["capacity"],
                            "contactMethod" : data["data"]["list"][dat]["contactMethod"],
                            "contactPerson" : data["data"]["list"][dat]["contactPerson"],
                            "gridConnectionDate": data["data"]["list"][dat]["gridConnectionDate"],
                            "latitude" : str(data["data"]["list"][dat]["latitude"]), #Conflict Type
                            "longitude" : str(data["data"]["list"][dat]["longitude"]), #Conflict Type
                            "plantAddress" : data["data"]["list"][dat]["plantAddress"],
                            "plantName" : data["data"]["list"][dat]["plantName"]
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement information_plant Success")
        except Exception as e:
            print("Error processing message: ", data["failCode"],data["data"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)
    print (stationList)
    return stationList
    
# V.1 Function GET Real KPI Station , Call Every 5 Minutes
def get_real_kpi_station(xsrf , customer):
    url = f"{BASE_URL}/thirdData/getStationRealKpi"
    join_customer = ",".join(customer)
    print (join_customer)
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        try:
            for dat in range(0,len(data["data"])):
                print(data["data"][dat])
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "realtime_plant", # Measurement of Sensor
                        "tags": {
                            "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                            "plant_name" : "test"
                        },
                        "fields": {
                            "total_income": data["data"][dat]["dataItemMap"]["total_income"],
                            "total_power" : data["data"][dat]["dataItemMap"]["total_power"],
                            "day_on_grid_energy" : data["data"][dat]["dataItemMap"]["day_on_grid_energy"],
                            "day_power": data["data"][dat]["dataItemMap"]["day_power"],
                            "day_use_energy" : data["data"][dat]["dataItemMap"]["day_use_energy"],
                            "day_income" : data["data"][dat]["dataItemMap"]["day_income"],
                            "real_health_state" : data["data"][dat]["dataItemMap"]["real_health_state"],
                            "month_power" : data["data"][dat]["dataItemMap"]["month_power"]
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement realtime_plant Success")
        except Exception as e:
            print("Error processing message: Get Real KPI", data["failCode"],data["data"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# V.2 Function GET Real KPI Station , Call Every 5 Minutes
def get_real_kpi_station_influx(xsrf):
    url = f"{BASE_URL}/thirdData/getStationRealKpi"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print (data)
        try:
            for dat in range(0,len(data["data"])):
                print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plan_name:",plant_name)
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "realtime_plant", # Measurement of Sensor
                        "tags": {
                            "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                            "plant_name" : plant_name
                        },
                        "fields": {
                            "total_income": data["data"][dat]["dataItemMap"]["total_income"],
                            "total_power" : data["data"][dat]["dataItemMap"]["total_power"],
                            "day_on_grid_energy" : data["data"][dat]["dataItemMap"]["day_on_grid_energy"],
                            "day_power": data["data"][dat]["dataItemMap"]["day_power"],
                            "day_use_energy" : data["data"][dat]["dataItemMap"]["day_use_energy"],
                            "day_income" : data["data"][dat]["dataItemMap"]["day_income"],
                            "real_health_state" : data["data"][dat]["dataItemMap"]["real_health_state"],
                            "month_power" : data["data"][dat]["dataItemMap"]["month_power"]
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement realtime_plant Success")
        except Exception as e:
            print("Error processing message: Get Real KPI", data["failCode"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# Function GET Hour KPI Station        
def get_hour_kpi_station(xsrf , customer):
    url = f"{BASE_URL}/thirdData/getKpiStationHour"
    join_customer = ",".join(customer)
    print (join_customer)
    yesterday = datetime.now() - timedelta(days=1)
    # yesterday = datetime.now()
    unix_time = int(yesterday.timestamp()* 1000)
    print(unix_time)
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        try:
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
                print(data["data"][dat]["collectTime"])
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "hour_kpi", # Measurement of Sensor
                        "tags": {
                            "station" : data["data"][dat]["stationCode"]  # Tags of sensor
                        },
                        "fields": {
                            "collectTime": data["data"][dat]["collectTime"],
                            "dischargeCap" : data["data"][dat]["dataItemMap"]["dischargeCap"],
                            "radiation_intensity" : data["data"][dat]["dataItemMap"]["radiation_intensity"],
                            "inverter_power": data["data"][dat]["dataItemMap"]["inverter_power"],
                            "inverterYield" : data["data"][dat]["dataItemMap"]["inverterYield"],
                            "power_profit" : data["data"][dat]["dataItemMap"]["power_profit"],
                            "theory_power" : data["data"][dat]["dataItemMap"]["theory_power"],
                            "PVYield" : data["data"][dat]["dataItemMap"]["PVYield"],
                            "ongrid_power" : data["data"][dat]["dataItemMap"]["ongrid_power"],
                            "chargeCap" : data["data"][dat]["dataItemMap"]["chargeCap"],
                            "selfProvide" : data["data"][dat]["dataItemMap"]["selfProvide"]
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement hour_kpi Success")
        except Exception as e:
            print("Error processing message: Get Hour KPI", data["failCode"],data["data"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)
 
# Function GET Day KPI Station 
def get_day_kpi_station(xsrf , customer):
    url = f"{BASE_URL}/thirdData/getKpiStationDay"
    join_customer = ",".join(customer)
    print (join_customer)
    yesterday = datetime.now() - timedelta(days=1)
    # yesterday = datetime.now()
    unix_time = int(yesterday.timestamp()* 1000)
    print(unix_time)
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        try :
            for dat in range(0,len(data["data"])):
                print(data["data"][dat])
                inverter_power = None 
                inverterYield = None 
                selfUsePower = None
                reduction_total_tree = None
                power_profit = None
                perpower_profit = None
                theory_power = None
                PVYield = None
                reduction_total_co2= None                  
                chargeCap = None
                selfProvide = None
                dischargeCap = None
                radiation_intensity = None
                installed_capacity = None
                use_power = None
                reduction_total_coal = None
                ongrid_power = None
                buyPower = None
                
                if "inverter_power" in data["data"][dat]["dataItemMap"]:
                    inverter_power = data["data"][dat]["dataItemMap"]["inverter_power"]
                if "inverterYield" in data["data"][dat]["dataItemMap"]:
                    inverterYield = data["data"][dat]["dataItemMap"]["inverterYield"]
                if "selfUsePower" in data["data"][dat]["dataItemMap"]:                
                    selfUsePower = data["data"][dat]["dataItemMap"]["selfUsePower"]
                if "reduction_total_tree" in data["data"][dat]["dataItemMap"]:
                    reduction_total_tree = data["data"][dat]["dataItemMap"]["reduction_total_tree"]
                if "power_profit" in data["data"][dat]["dataItemMap"]:
                    power_profit = data["data"][dat]["dataItemMap"]["power_profit"]
                if "perpower_profit" in data["data"][dat]["dataItemMap"]:                
                    perpower_profit = data["data"][dat]["dataItemMap"]["perpower_profit"]
                if "theory_power" in data["data"][dat]["dataItemMap"]:
                    theory_power = data["data"][dat]["dataItemMap"]["theory_power"]
                if "PVYield" in data["data"][dat]["dataItemMap"]:
                    PVYield = data["data"][dat]["dataItemMap"]["PVYield"]
                if "reduction_total_co2" in data["data"][dat]["dataItemMap"]:
                    reduction_total_co2 = data["data"][dat]["dataItemMap"]["reduction_total_co2"]
                if "chargeCap" in data["data"][dat]["dataItemMap"]:                
                    chargeCap = data["data"][dat]["dataItemMap"]["chargeCap"]
                if "selfProvide" in data["data"][dat]["dataItemMap"]:
                    selfProvide = data["data"][dat]["dataItemMap"]["selfProvide"]
                if "dischargeCap" in data["data"][dat]["dataItemMap"]:
                    dischargeCap = data["data"][dat]["dataItemMap"]["dischargeCap"]
                if "radiation_intensity" in data["data"][dat]["dataItemMap"]:
                    radiation_intensity = data["data"][dat]["dataItemMap"]["radiation_intensity"]
                if "installed_capacity" in data["data"][dat]["dataItemMap"]:
                    installed_capacity = data["data"][dat]["dataItemMap"]["installed_capacity"]
                if "use_power" in data["data"][dat]["dataItemMap"]:
                    use_power = data["data"][dat]["dataItemMap"]["use_power"]
                if "reduction_total_coal" in data["data"][dat]["dataItemMap"]:                
                    reduction_total_coal = data["data"][dat]["dataItemMap"]["reduction_total_coal"]
                if "ongrid_power" in data["data"][dat]["dataItemMap"]:
                    ongrid_power = data["data"][dat]["dataItemMap"]["ongrid_power"]
                if "buyPower" in data["data"][dat]["dataItemMap"]:
                    buyPower = data["data"][dat]["dataItemMap"]["buyPower"]
                    
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "day_kpi", # Measurement of Sensor
                        "tags": {
                            "station" : data["data"][dat]["stationCode"]  # Tags of sensor
                        },
                        "fields": {
                            "collectTime": data["data"][dat]["collectTime"],                        
                            "inverter_power": inverter_power,
                            "inverterYield" : inverterYield,
                            "selfUsePower" : selfUsePower,
                            "reduction_total_tree" : reduction_total_tree,
                            "power_profit" : power_profit,
                            "perpower_profit" : perpower_profit,
                            "theory_power" : theory_power,
                            "PVYield" : PVYield,
                            "reduction_total_co2" : reduction_total_co2,                        
                            "chargeCap" : chargeCap,
                            "selfProvide" : selfProvide,
                            "dischargeCap" : dischargeCap,
                            "radiation_intensity" : radiation_intensity,
                            "installed_capacity" : installed_capacity,
                            "use_power" : use_power,
                            "reduction_total_coal" : reduction_total_coal,
                            "ongrid_power" : ongrid_power,
                            "buyPower" : buyPower,
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement day_kpi Success")
        except Exception as e:
            print("Error processing message: Get Day KPI", data["failCode"],data["data"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# Function GET Month KPI Station
def get_month_kpi_station(xsrf , customer):
    url = f"{BASE_URL}/thirdData/getKpiStationMonth"
    join_customer = ",".join(customer)
    print (join_customer)
    # yesterday = datetime.now() - timedelta(days=1)
    yesterday = datetime.now()
    unix_time = int(yesterday.timestamp()* 1000)
    print(unix_time)
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        try:
            for dat in range(0,len(data["data"])):
                print(data["data"][dat])
                inverter_power = None 
                inverterYield = None 
                selfUsePower = None
                reduction_total_tree = None
                power_profit = None
                perpower_profit = None
                theory_power = None
                PVYield = None
                reduction_total_co2= None                  
                chargeCap = None
                selfProvide = None
                dischargeCap = None
                radiation_intensity = None
                installed_capacity = None
                use_power = None
                reduction_total_coal = None
                ongrid_power = None
                buyPower = None
                
                if "inverter_power" in data["data"][dat]["dataItemMap"]:
                    inverter_power = data["data"][dat]["dataItemMap"]["inverter_power"]
                if "inverterYield" in data["data"][dat]["dataItemMap"]:
                    inverterYield = data["data"][dat]["dataItemMap"]["inverterYield"]
                if "selfUsePower" in data["data"][dat]["dataItemMap"]:                
                    selfUsePower = data["data"][dat]["dataItemMap"]["selfUsePower"]
                if "reduction_total_tree" in data["data"][dat]["dataItemMap"]:
                    reduction_total_tree = data["data"][dat]["dataItemMap"]["reduction_total_tree"]
                if "power_profit" in data["data"][dat]["dataItemMap"]:
                    power_profit = data["data"][dat]["dataItemMap"]["power_profit"]
                if "perpower_profit" in data["data"][dat]["dataItemMap"]:                
                    perpower_profit = data["data"][dat]["dataItemMap"]["perpower_profit"]
                if "theory_power" in data["data"][dat]["dataItemMap"]:
                    theory_power = data["data"][dat]["dataItemMap"]["theory_power"]
                if "PVYield" in data["data"][dat]["dataItemMap"]:
                    PVYield = data["data"][dat]["dataItemMap"]["PVYield"]
                if "reduction_total_co2" in data["data"][dat]["dataItemMap"]:
                    reduction_total_co2 = data["data"][dat]["dataItemMap"]["reduction_total_co2"]
                if "chargeCap" in data["data"][dat]["dataItemMap"]:                
                    chargeCap = data["data"][dat]["dataItemMap"]["chargeCap"]
                if "selfProvide" in data["data"][dat]["dataItemMap"]:
                    selfProvide = data["data"][dat]["dataItemMap"]["selfProvide"]
                if "dischargeCap" in data["data"][dat]["dataItemMap"]:
                    dischargeCap = data["data"][dat]["dataItemMap"]["dischargeCap"]
                if "radiation_intensity" in data["data"][dat]["dataItemMap"]:
                    radiation_intensity = data["data"][dat]["dataItemMap"]["radiation_intensity"]
                if "installed_capacity" in data["data"][dat]["dataItemMap"]:
                    installed_capacity = data["data"][dat]["dataItemMap"]["installed_capacity"]
                if "use_power" in data["data"][dat]["dataItemMap"]:
                    use_power = data["data"][dat]["dataItemMap"]["use_power"]
                if "reduction_total_coal" in data["data"][dat]["dataItemMap"]:                
                    reduction_total_coal = data["data"][dat]["dataItemMap"]["reduction_total_coal"]
                if "ongrid_power" in data["data"][dat]["dataItemMap"]:
                    ongrid_power = data["data"][dat]["dataItemMap"]["ongrid_power"]
                if "buyPower" in data["data"][dat]["dataItemMap"]:
                    buyPower = data["data"][dat]["dataItemMap"]["buyPower"]
                    
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "month_kpi", # Measurement of Sensor
                        "tags": {
                            "station" : data["data"][dat]["stationCode"]  # Tags of sensor
                        },
                        "fields": {
                            "collectTime": data["data"][dat]["collectTime"],                        
                            "inverter_power": inverter_power,
                            "inverterYield" : inverterYield,
                            "selfUsePower" : selfUsePower,
                            "reduction_total_tree" : reduction_total_tree,
                            "power_profit" : power_profit,
                            "perpower_profit" : perpower_profit,
                            "theory_power" : theory_power,
                            "PVYield" : PVYield,
                            "reduction_total_co2" : reduction_total_co2,                        
                            "chargeCap" : chargeCap,
                            "selfProvide" : selfProvide,
                            "dischargeCap" : dischargeCap,
                            "radiation_intensity" : radiation_intensity,
                            "installed_capacity" : installed_capacity,
                            "use_power" : use_power,
                            "reduction_total_coal" : reduction_total_coal,
                            "ongrid_power" : ongrid_power,
                            "buyPower" : buyPower,
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement month_kpi Success")
        except Exception as e:
            print("Error processing message:", data["failCode"],data["data"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# Function GET Year KPI Station
def get_year_kpi_station(xsrf , customer):
    url = f"{BASE_URL}/thirdData/getKpiStationYear"
    join_customer = ",".join(customer)
    print (join_customer)
    # yesterday = datetime.now() - timedelta(days=1)
    yesterday = datetime.now()
    unix_time = int(yesterday.timestamp()* 1000)
    print(unix_time)
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print (data)
        try:
            for dat in range(0,len(data["data"])):
                print(data["data"][dat])
                inverter_power = None 
                inverterYield = None 
                selfUsePower = None
                reduction_total_tree = None
                power_profit = None
                perpower_profit = None
                theory_power = None
                PVYield = None
                reduction_total_co2= None                  
                chargeCap = None
                selfProvide = None
                dischargeCap = None
                radiation_intensity = None
                installed_capacity = None
                use_power = None
                reduction_total_coal = None
                ongrid_power = None
                buyPower = None
                
                if "inverter_power" in data["data"][dat]["dataItemMap"]:
                    inverter_power = data["data"][dat]["dataItemMap"]["inverter_power"]
                if "inverterYield" in data["data"][dat]["dataItemMap"]:
                    inverterYield = data["data"][dat]["dataItemMap"]["inverterYield"]
                if "selfUsePower" in data["data"][dat]["dataItemMap"]:                
                    selfUsePower = data["data"][dat]["dataItemMap"]["selfUsePower"]
                if "reduction_total_tree" in data["data"][dat]["dataItemMap"]:
                    reduction_total_tree = data["data"][dat]["dataItemMap"]["reduction_total_tree"]
                if "power_profit" in data["data"][dat]["dataItemMap"]:
                    power_profit = data["data"][dat]["dataItemMap"]["power_profit"]
                if "perpower_profit" in data["data"][dat]["dataItemMap"]:                
                    perpower_profit = data["data"][dat]["dataItemMap"]["perpower_profit"]
                if "theory_power" in data["data"][dat]["dataItemMap"]:
                    theory_power = data["data"][dat]["dataItemMap"]["theory_power"]
                if "PVYield" in data["data"][dat]["dataItemMap"]:
                    PVYield = data["data"][dat]["dataItemMap"]["PVYield"]
                if "reduction_total_co2" in data["data"][dat]["dataItemMap"]:
                    reduction_total_co2 = data["data"][dat]["dataItemMap"]["reduction_total_co2"]
                if "chargeCap" in data["data"][dat]["dataItemMap"]:                
                    chargeCap = data["data"][dat]["dataItemMap"]["chargeCap"]
                if "selfProvide" in data["data"][dat]["dataItemMap"]:
                    selfProvide = data["data"][dat]["dataItemMap"]["selfProvide"]
                if "dischargeCap" in data["data"][dat]["dataItemMap"]:
                    dischargeCap = data["data"][dat]["dataItemMap"]["dischargeCap"]
                if "radiation_intensity" in data["data"][dat]["dataItemMap"]:
                    radiation_intensity = data["data"][dat]["dataItemMap"]["radiation_intensity"]
                if "installed_capacity" in data["data"][dat]["dataItemMap"]:
                    installed_capacity = data["data"][dat]["dataItemMap"]["installed_capacity"]
                if "use_power" in data["data"][dat]["dataItemMap"]:
                    use_power = data["data"][dat]["dataItemMap"]["use_power"]
                if "reduction_total_coal" in data["data"][dat]["dataItemMap"]:                
                    reduction_total_coal = data["data"][dat]["dataItemMap"]["reduction_total_coal"]
                if "ongrid_power" in data["data"][dat]["dataItemMap"]:
                    ongrid_power = data["data"][dat]["dataItemMap"]["ongrid_power"]
                if "buyPower" in data["data"][dat]["dataItemMap"]:
                    buyPower = data["data"][dat]["dataItemMap"]["buyPower"]
                    
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "year_kpi", # Measurement of Sensor
                        "tags": {
                            "station" : data["data"][dat]["stationCode"]  # Tags of sensor
                        },
                        "fields": {
                            "collectTime": data["data"][dat]["collectTime"],                        
                            "inverter_power": inverter_power,
                            "inverterYield" : inverterYield,
                            "selfUsePower" : selfUsePower,
                            "reduction_total_tree" : reduction_total_tree,
                            "power_profit" : power_profit,
                            "perpower_profit" : perpower_profit,
                            "theory_power" : theory_power,
                            "PVYield" : PVYield,
                            "reduction_total_co2" : reduction_total_co2,                        
                            "chargeCap" : chargeCap,
                            "selfProvide" : selfProvide,
                            "dischargeCap" : dischargeCap,
                            "radiation_intensity" : radiation_intensity,
                            "installed_capacity" : installed_capacity,
                            "use_power" : use_power,
                            "reduction_total_coal" : reduction_total_coal,
                            "ongrid_power" : ongrid_power,
                            "buyPower" : buyPower,
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement year_kpi Success")
        except Exception as e:
            print("Error processing message:", data["failCode"],data["data"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# V.1 Function GET Device List , DeviceType [1,38,46] 
def get_device_list(xsrf , customer):
    url = f"{BASE_URL}/thirdData/getDevList"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)   
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    count = 0
    countId = []
    if response.status_code == 200:
        data = response.json()
        print (data)
        for dat in range(0,len(data["data"])):
            # print(data["data"][dat])
            if data["data"][dat]["devTypeId"] == 38 or data["data"][dat]["devTypeId"] == 46:
                count+=1
                countId.append(data["data"][dat]["devTypeId"])
    else:
        print("❌ HTTP Error:", response.status_code, response.text)
    print("Device Count : ",count)
    print("Other Type : ",set(countId))
    
# V.2 Function GET Device List , DeviceType [1,38,46] 
def get_device_list_influx(xsrf):
    url = f"{BASE_URL}/thirdData/getDevList"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)   
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    count = 0
    countId = []
    if response.status_code == 200:
        data = response.json()
        # print (data)
        for dat in range(0,len(data["data"])):
            # print(data["data"][dat])
            if data["data"][dat]["devTypeId"] == 1 or data["data"][dat]["devTypeId"] == 38 or data["data"][dat]["devTypeId"] == 46:
                count+=1
                countId.append(data["data"][dat]["devTypeId"])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plan_name:",plant_name)
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "device_plant", # Measurement of Sensor
                        "tags": {
                            "plant_code" : data["data"][dat]["stationCode"]+"/getDevList",  # Tags of sensor
                            "plant_name" : plant_name
                        },
                        "fields": {
                            "devDn": data["data"][dat]["devDn"],
                            "devName" : data["data"][dat]["devName"],
                            "devTypeId" : data["data"][dat]["devTypeId"],
                            "esnCode": data["data"][dat]["esnCode"],
                            "id" : str(data["data"][dat]["id"]),
                            "invType" : data["data"][dat]["invType"],
                            "latitude" : str(data["data"][dat]["latitude"]), #Conflict Type
                            "longitude" : str(data["data"][dat]["longitude"]), #Conflict Type
                            "model" : data["data"][dat]["model"],
                            "optimizerNumber" : "OPT",
                            "softwareVersion" : "SW"
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                influx_client.write_points(json_body)
                print(f"Insert measurement device_plant Success")
    else:
        print("❌ HTTP Error:", response.status_code, response.text)
    print("Device Count : ",count)
    print("Other Type : ",set(countId))

# Main System
if __name__ == '__main__':
    init_influxdb(INFLUXDB_DATABASE)
    # query_get_plant_code()
    # station="NE=52621567"    
    # station = ["NE=52621567","NE=51587902","NE=50958399"]
    # query_inf(station)
    token = get_token()
    # station = get_station_list_old(token)
    time.sleep(15)
    station = get_station_list(token)
    time.sleep(15)
    get_device_list_influx(token)
    time.sleep(15)
    # get_real_kpi_station(token,station)
    get_real_kpi_station_influx(token)
    # get_hour_kpi_station(token,station)
    # get_day_kpi_station(token,station)
    # get_month_kpi_station(token,station)
    # get_year_kpi_station(token,station)
    # get_device_list(token,station)
    
    
# Device Count (NE=52621567) :  125
# Device Type :  {1, 38, 39, 45577, 45578, 10, 46, 47, 17, 62, 63}
# Task : information_plant(station,device) 2 function , realtime_dev 3 