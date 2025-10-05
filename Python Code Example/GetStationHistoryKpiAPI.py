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

# Function GET Day KPI Station 
def get_day_kpi_station(xsrf):
    url = f"{BASE_URL}/thirdData/getKpiStationDay"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    date = datetime.now()
    unix_time = int(date.timestamp()* 1000)
    print(unix_time)
    # converted_time = datetime.fromtimestamp(unix_time/1000)
    # print(converted_time)
    # return
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    # payload = {"stationCodes" : "NE=52621567","collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        # return
        try :
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plant_name:",plant_name)
                
                inverter_power = 0.0 
                inverterYield = 0.0 
                selfUsePower = 0.0
                reduction_total_tree = 0.0
                power_profit = 0.0
                perpower_ratio = 0.0
                theory_power = 0.0
                PVYield = 0.0
                reduction_total_co2= 0.0                  
                chargeCap = 0.0
                selfProvide = 0.0
                dischargeCap = 0.0
                radiation_intensity = 0.0
                installed_capacity = 0.0
                use_power = 0.0
                reduction_total_coal = 0.0
                ongrid_power = 0.0
                buyPower = 0.0
                performance_ratio = 0.0
                
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
                if "perpower_ratio" in data["data"][dat]["dataItemMap"]:                
                    perpower_ratio = data["data"][dat]["dataItemMap"]["perpower_ratio"]
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
                if "performance_ratio" in data["data"][dat]["dataItemMap"]:
                    performance_ratio = data["data"][dat]["dataItemMap"]["performance_ratio"]
                
                converted_time = datetime.fromtimestamp(int(data["data"][dat]["collectTime"])/1000)
                # print(converted_time)            
                
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "day_kpi", # Measurement of Sensor
                        "tags": {                            
                            "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                            "plant_name" : plant_name
                        },
                        "fields": {
                            "collectTime": int(data["data"][dat]["collectTime"]),
                            "date": str(converted_time),
                            "inverter_power": float(inverter_power),
                            "inverterYield" : float(inverterYield),
                            "selfUsePower" : float(selfUsePower),
                            "reduction_total_tree" : float(reduction_total_tree),
                            "power_profit" : float(power_profit),
                            "perpower_ratio" : float(perpower_ratio),
                            "theory_power" : float(theory_power),
                            "PVYield" : float(PVYield),
                            "reduction_total_co2" : float(reduction_total_co2),                        
                            "chargeCap" : float(chargeCap),
                            "selfProvide" : float(selfProvide),
                            "dischargeCap" : float(dischargeCap),
                            "radiation_intensity" : float(radiation_intensity),
                            "installed_capacity" : float(installed_capacity),
                            "use_power" : float(use_power),
                            "reduction_total_coal" : float(reduction_total_coal),
                            "ongrid_power" : float(ongrid_power),
                            "buyPower" : float(buyPower),
                            "performance_ratio" : float(performance_ratio)
                        }
                    }
                ]                
                # Insert Data to InfluxDB
                # print(json.dumps(json_body))
                influx_client.write_points(json_body)
                print(f"Insert measurement day_kpi Success")
                # return
        except Exception as e:
            print("Error processing message: Get Day KPI", data["failCode"],e)
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# Function GET Day KPI Station 
def get_month_kpi_station(xsrf):
    url = f"{BASE_URL}/thirdData/getKpiStationMonth"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    date = datetime.now()
    unix_time = int(date.timestamp()* 1000)
    print(unix_time)
    # converted_time = datetime.fromtimestamp(unix_time/1000)
    # print(converted_time)
    # return
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    # payload = {"stationCodes" : "NE=52621567","collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        # return
        try :
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plan_name:",plant_name)
                
                inverter_power = 0.0 
                inverterYield = 0.0 
                selfUsePower = 0.0
                reduction_total_tree = 0.0
                power_profit = 0.0
                perpower_ratio = 0.0
                theory_power = 0.0
                PVYield = 0.0
                reduction_total_co2= 0.0                  
                chargeCap = 0.0
                selfProvide = 0.0
                dischargeCap = 0.0
                radiation_intensity = 0.0
                installed_capacity = 0.0
                use_power = 0.0
                reduction_total_coal = 0.0
                ongrid_power = 0.0
                buyPower = 0.0
                performance_ratio = 0.0
                
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
                if "perpower_ratio" in data["data"][dat]["dataItemMap"]:                
                    perpower_ratio = data["data"][dat]["dataItemMap"]["perpower_ratio"]
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
                if "performance_ratio" in data["data"][dat]["dataItemMap"]:
                    performance_ratio = data["data"][dat]["dataItemMap"]["performance_ratio"]
                
                converted_time = datetime.fromtimestamp(int(data["data"][dat]["collectTime"])/1000)
                # print(converted_time)
                
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "month_kpi", # Measurement of Sensor
                        "tags": {                            
                            "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                            "plant_name" : plant_name
                        },
                        "fields": {
                            "collectTime": int(data["data"][dat]["collectTime"]),
                            "date": str(converted_time),
                            "inverter_power": float(inverter_power),
                            "inverterYield" : float(inverterYield),
                            "selfUsePower" : float(selfUsePower),
                            "reduction_total_tree" : float(reduction_total_tree),
                            "power_profit" : float(power_profit),
                            "perpower_ratio" : float(perpower_ratio),
                            "theory_power" : float(theory_power),
                            "PVYield" : float(PVYield),
                            "reduction_total_co2" : float(reduction_total_co2),                        
                            "chargeCap" : float(chargeCap),
                            "selfProvide" : float(selfProvide),
                            "dischargeCap" : float(dischargeCap),
                            "radiation_intensity" : float(radiation_intensity),
                            "installed_capacity" : float(installed_capacity),
                            "use_power" : float(use_power),
                            "reduction_total_coal" : float(reduction_total_coal),
                            "ongrid_power" : float(ongrid_power),
                            "buyPower" : float(buyPower),                            
                            "performance_ratio" : float(performance_ratio)
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                # print(json.dumps(json_body))
                influx_client.write_points(json_body)
                print(f"Insert measurement month_kpi Success")
                # return
        except Exception as e:
            print("Error processing message: Get Month KPI", data["failCode"],e)
    else:
        print("❌ HTTP Error:", response.status_code, response.text)
 
# Function GET Day KPI Station 
def get_year_kpi_station(xsrf):
    url = f"{BASE_URL}/thirdData/getKpiStationYear"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    date = datetime.now()
    unix_time = int(date.timestamp()* 1000)
    print(unix_time)
    # converted_time = datetime.fromtimestamp(unix_time/1000)
    # print(converted_time)
    # return
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    # payload = {"stationCodes" : "NE=52621567","collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        # return
        try :
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plan_name:",plant_name)
                
                inverter_power = 0.0 
                inverterYield = 0.0 
                selfUsePower = 0.0
                reduction_total_tree = 0.0
                power_profit = 0.0
                perpower_ratio = 0.0
                theory_power = 0.0
                PVYield = 0.0
                reduction_total_co2= 0.0                  
                chargeCap = 0.0
                selfProvide = 0.0
                dischargeCap = 0.0
                radiation_intensity = 0.0
                installed_capacity = 0.0
                use_power = 0.0
                reduction_total_coal = 0.0
                ongrid_power = 0.0
                buyPower = 0.0
                performance_ratio = 0.0
                
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
                if "perpower_ratio" in data["data"][dat]["dataItemMap"]:                
                    perpower_ratio = data["data"][dat]["dataItemMap"]["perpower_ratio"]
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
                if "performance_ratio" in data["data"][dat]["dataItemMap"]:
                    performance_ratio = data["data"][dat]["dataItemMap"]["performance_ratio"]
                
                converted_time = datetime.fromtimestamp(int(data["data"][dat]["collectTime"])/1000)
                # print(converted_time)
                
                # Set Json Format For Write Data to InfluxDB
                json_body = [
                    {
                        "measurement": "year_kpi", # Measurement of Sensor
                        "tags": {                            
                            "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                            "plant_name" : plant_name
                        },
                        "fields": {
                            "collectTime": int(data["data"][dat]["collectTime"]),
                            "date": str(converted_time),
                            "inverter_power": float(inverter_power),
                            "inverterYield" : float(inverterYield),
                            "selfUsePower" : float(selfUsePower),
                            "reduction_total_tree" : float(reduction_total_tree),
                            "power_profit" : float(power_profit),
                            "perpower_ratio" : float(perpower_ratio),
                            "theory_power" : float(theory_power),
                            "PVYield" : float(PVYield),
                            "reduction_total_co2" : float(reduction_total_co2),                        
                            "chargeCap" : float(chargeCap),
                            "selfProvide" : float(selfProvide),
                            "dischargeCap" : float(dischargeCap),
                            "radiation_intensity" : float(radiation_intensity),
                            "installed_capacity" : float(installed_capacity),
                            "use_power" : float(use_power),
                            "reduction_total_coal" : float(reduction_total_coal),
                            "ongrid_power" : float(ongrid_power),
                            "buyPower" : float(buyPower),
                            "performance_ratio" : float(performance_ratio)
                        }
                    }
                ]          
                # Insert Data to InfluxDB
                # print(json.dumps(json_body))
                influx_client.write_points(json_body)
                print(f"Insert measurement year_kpi Success")
                # return
        except Exception as e:
            print("Error processing message: Get Year KPI", data["failCode"],e)
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# Function GET Hour Day KPI Station 
def get_current_hour_kpi_station(xsrf):
    url = f"{BASE_URL}/thirdData/getKpiStationHour"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    date = datetime.now()
    unix_time = int(date.timestamp()* 1000)
    print(unix_time)
    # converted_time = datetime.fromtimestamp(unix_time/1000)
    # print(converted_time)
    # return
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    # payload = {"stationCodes" : "NE=52621567","collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        # return
        try :
            for dat in range(0,len(data["data"])):
                print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plant_name:",plant_name)
                
                inverter_power = 0.0 
                inverterYield = 0.0
                power_profit = 0.0
                theory_power = 0.0
                PVYield = 0.0                  
                chargeCap = 0.0
                selfProvide = 0.0
                dischargeCap = 0.0
                radiation_intensity = 0.0
                ongrid_power = 0.0
                
                if "inverter_power" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["inverter_power"] == None :
                        inverter_power = 0.0
                    elif data["data"][dat]["dataItemMap"]["inverter_power"] != None :
                        inverter_power = data["data"][dat]["dataItemMap"]["inverter_power"]
                if "inverterYield" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["inverterYield"] == None :
                        inverterYield = 0.0
                    elif data["data"][dat]["dataItemMap"]["inverterYield"] != None :
                        inverterYield = data["data"][dat]["dataItemMap"]["inverterYield"]
                if "power_profit" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["power_profit"] == None :
                        power_profit = 0.0
                    elif data["data"][dat]["dataItemMap"]["power_profit"] != None :
                        power_profit = data["data"][dat]["dataItemMap"]["power_profit"]                
                if "theory_power" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["theory_power"] == None :
                        theory_power = 0.0
                    elif data["data"][dat]["dataItemMap"]["theory_power"] != None :
                        theory_power = data["data"][dat]["dataItemMap"]["theory_power"]
                if "PVYield" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["PVYield"] == None :
                        PVYield = 0.0
                    elif data["data"][dat]["dataItemMap"]["PVYield"] != None :
                        PVYield = data["data"][dat]["dataItemMap"]["PVYield"]                
                if "chargeCap" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["chargeCap"] == None :
                        chargeCap = 0.0
                    elif data["data"][dat]["dataItemMap"]["chargeCap"] != None :
                        chargeCap = data["data"][dat]["dataItemMap"]["chargeCap"]
                if "selfProvide" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["selfProvide"] == None :
                        selfProvide = 0.0
                    elif data["data"][dat]["dataItemMap"]["selfProvide"] != None :
                        selfProvide = data["data"][dat]["dataItemMap"]["selfProvide"]
                if "dischargeCap" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["dischargeCap"] == None :
                        dischargeCap = 0.0
                    elif data["data"][dat]["dataItemMap"]["dischargeCap"] != None :
                        dischargeCap = data["data"][dat]["dataItemMap"]["dischargeCap"]
                if "radiation_intensity" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["radiation_intensity"] == None :
                        radiation_intensity = 0.0
                    elif data["data"][dat]["dataItemMap"]["radiation_intensity"] != None :
                        radiation_intensity = data["data"][dat]["dataItemMap"]["radiation_intensity"]                
                if "ongrid_power" in data["data"][dat]["dataItemMap"]:
                    if data["data"][dat]["dataItemMap"]["ongrid_power"] == None :
                        ongrid_power = 0.0
                    elif data["data"][dat]["dataItemMap"]["ongrid_power"] != None :
                        ongrid_power = data["data"][dat]["dataItemMap"]["ongrid_power"]
                
                converted_time = datetime.fromtimestamp(int(data["data"][dat]["collectTime"])/1000)
                converted_time_str = datetime.fromtimestamp(data["data"][dat]["collectTime"]/1000)
                # print(converted_time)
                current_datetime = datetime.now()
                if (converted_time_str.hour == current_datetime.hour-1):
                    print ("Time : ",current_datetime,converted_time_str,plant_name)
                    print(data["data"][dat])
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "hour_kpi", # Measurement of Sensor
                            "tags": {                            
                                "plant_code" : data["data"][dat]["stationCode"]+"/getStationHourKpi",  # Tags of sensor
                                "plant_name" : plant_name
                            },
                            "fields": {
                                "collectTime": int(data["data"][dat]["collectTime"]),
                                "date": str(converted_time),
                                "inverter_power": float(inverter_power),
                                "inverterYield" : float(inverterYield),
                                "power_profit" : float(power_profit),
                                "theory_power" : float(theory_power),
                                "PVYield" : float(PVYield),                      
                                "chargeCap" : float(chargeCap),
                                "selfProvide" : float(selfProvide),
                                "dischargeCap" : float(dischargeCap),
                                "radiation_intensity" : float(radiation_intensity),                                
                                "ongrid_power" : float(ongrid_power)
                            }
                        }
                    ]                
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement hour_kpi Success")
                    # return
        except Exception as e:
            print("Error processing message: Get Current Hour KPI", data["failCode"],e)
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# Function GET Current Day KPI Station 
def get_current_day_kpi_station(xsrf):
    url = f"{BASE_URL}/thirdData/getKpiStationDay"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    date = datetime.now()
    unix_time = int(date.timestamp()* 1000)
    print(unix_time)
    # converted_time = datetime.fromtimestamp(unix_time/1000)
    # print(converted_time)
    # return
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    # payload = {"stationCodes" : "NE=52621567","collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        # return
        try :
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plant_name:",plant_name)
                
                inverter_power = 0.0 
                inverterYield = 0.0 
                selfUsePower = 0.0
                reduction_total_tree = 0.0
                power_profit = 0.0
                perpower_ratio = 0.0
                theory_power = 0.0
                PVYield = 0.0
                reduction_total_co2= 0.0                  
                chargeCap = 0.0
                selfProvide = 0.0
                dischargeCap = 0.0
                radiation_intensity = 0.0
                installed_capacity = 0.0
                use_power = 0.0
                reduction_total_coal = 0.0
                ongrid_power = 0.0
                buyPower = 0.0
                performance_ratio = 0.0
                
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
                if "perpower_ratio" in data["data"][dat]["dataItemMap"]:                
                    perpower_ratio = data["data"][dat]["dataItemMap"]["perpower_ratio"]
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
                if "performance_ratio" in data["data"][dat]["dataItemMap"]:
                    performance_ratio = data["data"][dat]["dataItemMap"]["performance_ratio"]
                
                converted_time = datetime.fromtimestamp(int(data["data"][dat]["collectTime"])/1000)
                converted_time_str = datetime.fromtimestamp(data["data"][dat]["collectTime"]/1000)
                # print(converted_time)
                current_datetime = datetime.now()
                if (converted_time_str.date()==current_datetime.date()):
                    print ("Time : ",current_datetime,converted_time_str,plant_name)
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "day_kpi", # Measurement of Sensor
                            "tags": {                            
                                "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                                "plant_name" : plant_name
                            },
                            "fields": {
                                "collectTime": int(data["data"][dat]["collectTime"]),
                                "date": str(converted_time),
                                "inverter_power": float(inverter_power),
                                "inverterYield" : float(inverterYield),
                                "selfUsePower" : float(selfUsePower),
                                "reduction_total_tree" : float(reduction_total_tree),
                                "power_profit" : float(power_profit),
                                "perpower_ratio" : float(perpower_ratio),
                                "theory_power" : float(theory_power),
                                "PVYield" : float(PVYield),
                                "reduction_total_co2" : float(reduction_total_co2),                        
                                "chargeCap" : float(chargeCap),
                                "selfProvide" : float(selfProvide),
                                "dischargeCap" : float(dischargeCap),
                                "radiation_intensity" : float(radiation_intensity),
                                "installed_capacity" : float(installed_capacity),
                                "use_power" : float(use_power),
                                "reduction_total_coal" : float(reduction_total_coal),
                                "ongrid_power" : float(ongrid_power),
                                "buyPower" : float(buyPower), 
                                "performance_ratio" : float(performance_ratio)
                            }
                        }
                    ]                
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement day_kpi Success")
                    # return
        except Exception as e:
            print("Error processing message: Get Current Day KPI", data["failCode"],e)
    else:
        print("❌ HTTP Error:", response.status_code, response.text)

# Function GET Current Month KPI Station 
def get_current_month_kpi_station(xsrf):
    url = f"{BASE_URL}/thirdData/getKpiStationMonth"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    date = datetime.now()
    unix_time = int(date.timestamp()* 1000)
    print(unix_time)
    # converted_time = datetime.fromtimestamp(unix_time/1000)
    # print(converted_time)
    # return
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    # payload = {"stationCodes" : "NE=52621567","collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        # return
        try :
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plant_name:",plant_name)
                
                inverter_power = 0.0 
                inverterYield = 0.0 
                selfUsePower = 0.0
                reduction_total_tree = 0.0
                power_profit = 0.0
                perpower_ratio = 0.0
                theory_power = 0.0
                PVYield = 0.0
                reduction_total_co2= 0.0                  
                chargeCap = 0.0
                selfProvide = 0.0
                dischargeCap = 0.0
                radiation_intensity = 0.0
                installed_capacity = 0.0
                use_power = 0.0
                reduction_total_coal = 0.0
                ongrid_power = 0.0
                buyPower = 0.0
                performance_ratio = 0.0
                
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
                if "perpower_ratio" in data["data"][dat]["dataItemMap"]:                
                    perpower_ratio = data["data"][dat]["dataItemMap"]["perpower_ratio"]
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
                if "performance_ratio" in data["data"][dat]["dataItemMap"]:
                    performance_ratio = data["data"][dat]["dataItemMap"]["performance_ratio"]
                
                converted_time = datetime.fromtimestamp(int(data["data"][dat]["collectTime"])/1000)
                converted_time_str = datetime.fromtimestamp(data["data"][dat]["collectTime"]/1000)
                # print(converted_time)
                current_datetime = datetime.now()
                if (converted_time_str.month == current_datetime.month):
                    print ("Time : ",current_datetime,converted_time_str,plant_name)
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "month_kpi", # Measurement of Sensor
                            "tags": {                            
                                "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                                "plant_name" : plant_name
                            },
                            "fields": {
                                "collectTime": int(data["data"][dat]["collectTime"]),
                                "date": str(converted_time),
                                "inverter_power": float(inverter_power),
                                "inverterYield" : float(inverterYield),
                                "selfUsePower" : float(selfUsePower),
                                "reduction_total_tree" : float(reduction_total_tree),
                                "power_profit" : float(power_profit),
                                "perpower_ratio" : float(perpower_ratio),
                                "theory_power" : float(theory_power),
                                "PVYield" : float(PVYield),
                                "reduction_total_co2" : float(reduction_total_co2),                        
                                "chargeCap" : float(chargeCap),
                                "selfProvide" : float(selfProvide),
                                "dischargeCap" : float(dischargeCap),
                                "radiation_intensity" : float(radiation_intensity),
                                "installed_capacity" : float(installed_capacity),
                                "use_power" : float(use_power),
                                "reduction_total_coal" : float(reduction_total_coal),
                                "ongrid_power" : float(ongrid_power),
                                "buyPower" : float(buyPower), 
                                "performance_ratio" : float(performance_ratio)
                            }
                        }
                    ]                
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement month_kpi Success")
                    # return
        except Exception as e:
            print("Error processing message: Get Current Month KPI", data["failCode"],e)
    else:
        print("❌ HTTP Error:", response.status_code, response.text) 

# Function GET Current Year KPI Station 
def get_current_year_kpi_station(xsrf):
    url = f"{BASE_URL}/thirdData/getKpiStationYear"
    customer = query_get_plant_code()
    join_customer = ",".join(customer)
    print (join_customer)
    date = datetime.now()
    unix_time = int(date.timestamp()* 1000)
    print(unix_time)
    # converted_time = datetime.fromtimestamp(unix_time/1000)
    # print(converted_time)
    # return
    headers = {"Content-Type": "application/json","xsrf-token":xsrf}
    payload = {"stationCodes" : join_customer,"collectTime":unix_time}
    # payload = {"stationCodes" : "NE=52621567","collectTime":unix_time}
    session = requests.Session() 
    response = session.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # print (data)
        # return
        try :
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
                plant_name = query_get_plant_name(str(data["data"][dat]["stationCode"]))
                print("plant_name:",plant_name)
                
                inverter_power = 0.0 
                inverterYield = 0.0 
                selfUsePower = 0.0
                reduction_total_tree = 0.0
                power_profit = 0.0
                perpower_ratio = 0.0
                theory_power = 0.0
                PVYield = 0.0
                reduction_total_co2= 0.0                  
                chargeCap = 0.0
                selfProvide = 0.0
                dischargeCap = 0.0
                radiation_intensity = 0.0
                installed_capacity = 0.0
                use_power = 0.0
                reduction_total_coal = 0.0
                ongrid_power = 0.0
                buyPower = 0.0
                performance_ratio = 0.0
                
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
                if "perpower_ratio" in data["data"][dat]["dataItemMap"]:                
                    perpower_ratio = data["data"][dat]["dataItemMap"]["perpower_ratio"]
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
                if "performance_ratio" in data["data"][dat]["dataItemMap"]:
                    performance_ratio = data["data"][dat]["dataItemMap"]["performance_ratio"]
                
                converted_time = datetime.fromtimestamp(int(data["data"][dat]["collectTime"])/1000)
                converted_time_str = datetime.fromtimestamp(data["data"][dat]["collectTime"]/1000)
                # print(converted_time)
                current_datetime = datetime.now()
                if (converted_time_str.year == current_datetime.year):
                    print ("Time : ",current_datetime,converted_time_str,plant_name)
                    # Set Json Format For Write Data to InfluxDB
                    json_body = [
                        {
                            "measurement": "year_kpi", # Measurement of Sensor
                            "tags": {                            
                                "plant_code" : data["data"][dat]["stationCode"]+"/getStationRealKpi",  # Tags of sensor
                                "plant_name" : plant_name
                            },
                            "fields": {
                                "collectTime": int(data["data"][dat]["collectTime"]),
                                "date": str(converted_time),
                                "inverter_power": float(inverter_power),
                                "inverterYield" : float(inverterYield),
                                "selfUsePower" : float(selfUsePower),
                                "reduction_total_tree" : float(reduction_total_tree),
                                "power_profit" : float(power_profit),
                                "perpower_ratio" : float(perpower_ratio),
                                "theory_power" : float(theory_power),
                                "PVYield" : float(PVYield),
                                "reduction_total_co2" : float(reduction_total_co2),                        
                                "chargeCap" : float(chargeCap),
                                "selfProvide" : float(selfProvide),
                                "dischargeCap" : float(dischargeCap),
                                "radiation_intensity" : float(radiation_intensity),
                                "installed_capacity" : float(installed_capacity),
                                "use_power" : float(use_power),
                                "reduction_total_coal" : float(reduction_total_coal),
                                "ongrid_power" : float(ongrid_power),
                                "buyPower" : float(buyPower), 
                                "performance_ratio" : float(performance_ratio)
                            }
                        }
                    ]                
                    # Insert Data to InfluxDB
                    # print(json.dumps(json_body))
                    influx_client.write_points(json_body)
                    print(f"Insert measurement year_kpi Success")
                    # return
        except Exception as e:
            print("Error processing message: Get Year KPI", data["failCode"],e)
    else:
        print("❌ HTTP Error:", response.status_code, response.text)
        
if __name__ == '__main__':
    init_influxdb(INFLUXDB_DATABASE)
    token = get_token()
    # time.sleep(5)
    # get_real_kpi_station_influx(token)
    # get_day_kpi_station(token)
    # get_month_kpi_station(token)    
    # get_year_kpi_station(token)
    get_current_hour_kpi_station(token)
    # get_current_day_kpi_station(token)
    # get_current_month_kpi_station(token)
    # get_current_year_kpi_station(token)