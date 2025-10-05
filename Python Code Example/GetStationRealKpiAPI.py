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
        # print (data)
        try:
            for dat in range(0,len(data["data"])):
                # print(data["data"][dat])
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

# def call_api():
    # init_influxdb(INFLUXDB_DATABASE)
    # token = get_token()
    # time.sleep(5)
    # get_real_kpi_station_influx(token)

# # Main System
# schedule.every(5).minutes.do(call_api)
# while True:
    # schedule.run_pending()
    # time.sleep(1)

if __name__ == '__main__':
    init_influxdb(INFLUXDB_DATABASE)
    token = get_token()
    time.sleep(5)
    get_real_kpi_station_influx(token)    