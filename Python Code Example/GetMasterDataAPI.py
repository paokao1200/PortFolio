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
                            "capacity": str(data["data"]["list"][dat]["capacity"]),
                            "contactMethod" : str(data["data"]["list"][dat]["contactMethod"]),
                            "contactPerson" : str(data["data"]["list"][dat]["contactPerson"]),
                            "gridConnectionDate": str(data["data"]["list"][dat]["gridConnectionDate"]),
                            "latitude" : str(data["data"]["list"][dat]["latitude"]), #Conflict Type
                            "longitude" : str(data["data"]["list"][dat]["longitude"]), #Conflict Type
                            "plantAddress" : str(data["data"]["list"][dat]["plantAddress"]),
                            "plantName" : str(data["data"]["list"][dat]["plantName"])
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
            # if data["data"][dat]["devTypeId"] == 1 or data["data"][dat]["devTypeId"] == 17 or data["data"][dat]["devTypeId"] == 38 or data["data"][dat]["devTypeId"] == 46:
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
                        "devDn": str(data["data"][dat]["devDn"]),
                        "devName" : str(data["data"][dat]["devName"]),
                        "devTypeId" : str(data["data"][dat]["devTypeId"]),
                        "esnCode": str(data["data"][dat]["esnCode"]),
                        "id" : str(data["data"][dat]["id"]), #Fix
                        "invType" : str(data["data"][dat]["invType"]),
                        "latitude" : str(data["data"][dat]["latitude"]), #Conflict Type
                        "longitude" : str(data["data"][dat]["longitude"]), #Conflict Type
                        "model" : str(data["data"][dat]["model"]),
                        "optimizerNumber" : str(data["data"][dat]["optimizerNumber"]),
                        "softwareVersion" : str(data["data"][dat]["softwareVersion"])
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
    token = get_token()
    time.sleep(3)
    station = get_station_list(token)
    time.sleep(5)
    get_device_list_influx(token)
    time.sleep(5)
 