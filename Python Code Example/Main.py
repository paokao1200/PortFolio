import GetStationRealKpiAPI
import GetRealKpiDeviceInverterAPI
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

if __name__ == '__main__':    
    GetStationRealKpiAPI.init_influxdb(INFLUXDB_DATABASE)
    token = GetStationRealKpiAPI.get_token()
    time.sleep(5)
    GetStationRealKpiAPI.get_real_kpi_station_influx(token)
    time.sleep(5)
    GetRealKpiDeviceInverterAPI.get_real_kpi_device_inverter(token)
    time.sleep(5)
    GetRealKpiDeviceInverterAPI.get_real_kpi_device_power_meter(token)
    time.sleep(5)
    GetRealKpiDeviceInverterAPI.get_real_kpi_device_battery(token)
    time.sleep(5)
    GetRealKpiDeviceInverterAPI.get_real_kpi_device_emi(token)
    
# def call_api():
    # GetStationRealKpiAPI.init_influxdb(INFLUXDB_DATABASE)
    # token = GetStationRealKpiAPI.get_token()
    # time.sleep(5)
    # GetStationRealKpiAPI.get_real_kpi_station_influx(token)
    # time.sleep(5)
    # GetRealKpiDeviceInverterAPI.get_real_kpi_device_inverter(token)
    # time.sleep(5)
    # GetRealKpiDeviceInverterAPI.get_real_kpi_device_power_meter(token)

# # Main System
# schedule.every(5).minutes.do(call_api)
# while True:
    # schedule.run_pending()
    # time.sleep(1)