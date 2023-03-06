from fastapi import FastAPI
from fastapi.responses import JSONResponse
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from fastapi.openapi.utils import get_openapi

import secret
# Initialize FastAPI
app = FastAPI()

# Initialize InfluxDB client
influxdb_client = InfluxDBClient(url=secret.INFLUX_URL,
                                 username=secret.INFLUX_USERNAME,
                                 password=secret.INFLUX_PASSWORD,
                                 org=secret.INFLUX_ORG)

# Endpoint to show the location of all buses
#http://localhost:80/buses/location
@app.get("/buses/location")
async def get_bus_location():
    # Query InfluxDB for bus locations
    query = f'from(bucket:"{secret.INFLUX_BUCKET}") |> range(start: -24h) |> filter(fn: (r) => r["_measurement"] == "All_buses")'
    result = influxdb_client.query_api().query(query)
    
    # Extract bus location data from InfluxDB result
    buses_location = {}
    for table in result:
        for record in table.records:
            bus_id = record.values.get("bus_id", "")
            if bus_id not in buses_location:
                buses_location[bus_id] = {"lat": None, "lon": None}
            if record.values["_field"] == "lat":
                buses_location[bus_id]["lat"] = record.values["_value"]
            elif record.values["_field"] == "lon":
                buses_location[bus_id]["lon"] = record.values["_value"]

    response = JSONResponse(content=buses_location)
    response.media_type = "application/json"
    response.content = json.dumps(buses_location, indent=20)
    return response

#http://localhost:80/buses/near?lat=60.1837&lon=24.9579
@app.get("/buses/near")
async def get_bus_location(lat: float, lon: float):
    # Calculate bounding box coordinates
    lat_min, lat_max = round(lat - 0.01, 2), round(lat + 0.01, 2)
    lon_min, lon_max = round(lon - 0.02, 2), round(lon + 0.02, 2)
    
    # Query InfluxDB to get all records with latitude and longitude in the range
    query1 = (
        f'from(bucket:"{secret.INFLUX_BUCKET}") '
        f'|> range(start: -3m) '
        f'|> filter(fn: (r) => r["_measurement"] == "All_buses" and '
        f'((r["_field"] == "lat" and r["_value"] >= {lat_min} and r["_value"] < {lat_max}) or '
        f'(r["_field"] == "lon" and r["_value"] >= {lon_min} and r["_value"] < {lon_max}))) '
    )
    

    result = influxdb_client.query_api().query(query1)
    
    # Extract bus IDs from InfluxDB result
    buses_id = []
    for table in result:
        for record in table.records:
            print(record)
            bus_id = record.values.get("bus_id", "")
            if bus_id not in buses_id:
                buses_id.append(bus_id)

    buses_location = {}
    for table in result:
        for record in table.records:
            bus_id = record.values.get("bus_id", "")
            if bus_id not in buses_location:
                buses_location[bus_id] = {"lat": None, "lon": None, "next_stop": None}
            if record.values["_field"] == "lat":
                buses_location[bus_id]["lat"] = record.values["_value"]
            elif record.values["_field"] == "lon":
                buses_location[bus_id]["lon"] = record.values["_value"]
    
    # Query InfluxDB again to get all records with fields "next_stop" and "bus_id" from the buses_id list
    query2 = (
        f'from(bucket:"{secret.INFLUX_BUCKET}") '
        f'|> range(start: -3m) '
        f'|> filter(fn: (r) => r["_measurement"] == "All_buses" and '
        f'r["_field"] == "next_stop" and r["bus_id"] =~ /{ "|".join(buses_id) }$/ ) '
    )
    
    result = influxdb_client.query_api().query(query2)
    
    # Extract bus location data from InfluxDB result
    for table in result:
        for record in table.records:
            bus_id = record.values.get("bus_id", "")
            if bus_id in buses_location:
                if record.values["_field"] == "next_stop":
                    buses_location[bus_id]["next_stop"] = record.values["_value"]
            
    keys_to_remove = []
    for key, value in buses_location.items():
        if value["lat"] is None or value["lon"] is None or value["next_stop"] == 0:
            keys_to_remove.append(key)

    for key in keys_to_remove:
        buses_location.pop(key)


    response = JSONResponse(content=buses_location)
    response.media_type = "application/json"
    response.content = json.dumps(buses_location, indent=20)
    return response

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Bus Tracker API Doc",
        version="1.0",
        description="This is the API documentation for the project 'Bus Tracker API Doc'.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
