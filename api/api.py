import json
import logging
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from influxdb_client import InfluxDBClient
from fastapi.openapi.utils import get_openapi
import secret

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


# Initialize FastAPI
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Initialize InfluxDB client
influxdb_client = InfluxDBClient(url=secret.INFLUX_URL,
                                 username=secret.INFLUX_USERNAME,
                                 password=secret.INFLUX_PASSWORD,
                                 org=secret.INFLUX_ORG)


# Part I - Functions for creating an endpoint to show
# the location of all buses
# http://localhost:80/buses/location


@app.get("/buses/location")
async def get_bus_location():
    """
    Returns the location of all buses.
    """
    now = datetime.now()
    start_time = now - timedelta(minutes=30)
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Query InfluxDB for bus locations
    query = (
             f'from(bucket:"{secret.INFLUX_BUCKET}")'
             f'|> range(start: {start_str}, stop: {end_str})'
             f'|> filter(fn: (r) => r["_measurement"] == "All_buses")'
    )
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

    buses_location = clean_dictionary(buses_location)

    response = JSONResponse(content=buses_location)
    response.media_type = "application/json"
    response.content = json.dumps(buses_location, indent=20)
    return response

# Part II - Functions for creating an endpoint to show
# the location of buses near with user and
# show what their next stop is.
# http://localhost:80/buses/near?lat=60.1837&lon=24.9579


def calculate_bounding_box(lat: float, lon: float):
    """
    Calculates the bounding box coordinates
    for a given latitude and longitude.
    One-degree change in longitude corresponds
    to a distance on the Earth's surface that
    is twice as large as a one-degree change in latitude.
    Therefore, to create a bounding box with proportional sides,
    a value that is twice as large is used for longitude compared to latitude.
    """
    lat_min, lat_max = round(lat - 0.01, 2), round(lat + 0.01, 2)
    lon_min, lon_max = round(lon - 0.02, 2), round(lon + 0.02, 2)
    return lat_min, lat_max, lon_min, lon_max


def extract_bus_id(result):
    """
    This function is necessary because we have to find
    records with the next stop with the same bus_id as
    in records with lat and lon in the range.
    """
    # Extract bus IDs from InfluxDB result
    buses_id = list(set(
        record.values.get("bus_id", "")
        for table in result
        for record in table.records
    ))
    return buses_id


def clean_dictionary(buses_location):
    """
    If only the longitude or latitude is in the desired range,
    then the second value in the dictionary will be None.
    If the bus is not on the route,
    then the value of the next stop will be EOL or None.
    Clearing the dictionary of these positions.
    """
    keys_to_remove = []
    for key, value in buses_location.items():
        if (value["lat"] is None or value["lon"] is None
            or ('next_stop' in value and value["next_stop"] == 0)):
            keys_to_remove.append(key)

    for key in keys_to_remove:
        buses_location.pop(key)

    return buses_location


@app.get("/buses/near")
async def get_bus_location(lat: float, lon: float):
    """
    Returns the location of buses near the given latitude and longitude.
    """
    # Calculate bounding box coordinates
    lat_min, lat_max, lon_min, lon_max = calculate_bounding_box(lat, lon)

    now = datetime.now()
    start_time = now - timedelta(minutes=3)
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Query InfluxDB to get all records with latitude or
    # longitude in the range for the last 3 minutes.
    query1 = (
        f'from(bucket:"{secret.INFLUX_BUCKET}") '
        f'|> range(start: {start_str}, stop: {end_str}) '
        f'|> filter(fn: (r) => r["_measurement"] == "All_buses" and '
        f'((r["_field"] == "lat" and r["_value"] >= {lat_min} and r["_value"] < {lat_max}) or '
        f'(r["_field"] == "lon" and r["_value"] >= {lon_min} and r["_value"] < {lon_max}))) '
    )

    result = influxdb_client.query_api().query(query1)

    # Creating a dictionary with bus_id, lon, lat.
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

    # Extract bus IDs from InfluxDB result
    buses_id = extract_bus_id(result)

    # Query InfluxDB again to get all records with
    # fields "next_stop" and "bus_id" from the buses_id list
    query2 = (
        f'from(bucket:"{secret.INFLUX_BUCKET}") '
        f'|> range(start: {start_str}, stop: {end_str}) '
        f'|> filter(fn: (r) => r["_measurement"] == "All_buses" and '
        f'r["_field"] == "next_stop" and r["bus_id"] =~ /{ "|".join(buses_id) }$/ ) '
    )
    result = influxdb_client.query_api().query(query2)

    # Adding information about the following stops to the dictionary
    for table in result:
        for record in table.records:
            bus_id = record.values.get("bus_id", "")
            if bus_id in buses_location:
                if record.values["_field"] == "next_stop":
                    buses_location[bus_id]["next_stop"] = record.values["_value"]

    # Clearing the dictionary
    buses_location = clean_dictionary(buses_location)
    keys_to_remove = []
    for key, value in buses_location.items():
        if value["lat"] is None or value["lon"] is None:
            keys_to_remove.append(key)

    for key in keys_to_remove:
        buses_location.pop(key)

    response = JSONResponse(content=buses_location)
    response.media_type = "application/json"
    response.content = json.dumps(buses_location, indent=20)
    return response

# Part III - Creating API documentation(Swagger)
# http://localhost:80/docs


def custom_openapi():
    """
    Creating API documentation (Swagger)
    """
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Bus Tracker API Doc",
        version="1.0",
        description="API documentation for 'Bus Tracker'.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
