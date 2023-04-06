import json
import logging
from influxdb_client import InfluxDBClient
from confluent_kafka import Consumer, KafkaError
import secret
import time

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def setup_kafka_consumer():
    """
    Configure and return a Kafka consumer instance.
    """
    conf = {'bootstrap.servers': secret.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': secret.KAFKA_GROUP,
            'auto.offset.reset': secret.KAFKA_CONFIG}

    consumer = Consumer(conf)
    consumer.subscribe([secret.TOPIC_NAME])

    return consumer


def setup_influxdb_client():
    """
    Configure and return an InfluxDB client and write API instance.
    """
    influxdb_client = InfluxDBClient(url=secret.INFLUX_URL,
                                     username=secret.INFLUX_USERNAME,
                                     password=secret.INFLUX_PASSWORD,
                                     org=secret.INFLUX_ORG)
    influxdb_write_api = influxdb_client.write_api()

    return influxdb_write_api


def extract_telemetry_dict(telemetry_data):
    """
    Extract the telemetry dictionary from the binary payload
    part of message from MQTT server.
    """
    telemetry_dict = json.loads(telemetry_data)['payload']['VP']
    return telemetry_dict


def extract_lat_long(telemetry_dict):
    """
    Extract the latitude and longitude from
    the telemetry dictionary and return them.
    """
    if 'lat' in telemetry_dict and telemetry_dict['lat'] is not None:
        lat = float(telemetry_dict['lat'])
    else:
        lat = None

    if 'long' in telemetry_dict and telemetry_dict['long'] is not None:
        long = float(telemetry_dict['long'])
    else:
        long = None

    return lat, long


def extract_next_stop(telemetry_data):
    """
    Extract the next stop from the topic part
    of message from MQTT server.
    Next stop - the ID of next stop or station.
    Updated on each departure from or passing of a stop.
    EOL (end of line) after final stop and empty
    if the vehicle is leaving HSL area.
    Matches stop_id in GTFS (value of gtfsId field,
    without HSL: prefix, in Stop type in the routing API).
    """
    telemetry_dict_topic = json.loads(telemetry_data)['topic']

    next_stop_str = telemetry_dict_topic.split('/')[13]
    if next_stop_str.isdigit():
        next_stop = int(next_stop_str)
    elif telemetry_dict_topic.split('/')[13] == 'EOL':
        next_stop = 0
    else:
        next_stop = None

    return next_stop


def write_telemetry_data(influxdb_write_api, bus_number, lat, long, next_stop):
    """
    Write the telemetry data to InfluxDB.
    """
    influxdb_write_api.write(
        bucket=secret.INFLUX_BUCKET,
        record=[{
            "measurement": "All_buses",
            "tags": {
                "bus_id": bus_number,
            },
            "fields": {
                "lat": lat,
                "lon": long,
                "next_stop": next_stop,
            },
        }]
    )


def consume_messages(consumer, influxdb_write_api):
    """
    Consume messages from Kafka and write to InfluxDB.
    """
    while True:
        time.sleep(10)
        msg = consumer.poll(30.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('Reached end of partition')
            else:
                logging.error('Error while consuming message: %s', msg.error())
            continue

        telemetry_data = msg.value().decode('utf-8')

        telemetry_dict = extract_telemetry_dict(telemetry_data)
        bus_number = f"{telemetry_dict['oper']}№{telemetry_dict['veh']}"
        lat, long = extract_lat_long(telemetry_dict)
        next_stop = extract_next_stop(telemetry_data)

        write_telemetry_data(influxdb_write_api,
                             bus_number,
                             lat,
                             long,
                             next_stop)
        consumer.commit()


if __name__ == '__main__':
    consumer = setup_kafka_consumer()
    influxdb_write_api = setup_influxdb_client()
    consume_messages(consumer, influxdb_write_api)
