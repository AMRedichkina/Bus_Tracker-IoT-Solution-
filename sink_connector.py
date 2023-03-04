import json
from influxdb_client import InfluxDBClient
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

import secret

# Configure Kafka consumer
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)
consumer.subscribe([secret.TOPIC_NAME])

# Configure InfluxDB client
influxdb_client = InfluxDBClient(url=secret.INFLUX_URL,
                                 token=secret.INFLUX_TOKEN,
                                 org=secret.INFLUX_ORG)
influxdb_write_api = influxdb_client.write_api()

# Consume messages from Kafka and write to InfluxDB
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        print('None')
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print('Error while consuming message: {}'.format(msg.error()))
        continue

    # Extract telemetry data from message

    telemetry_data = msg.value().decode('utf-8')
    print(telemetry_data)

    telemetry_dict_topic = json.loads(telemetry_data)['topic']
    print(telemetry_dict_topic)

    next_stop_str = telemetry_dict_topic.split('/')[13]
    if next_stop_str.isdigit():
        next_stop = int(next_stop_str)
    elif telemetry_dict_topic.split('/')[13] == 'EoL':
        next_stop = 0
    else:
        next_stop = None
    print(next_stop)


    telemetry_dict = json.loads(telemetry_data)['payload']['VP']
    print(telemetry_dict)

    
    # Write telemetry data to InfluxDB
    bus_number = f"{telemetry_dict['oper']}№{telemetry_dict['veh']}"

    if 'lat' in telemetry_dict and telemetry_dict['lat'] is not None:
        lat = float(telemetry_dict['lat'])
    else:
        lat = None

    if 'long' in telemetry_dict and telemetry_dict['long'] is not None:
        long = float(telemetry_dict['long'])
    else:
        long = None

    influxdb_client.write_api().write(
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
    
    print('Finished')
    consumer.commit()
