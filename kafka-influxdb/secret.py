TOPIC_NAME = 'bus_data_mqtt'

INFLUX_USERNAME = 'admin1'
INFLUX_PASSWORD = 'admin1admin1'
INFLUX_URL = 'http://influxdb:8086'
INFLUX_ORG = 'bus_group'
INFLUX_BUCKET = 'bus_tracker'

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_GROUP = 'mygroup'

# 'auto.offset.reset': 'earliest'
# tells the consumer to start
# reading messages from
# the earliest offset available
# in the Kafka topic.
KAFKA_CONFIG = 'earliest'
