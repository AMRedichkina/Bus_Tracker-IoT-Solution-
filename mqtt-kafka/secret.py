# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'bus_data_mqtt'

# MQTT configuration
MQTT_BROKER_ADDRESS = 'mqtt.hsl.fi'
MQTT_BROKER_PORT = 1883
MQTT_ATTEMPTS = 60
MQTT_CLIENT_ID = 'mqtt_client'
MQTT_SUBSCRIPTION_TOPIC = '/hfp/v2/journey/ongoing/vp/bus/#'
