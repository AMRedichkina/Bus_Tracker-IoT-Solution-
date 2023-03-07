import json
import logging
import paho.mqtt.client as mqtt
from confluent_kafka import Producer
import secret

# Set up a logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def on_connect(client, userdata, flags, rc):
    """
    Connecting the client to the topic.
    """
    client.subscribe(secret.MQTT_SUBSCRIPTION_TOPIC)


def on_message(client, userdata, msg):
    """
    Create a Kafka producer instance and
    send the message to the Kafka topic.
    """
    topic_info = {'topic': msg.topic}
    topic_value = {'payload': json.loads(msg.payload.decode('utf-8'))}
    message_dict = {**topic_info, **topic_value}
    message_str = json.dumps(message_dict)
    producer = Producer({'bootstrap.servers': secret.KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(secret.KAFKA_TOPIC, value=message_str)
    producer.flush()


if __name__ == '__main__':
    # Set up the MQTT client and connect to the broker
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(secret.MQTT_BROKER_ADDRESS,
                   secret.MQTT_BROKER_PORT,
                   secret.MQTT_ATTEMPTS)

    # Start the MQTT client loop
    logging.info('Starting MQTT client loop')
    client.loop_forever()
