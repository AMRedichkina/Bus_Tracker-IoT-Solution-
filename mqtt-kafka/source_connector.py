from confluent_kafka import Producer
import paho.mqtt.client as mqtt
import json
from confluent_kafka.admin import AdminClient, NewTopic
import time


def on_connect(client, userdata, flags, rc):
    client.subscribe("/hfp/v2/journey/ongoing/vp/bus/#")

def on_message(client, userdata, msg):
    # Create a Kafka producer instance and send the message to the Kafka topic
    topic_info = {'topic': msg.topic}
    topic_value = {'payload': json.loads(msg.payload.decode('utf-8'))}
    message_dict = {**topic_info, **topic_value}
    message_str = json.dumps(message_dict)
    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    producer.produce('bus_data_mqtt', value=message_str)
    producer.flush()
    print('СООБЩЕНИЕ УШЛО В КАФКУ!!!!!!!!')
    time.sleep(10)
    

# Set up the MQTT client and connect to the broker
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("mqtt.hsl.fi", 1883, 60)

# Start the MQTT client loop
client.loop_forever()
