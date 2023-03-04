from confluent_kafka import Producer
import paho.mqtt.client as mqtt
import json

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe("/hfp/v2/journey/ongoing/vp/bus/#")

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))
    # Create a Kafka producer instance and send the message to the Kafka topic
    topic_info = {'topic': msg.topic}
    topic_value = {'payload': json.loads(msg.payload.decode('utf-8'))}
    message_dict = {**topic_info, **topic_value}
    message_str = json.dumps(message_dict)
    print(message_str)
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    producer.produce('bus_data_mqtt', value=message_str)
    producer.flush()

# Set up the MQTT client and connect to the broker
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("mqtt.hsl.fi", 1883, 60)

# Start the MQTT client loop
client.loop_forever()
