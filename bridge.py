import json
import logging
import paho.mqtt.client as mqtt
from confluent_kafka import Producer, KafkaError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# 配置日志
logging.basicConfig(level=logging.INFO)

# 配置文件
CONFIG_FILE = 'config.json'

# 加载配置
with open(CONFIG_FILE) as config_file:
    config = json.load(config_file)

mqtt_broker = config['mqtt_broker']
mqtt_port = config['mqtt_port']
mqtt_topics = config['mqtt_topics']
kafka_broker = config['kafka_broker']
topic_mapping = config['topic_mapping']

# 创建Kafka生产者
kafka_producer = Producer({'bootstrap.servers': kafka_broker})

# 消息队列
message_queue = []


def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        for topic in mqtt_topics:
            client.subscribe(topic)
    else:
        logging.error(f"Failed to connect to MQTT broker, return code {rc}")


def on_message(client, userdata, msg):
    logging.info(f"Received message on {msg.topic}: {msg.payload.decode()}")
    kafka_topic = topic_mapping.get(msg.topic)
    if kafka_topic:
        message_queue.append((kafka_topic, msg.payload))


def process_messages():
    while True:
        if message_queue:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(forward_to_kafka, topic, payload) for topic, payload in message_queue]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f'Error processing message: {e}')
            message_queue.clear()
        time.sleep(1)


def forward_to_kafka(kafka_topic, message):
    try:
        kafka_producer.produce(kafka_topic, message, callback=delivery_report)
        kafka_producer.poll(0)
    except BufferError:
        logging.warning('Local producer queue is full, retrying...')
        time.sleep(1)
        forward_to_kafka(kafka_topic, message)


# 创建MQTT客户端并配置
mqtt_client = mqtt.Client(client_id="mqtt_kafka_bridge")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# 连接到MQTT代理
mqtt_client.connect(mqtt_broker, mqtt_port, 60)

# 启动消息处理线程
import threading

message_processing_thread = threading.Thread(target=process_messages)
message_processing_thread.daemon = True
message_processing_thread.start()

# 开始循环处理网络流量和调度回调
mqtt_client.loop_forever()
