

# MQTT2KafkaBridge

A simple MQTT to Kafka message bridge. Easy to use, configured simply. Based on Python. (Made with GPT4o)

Environment:

```
- Python 3.11.9
- paho-mqtt==2.1.0
- confluent-kafka==2.4.0
```

How to use:

1. Download source code
2. Edit "config.json", modify the broker address and port in the file; `mqtt_topics` is an array for topics on MQTT Broker, `topic_mapping` is an object array for mapping between Kafka and MQTT, key is MQTT Topic, value is Kafka topic。
3. Before running bridge.py, remember add topics on Kafka.
4. Run `bridge.py`, send some MQTT messages to specified topic, and view the data under Kafka.

![](https://cdn.jsdelivr.net/gh/bochili/cdn3/202405270328516.png)
