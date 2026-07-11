from kafka import KafkaConsumer

consumer = KafkaConsumer(
  bootstrap_servers="<bootstrap_server_URL>",
  security_protocol="SASL_SSL",
  sasl_mechanism="<SCRAM-SHA-256 or SCRAM-SHA-512>",
  sasl_plain_username="<username>",
  sasl_plain_password="<password>",
  auto_offset_reset="earliest",
  enable_auto_commit=False,
  consumer_timeout_ms=10000
)
consumer.subscribe("demo-topic")

for message in consumer:
  topic_info = f"topic: {message.topic} ({message.partition}|{message.offset})"
  message_info = f"key: {message.key}, {message.value}"
  print(f"{topic_info}, {message_info}")
