import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(
  bootstrap_servers="<bootstrap_server_URL>",
  security_protocol="SASL_SSL",
  sasl_mechanism="<SCRAM-SHA-256 or SCRAM-SHA-512>",
  sasl_plain_username="<username>",
  sasl_plain_password="<password>",
)
hostname = str.encode(socket.gethostname())

def on_success(metadata):
  print(f"Sent to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  print(f"Error sending message: {e}")

# Produce 100 messages asynchronously
for i in range(100):
  msg = f"asynchronous message #{i}"
  future = producer.send(
    "demo-topic",
    key=hostname,
    value=str.encode(msg)
  )
  future.add_callback(on_success)
  future.add_errback(on_error)
producer.flush()
producer.close()
