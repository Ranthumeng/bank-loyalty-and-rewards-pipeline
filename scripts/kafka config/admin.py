from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

admin = KafkaAdminClient(
  bootstrap_servers="<bootstrap_server_URL",
  security_protocol="SASL_SSL",
  sasl_mechanism="<SCRAM-SHA-256 or SCRAM-SHA-512>",
  sasl_plain_username="<username>",
  sasl_plain_password="<password>",
)

try:
  topic = NewTopic(name="demo-topic", num_partitions=1, replication_factor=-1, replica_assignments=[])
  admin.create_topics(new_topics=[topic])
  print("Created topic")
except TopicAlreadyExistsError as e:
  print("Topic already exists")
finally:
  admin.close()
