import os
from confluent_kafka import Consumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
TOPIC = os.getenv("TOPIC")
GROUP_ID = os.getenv("GROUP_ID")

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

def get_consumer():

  config = {
    # The host/port of your Kafka broker(s)
    'bootstrap.servers': KAFKA_BOOTSTRAP, 
    
    # Identifies the consumer group this client belongs to
    'group.id': GROUP_ID,
    
    # What to do when there is no initial offset (earliest reads from the beginning)
    'auto.offset.reset': 'earliest',
    
    # Automatically commit offsets periodically
    'enable.auto.commit': False
  }

  consumer = Consumer(config)

  topics = ["test-topic"]

  consumer.subscribe(topics)

  print(f"Subscribed to topics: {topics}. Waiting for messages...")