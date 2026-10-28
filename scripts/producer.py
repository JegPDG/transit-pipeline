import requests
import json
from google.transit import gtfs_realtime_pb2
import os
from dotenv import load_dotenv
import pandas as pd
from confluent_kafka import Producer
import time


load_dotenv()

FEED_URL = os.getenv("FEED_URL")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
TOPIC = "vehicle_positions"

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP
}
producer = Producer(conf)

# Fetch Feed

def fetch_feed():

  response = requests.get(FEED_URL, timeout=20) 

  # Raise an exception if the result is unsuccessful
  response.raise_for_status()

  feed = gtfs_realtime_pb2.FeedMessage()

  feed.ParseFromString(response.content)
  
  return feed


def opt(msg, field):
    return getattr(msg, field) if msg.HasField(field) else None


def flatten_vehicle(entity):

  vehicle = entity.vehicle

  return {
    "entity_id": entity.id,
    "vehicle_id": opt(vehicle.vehicle, "id"),
    "vehicle_label": opt(vehicle.vehicle, "label"),

    "trip_id": opt(vehicle.trip, "trip_id"),
    "trip_start_time":opt(vehicle.trip, "start_time"),
    "trip_start_date": opt(vehicle.trip, "start_date"),
    "trip_schedule_relationship": opt(vehicle.trip, "schedule_relationship"),
    "trip_route_id": opt(vehicle.trip, "route_id"),
    "trip_direction_id": opt(vehicle.trip, "direction_id"),

    "postion_latitude": opt(vehicle.position, "latitude"),
    "postion_longitude": opt(vehicle.position, "longitude"),
    "postion_bearing": opt(vehicle.position, "bearing"),

    "current_stop_sequence": opt(vehicle, "current_stop_sequence"),
    "current_status": opt(vehicle, "current_status"),
    "timestamp": opt(vehicle, "timestamp"),
    "stop_id": opt(vehicle, "stop_id"),

    "occupancy_status": opt(vehicle, "occupancy_status"),
    "occupancy_percentage": opt(vehicle, "occupancy_percentage"),
  }


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"at offset {msg.offset()}"
        )


def publish_all(feed):

  count = 0 

  for entity in feed.entity:
    if not entity.HasField("vehicle"):
        continue

    record = flatten_vehicle(entity)

    producer.produce(
        topic=TOPIC,
        key=record["entity_id"],
        value=json.dumps(record),
        callback=delivery_report
    )

    producer.poll(0)
    count += 1

  producer.flush()

  return count



def main():
   while True:
      try:
        feed = fetch_feed()
        count = publish_all(feed)
        print(count)
      except requests.RequestException as err:
         print(f"Feed fetch failed {err}")
      time.sleep(20)
      
if __name__ == "__main__":
    main()