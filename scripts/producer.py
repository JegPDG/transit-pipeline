import requests
import json
from google.transit import gtfs_realtime_pb2
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

FEED_URL = os.getenv("FEED_URL")

# Fetch Feed

def fetch_feed():

  response = requests.get(FEED_URL, timeout=20) 

  # Raise an exception if the result is unsuccessful
  response.raise_for_status()

  feed = gtfs_realtime_pb2.FeedMessage()

  feed.ParseFromString(response.content)
  
  return feed

# print(fetch_feed())

feed = fetch_feed()

flat_vehicle = []

def flatten_vehicle(feed):

  for entity in feed.entity:
    if entity.HasField("vehicle"):
      vehicle = entity.vehicle

      flat_vehicle.append({
        "entity_id": entity.id,
        "vehicle_id": vehicle.vehicle.id,
        "vehicle_label": vehicle.vehicle.label,

        "trip_id":vehicle.trip.trip_id,
        "trip_start_time":vehicle.trip.start_time,
        "trip_start_date":vehicle.trip.start_date,
        "trip_schedule_relationship": vehicle.trip.schedule_relationship,
        "trip_route_id":vehicle.trip.route_id,
        "trip_direction_id":vehicle.trip.direction_id,

        "postion_latitude": vehicle.position.latitude,
        "postion_longitude": vehicle.position.longitude,
        "postion_bearing": vehicle.position.bearing,

        "current_stop_sequence": vehicle.current_stop_sequence,
        "current_status": vehicle.current_status,
        "timestamp": vehicle.timestamp,
        "stop_id": vehicle.stop_id,

        "occupancy_status": vehicle.occupancy_status,
        "occupancy_percentage": vehicle.occupancy_percentage,
      })

  df = pd.DataFrame(flat_vehicle)
  print(df)

# flatten_vehicle(feed)


# def publish_all(feed):
#   for entity in feed.entity:
#     if entity.HasField("vehicle"):
#       flatten_vehicle(feed)
    

    

# publish_all(feed)

