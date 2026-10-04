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

def flatten_vehicle(entity):
  entity = list(entity)
  entity = pd.json_normalize(entity)
  print(type(entity))

flatten_vehicle(fetch_feed().entity)