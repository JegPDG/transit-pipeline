import requests
import json
from google.transit import gtfs_realtime_pb2
import os
from dotenv import load_dotenv

load_dotenv()

FEED_URL = os.getenv("FEED_URL")

# query_params = {"limit":10, "headers": "Content-Type"}

# Fetch Feed

def fetch_feed():
  response = requests.get(FEED_URL, timeout=20) 

  response.raise_for_status()

  feed = gtfs_realtime_pb2.FeedMessage()

  feed.ParseFromString(response.content)
  
  print(feed)


fetch_feed()