from google.transit import gtfs_realtime_pb2
import urllib2
import auth
import GTFS
import requests
import os
import glob
import zipfile
import MySQLdb
import time
import warnings
import json
from multiprocessing import Process
import datetime
warnings.filterwarnings('ignore', category=MySQLdb.Warning)

auth_key=auth.auth_key
ftp_url=auth.ftp_url
fb_timestamp_url=auth.fb_timestamp_url
fb_vehicle_url=auth.fb_vehicle_url
fb_trip_url=auth.fb_trip_url
fb_stop_url=auth.fb_stop_url
fb_base_url=auth.fb_base_url
feed_url=auth.feed_url
trip_feed_url=auth.trip_feed_url
vehicle_feed_url=auth.vehicle_feed_url

def firebaseCall(_url, _method, _data):
  try:
    if(_method == "post"):
      response = requests.post(_url, _data)
    elif(_method == "get"):
      response = requests.get(_url)
    elif(_method == "put"):
      response = requests.put(_url, _data)
    elif(_method == "delete"):
      response = requests.delete(_url)
    elif(_method == "patch"):
      response = requests.patch(_url, _data)
    content = response.content
    code = response.status_code
    return response
  except Exception, e:
    print("firebaseCall - Error: " + str(e))
    return False
  
def getTrips():
  trip_feed = gtfs_realtime_pb2.FeedMessage()
  trip_feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
  return trip_feed
  
def getVehicles():
  vehicle_feed = gtfs_realtime_pb2.FeedMessage()
  vehicle_feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
  return vehicle_feed

print(getTrips())
#print(getVehicles())
