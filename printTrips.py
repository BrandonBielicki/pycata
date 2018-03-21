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
  try:
    vehicle_feed = gtfs_realtime_pb2.FeedMessage()
    vehicle_feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    return vehicle_feed
    buses={}
    for entity in vehicle_feed.entity:
      buses[entity.id] = [entity.vehicle.position.latitude, entity.vehicle.position.longitude, entity.vehicle.position.bearing]
    trip_feed = gtfs_realtime_pb2.FeedMessage()
    trip_feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
    data = {}
    for entity in trip_feed.entity:
      trip_id=entity.id
      route_number=entity.trip_update.trip.route_id
      if(str(route_number) == "261"):
        route_number ="26"
      bus_id=entity.trip_update.vehicle.id
      latitude = buses[bus_id][0]
      longitude = buses[bus_id][1]
      try:
        bearing = buses[bus_id][2]
      except:
        bearing = "None"
      route = {"route":str(route_number),"bus":str(bus_id),"latitude":str(latitude),"longitude":str(longitude),"bearing":str(bearing),"stops":{}}
      data[str(trip_id)] = route
      
      for stop in entity.trip_update.stop_time_update:
        stop_seq = stop.stop_sequence
        delay = stop.arrival.delay
        arrival = time.strftime('%-I:%M', time.localtime(stop.arrival.time))
        departure = time.strftime('%-I:%M', time.localtime(stop.departure.time))
        stop_id = stop.stop_id        
        stop_item = {"delay":str(delay),"arrival":str(arrival),"departure":str(departure),"stop_id":str(stop_id)}
        data[str(trip_id)]["stops"][str(stop_seq)] = stop_item
     
    firebaseCall(fb_trip_url,"put",json.dumps(data))
    return True
  except Exception, e:
    print("updateTrips - Error: " + str(e))
    return False

print(getTrips())
