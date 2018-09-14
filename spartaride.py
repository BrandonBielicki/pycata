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
fb_bus_url=auth.fb_bus_url
fb_base_url=auth.fb_base_url
feed_url=auth.feed_url
trip_feed_url=auth.trip_feed_url
vehicle_feed_url=auth.vehicle_feed_url
route_number_dict = {}

def getCurrentTime():
  return int(round(time.time()*1000))
            
def waitForUpdate(in_time = None):
  feed = gtfs_realtime_pb2.FeedMessage()
  try:
    feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
  except Exception, e:
      print("ERROR opening vehicle feed url: " + str(e))
  if(in_time == None):
    timestamp1 = feed.header.timestamp
  else:
    timestamp1 = in_time
  timestamp2 = feed.header.timestamp
  #print("waiting for next update...")
  while(timestamp1 == timestamp2):
    try:
      feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
      timestamp2 = feed.header.timestamp
    except Exception, e:
      print("ERROR retrieving feed timestamp: " + str(e))
  #print("update detected. Continuing")
  return(timestamp2)
           
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
  
def updateTimeStamp():
  try:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    firebaseCall(fb_timestamp_url,"put",str(feed.header.timestamp))
    return True
  except Exception, e:
    print("updateTimeStamp - Error: " + str(e))
    return False

def deleteTrips():
  try:
    firebaseCall(fb_trip_url,"delete","")
    return True
  except Exception, e:
    print("deleteTrips - Error: " + str(e))
    return False
  
def getRouteStops(route, gtfs):
  try:
    stops = gtfs.executeQuery("""SELECT DISTINCT id, code, name, latitude, longitude
    FROM stops S
    INNER JOIN stop_times T
        ON T.stop_id = S.id
    INNER JOIN trips Tr
        ON Tr.trip_id = T.trip_id
    WHERE Tr.route_id = %s""", (route,))
    return stops
  except Exception, e:
    print("getRouteStops - Error: " + str(e))
    return False

def getRoutes(gtfs):
  routes = gtfs.executeQuery("""SELECT id, name
  FROM routes
  """)
  return routes

def updateStops(gtfs_sql):
  route_id_list = gtfs_sql.getRouteIds()
  stops_string = "{"
  if(route_id_list):
    for route_id in route_id_list:
      route_stops = gtfs_sql.getStopsForRoute(route_id)
      stops_string += json.dumps(route_stops)[1:-1] + ","
    stops_string = stops_string[:-1]
    stops_string += "}"
    firebaseCall(fb_stop_url,"put",stops_string)
  
def updateBuses():
  try:
    vehicle_feed = gtfs_realtime_pb2.FeedMessage()
    vehicle_feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    buses={}
    for entity in vehicle_feed.entity:
      bus_id = entity.id
      bus_latitude = entity.vehicle.position.latitude
      bus_longitude = entity.vehicle.position.longitude
      bus_bearing = entity.vehicle.position.bearing
      bus_route_number = entity.vehicle.trip.route_id
      bus_route_number = route_number_dict[entity.vehicle.trip.route_id]
      if(buses.has_key(bus_route_number)):
        buses[bus_route_number][bus_id]={"bus":str(bus_id),"latitude":str(bus_latitude),"longitude":str(bus_longitude),"route":str(bus_route_number),"bearing":str(bus_bearing)}
      else:
        buses[bus_route_number] = {}
        buses[bus_route_number][bus_id]={"bus":str(bus_id),"latitude":str(bus_latitude),"longitude":str(bus_longitude),"route":str(bus_route_number),"bearing":str(bus_bearing)}
    firebaseCall(fb_bus_url,"put",json.dumps(buses))
    return True
  except Exception, e:
    print("updateTrips - Error: " + str(e))
    return False

def addRouteIdToDict(route_id, trip_id, gtfs_sql):
  gtfs_sql.reconnect()
  route_number = gtfs_sql.getRouteNumberFromTripId(trip_id)
  route_number_dict[route_id] = str("%02d" % (int(route_number)))

def updateTrips():
  try:
    trip_feed = gtfs_realtime_pb2.FeedMessage()
    trip_feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
    trips = {}
    for entity in trip_feed.entity:
      trip_id=entity.id
      try:
        route_number=route_number_dict[entity.trip_update.trip.route_id]
      except Exception, e:
        print("route_number not in route_number_dict: " + str(e) + " for trip_id: " + trip_id + "... adding it")
        addRouteIdToDict(entity.trip_update.trip.route_id, trip_id,gtfs_sql)
        route_number=route_number_dict[entity.trip_update.trip.route_id]
      bus_id=entity.trip_update.vehicle.id
      
      for stop in entity.trip_update.stop_time_update:
        stop_seq = stop.stop_sequence
        delay = stop.arrival.delay
        arrival = time.strftime('%-I:%M', time.localtime(stop.arrival.time))
        departure = time.strftime('%-I:%M', time.localtime(stop.departure.time))
        stop_id = stop.stop_id
        
        if(trips.has_key(route_number)):
          if(trips[route_number].has_key(stop_id)):
            trips[route_number][stop_id][trip_id] = {}
          else:
            trips[route_number][stop_id] = {}
            trips[route_number][stop_id][trip_id] = {}
        else:
          trips[route_number] = {}
          trips[route_number][stop_id] = {}
        
        stop_item = {"delay":str(delay),"arrival":str(arrival),"departure":str(departure),"stop_id":str(stop_id),"sequence":str(stop_seq)}
        trips[route_number][stop_id][trip_id] = stop_item

    firebaseCall(fb_trip_url,"put",json.dumps(trips))
    return True
  except Exception, e:
    print("updateTrips - Error: " + str(e))
    return False

gtfs_sql = GTFS.GTFS(auth,"gtfs","gtfs.txt")
gtfs_sql.fullUpdate()
updateStops(gtfs_sql)
timer = getCurrentTime()
feed_timestamp = waitForUpdate()
print("Starting main loop")
while(True):
  if(getCurrentTime() - timer >= (1000*60*60*24*1)):
    print("GTFS timer complete, starting full gtfs update")
    gtfs_sql.reconnect()
    gtfs_sql.fullUpdate()
    updateStops(gtfs_sql)
    timer = getCurrentTime()
    
  feed_timestamp = waitForUpdate(feed_timestamp)
  updateTimeStamp()
  updateBuses()
  updateTrips()
