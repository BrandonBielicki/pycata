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
route_number_dict = {
  "8540":"01",
  "8541":"02",
  "8542":"03",
  "8544":"05",
  "8546":"07",
  "8547":"08",
  "8548":"09",
  "8549":"10",
  "8550":"11",
  "8551":"12",
  "8552":"13",
  "8553":"14",
  "8554":"15",
  "8555":"16",
  "8556":"20",
  "8557":"22",
  "8558":"23",
  "8559":"24",
  "8560":"25",
  "8561":"26",
  "8563":"30",
  "8564":"31",
  "8565":"32",
  "8566":"33",
  "8570":"39",
  "8571":"46",
  "8572":"48"
}

def getCurrentTime():
  return int(round(time.time()*1000))
          
def waitForUpdate(in_time = None):
  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
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
  
def updateTrips():
  try:
    vehicle_feed = gtfs_realtime_pb2.FeedMessage()
    vehicle_feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    buses={}
    for entity in vehicle_feed.entity:
      buses[entity.id] = [entity.vehicle.position.latitude, entity.vehicle.position.longitude, entity.vehicle.position.bearing]
    trip_feed = gtfs_realtime_pb2.FeedMessage()
    trip_feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
    data = {}
    for entity in trip_feed.entity:
      trip_id=entity.id
      try:
        route_number=route_number_dict[entity.trip_update.trip.route_id]
      except Exception, e:
        print("route_number not in route_number_dict: " + str(e))
        route_number=entity.trip_update.trip.route_id
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

def updateBuses():
  try:
    vehicle_feed = gtfs_realtime_pb2.FeedMessage()
    vehicle_feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    buses={}
    for entity in vehicle_feed.entity:
      buses[entity.id] = [entity.vehicle.position.latitude, entity.vehicle.position.longitude, entity.vehicle.position.bearing]
    trip_feed = gtfs_realtime_pb2.FeedMessage()
    trip_feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
    data = {}
    for entity in trip_feed.entity:
      trip_id=entity.id
      try:
        route_number=route_number_dict[entity.trip_update.trip.route_id]
      except Exception, e:
        print("route_number not in route_number_dict: " + str(e))
        route_number=entity.trip_update.trip.route_id
      bus_id=entity.trip_update.vehicle.id
      latitude = buses[bus_id][0]
      longitude = buses[bus_id][1]
      try:
        bearing = buses[bus_id][2]
      except:
        bearing = "None"
      route = {"route":str(route_number),"bus":str(bus_id),"latitude":str(latitude),"longitude":str(longitude),"bearing":str(bearing),"stops":{}}
      data[str(trip_id)] = route
     
    firebaseCall(fb_bus_url,"put",json.dumps(data))
    return True
  except Exception, e:
    print("updateBuses - Error: " + str(e))
    return False

def deleteStops():
  try:
    firebaseCall(fb_stop_url,"delete","")
    return True
  except Exception, e:
    print("deleteStops - Error: " + str(e))
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

gtfs_sql = GTFS.GTFS(auth,"gtfs","gtfs.txt")
#gtfs_sql.fullUpdate()
timer = getCurrentTime()
feed_timestamp = waitForUpdate()
print("Starting main loop")
while(True):
  if(getCurrentTime() - timer >= (1000*60*60*24*7)):
    print("GTFS timer complete, starting full gtfs update")
    gtfs_sql.fullUpdate()
    #deleteStops()
    timer = getCurrentTime()
    
  feed_timestamp = waitForUpdate(feed_timestamp) 
  updateTimeStamp()
  updateBuses()
  updateTrips()
