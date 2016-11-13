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
import threading
warnings.filterwarnings('ignore', category=MySQLdb.Warning)

auth_key=auth.auth_key
ftp_url=auth.ftp_url
db_host=auth.db_host
db_user=auth.db_user
db_pass=auth.db_pass
db_db=auth.db_db
fb_timestamp_url=auth.fb_timestamp_url
fb_vehicle_url=auth.fb_vehicle_url
fb_trip_url=auth.fb_trip_url
fb_stop_url=auth.fb_stop_url
feed_url=auth.feed_url
trip_feed_url=auth.trip_feed_url
vehicle_feed_url=auth.vehicle_feed_url

def getCurrentTime():
  return int(round(time.time()*1000))
          
def sync():
  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
  timestamp1 = feed.header.timestamp
  timestamp2 = feed.header.timestamp
  print("syncing")
  while(timestamp1 == timestamp2):
    feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    timestamp2 = feed.header.timestamp
           
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
  except:
    return False
  
def updateTimeStamp():
  try:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    firebaseCall(fb_timestamp_url,"put",str(feed.header.timestamp))
  except:
    return False

def deleteTrips():
  try:
    firebaseCall(fb_trip_url,"delete","")
    return True
  except:
    return False
  
def updateTrips():
  try:
    vFeed = gtfs_realtime_pb2.FeedMessage()
    vFeed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    buses={}
    for entity in vFeed.entity:
      buses[entity.id] = [entity.vehicle.position.latitude, entity.vehicle.position.longitude]
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
    update_str="{ "
    for entity in feed.entity:
      _id=entity.id
      _route_id=entity.trip_update.trip.route_id
      if(str(_route_id) == "261"):
        _route_id ="26"
      _vehicle_id=entity.trip_update.vehicle.id
      _lat = buses[_vehicle_id][0]
      _long = buses[_vehicle_id][1]
      update_str += "\"" + _id + "\" : { \"route\": \"" + _route_id + "\", \"bus\": \"" + _vehicle_id + "\", \"latitude\": \"" + str(_lat) + "\", \"longitude\": \"" + str(_long) + "\", \"stops\": {"
      for stop in entity.trip_update.stop_time_update:
        stop_seq = stop.stop_sequence
        delay = stop.arrival.delay
        arrival = stop.arrival.time
        departure = stop.departure.time
        stop_id = stop.stop_id
        update_str += "\"" + str(stop_seq) + "\" : { \"delay\": \"" + str(delay) +"\", \"arrival\": \""+ str(arrival) + "\", \"departure\": \""+ str(departure) + "\", \"stop_id\": \"" + str(stop_id) + "\"}, "
      update_str += "} }, "
      
    update_str += " }"
    update_str = update_str.replace(", }"," }")
    update_str = update_str.replace(",  }"," }")
    firebaseCall(fb_trip_url,"put",update_str)
    return True
  except:
    return False

def deleteStops():
  try:
    firebaseCall(fb_stop_url,"delete","")
    return True
  except:
    return False

def updateStops():
  try:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
    stops={}
    
    ################## QUESTIONABLE ##################
    json_data = json.loads(firebaseCall(fb_stop_url,"get","").content)
    if(json_data is not None):
      for key, value in json_data.iteritems():
        _route_num = key
        if(_route_num not in stops):
          stops[_route_num]=[]
        for key, value in value.iteritems():
          stops[_route_num].append(str(key))
    ################## QUESTIONABLE ##################
    
    for entity in feed.entity:
      _route_id=entity.trip_update.trip.route_id
      if(_route_id not in stops):
        stops[_route_id]=[]
      for stop in entity.trip_update.stop_time_update:
        stop_id = stop.stop_id
        stops[_route_id].append(str(stop_id))
    
      stop_str = "{ "
      for key, value in stops.iteritems():
        stop_names = set(value)
        stop_str += "\"" + key + "\" : { "
        for item in stop_names:
          _lat=0
          _long=0
          conn.execute("SELECT latitude, longitude FROM stops WHERE code='"+ item +"'")
          row = conn.fetchone()
          if row is not None:
            _lat=row[0]
            _long=row[1]
          stop_str += "\""+ item +"\": { \"latitude\": \""+str(_lat)+"\", \"longitude\": \""+str(_long)+"\"}, "
        stop_str += "},"
      
    stop_str += " }" 
    stop_str = stop_str.replace(", }"," }")
    stop_str = stop_str.replace(",  }"," }")
    firebaseCall(fb_stop_url,"put",stop_str)
    return True
  except:
    return False

db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
conn = db.cursor()
gtfs_sql = GTFS.GTFS(conn, db, ftp_url,"gtfs","gtfs.txt")
updateStopsThread = threading.Thread(target=updateStops)

gtfs_sql.fullUpdate()
#deleteStops()
#deleteTrips()
updateTrips()
updateStops()
sync()
timer = getCurrentTime()
while(True):
  if(getCurrentTime() - timer >= (1000*60*60*24*7)):
    gtfs_sql.fullUpdate()
    deleteStops()
    sync()
    timer = getCurrentTime()
  if(getCurrentTime() - timer >= (1000*30)):
    if(getCurrentTime() - timer >= (1000*60*60*24)):
      synch_time = getCurrentTime()
      print("Hourly Sync")
      sync()
      print("    Sync took " + getCurrentTime()-synch_time + " seconds")
    print("Starting main loop")
    timer = getCurrentTime()
    updateTimeStamp()
    #deleteTrips()
    updateTrips()
    if(not updateStopsThread.isAlive()):
      updateStopsThread = threading.Thread(target=updateStops)
      updateStopsThread.start()
    print("Loop took " + str((getCurrentTime()-timer)/1000) + " seconds")