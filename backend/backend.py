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
import datetime
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
fb_base_url=auth.fb_base_url
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
          conn.execute("SELECT id, code, latitude, longitude FROM stops WHERE code='"+ item +"'")
          row = conn.fetchone()
          if row is not None:
            _id = row[0]
            _code = row[1]
            _lat=row[2]
            _long=row[3]
          stop_str += "\""+ item +"\": { \"id\": \""+str(_id)+"\", \"code\": \""+str(_code)+"\", \"latitude\": \""+str(_lat)+"\", \"longitude\": \""+str(_long)+"\"}, "
        stop_str += "},"
      
    stop_str += " }" 
    stop_str = stop_str.replace(", }"," }")
    stop_str = stop_str.replace(",  }"," }")
    firebaseCall(fb_stop_url,"put",stop_str)
    return True
  except:
    return False

def updateStopTimes():
  def getTripsFromRoute(feed, route):
    trips=[]
    for entity in feed.entity:
      _trip_id=entity.id
      _route_id=entity.trip_update.trip.route_id
      if(str(_route_id) == "261"):
        _route_id ="26"
      if(_route_id == route):
        trips.append(_trip_id)
    return trips
    
  #try:
  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
  json_data = json.loads(firebaseCall(fb_stop_url,"get","").content)
  if(json_data is not None):
    for key, value in json_data.iteritems():
      _route_num = key
      trips = getTripsFromRoute(feed,_route_num)
      cur_time = str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))
      if(len(trips) > 0):
        trip_id=str(trips[0])
        for key, value in value.iteritems():
            _stop_code = key
            stop_id = str(value['id'])
            sql_stmt="select arrival from trips t, stop_times s where service_id=(select service_id from trips where trip_id="+trip_id+") and t.trip_id=s.trip_id and t.route_id="+_route_num+" and s.stop_id="+stop_id+" and arrival>='"+cur_time+"' order by arrival limit 3"
            conn.execute(sql_stmt)
            time="0"
            try:
              time=conn.fetchone()[0]
            except:
              x=1
            update_str="{ "
            update_str += "\"1\" : \""+str(time)+"\"}"
            firebaseCall(fb_base_url+"/stops/"+str(_route_num)+"/"+str(_stop_code)+".json?auth="+auth_key,"patch",update_str) 
      #return True
  #except:
  #  return False

db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
conn = db.cursor()
gtfs_sql = GTFS.GTFS(conn, db, ftp_url,"gtfs","gtfs.txt")
updateStopsThread = threading.Thread(target=updateStops)

updateStopTimes()
#gtfs_sql.fullUpdate()
#updateTrips()
#deleteStops()
#updateStops()
timer = getCurrentTime()
while(False):
  if(getCurrentTime() - timer >= (1000*60*60*24*7)):
    gtfs_sql.fullUpdate()
    deleteStops()
    timer = getCurrentTime()
  if(getCurrentTime() - timer >= (1000*5)):
    print("Starting main loop")
    timer = getCurrentTime()
    updateTimeStamp()
    updateTrips()
    if(not updateStopsThread.isAlive()):
      updateStopsThread = threading.Thread(target=updateStops)
      updateStopsThread.start()
    print("Loop took " + str((getCurrentTime()-timer)/1000) + " seconds")