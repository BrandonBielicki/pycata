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
          
def waitForUpdate(in_time = None):
  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
  if(in_time == None):
    timestamp1 = feed.header.timestamp
  else:
    timestamp1 = in_time
  timestamp2 = feed.header.timestamp
  print("syncing")
  while(timestamp1 == timestamp2):
    feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    timestamp2 = feed.header.timestamp
  print("update detected. Continuing")
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
  except:
    return False
  
def updateTimeStamp():
  try:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    firebaseCall(fb_timestamp_url,"put",str(feed.header.timestamp))
    return True
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
    print("Error in updateTrips - Error: " + str(e))
    return False

def deleteStops():
  try:
    firebaseCall(fb_stop_url,"delete","")
    return True
  except:
    return False

def getRouteStops(route, gtfs):
    stops = gtfs.executeQuery("""SELECT DISTINCT id 
    FROM stops S
    INNER JOIN stop_times T
        ON T.stop_id = S.id
    INNER JOIN trips Tr
        ON Tr.trip_id = T.trip_id
    WHERE Tr.route_id = %s""", (route,))
    stops_list = []
    for x in stops:
        stops_list.append(str(x[0]))
    return stops_list

#unused
def updateStops():
  stops_db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
  stops_conn = stops_db.cursor()
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
      if(_route_id == 261):
        _route_id = 26
      if(_route_id not in stops):
        stops[_route_id]=[]
      for stop in entity.trip_update.stop_time_update:
        stop_id = stop.stop_id
        stops[_route_id].append(str(stop_id))
    
      for key, value in stops.iteritems():
        stop_names = set(value)
        _route_id = key
        for item in stop_names:
          try:
            _lat=0
            _long=0
            stops_conn.execute("SELECT id, code, latitude, longitude FROM stops WHERE code='"+ item +"'")
            row = stops_conn.fetchone()
            if row is not None:
              _id = row[0]
              _code = row[1]
              _lat=row[2]
              _long=row[3]
              update_str ="{ \"id\": \""+str(_id)+"\", \"code\": \""+str(_code)+"\", \"latitude\": \""+str(_lat)+"\", \"longitude\": \""+str(_long)+"\"}"
              firebaseCall(fb_base_url+"/stops/"+str(_route_id)+"/"+item+".json?auth="+auth_key,"patch",update_str)
          except:
            #print("    Error in " + item + " route " + _route_id )
            pass
    return True
  except:
    print("    Error in updateStops")
    return False

#unused
def updateStopTimes(bottom, top):
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
     
  try:
    time_db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
    time_conn = time_db.cursor()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
    json_data = json.loads(firebaseCall(fb_stop_url,"get","").content)
    if(json_data is not None):
      for key, value in json_data.iteritems():
        if(int(key) > bottom and int(key) <= top):
          _route_num = key
          trips = getTripsFromRoute(feed,_route_num)
          cur_time = str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))
          if(len(trips) > 0):
            trip_id=str(trips[0])
            for key, value in value.iteritems():
                _stop_code = key
                stop_id = str(value['id'])
                sql_stmt="select arrival from trips t, stop_times s where service_id=(select service_id from trips where trip_id="+trip_id+") and t.trip_id=s.trip_id and t.route_id="+_route_num+" and s.stop_id="+stop_id+" and arrival>='"+cur_time+"' order by arrival limit 3"
                time_conn.execute(sql_stmt)
                time1="No Time Available"
                time2="No Time Available"
                time3="No Time Available"
                try:
                  time = time_conn.fetchone()[0]
                  time1 = datetime.datetime.strptime(str(time), "%H:%M:%S").strftime("%I:%M")
                  time = time_conn.fetchone()[0]
                  time2 = datetime.datetime.strptime(str(time), "%H:%M:%S").strftime("%I:%M")
                  time = time_conn.fetchone()[0]
                  time3 = datetime.datetime.strptime(str(time), "%H:%M:%S").strftime("%I:%M")
                except:
                  pass
                update_str = "{ \"1\" : \""+str(time1)+"\",\"2\" : \""+str(time2)+"\",\"3\" : \""+str(time3)+"\"}"
                firebaseCall(fb_base_url+"/stops/"+str(_route_num)+"/"+str(_stop_code)+".json?auth="+auth_key,"patch",update_str) 
    return True
  except:
    print("Error in updateStopTimes")
    return False

#db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
#conn = db.cursor()
#gtfs_sql = GTFS.GTFS(conn, db, ftp_url,"gtfs","gtfs.txt")

#gtfs_sql.fullUpdate()
timer = getCurrentTime()
feed_timestamp = waitForUpdate()
while(True):
  if(getCurrentTime() - timer >= (1000*60*60*24*7)):
    #gtfs_sql.fullUpdate()
    #deleteStops()
    timer = getCurrentTime()
    
  feed_timestamp = waitForUpdate(feed_timestamp) 
  print("Starting main loop at " + str(feed_timestamp))
  updateTimeStamp()
  updateTrips()
