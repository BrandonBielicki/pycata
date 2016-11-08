#import gtfs_realtime_pb2
from google.transit import gtfs_realtime_pb2
import urllib2
import auth
import requests
import os
import glob
import zipfile
import MySQLdb
import time
import warnings
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

db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
conn = db.cursor()

def getCurrentTime():
  return int(round(time.time()*1000))
          
def clearSqlGtfs(conn):
  conn.execute('delete from trips;')
  conn.execute('delete from routes;')
  conn.execute('delete from shapes;')
  conn.execute('delete from stops;')
  conn.execute('delete from stop_times;')
  db.commit()

def sync():
  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
  timestamp1 = feed.header.timestamp
  timestamp2 = feed.header.timestamp
  print("syncing")
  while(timestamp1 == timestamp2):
    feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
    timestamp2 = feed.header.timestamp

def loadFile(conn, name, table, col_str):
  conn.execute('LOAD DATA LOCAL INFILE "/root/dev/pycata/backend/gtfs/'+name+'"INTO TABLE '+table+' FIELDS TERMINATED BY "," IGNORE 1 LINES '+col_str)
  db.commit()
  
def uploadGtfs(conn):
  loadFile(conn,"shapes.txt","shapes","")
  loadFile(conn,"trips.txt","trips","(route_id,service_id,trip_id,head_sign,@col5,direction,block_id,shape_id,@col9,@col10)")
  loadFile(conn,"stops.txt","stops","(id,code,name,description,latitude,longitude)")
  loadFile(conn,"routes.txt","routes","(id,@col2,@col3,name,@col5,@col6,@col7,color,@col9)")
  loadFile(conn,"stop_times.txt","stop_times","(trip_id,arrival,departure,stop_id,stop_sequence,@col6,@col7,@col8,@col9)")
     
def extractZip(in_path, out_path):
  zip_file = zipfile.ZipFile(in_path, 'r')
  zip_file.extractall(out_path)
  zip_file.close()
  
def delFromDir(path):
  filelist = glob.glob(path)
  for f in filelist:
      os.remove(f)
  
def getGtfs(url, directory, filename):
  if not os.path.exists(directory):
    os.makedirs(directory)
  delFromDir(directory + "/*")
  response = urllib2.urlopen(url)
  zipcontent = response.read()
  with open(directory+"/"+filename, 'w') as f:
      f.write(zipcontent)
  extractZip(directory+"/"+filename, directory)

def firebaseCall(_url, _method, _data):
  if(_method == "post"):
    response = requests.post(_url, _data)
  elif(_method == "get"):
    response = requests.get(_url, _data)
  elif(_method == "put"):
    response = requests.put(_url, _data)
  elif(_method == "delete"):
    response = requests.delete(_url)
  content = response.content
  code = response.status_code
  return response
  
def updateTimeStamp():
  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
  firebaseCall(fb_timestamp_url,"put",str(feed.header.timestamp))

def deleteTrips():
  firebaseCall(fb_trip_url,"delete","")
  
def updateTrips():
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
    _vehicle_id=entity.trip_update.vehicle.id
    _lat = buses[_vehicle_id][0]
    _long = buses[_vehicle_id][1]
    update_str += "\"" + _id + "\" : { \"route\": \"" + _route_id + "\", \"bus\": \"" + _vehicle_id + "\", \"lat\": \"" + str(_lat) + "\", \"long\": \"" + str(_long) + "\", \"stops\": {"
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

def updateStops():
  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(urllib2.urlopen(trip_feed_url).read())
  stops={}
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
        stop_str += "\""+ item +"\": { \"lat\": \"y\", \"long\": \"y\"}, "
      stop_str += "},"
    
  stop_str += " }" 
  stop_str = stop_str.replace(", }"," }")
  stop_str = stop_str.replace(",  }"," }")
  firebaseCall(fb_stop_url,"put",stop_str)

clearSqlGtfs(conn)
getGtfs(ftp_url,"gtfs","gtfs.txt")
uploadGtfs(conn)
deleteTrips()
updateTrips()
updateStops()
sync()
timer = getCurrentTime()
while(True):
  if(getCurrentTime() - timer >= (1000*60*60*24*7)):
    clearSqlGtfs(conn)
    getGtfs(ftp_url,"gtfs","gtfs.txt")
    uploadGtfs(conn)
    sync()
    timer = getCurrentTime()
  if(getCurrentTime() - timer >= (1000*30)):  
    timer = getCurrentTime()
    updateTimeStamp()
    deleteTrips()
    updateTrips()
    updateStops()
  


#print(feed.header.timestamp)
#getGtfs(ftp_url, "gtfs", "gtfs.zip")
#updateShapes()
#uploadGtfs(conn)


#for entity in feed.entity:
#  print (entity.trip_update)
