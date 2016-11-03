#import gtfs_realtime_pb2
from google.transit import gtfs_realtime_pb2
import urllib2
import auth
import requests
import os
import glob
import zipfile
import MySQLdb

auth_key=auth.auth_key
ftp_url=auth.ftp_url
db_host=auth.db_host
db_user=auth.db_user
db_pass=auth.db_pass
db_db=auth.db_db
fb_timestamp_url="https://sizzling-fire-5776.firebaseio.com/timestamp.json?auth="+auth_key
fb_shapes_url="https://sizzling-fire-5776.firebaseio.com/shapes.json?auth="+auth_key
feed_url="http://developers.cata.org/gtfsrt/GTFS-RealTime/TrapezeRealTimeFeed.pb"
vehicle_feed_url="http://developers.cata.org/gtfsrt/vehicle/vehiclepositions.pb"

db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
conn = db.cursor()

def loadFile(conn, name, table, col_str):
  conn.execute('LOAD DATA LOCAL INFILE "/home/brandon/dev/pycata/backend/gtfs/'+name+'"INTO TABLE '+table+' FIELDS TERMINATED BY "," IGNORE 1 LINES '+col_str)
  db.commit()
  
def uploadGtfs(conn):
  loadFile(conn,"shapes.txt","shapes","")
  #loadFile(conn,"trips.txt","trips")
  #loadFile(conn,"stops.txt","stops")
  loadFile(conn,"routes.txt","routes","(route_id,@col2,@col3,route_long_name,@col5,@col6,@col7,route_color,@col9)")
  #loadFile(conn,"stop_times.txt","stop_times")
  
def updateShapes():
  with open("gtfs/shapes.txt") as in_file:
   for line in in_file:
    print(line)
     
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
    response = requests.delete(_url, _data)
  content = response.content
  code = response.status_code
  return response
  
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
firebaseCall(fb_timestamp_url,"put",str(feed.header.timestamp));
print(feed.header.timestamp)
#getGtfs(ftp_url, "gtfs", "gtfs.zip")
#updateShapes()
uploadGtfs(conn)

#for entity in feed.entity:
#  print (entity.trip_update)
