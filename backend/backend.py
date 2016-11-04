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
fb_shapes_url=auth.fb_shapes_url
feed_url=auth.feed_url
vehicle_feed_url=auth.vehicle_feed_url

db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
conn = db.cursor()

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
  conn.execute('LOAD DATA LOCAL INFILE "/home/brandon/dev/pycata/backend/gtfs/'+name+'"INTO TABLE '+table+' FIELDS TERMINATED BY "," IGNORE 1 LINES '+col_str)
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
    response = requests.delete(_url, _data)
  content = response.content
  code = response.status_code
  return response
  
clearSqlGtfs(conn)
getGtfs(ftp_url,"gtfs","gtfs.txt")
uploadGtfs(conn)
sync()
start = int(round(time.time()*1000))
while(False):
  if(int(round(time.time()*1000) - start) >= (1000*60*60*24*7)):
    clearSqlGtfs(conn)
    getGtfs(ftp_url,"gtfs","gtfs.txt")
    uploadGtfs(conn)
    sync()
    start = int(round(time.time()*1000))
  if(int(round(time.time()*1000) - start) >= (1000*30)):  
    start = int(round(time.time()*1000))
    print("UPDATE")
    
  

#feed = gtfs_realtime_pb2.FeedMessage()
#feed.ParseFromString(urllib2.urlopen(vehicle_feed_url).read())
#firebaseCall(fb_timestamp_url,"put",str(feed.header.timestamp));
#print(feed.header.timestamp)
#getGtfs(ftp_url, "gtfs", "gtfs.zip")
#updateShapes()
#uploadGtfs(conn)


#for entity in feed.entity:
#  print (entity.trip_update)
