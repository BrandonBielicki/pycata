#import gtfs_realtime_pb2
from google.transit import gtfs_realtime_pb2
import urllib2
import auth
import requests
import os
import glob
import zipfile

auth_key=auth.auth_key
ftp_url=auth.ftp_url
fb_timestamp_url="https://sizzling-fire-5776.firebaseio.com/timestamp.json?auth="+auth_key
feed_url="http://developers.cata.org/gtfsrt/GTFS-RealTime/TrapezeRealTimeFeed.pb"
vehicle_feed_url="http://developers.cata.org/gtfsrt/vehicle/vehiclepositions.pb"

def extractZip(in_path, out_path):
  zip_file = zipfile.ZipFile(in_path, 'r')
  zip_file.extractall(out_path)
  zip_file.close()
  
def delFromDir(path):
  filelist = glob.glob(path)
  for f in filelist:
      os.remove(f)
  
def getGtfs(url, path):
  delFromDir("gtfs/*")
  response = urllib2.urlopen(url)
  zipcontent = response.read()
  with open(path, 'w') as f:
      f.write(zipcontent)
  extractZip("gtfs/gtfs.zip", "gtfs")

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
getGtfs(ftp_url, "gtfs/gtfs.zip")


#for entity in feed.entity:
#  print (entity.trip_update)
